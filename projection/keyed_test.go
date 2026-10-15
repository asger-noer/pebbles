package projection_test

import (
	"context"
	"testing"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	paymentv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/payment/v1"
	"github.com/asger-noer/pebbles/projection"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/google/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// --- keyed test events ---
//
// orderv2.ItemAdded deliberately does NOT carry the customer — it must be
// resolved via the learned index from the order's OrderPlaced.
// paymentv1.PaymentCaptured lives on a different stream and carries a payload
// FK (OrderId).

type CustomerOrders struct {
	Orders int `json:"orders"`
	Spent  int `json:"spent"`
}

// orderStream and paymentStream are minimal StreamTyped types used to scope
// the per-stream test registries.
type (
	orderStream   struct{}
	paymentStream struct{}
)

func (orderStream) StreamType() string   { return "order" }
func (paymentStream) StreamType() string { return "payment" }

func newOrderCodecs(t *testing.T) *codec.Registry[orderStream] {
	t.Helper()
	r := codec.NewRegistry[orderStream]()
	codec.Register[*orderv1.CartMade](r)
	codec.Register[*orderv2.ItemAdded](r)
	return r
}

func newPaymentCodecs(t *testing.T) *codec.Registry[paymentStream] {
	t.Helper()
	r := codec.NewRegistry[paymentStream]()
	codec.Register[*paymentv1.PaymentCaptured](r)
	return r
}

// --- managed projector: key named by OrderPlaced, inherited by ItemAdded ---

type customerOrders struct{}

func (customerOrders) Name() string { return "customer-orders" }
func (customerOrders) Subscriptions() pebbles.Subscriptions {
	return pebbles.Subscribe(orderStream{}, &orderv1.CartMade{}, &orderv2.ItemAdded{})
}
func (customerOrders) Initial() CustomerOrders { return CustomerOrders{} }
func (customerOrders) Key(e pebbles.Event) (uuid.UUID, bool) {
	if p, ok := e.(*orderv1.CartMade); ok {
		id, err := uuid.Parse(p.GetCustomerId())
		if err != nil {
			panic(err)
		}
		return id, true
	}
	return uuid.Nil, false
}

func (customerOrders) Apply(m CustomerOrders, e pebbles.Event) CustomerOrders {
	switch ev := e.(type) {
	case *orderv1.CartMade:
		m.Orders++
	case *orderv2.ItemAdded:
		m.Spent += int(ev.GetPriceCents())
	}
	return m
}

func TestKeyedManagedResolvesViaIndex(t *testing.T) {
	ctx := context.Background()
	orderCodecs := newOrderCodecs(t)
	engine := memory.New()
	st := store.New(engine)
	snaps := memory.NewKeyedSnapshotStore[CustomerOrders](engine)

	custA, custB := uuid.New(), uuid.New()
	orderA1, orderA2, orderB1 := uuid.New(), uuid.New(), uuid.New()

	appendToStream(t, engine, orderCodecs, orderA1, 1, &orderv1.CartMade{CustomerId: custA.String()})
	appendToStream(t, engine, orderCodecs, orderA1, 2, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 100})
	appendToStream(t, engine, orderCodecs, orderA2, 1, &orderv1.CartMade{CustomerId: custA.String()})
	appendToStream(t, engine, orderCodecs, orderA2, 2, &orderv2.ItemAdded{Name: "Mouse", PriceCents: 50})
	appendToStream(t, engine, orderCodecs, orderB1, 1, &orderv1.CartMade{CustomerId: custB.String()})
	appendToStream(t, engine, orderCodecs, orderB1, 2, &orderv2.ItemAdded{Name: "Monitor", PriceCents: 999})

	proj := projection.NewKeyed(customerOrders{}, st, snaps, []codec.Registrar{orderCodecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, proj.Run, func() bool {
		a, _ := proj.Get(ctx, custA)
		return a.Orders == 2 && a.Spent == 150
	})

	a, _ := proj.Get(ctx, custA)
	if a.Orders != 2 || a.Spent != 150 {
		t.Fatalf("custA: want {2 150}, got %+v — ItemAdded was not resolved via the index", a)
	}
	b, _ := proj.Get(ctx, custB)
	if b.Orders != 1 || b.Spent != 999 {
		t.Fatalf("custB: want {1 999}, got %+v", b)
	}
}

func TestKeyedTelemetryUsesTraceProvider(t *testing.T) {
	ctx := context.Background()
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { _ = tracerProvider.Shutdown(ctx) })

	orderCodecs := newOrderCodecs(t)
	engine := memory.New()
	st := store.New(engine)
	snaps := memory.NewKeyedSnapshotStore[CustomerOrders](engine)
	customerID := uuid.New()
	appendToStream(t, engine, orderCodecs, uuid.New(), 1, &orderv1.CartMade{CustomerId: customerID.String()})

	proj := projection.NewKeyed(
		customerOrders{}, st, snaps, []codec.Registrar{orderCodecs},
		projection.WithPollInterval(time.Millisecond),
		projection.WithTraceProvider(tracerProvider),
	)
	runUntil(t, proj.Run, func() bool {
		model, _ := proj.Get(ctx, customerID)
		return model.Orders == 1
	})

	for _, span := range spanRecorder.Ended() {
		if span.Name() == "projection.event" {
			return
		}
	}
	t.Fatal("missing projection.event span")
}

func TestKeyedResumeKeepsIndex(t *testing.T) {
	ctx := context.Background()
	orderCodecs := newOrderCodecs(t)
	engine := memory.New()
	st := store.New(engine)
	snaps := memory.NewKeyedSnapshotStore[CustomerOrders](engine)
	cust, order := uuid.New(), uuid.New()

	appendToStream(t, engine, orderCodecs, order, 1, &orderv1.CartMade{CustomerId: cust.String()})
	p1 := projection.NewKeyed(customerOrders{}, st, snaps, []codec.Registrar{orderCodecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, p1.Run, func() bool { c, _ := p1.Get(ctx, cust); return c.Orders == 1 })

	// New item after the first run caught up; resume with a fresh projection.
	// Its OrderPlaced (the index entry) was processed in the first run.
	appendToStream(t, engine, orderCodecs, order, 2, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 70})
	p2 := projection.NewKeyed(customerOrders{}, st, snaps, []codec.Registrar{orderCodecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, p2.Run, func() bool { c, _ := p2.Get(ctx, cust); return c.Spent == 70 })

	c, _ := p2.Get(ctx, cust)
	if c.Orders != 1 || c.Spent != 70 {
		t.Fatalf("resumed: want {1 70}, got %+v — index lost on resume or OrderPlaced reprocessed", c)
	}
}

// --- explicit routed projector: payment FK resolved via the index ---

type customerSpend struct{}

func (customerSpend) Name() string { return "customer-spend" }
func (customerSpend) Subscriptions() pebbles.Subscriptions {
	return pebbles.Subscribe(orderStream{}, &orderv1.CartMade{}).
		And(paymentStream{}, &paymentv1.PaymentCaptured{})
}
func (customerSpend) Initial() CustomerOrders { return CustomerOrders{} }
func (customerSpend) Route(p pebbles.Pebble[pebbles.Event], idx pebbles.Index) []uuid.UUID {
	switch ev := p.Event.(type) {
	case *orderv1.CartMade:
		custID, err := uuid.Parse(ev.GetCustomerId())
		if err != nil {
			panic(err)
		}
		idx.Put(p.StreamID, custID) // learn orderID -> customer
		return []uuid.UUID{custID}
	case *paymentv1.PaymentCaptured:
		orderID, err := uuid.Parse(ev.GetOrderId())
		if err != nil {
			panic(err)
		}
		if cust, ok := idx.Get(orderID); ok { // resolve the FK
			return []uuid.UUID{cust}
		}
	}
	return nil
}

func (customerSpend) Apply(m CustomerOrders, e pebbles.Event) CustomerOrders {
	switch ev := e.(type) {
	case *orderv1.CartMade:
		m.Orders++
	case *paymentv1.PaymentCaptured:
		m.Spent += int(ev.GetAmount())
	}
	return m
}

func TestKeyedRoutedResolvesForeignKey(t *testing.T) {
	ctx := context.Background()
	orderCodecs := newOrderCodecs(t)
	paymentCodecs := newPaymentCodecs(t)
	engine := memory.New()
	st := store.New(engine)
	snaps := memory.NewKeyedSnapshotStore[CustomerOrders](engine)
	cust, order, payment := uuid.New(), uuid.New(), uuid.New()

	appendToStream(t, engine, orderCodecs, order, 1, &orderv1.CartMade{CustomerId: cust.String()})
	appendToStream(t, engine, paymentCodecs, payment, 1, &paymentv1.PaymentCaptured{OrderId: order.String(), Amount: 500})

	proj := projection.NewRouted(customerSpend{}, st, snaps, []codec.Registrar{orderCodecs, paymentCodecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, proj.Run, func() bool { c, _ := proj.Get(ctx, cust); return c.Spent == 500 })

	c, _ := proj.Get(ctx, cust)
	if c.Orders != 1 || c.Spent != 500 {
		t.Fatalf("routed: want {1 500}, got %+v — payment FK not resolved to customer", c)
	}
}

func TestKeyedSkipsUnresolvedButAdvances(t *testing.T) {
	ctx := context.Background()
	orderCodecs := newOrderCodecs(t)
	engine := memory.New()
	st := store.New(engine)
	snaps := memory.NewKeyedSnapshotStore[CustomerOrders](engine)
	order := uuid.New()

	// ItemAdded with no preceding OrderPlaced: the key can't be resolved.
	appendToStream(t, engine, orderCodecs, order, 1, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10})

	proj := projection.NewKeyed(customerOrders{}, st, snaps, []codec.Registrar{orderCodecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, proj.Run, func() bool {
		pos, _ := snaps.Position(ctx, "customer-orders")
		return pos == 1
	})

	pos, _ := snaps.Position(ctx, "customer-orders")
	if pos != 1 {
		t.Fatalf("cursor should advance past an unresolved event, got %d", pos)
	}
}

func TestKeyedRemovesModelOnDeleted(t *testing.T) {
	ctx := context.Background()
	orderCodecs := newOrderCodecs(t)
	engine := memory.New()
	st := store.New(engine)
	snaps := memory.NewKeyedSnapshotStore[CustomerOrders](engine)

	cust := uuid.New()
	order := uuid.New()

	// Order placed — model is created for cust.
	appendToStream(t, engine, orderCodecs, order, 1, &orderv1.CartMade{CustomerId: cust.String()})
	appendToStream(t, engine, orderCodecs, order, 2, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 50})

	proj := projection.NewKeyed(customerOrders{}, st, snaps, []codec.Registrar{orderCodecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, proj.Run, func() bool { c, _ := proj.Get(ctx, cust); return c.Orders == 1 })

	c, _ := proj.Get(ctx, cust)
	if c.Orders != 1 || c.Spent != 50 {
		t.Fatalf("want {1 50} before delete, got %+v", c)
	}

	// Append tombstone for the specific order stream.
	appendStreamTombstone(t, engine, orderCodecs.StreamType(), order, 3)

	// After the tombstone the model for cust should be removed.
	runUntil(t, proj.Run, func() bool {
		pos, _ := snaps.Position(ctx, "customer-orders")
		return pos >= 3 // positions: placed(1) + added(2) + tombstone(3)
	})

	c, _ = proj.Get(ctx, cust)
	if c.Orders != 0 || c.Spent != 0 {
		t.Fatalf("want zero model after delete, got %+v", c)
	}
}
