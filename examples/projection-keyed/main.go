// Example: a keyed projection — one read model per customer — built by folding
// order events grouped by customer id.
//
// Only the creation event (OrderPlaced) carries the customer id; the later
// ItemAdded events carry only their order's stream id. The managed KeyedProjector
// names the key on OrderPlaced via Key, and the framework learns
// orderStreamID -> customerID and resolves the ItemAdded events automatically.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/projection"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
)

// --- write-side order aggregate (used here only to append streams) ---

type OrderState struct {
	CustomerID uuid.UUID
	TotalCents int
}

type OrderAggregate struct{}

func (OrderAggregate) StreamType() string  { return "order" }
func (OrderAggregate) Initial() OrderState { return OrderState{} }
func (OrderAggregate) Apply(o OrderState, e pebbles.Event) OrderState {
	switch ev := e.(type) {
	case *orderv1.CartMade:
		id, err := uuid.Parse(ev.GetCustomerId())
		if err != nil {
			panic(err)
		}
		o.CustomerID = id
	case *orderv2.ItemAdded:
		o.TotalCents += int(ev.GetPriceCents())
	}
	return o
}

// --- keyed projection: per-customer order stats ---

type CustomerStats struct {
	Orders     int `json:"orders"`
	SpentCents int `json:"spent_cents"`
}

type CustomerOrders struct{}

func (CustomerOrders) Name() string { return "customer-orders" }
func (CustomerOrders) Subscriptions() pebbles.Subscriptions {
	return pebbles.Subscribe(OrderAggregate{}, &orderv1.CartMade{}, &orderv2.ItemAdded{})
}
func (CustomerOrders) Initial() CustomerStats { return CustomerStats{} }

// Key names the customer on OrderPlaced; ItemAdded returns ok=false and is
// resolved from the key the framework already learned for this order's stream.
func (CustomerOrders) Key(e pebbles.Event) (uuid.UUID, bool) {
	if p, ok := e.(*orderv1.CartMade); ok {
		id, err := uuid.Parse(p.GetCustomerId())
		if err != nil {
			panic(err)
		}
		return id, true
	}
	return uuid.Nil, false
}

func (CustomerOrders) Apply(m CustomerStats, e pebbles.Event) CustomerStats {
	switch ev := e.(type) {
	case *orderv1.CartMade:
		m.Orders++
	case *orderv2.ItemAdded:
		m.SpentCents += int(ev.GetPriceCents())
	}
	return m
}

func main() {
	ctx := context.Background()

	codecs := codec.NewRegistry[OrderAggregate]()
	codec.Register[*orderv1.CartMade](codecs)
	codec.Register[*orderv2.ItemAdded](codecs)

	engine := memory.New()
	st := store.New(engine)
	orders := stream.NewRepository(codecs, st)

	customer := uuid.New()

	// Two orders for the same customer, on their own streams. Only OrderPlaced
	// carries the customer id.
	order1, order2 := uuid.New(), uuid.New()
	if err := orders.Append(ctx, order1,
		&orderv1.CartMade{CustomerId: customer.String()},
		&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 4200, CurrencyCode: "USD"},
		&orderv2.ItemAdded{Name: "Mouse", PriceCents: 1800, CurrencyCode: "USD"},
	); err != nil {
		panic(err)
	}
	if err := orders.Append(ctx, order2,
		&orderv1.CartMade{CustomerId: customer.String()},
		&orderv2.ItemAdded{Name: "USB Cable", PriceCents: 999, CurrencyCode: "USD"},
	); err != nil {
		panic(err)
	}

	snaps := memory.NewKeyedSnapshotStore[CustomerStats](engine)
	customers := projection.NewKeyed(CustomerOrders{}, st, snaps, []codec.Registrar{codecs},
		projection.WithPollInterval(5*time.Millisecond))

	// Run until the projection has caught up, then look the customer up by id.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := customers.Run(runCtx); err != nil {
			panic(err)
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, err := customers.Get(ctx, customer)
		if err != nil {
			panic(err)
		}
		if stats.Orders == 2 {
			cancel()
			fmt.Printf("customer %s: %+v\n", customer, stats)
			return
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("projection did not catch up; last: %+v", stats))
		}
		time.Sleep(5 * time.Millisecond)
	}
}
