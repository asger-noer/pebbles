// Example: a keyed projection fed by TWO streams — a customer's own stream and
// its orders — folded into one per-customer read model.
//
// The two streams resolve their grouping key differently, which is why this uses
// the explicit RoutedProjector instead of the managed KeyedProjector:
//
//   - customer stream: the stream id IS the customer id (the key), taken from the
//     envelope (p.StreamID).
//   - order stream:    OrderPlaced carries the customer id (learned into the
//     index as orderID -> customerID); ItemAdded carries neither and is resolved
//     from that index.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	customerv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/customer/v1"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/projection"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
)

// --- customer stream (keyed by its own stream id) ---

type Customer struct{ Name string }

type CustomerAggregate struct{}

func (CustomerAggregate) StreamType() string { return "customer" }
func (CustomerAggregate) Initial() Customer  { return Customer{} }
func (CustomerAggregate) Apply(c Customer, e pebbles.Event) Customer {
	if ev, ok := e.(*customerv1.CustomerNameUpdated); ok {
		c.Name = ev.GetName()
	}
	return c
}

// --- order stream (keyed by the customer id it references) ---

type Order struct {
	CustomerID uuid.UUID
	TotalCents int
}

type OrderAggregate struct{}

func (OrderAggregate) StreamType() string { return "order" }
func (OrderAggregate) Initial() Order     { return Order{} }
func (OrderAggregate) Apply(o Order, e pebbles.Event) Order {
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

// --- keyed projection across both streams ---

type CustomerStats struct {
	Name       string `json:"name"`
	Orders     int    `json:"orders"`
	SpentCents int    `json:"spent_cents"`
}

type CustomerView struct{}

func (CustomerView) Name() string { return "customer-view" }
func (CustomerView) Subscriptions() pebbles.Subscriptions {
	return pebbles.Subscribe(OrderAggregate{}, &orderv1.CartMade{}, &orderv2.ItemAdded{}).
		And(CustomerAggregate{}, &customerv1.CustomerNameUpdated{})
}
func (CustomerView) Initial() CustomerStats { return CustomerStats{} }

func (CustomerView) Route(p pebbles.Pebble[pebbles.Event], idx pebbles.Index) []uuid.UUID {
	switch ev := p.Event.(type) {
	case *customerv1.CustomerNameUpdated:
		return []uuid.UUID{p.StreamID} // customer stream: the stream id IS the key
	case *orderv1.CartMade:
		custID, err := uuid.Parse(ev.GetCustomerId())
		if err != nil {
			panic(err)
		}
		idx.Put(p.StreamID, custID) // learn orderID -> customer
		return []uuid.UUID{custID}
	case *orderv2.ItemAdded:
		if cust, ok := idx.Get(p.StreamID); ok { // resolve via the index
			return []uuid.UUID{cust}
		}
	}
	return nil
}

func (CustomerView) Apply(m CustomerStats, e pebbles.Event) CustomerStats {
	switch ev := e.(type) {
	case *customerv1.CustomerNameUpdated:
		m.Name = ev.GetName()
	case *orderv1.CartMade:
		m.Orders++
	case *orderv2.ItemAdded:
		m.SpentCents += int(ev.GetPriceCents())
	}
	return m
}

func main() {
	ctx := context.Background()

	customerCodecs := codec.NewRegistry[CustomerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)

	orderCodecs := codec.NewRegistry[OrderAggregate]()
	codec.Register[*orderv1.CartMade](orderCodecs)
	codec.Register[*orderv2.ItemAdded](orderCodecs)

	engine := memory.New()
	st := store.New(engine)
	customers := stream.NewRepository(customerCodecs, st)
	orders := stream.NewRepository(orderCodecs, st)

	customerID := uuid.New()

	// The customer's own stream (stream id == customer id).
	if err := customers.Append(ctx, nil, customerID, &customerv1.CustomerNameUpdated{Name: "Ada"}); err != nil {
		panic(err)
	}

	// Two orders for that customer, on their own streams.
	order1, order2 := uuid.New(), uuid.New()
	if err := orders.Append(ctx, nil, order1,
		&orderv1.CartMade{CustomerId: customerID.String()},
		&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 4200, CurrencyCode: "USD"},
		&orderv2.ItemAdded{Name: "Mouse", PriceCents: 1800, CurrencyCode: "USD"},
	); err != nil {
		panic(err)
	}
	if err := orders.Append(ctx, nil, order2,
		&orderv1.CartMade{CustomerId: customerID.String()},
		&orderv2.ItemAdded{Name: "USB Cable", PriceCents: 999, CurrencyCode: "USD"},
	); err != nil {
		panic(err)
	}

	snaps := memory.NewKeyedSnapshotStore[CustomerStats](engine)
	view := projection.NewRouted(CustomerView{}, st, snaps, []codec.Registrar{customerCodecs, orderCodecs},
		projection.WithPollInterval(5*time.Millisecond))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := view.Run(runCtx); err != nil {
			panic(err)
		}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, err := view.Get(ctx, customerID)
		if err != nil {
			panic(err)
		}
		if stats.Name != "" && stats.Orders == 2 {
			cancel()
			fmt.Printf("customer %s: %+v\n", customerID, stats)
			return
		}
		if time.Now().After(deadline) {
			panic(fmt.Sprintf("projection did not catch up; last: %+v", stats))
		}
		time.Sleep(5 * time.Millisecond)
	}
}
