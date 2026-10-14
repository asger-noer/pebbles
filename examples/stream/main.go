// Example: a read-only, single-stream projection.
//
// The order's events are written through a normal write-side aggregate
// (repository). A separate [pebbles.Stream] projector then folds that same
// stream into a different, read-model shape on demand — showing that a projection
// is just a read-side fold over one stream, distinct from the aggregate it reads.
package main

import (
	"context"
	"fmt"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
)

// --- write-side aggregate (used here only to append the stream) ---

type Order struct {
	Items      []string
	TotalCents int
	Shipped    bool
}

type OrderAggregate struct{}

func (OrderAggregate) StreamType() string { return "order" }
func (OrderAggregate) Initial() Order     { return Order{} }
func (OrderAggregate) Apply(o Order, e pebbles.Event) Order {
	switch ev := e.(type) {
	case *orderv2.ItemAdded:
		o.Items = append(o.Items, ev.GetName())
		o.TotalCents += int(ev.GetPriceCents())
	case *orderv1.OrderShipped:
		o.Shipped = true
	}
	return o
}

// --- read-only stream projection: a compact summary, a different shape than Order ---

type OrderSummary struct {
	ItemCount  int
	TotalCents int
	Status     string
}

type OrderSummaryProjector struct{}

func (OrderSummaryProjector) StreamType() string    { return "order" }
func (OrderSummaryProjector) Initial() OrderSummary { return OrderSummary{Status: "open"} }
func (OrderSummaryProjector) Apply(s OrderSummary, e pebbles.Event) OrderSummary {
	switch ev := e.(type) {
	case *orderv2.ItemAdded:
		s.ItemCount++
		s.TotalCents += int(ev.GetPriceCents())
	case *orderv1.OrderShipped:
		s.Status = "shipped"
	}
	return s
}

func main() {
	ctx := context.Background()

	codecs := codec.NewRegistry[OrderAggregate]()
	codec.Register[*orderv2.ItemAdded](codecs)
	codec.Register[*orderv1.OrderShipped](codecs)

	st := store.New(memory.New())

	// Write side: append the order's events through a repository.
	orders := stream.NewRepository(codecs, st)
	orderID := uuid.New()
	if err := orders.Append(ctx, orderID,
		&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 4200, CurrencyCode: "USD"},
		&orderv2.ItemAdded{Name: "Mouse", PriceCents: 1800, CurrencyCode: "USD"},
		&orderv1.OrderShipped{Tracking: "1Z999AA10123456784"},
	); err != nil {
		panic(err)
	}

	// Read side: fold the same stream into the summary read model on demand.
	summaries := stream.NewView(OrderSummaryProjector{}, codecs, st)

	summary, err := summaries.Load(ctx, orderID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("order summary: %+v\n", summary)

	// An unknown stream id folds to Initial().
	empty, err := summaries.Load(ctx, uuid.New())
	if err != nil {
		panic(err)
	}
	fmt.Printf("unknown order:  %+v\n", empty)
}
