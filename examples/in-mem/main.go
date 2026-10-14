// Example: persist an order stream to the in-memory store, then replay it
// through the upcast registry and the OrderAggregate's Apply rule.
//
// Demonstrates a versioned event (order/v1.ItemAdded -> order/v2.ItemAdded)
// being upcast at read time while the rest of the stream rides through
// unchanged. Schema evolution is expressed as a new proto package version,
// not a new message name — v1.ItemAdded and v2.ItemAdded are distinct by
// their fully-qualified message name alone. v1 priced in whole dollars; v2
// switches to integer cents plus an explicit currency.
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

// --- Aggregate ---

type Order struct {
	CustomerID uuid.UUID
	TotalCents int
	Currency   string
	Address    string
	Shipped    bool
	Tracking   string
}

type OrderAggregate struct{}

func (OrderAggregate) StreamType() string { return "order" }
func (OrderAggregate) Initial() Order     { return Order{} }
func (OrderAggregate) Apply(s Order, e pebbles.Event) Order {
	switch ev := e.(type) {
	case *orderv1.CartMade:
		id, err := uuid.Parse(ev.GetCustomerId())
		if err != nil {
			panic(err)
		}
		s.CustomerID = id
	case *orderv2.ItemAdded:
		s.TotalCents += int(ev.GetPriceCents())
		s.Currency = ev.GetCurrencyCode()
	case *orderv1.DeliveryAddressUpdated:
		s.Address = ev.GetAddress()
	case *orderv1.OrderShipped:
		s.Shipped = true
		s.Tracking = ev.GetTracking()
	default:
		panic(fmt.Sprintf("unexpected event: %#v", ev))
	}
	return s
}

func main() {
	ctx := context.Background()

	codecs := codec.NewRegistry[OrderAggregate]()
	codec.Register[*orderv1.CartMade](codecs)
	codec.Register[*orderv1.ItemAdded](codecs)
	codec.Register[*orderv2.ItemAdded](codecs)
	codec.Register[*orderv1.DeliveryAddressUpdated](codecs)
	codec.Register[*orderv1.OrderShipped](codecs)

	codec.RegisterUpcaster(codecs, func(v1 *orderv1.ItemAdded) (*orderv2.ItemAdded, error) {
		return &orderv2.ItemAdded{
			Name:         v1.GetName(),
			PriceCents:   v1.GetPrice() * 100, // v1 priced in whole dollars
			CurrencyCode: "USD",               // historical assumption baked in here
		}, nil
	})

	st := store.New(memory.New(),
		memory.NewCryptoShredMiddleware(),
	)
	repo := stream.NewRepository(codecs, st)

	oldOrderID := uuid.New()
	if err := repo.Append(ctx, oldOrderID,
		&orderv1.CartMade{CustomerId: uuid.New().String()},
		&orderv1.ItemAdded{Name: "Keyboard", Price: 42},
		&orderv1.DeliveryAddressUpdated{Address: "1 Infinite Loop, Cupertino"},
		&orderv1.OrderShipped{Carrier: "UPS", Tracking: "1Z999AA10123456784"},
	); err != nil {
		panic(err)
	}

	newOrderID := uuid.New()
	newOrder := func(ctx context.Context, o Order) ([]pebbles.Event, error) {
		if o.CustomerID != uuid.Nil {
			return nil, fmt.Errorf("order already exists with customer id %s", o.CustomerID)
		}

		return []pebbles.Event{
			&orderv1.CartMade{CustomerId: uuid.New().String()},
			&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 4200, CurrencyCode: "USD"},
			&orderv1.DeliveryAddressUpdated{Address: "1 Infinite Loop, Cupertino"},
			&orderv1.OrderShipped{Carrier: "UPS", Tracking: "1Z999AA10123456784"},
		}, nil
	}

	state, err := repo.Execute(ctx, newOrderID, newOrder)
	if err != nil {
		panic(err)
	}

	fmt.Printf("new order state: %#v\n", state)

	oldState, err := repo.Load(ctx, oldOrderID)
	if err != nil {
		panic(err)
	}

	fmt.Printf("old order state: %#v\n", oldState)
}
