package main

import (
	"context"
	"fmt"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	customerv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/customer/v1"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/postgres"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- customer stream ---

type Customer struct {
	Name  string
	Email string
}

type CustomerAggregate struct{}

func (CustomerAggregate) StreamType() string { return "customer" }
func (CustomerAggregate) Initial() Customer  { return Customer{} }
func (CustomerAggregate) Apply(c Customer, e pebbles.Event) Customer {
	switch ev := e.(type) {
	case *customerv1.CustomerNameUpdated:
		c.Name = ev.GetName()
	case *customerv1.CustomerEmailUpdated:
		c.Email = ev.GetEmail()
	}
	return c
}

// --- order stream ---

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

func main() {
	ctx := context.Background()
	dsn := "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	engine, err := postgres.New(ctx, pool) // runs migrations
	if err != nil {
		panic(err)
	}

	st := store.New(engine, postgres.NewCryptoShredMiddleware(pool))

	customerCodecs := codec.NewRegistry[CustomerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)
	codec.Register[*customerv1.CustomerEmailUpdated](customerCodecs)

	orderCodecs := codec.NewRegistry[OrderAggregate]()
	codec.Register[*orderv1.CartMade](orderCodecs)
	codec.Register[*orderv2.ItemAdded](orderCodecs)
	codec.Register[*orderv1.DeliveryAddressUpdated](orderCodecs)
	codec.Register[*orderv1.OrderPlaced](orderCodecs)
	codec.Register[*orderv1.OrderShipped](orderCodecs)

	customers := stream.NewRepository(customerCodecs, st)
	orders := stream.NewRepository(orderCodecs, st)

	customerID, orderID := uuid.New(), uuid.New()

	// Open one transaction and share it across both repositories: the
	// customer's registration and their first order either both land in the
	// log, or neither does.
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := customers.Append(ctx, tx, customerID,
			&customerv1.CustomerNameUpdated{Name: "Ada"},
			&customerv1.CustomerEmailUpdated{Email: "ada@pebbles.com"},
		); err != nil {
			panic(err)
		}

		if err := orders.Append(ctx, tx, orderID,
			&orderv1.CartMade{CustomerId: customerID.String()},
			&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 4200, CurrencyCode: "USD"},
			&orderv2.ItemAdded{Name: "Mouse", PriceCents: 2500, CurrencyCode: "USD"},
			&orderv2.ItemAdded{Name: "Monitor", PriceCents: 24900, CurrencyCode: "USD"},
			&orderv2.ItemAdded{Name: "USB-C Hub", PriceCents: 7900, CurrencyCode: "USD"},
			&orderv1.DeliveryAddressUpdated{Address: "1 Infinite Loop, Cupertino"},
			&orderv1.OrderPlaced{},
			&orderv1.OrderShipped{},
		); err != nil {
			panic(err)
		}

		return nil
	}); err != nil {
		panic(err)
	}

	customer, err := customers.Load(ctx, customerID)
	if err != nil {
		panic(err)
	}
	order, err := orders.Load(ctx, orderID)
	if err != nil {
		panic(err)
	}

	fmt.Printf("customer %s: %+v\n", customerID, customer)
	fmt.Printf("order %s:    %+v\n", orderID, order)
}
