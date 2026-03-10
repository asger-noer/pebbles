package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	customerv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/customer/v1"
	orderv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v1"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/postgres"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type customerAggregate struct{}

func (customerAggregate) StreamType() string                         { return "integration-customer" }
func (customerAggregate) Initial() struct{}                          { return struct{}{} }
func (customerAggregate) Apply(s struct{}, _ pebbles.Event) struct{} { return s }

type orderAggregate struct{}

func (orderAggregate) StreamType() string                         { return "integration-order" }
func (orderAggregate) Initial() struct{}                          { return struct{}{} }
func (orderAggregate) Apply(s struct{}, _ pebbles.Event) struct{} { return s }

// newTestPool connects to PEBBLES_TEST_DATABASE_URL, or skips the test if it
// is not set.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("PEBBLES_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("PEBBLES_TEST_DATABASE_URL not set; skipping postgres integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connecting to test database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestSharedTransactionCommits proves that two repositories backed by
// different streams, sharing one pgx.Tx, become visible together only after
// Commit.
func TestSharedTransactionCommits(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	customerCodecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)
	orderCodecs := codec.NewRegistry[orderAggregate]()
	codec.Register[*orderv1.CartMade](orderCodecs)

	customers := stream.NewRepository(customerCodecs, st)
	orders := stream.NewRepository(orderCodecs, st)

	customerID, orderID := uuid.New(), uuid.New()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if err := customers.Append(ctx, tx, customerID, &customerv1.CustomerNameUpdated{Name: "Ada"}); err != nil {
		t.Fatalf("appending customer: %v", err)
	}
	if err := orders.Append(ctx, tx, orderID, &orderv1.CartMade{CustomerId: customerID.String()}); err != nil {
		t.Fatalf("appending order: %v", err)
	}

	// Before commit, neither stream is visible outside the transaction.
	customerEvents, err := st.Read(ctx, customerAggregate{}.StreamType(), customerID, 0)
	if err != nil {
		t.Fatalf("reading customer stream pre-commit: %v", err)
	}
	if len(customerEvents) != 0 {
		t.Fatalf("want 0 customer events before commit, got %d", len(customerEvents))
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	customerEvents, err = st.Read(ctx, customerAggregate{}.StreamType(), customerID, 0)
	if err != nil {
		t.Fatalf("reading customer stream post-commit: %v", err)
	}
	if len(customerEvents) != 1 {
		t.Fatalf("want 1 customer event after commit, got %d", len(customerEvents))
	}

	orderEvents, err := st.Read(ctx, orderAggregate{}.StreamType(), orderID, 0)
	if err != nil {
		t.Fatalf("reading order stream post-commit: %v", err)
	}
	if len(orderEvents) != 1 {
		t.Fatalf("want 1 order event after commit, got %d", len(orderEvents))
	}
}

// TestSharedTransactionRollsBack proves that a rollback leaves neither stream
// persisted.
func TestSharedTransactionRollsBack(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	customerCodecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)
	orderCodecs := codec.NewRegistry[orderAggregate]()
	codec.Register[*orderv1.CartMade](orderCodecs)

	customers := stream.NewRepository(customerCodecs, st)
	orders := stream.NewRepository(orderCodecs, st)

	customerID, orderID := uuid.New(), uuid.New()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if err := customers.Append(ctx, tx, customerID, &customerv1.CustomerNameUpdated{Name: "Ada"}); err != nil {
		t.Fatalf("appending customer: %v", err)
	}
	if err := orders.Append(ctx, tx, orderID, &orderv1.CartMade{CustomerId: customerID.String()}); err != nil {
		t.Fatalf("appending order: %v", err)
	}

	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	customerEvents, err := st.Read(ctx, customerAggregate{}.StreamType(), customerID, 0)
	if err != nil {
		t.Fatalf("reading customer stream: %v", err)
	}
	if len(customerEvents) != 0 {
		t.Fatalf("want 0 customer events after rollback, got %d", len(customerEvents))
	}

	orderEvents, err := st.Read(ctx, orderAggregate{}.StreamType(), orderID, 0)
	if err != nil {
		t.Fatalf("reading order stream: %v", err)
	}
	if len(orderEvents) != 0 {
		t.Fatalf("want 0 order events after rollback, got %d", len(orderEvents))
	}
}

// TestSharedTransactionSameStreamMultipleAppends proves that two Append calls
// against the *same* stream, sharing one pgx.Tx, each see the other's
// uncommitted write when computing the next sequence number — regressing
// against a bug where LastSequence always read committed state, so a second
// append in the same tx recomputed the first append's sequence and the
// transaction failed on commit with store.ErrConflict.
func TestSharedTransactionSameStreamMultipleAppends(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	customerCodecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](customerCodecs)
	codec.Register[*customerv1.CustomerEmailUpdated](customerCodecs)

	customers := stream.NewRepository(customerCodecs, st)

	customerID := uuid.New()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	if err := customers.Append(ctx, tx, customerID, &customerv1.CustomerNameUpdated{Name: "Ada"}); err != nil {
		t.Fatalf("appending first event: %v", err)
	}
	if err := customers.Append(ctx, tx, customerID, &customerv1.CustomerEmailUpdated{Email: "ada@example.com"}); err != nil {
		t.Fatalf("appending second event: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	events, err := st.Read(ctx, customerAggregate{}.StreamType(), customerID, 0)
	if err != nil {
		t.Fatalf("reading customer stream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 customer events, got %d", len(events))
	}
	if events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("want sequences 1, 2, got %d, %d", events[0].Sequence, events[1].Sequence)
	}
}
