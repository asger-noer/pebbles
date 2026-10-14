package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	customerv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/customer/v1"
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

type transitionAggregate struct{}

func (transitionAggregate) StreamType() string { return "integration-transition" }
func (transitionAggregate) Initial() bool      { return false }
func (transitionAggregate) Apply(_ bool, _ pebbles.Event) bool {
	return true
}

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

func TestConcurrentExecuteRejectsStaleTransitionAcrossClients(t *testing.T) {
	ctx := context.Background()
	poolA := newTestPool(t)
	poolB := newTestPool(t)

	engineA, err := postgres.New(ctx, poolA)
	if err != nil {
		t.Fatalf("creating first engine: %v", err)
	}
	engineB, err := postgres.New(ctx, poolB)
	if err != nil {
		t.Fatalf("creating second engine: %v", err)
	}

	codecsA := codec.NewRegistry[transitionAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](codecsA)
	codecsB := codec.NewRegistry[transitionAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](codecsB)
	repoA := stream.NewRepository(codecsA, store.New(engineA))
	repoB := stream.NewRepository(codecsB, store.New(engineB))
	id := uuid.New()

	loaded := make(chan struct{})
	resume := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := repoA.Execute(ctx, id, func(context.Context, bool) ([]pebbles.Event, error) {
			close(loaded)
			<-resume
			return []pebbles.Event{&customerv1.CustomerNameUpdated{Name: "completed"}}, nil
		})
		result <- err
	}()

	<-loaded
	if _, err := repoB.Execute(ctx, id, func(_ context.Context, complete bool) ([]pebbles.Event, error) {
		if complete {
			t.Fatal("expected incomplete state")
		}
		return []pebbles.Event{&customerv1.CustomerNameUpdated{Name: "completed"}}, nil
	}); err != nil {
		t.Fatalf("executing first transition: %v", err)
	}
	close(resume)

	if err := <-result; !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want stale transition to return ErrConflict, got %v", err)
	}
	events, err := store.New(engineA).Read(ctx, transitionAggregate{}.StreamType(), id, 0)
	if err != nil {
		t.Fatalf("reading events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("want exactly one completion event, got %d", len(events))
	}
}

func TestAppendMultipleEvents(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	engine, err := postgres.New(ctx, pool)
	if err != nil {
		t.Fatalf("creating engine: %v", err)
	}
	st := store.New(engine)

	codecs := codec.NewRegistry[customerAggregate]()
	codec.Register[*customerv1.CustomerNameUpdated](codecs)
	codec.Register[*customerv1.CustomerEmailUpdated](codecs)
	customers := stream.NewRepository(codecs, st)
	customerID := uuid.New()

	if err := customers.Append(ctx, customerID,
		&customerv1.CustomerNameUpdated{Name: "Ada"},
		&customerv1.CustomerEmailUpdated{Email: "ada@example.com"},
	); err != nil {
		t.Fatalf("appending events: %v", err)
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

	conflictingBatch := []store.Event{
		{ID: uuid.New(), StreamType: customerAggregate{}.StreamType(), StreamID: customerID, Sequence: 3, Name: "test.Event"},
		{ID: uuid.New(), StreamType: customerAggregate{}.StreamType(), StreamID: customerID, Sequence: 3, Name: "test.Event"},
	}
	if err := st.AppendBatch(ctx, conflictingBatch...); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("want conflicting batch to return ErrConflict, got %v", err)
	}
	events, err = st.Read(ctx, customerAggregate{}.StreamType(), customerID, 0)
	if err != nil {
		t.Fatalf("reading customer stream after conflict: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want failed batch to leave 2 events, got %d", len(events))
	}
}
