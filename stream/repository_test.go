package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
)

type orderAggregate struct{}

func (orderAggregate) StreamType() string { return "order" }
func (orderAggregate) Initial() Summary   { return Summary{} }
func (orderAggregate) Apply(s Summary, e pebbles.Event) Summary {
	if c, ok := e.(*orderv2.ItemAdded); ok {
		s.Count++
		s.Total += int(c.GetPriceCents())
	}
	return s
}

func newOrderRegistry(t *testing.T) *codec.Registry[orderAggregate] {
	t.Helper()
	r := codec.NewRegistry[orderAggregate]()
	codec.Register[*orderv2.ItemAdded](r)
	return r
}

func newRepo(t *testing.T, s *store.Store) *stream.Repository[Summary, pebbles.Event] {
	t.Helper()
	return stream.NewRepository(newOrderRegistry(t), s)
}

func TestRepositoryAppendAndLoad(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	repo := newRepo(t, st)
	id := uuid.New()

	if err := repo.Append(ctx, nil, id,
		&orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10},
		&orderv2.ItemAdded{Name: "Mouse", PriceCents: 5},
	); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got != (Summary{Count: 2, Total: 15}) {
		t.Fatalf("want {2 15}, got %+v", got)
	}
}

func TestRepositoryDeleteReturnsErrDeleted(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	repo := newRepo(t, st)
	id := uuid.New()

	if err := repo.Append(ctx, nil, id, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, nil, id); err != nil {
		t.Fatal(err)
	}

	_, err := repo.Load(ctx, id)
	if !errors.Is(err, pebbles.ErrDeleted) {
		t.Fatalf("want ErrDeleted, got %v", err)
	}
}

func TestRepositoryExecuteReturnsErrDeletedOnDeletedStream(t *testing.T) {
	ctx := context.Background()
	st := store.New(memory.New())
	repo := newRepo(t, st)
	id := uuid.New()

	if err := repo.Append(ctx, nil, id, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10}); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, nil, id); err != nil {
		t.Fatal(err)
	}

	_, err := repo.Execute(ctx, nil, id, func(context.Context, Summary) ([]pebbles.Event, error) {
		return []pebbles.Event{&orderv2.ItemAdded{Name: "Mouse", PriceCents: 5}}, nil
	})
	if !errors.Is(err, pebbles.ErrDeleted) {
		t.Fatalf("want ErrDeleted, got %v", err)
	}
}
