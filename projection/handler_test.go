package projection_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/projection"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
)

// collector is a Handler that records the message names it receives.
type collector struct {
	subs pebbles.Subscriptions
	mu   sync.Mutex
	seen []string
}

func (c *collector) Name() string                         { return "collector" }
func (c *collector) Subscriptions() pebbles.Subscriptions { return c.subs }
func (c *collector) Handle(_ context.Context, p pebbles.Pebble[pebbles.Event]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, string(p.Event.ProtoReflect().Descriptor().FullName()))
	return nil
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

func TestRunnerHandlesAndResumesFromCheckpoint(t *testing.T) {
	ctx := context.Background()
	codecs := newRegistry(t)
	engine := memory.New()
	st := store.New(engine)
	cp := memory.NewCheckpointStore(engine)

	appendEvent(t, engine, codecs, 1, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 1})
	appendEvent(t, engine, codecs, 2, &orderv2.ItemAdded{Name: "Mouse", PriceCents: 2})

	h := &collector{subs: pebbles.Subscribe(orderStream{}, &orderv2.ItemAdded{})}
	r := projection.NewRunner(st, cp, h, []codec.Registrar{codecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, r.Run, func() bool { return h.count() == 2 })

	pos, err := cp.Load(ctx, h.Name())
	if err != nil {
		t.Fatal(err)
	}
	if pos != 2 {
		t.Fatalf("want checkpoint at position 2, got %d", pos)
	}

	// Fresh runner + handler over the same checkpoint must only see new events.
	appendEvent(t, engine, codecs, 3, &orderv2.ItemAdded{Name: "Monitor", PriceCents: 3})

	h2 := &collector{subs: pebbles.Subscribe(orderStream{}, &orderv2.ItemAdded{})}
	r2 := projection.NewRunner(st, cp, h2, []codec.Registrar{codecs}, projection.WithPollInterval(time.Millisecond))
	runUntil(t, r2.Run, func() bool { return h2.count() == 1 })

	if h2.count() != 1 {
		t.Fatalf("want fresh runner to process only 1 new event, got %d", h2.count())
	}
	pos, _ = cp.Load(ctx, h.Name())
	if pos != 3 {
		t.Fatalf("want checkpoint advanced to 3, got %d", pos)
	}
}

func TestNewRunnerPanicsOnInvalidSubscriptions(t *testing.T) {
	codecs := newRegistry(t)
	engine := memory.New()
	st := store.New(engine)
	cp := memory.NewCheckpointStore(engine)

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on empty subscriptions")
		}
	}()
	projection.NewRunner(st, cp, &collector{subs: pebbles.Subscriptions{}}, []codec.Registrar{codecs})
}

func TestRunnerReceivesDeletedTombstone(t *testing.T) {
	codecs := newRegistry(t)
	engine := memory.New()
	st := store.New(engine)
	cp := memory.NewCheckpointStore(engine)

	// Append a normal event then a tombstone for the same stream.
	appendEvent(t, engine, codecs, 1, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 1})
	appendTombstone(t, engine, codecs.StreamType())

	h := &collector{subs: pebbles.Subscribe(orderStream{}, &orderv2.ItemAdded{})}
	r := projection.NewRunner(st, cp, h, []codec.Registrar{codecs}, projection.WithPollInterval(time.Millisecond))

	// Two log positions: one ItemAdded + one Deleted tombstone.
	// The handler should receive the tombstone as pebbles.Deleted even though it
	// is not in its explicit subscription names list.
	runUntil(t, r.Run, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return slices.Contains(h.seen, pebbles.DeletedName)
	})
}
