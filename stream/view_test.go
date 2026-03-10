package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	paymentv1 "github.com/asger-noer/pebbles/gen/pebbles/examples/payment/v1"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/asger-noer/pebbles/stream"
	"github.com/google/uuid"
)

// --- test events ---
//
// paymentv1.PaymentCaptured satisfies pebbles.Event but is deliberately never
// registered with the registry — used to exercise Hydrate's failure path for
// an unknown message name.

type Summary struct {
	Count int `json:"count"`
	Total int `json:"total"`
}

// summaryView folds one "order" stream into Summary.
type summaryView struct{}

func (summaryView) StreamType() string { return "order" }
func (summaryView) Initial() Summary   { return Summary{} }
func (summaryView) Apply(s Summary, e pebbles.Event) Summary {
	if c, ok := e.(*orderv2.ItemAdded); ok {
		s.Count++
		s.Total += int(c.GetPriceCents())
	}
	return s
}

func newRegistry(t *testing.T) *codec.Registry[summaryView] {
	t.Helper()
	r := codec.NewRegistry[summaryView]()
	codec.Register[*orderv2.ItemAdded](r)
	return r
}

func appendToStream(t *testing.T, s *memory.Engine, codecs codec.Registrar, streamID uuid.UUID, seq int64, e pebbles.Event) {
	t.Helper()
	data, name, err := codecs.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), nil, store.Event{
		ID:         id,
		StreamType: codecs.StreamType(),
		StreamID:   streamID,
		Sequence:   seq,
		Name:       name,
		At:         time.Now().UTC(),
		Data:       data,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestViewFolds(t *testing.T) {
	ctx := context.Background()
	codecs := newRegistry(t)
	engine := memory.New()
	st := store.New(engine)
	id := uuid.New()

	appendToStream(t, engine, codecs, id, 1, &orderv2.ItemAdded{Name: "Keyboard", PriceCents: 10})
	appendToStream(t, engine, codecs, id, 2, &orderv2.ItemAdded{Name: "Mouse", PriceCents: 5})
	// A different stream must not bleed into the fold.
	appendToStream(t, engine, codecs, uuid.New(), 1, &orderv2.ItemAdded{Name: "Monitor", PriceCents: 999})

	v := stream.NewView(summaryView{}, codecs, st)
	got, err := v.Load(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got != (Summary{Count: 2, Total: 15}) {
		t.Fatalf("want {2 15}, got %+v", got)
	}
}

func TestViewEmptyReturnsInitial(t *testing.T) {
	codecs := newRegistry(t)
	st := store.New(memory.New())

	v := stream.NewView(summaryView{}, codecs, st)
	got, err := v.Load(context.Background(), uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if got != (Summary{}) {
		t.Fatalf("want zero summary, got %+v", got)
	}
}

func TestViewErrorsOnForeignEvent(t *testing.T) {
	codecs := newRegistry(t)
	engine := memory.New()
	st := store.New(engine)
	id := uuid.New()

	appendToStream(t, engine, codecs, id, 1, &paymentv1.PaymentCaptured{})

	v := stream.NewView(summaryView{}, codecs, st)
	if _, err := v.Load(context.Background(), id); err == nil {
		t.Fatal("want error for event not satisfying the sum interface, got nil")
	}
}
