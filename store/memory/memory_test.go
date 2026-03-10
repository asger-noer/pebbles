package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/google/uuid"
)

func appendEvent(t *testing.T, s *memory.Engine, streamType string, seq int64) {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), nil, store.Event{
		ID:         id,
		StreamType: streamType,
		StreamID:   uuid.New(),
		Sequence:   seq,
		Name:       "test.Event",
		At:         time.Now().UTC(),
		Data:       []byte{},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReadAllOrderAndPaging(t *testing.T) {
	ctx := context.Background()
	s := memory.New()

	// Interleave two stream types; ReadAll must return them in append order
	// with 1-based positions regardless of stream.
	appendEvent(t, s, "order", 1)
	appendEvent(t, s, "payment", 1)
	appendEvent(t, s, "order", 2)
	appendEvent(t, s, "payment", 2)

	all, err := s.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("want 4 events, got %d", len(all))
	}
	wantTypes := []string{"order", "payment", "order", "payment"}
	for i, rec := range all {
		if rec.Position != int64(i+1) {
			t.Errorf("event %d: want position %d, got %d", i, i+1, rec.Position)
		}
		if rec.Event.StreamType != wantTypes[i] {
			t.Errorf("event %d: want stream type %q, got %q", i, wantTypes[i], rec.Event.StreamType)
		}
	}

	// Cursor is exclusive: from=2 starts at position 3.
	after, err := s.ReadAll(ctx, 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 2 || after[0].Position != 3 {
		t.Fatalf("want 2 events starting at position 3, got %d starting at %d", len(after), after[0].Position)
	}

	// Limit caps the batch.
	limited, err := s.ReadAll(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Fatalf("want 2 events, got %d", len(limited))
	}
}
