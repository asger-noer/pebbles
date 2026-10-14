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
	if err := s.Append(context.Background(), store.Event{
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

func TestAppendBatchIsAtomicOnSequenceConflict(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	streamID := uuid.New()
	event := func(sequence int64) store.Event {
		return store.Event{
			ID:         uuid.New(),
			StreamType: "order",
			StreamID:   streamID,
			Sequence:   sequence,
			Name:       "test.Event",
			At:         time.Now().UTC(),
		}
	}

	if err := s.AppendBatch(ctx, []store.Event{event(1), event(1)}); err != store.ErrConflict {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	last, err := s.LastSequence(ctx, "order", streamID)
	if err != nil {
		t.Fatal(err)
	}
	if last != 0 {
		t.Fatalf("want failed batch to leave stream empty, last sequence is %d", last)
	}
}

func TestAppendBatchRejectsMixedStreams(t *testing.T) {
	s := memory.New()
	events := []store.Event{
		{ID: uuid.New(), StreamType: "order", StreamID: uuid.New(), Sequence: 1},
		{ID: uuid.New(), StreamType: "order", StreamID: uuid.New(), Sequence: 1},
	}
	if err := s.AppendBatch(context.Background(), events); err != store.ErrMixedStreams {
		t.Fatalf("want ErrMixedStreams, got %v", err)
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
