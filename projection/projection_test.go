package projection_test

// This file holds shared test fixtures (registry, append, and the runUntil
// helper) used across the projection package's tests.

import (
	"context"
	"testing"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	orderv2 "github.com/asger-noer/pebbles/gen/pebbles/examples/order/v2"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/google/uuid"
)

// --- helpers ---

func newRegistry(t *testing.T) *codec.Registry[orderStream] {
	t.Helper()
	r := codec.NewRegistry[orderStream]()
	codec.Register[*orderv2.ItemAdded](r)
	return r
}

// appendEvent appends one event to a random stream. The stream type is derived
// from the registry so call sites pass the right codec for the stream.
func appendEvent(t *testing.T, s *memory.Engine, codecs codec.Registrar, seq int64, e pebbles.Event) {
	t.Helper()
	data, name, err := codecs.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), store.Event{
		ID:         id,
		StreamType: codecs.StreamType(),
		StreamID:   uuid.New(),
		Sequence:   seq,
		Name:       name,
		At:         time.Now().UTC(),
		Data:       data,
	}); err != nil {
		t.Fatal(err)
	}
}

// appendToStream appends one event to a specific stream id (appendEvent
// randomizes the stream id, which keyed tests can't use). The stream type is
// derived from the registry so call sites pass the right codec for the stream.
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
	if err := s.Append(context.Background(), store.Event{
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

// appendTombstone appends a pebbles.Deleted tombstone for a new random stream
// of the given streamType directly to the store (no codec needed).
func appendTombstone(t *testing.T, s *memory.Engine, streamType string) {
	t.Helper()
	appendStreamTombstone(t, s, streamType, uuid.New(), 1)
}

// appendStreamTombstone appends a pebbles.Deleted tombstone for a specific stream.
func appendStreamTombstone(t *testing.T, s *memory.Engine, streamType string, streamID uuid.UUID, seq int64) {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), store.Event{
		ID:         id,
		StreamType: streamType,
		StreamID:   streamID,
		Sequence:   seq,
		Name:       pebbles.DeletedName,
		At:         time.Now().UTC(),
		Headers:    map[string][]string{},
	}); err != nil {
		t.Fatal(err)
	}
}

// runUntil runs in a goroutine, polling cond; once cond holds it cancels and
// waits for the run to return. It fails on timeout or an unexpected run error.
func runUntil(t *testing.T, run func(context.Context) error, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("run returned error: %v", err)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for condition")
		case <-tick.C:
			if cond() {
				cancel()
				if err := <-done; err != nil {
					t.Fatalf("run returned error: %v", err)
				}
				return
			}
		}
	}
}
