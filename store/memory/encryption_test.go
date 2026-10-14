package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/memory"
	"github.com/google/uuid"
)

func newEncryptedStore(t *testing.T) (st *store.Store, inner *memory.Engine) {
	t.Helper()
	inner = memory.New()
	st = store.New(inner, memory.NewCryptoShredMiddleware())
	return
}

func appendEncEvent(t *testing.T, s *store.Store, streamType string, streamID uuid.UUID, seq int64, data []byte) {
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
		Name:       "test.Event",
		At:         time.Now().UTC(),
		Headers:    map[string][]string{},
		Data:       data,
	}); err != nil {
		t.Fatal(err)
	}
}

func appendTombstone(t *testing.T, s *store.Store, streamType string, streamID uuid.UUID, seq int64) {
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

func TestCryptoShredMiddlewareRoundTrip(t *testing.T) {
	ctx := context.Background()
	st, _ := newEncryptedStore(t)
	streamID := uuid.New()

	want := []byte(`{"amount":42}`)
	appendEncEvent(t, st, "order", streamID, 1, want)

	events, err := st.Read(ctx, "order", streamID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if string(events[0].Data) != string(want) {
		t.Fatalf("want %s, got %s", want, events[0].Data)
	}
}

func TestCryptoShredMiddlewareStoredDataIsOpaque(t *testing.T) {
	ctx := context.Background()
	st, inner := newEncryptedStore(t)
	streamID := uuid.New()

	plain := []byte(`{"secret":"value"}`)
	appendEncEvent(t, st, "order", streamID, 1, plain)

	// Read through the inner engine (bypassing decryption) — must NOT be plaintext.
	raw, err := inner.Read(ctx, "order", streamID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[0].Data) == string(plain) {
		t.Fatal("plaintext found in inner store — data was not encrypted")
	}
}

func TestCryptoShredMiddlewareReadReturnsTombstoneAfterDelete(t *testing.T) {
	ctx := context.Background()
	st, _ := newEncryptedStore(t)
	streamID := uuid.New()

	appendEncEvent(t, st, "order", streamID, 1, []byte(`{}`))
	// Tombstone triggers key deletion inside the middleware.
	appendTombstone(t, st, "order", streamID, 2)

	events, err := st.Read(ctx, "order", streamID, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The first event (now keyless) is surfaced as a tombstone; the real
	// tombstone at seq 2 passes through unchanged.
	for _, e := range events {
		if e.Name != pebbles.DeletedName {
			t.Fatalf("want tombstone name for all events after deletion, got %q (seq %d)", e.Name, e.Sequence)
		}
	}
}

func TestCryptoShredMiddlewareReadAllSurfacesTombstoneAfterDelete(t *testing.T) {
	ctx := context.Background()
	st, _ := newEncryptedStore(t)
	streamID := uuid.New()

	appendEncEvent(t, st, "order", streamID, 1, []byte(`{}`))
	appendTombstone(t, st, "order", streamID, 2)

	batch, err := st.ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range batch {
		if p.Event.Name != pebbles.DeletedName {
			t.Fatalf("want tombstone name, got %q at position %d", p.Event.Name, p.Position)
		}
	}
}

// verify compile-time interface satisfaction
var _ store.Middleware = (*memory.CryptoShredMiddleware)(nil)
