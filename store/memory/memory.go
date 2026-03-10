package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Engine is an in-memory implementation of store.Engine.
// It is safe for concurrent use and is intended for tests and local development.
//
// It also backs the in-memory CheckpointStore and KeyedSnapshotStore returned by
// NewCheckpointStore and NewKeyedSnapshotStore, so a single Engine holds the event
// log together with any projection state built from it.
type Engine struct {
	mu sync.RWMutex
	// streams maps each stream to the indices in log of its events, in append
	// order. The events themselves live only in log; this is just an index.
	streams map[streamKey][]int
	// log records every appended event in append order. Because appends are
	// serialized under mu and there are no in-flight transactions, the slice is
	// gap-free and a global position is simply the 1-based index.
	log []store.Event
	// projection state, guarded by mu.
	checkpoints map[string]int64
	keyed       map[string]*keyedState
}

// keyedState is one keyed projection's state: per-key models, the resolver index
// (id -> key), and the cursor — all updated together under the Store mutex.
type keyedState struct {
	position int64
	models   map[uuid.UUID]any
	index    map[uuid.UUID]uuid.UUID
}

type streamKey struct {
	streamType string
	streamID   uuid.UUID
}

func New() *Engine {
	return &Engine{
		streams:     make(map[streamKey][]int),
		checkpoints: make(map[string]int64),
		keyed:       make(map[string]*keyedState),
	}
}

// Append persists e. tx is ignored — the in-memory engine has no real
// transaction concept.
func (s *Engine) Append(ctx context.Context, tx pgx.Tx, e store.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := streamKey{e.StreamType, e.StreamID}
	for _, idx := range s.streams[key] {
		if s.log[idx].Sequence == e.Sequence {
			return store.ErrConflict
		}
	}
	s.streams[key] = append(s.streams[key], len(s.log))
	s.log = append(s.log, clone(e))
	return nil
}

func (s *Engine) ReadAll(ctx context.Context, from int64, limit int) ([]store.Positioned, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]store.Positioned, 0, limit)
	// Positions are 1-based indices, so events with position > from start at
	// index `from` in the log.
	for i := from; i < int64(len(s.log)) && len(out) < limit; i++ {
		out = append(out, store.Positioned{
			Position: i + 1,
			Event:    clone(s.log[i]),
		})
	}
	return out, nil
}

func (s *Engine) Read(ctx context.Context, streamType string, streamID uuid.UUID, fromSequence int64) ([]store.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	src := s.streams[streamKey{streamType, streamID}]
	out := make([]store.Event, 0, len(src))
	for _, idx := range src {
		if e := s.log[idx]; e.Sequence >= fromSequence {
			out = append(out, clone(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out, nil
}

func (s *Engine) LastSequence(ctx context.Context, tx pgx.Tx, streamType string, streamID uuid.UUID) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var max int64
	for _, idx := range s.streams[streamKey{streamType, streamID}] {
		if e := s.log[idx]; e.Sequence > max {
			max = e.Sequence
		}
	}
	return max, nil
}

func (s *Engine) ListStreams(ctx context.Context, streamType string) ([]uuid.UUID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ids []uuid.UUID
	for key := range s.streams {
		if key.streamType == streamType {
			ids = append(ids, key.streamID)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := ids[i], ids[j]
		for k := range a {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return false
	})
	return ids, nil
}

// clone defensively copies the mutable fields so callers can't mutate stored
// events, and stored events can't mutate caller-owned slices/maps.
func clone(e store.Event) store.Event {
	if e.Data != nil {
		data := make([]byte, len(e.Data))
		copy(data, e.Data)
		e.Data = data
	}
	if e.Headers != nil {
		h := make(map[string][]string, len(e.Headers))
		for k, v := range e.Headers {
			vv := make([]string, len(v))
			copy(vv, v)
			h[k] = vv
		}
		e.Headers = h
	}
	return e
}

var _ store.Engine = (*Engine)(nil)
