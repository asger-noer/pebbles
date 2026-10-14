package store

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
)

// ErrConflict is returned when an append violates the (stream_id, sequence)
// uniqueness invariant — i.e. another writer has already produced an event at
// the given sequence number.
var ErrConflict = errors.New("pebbles: sequence conflict")

// ErrMixedStreams is returned when an atomic append batch contains events for
// more than one stream.
var ErrMixedStreams = errors.New("pebbles: append batch must target one stream")

// ErrNotFound is returned when a requested event or stream does not exist.
var ErrNotFound = errors.New("pebbles: not found")

// ErrSnapshotNotFound is returned by KeyedSnapshotStore.Load when a partition
// has no snapshot yet.
var ErrSnapshotNotFound = errors.New("pebbles: snapshot not found")

// Event is the serialized representation of a Pebble as seen by a Store.
// Encoding of the typed payload happens above this layer; the Store works on
// opaque bytes so it stays agnostic to the event schema.
type Event struct {
	ID         uuid.UUID
	StreamType string
	StreamID   uuid.UUID
	Sequence   int64
	// Name is the fully-qualified protobuf message name identifying the
	// event's type (e.g. "myapp.order.v1.OrderPlaced").
	Name    string
	At      time.Time
	Headers map[string][]string
	Data    []byte
}

// Positioned pairs a stored Event with its global position in the log. The
// position lives here, on the cross-stream read result, rather than on Event
// itself — it is a concern of ReadAll alone and is consumed only by a
// projection driver to track and resume its cursor.
type Positioned struct {
	// Position is the monotonic, gap-safe global position of the event. It is
	// stable once assigned and is used as the exclusive cursor for ReadAll.
	Position int64
	Event    Event
}

// Engine is the raw persistence contract implemented by each backend
// (memory, postgres, …). Use [New] to wrap an Engine with optional middleware
// into a [Store].
//
// Append persists one event. AppendBatch persists events for one stream
// atomically. Each backend owns the transaction boundary for both operations.
type Engine interface {
	Append(ctx context.Context, e Event) error
	AppendBatch(ctx context.Context, events []Event) error
	Read(ctx context.Context, streamType string, streamID uuid.UUID, fromSequence int64) ([]Event, error)
	ReadAll(ctx context.Context, from int64, limit int) ([]Positioned, error)
	LastSequence(ctx context.Context, streamType string, streamID uuid.UUID) (int64, error)
	ListStreams(ctx context.Context, streamType string) ([]uuid.UUID, error)
}

// KeyedSnapshotStore persists a keyed projection's state: one model M per
// grouping key, the resolver index (id -> key) it learned, and its global
// cursor. Commit applies all three atomically so models, index, and position
// never diverge across a crash.
//
// Implementations live next to the engines: see store/memory and store/postgres.
type KeyedSnapshotStore[M any] interface {
	// Position returns the projection's cursor, or 0 if it has not run.
	Position(ctx context.Context, name string) (int64, error)
	// LoadIndex returns the whole id -> key index for the projection.
	LoadIndex(ctx context.Context, name string) (map[uuid.UUID]uuid.UUID, error)
	// Load returns the model for (name, key), or ErrSnapshotNotFound if absent.
	Load(ctx context.Context, name string, key uuid.UUID) (M, error)
	// Commit atomically upserts the changed models, new index entries, and
	// advances the cursor to position. deletedKeys removes models and their
	// index entries for permanently deleted streams.
	Commit(ctx context.Context, name string, position int64,
		models map[uuid.UUID]M, index map[uuid.UUID]uuid.UUID, deletedKeys []uuid.UUID) error
}

// CheckpointStore persists the last global position a Handler-based projection
// has processed, keyed by the projection's name.
//
// Implementations live next to the engines: see store/memory and store/postgres.
type CheckpointStore interface {
	// Load returns the last saved position for name, or 0 if none exists.
	Load(ctx context.Context, name string) (int64, error)
	// Save records position as the latest processed for name.
	Save(ctx context.Context, name string, position int64) error
}

// AppendFunc is a step in the append middleware chain. Calling it persists the
// events or passes them to the next middleware.
type AppendFunc func(ctx context.Context, events []Event) error

// Middleware intercepts store operations. Append wraps an atomic single-stream
// batch write. OnRead transforms events as they leave the store. An error from
// either hook aborts the operation.
type Middleware interface {
	Append(ctx context.Context, events []Event, next AppendFunc) error
	OnRead(ctx context.Context, e Event) (Event, error)
}

// Store is a configured event store: an [Engine] plus zero or more [Middleware]
// layers. Construct one with [New]; pass it to repositories, views, and
// projection runners.
type Store struct {
	engine Engine
	mw     []Middleware
}

// New wraps engine with the given middleware and returns a Store ready for use.
// Middleware is applied in registration order on writes and in reverse order on
// reads, matching the pipeline semantics of HTTP middleware stacks.
func New(engine Engine, mw ...Middleware) *Store {
	return &Store{engine: engine, mw: mw}
}

// Append persists one event through the middleware chain.
func (s *Store) Append(ctx context.Context, e Event) error {
	return s.AppendBatch(ctx, e)
}

// AppendBatch persists all events for one stream as one atomic write through
// the middleware chain.
func (s *Store) AppendBatch(ctx context.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	first := events[0]
	for _, e := range events[1:] {
		if e.StreamType != first.StreamType || e.StreamID != first.StreamID {
			return ErrMixedStreams
		}
	}
	return s.buildChain(0)(ctx, events)
}

func (s *Store) buildChain(i int) AppendFunc {
	if i >= len(s.mw) {
		return s.engine.AppendBatch
	}
	return func(ctx context.Context, events []Event) error {
		return s.mw[i].Append(ctx, events, s.buildChain(i+1))
	}
}

func (s *Store) Read(ctx context.Context, streamType string, streamID uuid.UUID, fromSequence int64) ([]Event, error) {
	events, err := s.engine.Read(ctx, streamType, streamID, fromSequence)
	if err != nil {
		return nil, err
	}
	return s.applyOnRead(ctx, events)
}

func (s *Store) ReadAll(ctx context.Context, from int64, limit int) ([]Positioned, error) {
	batch, err := s.engine.ReadAll(ctx, from, limit)
	if err != nil {
		return nil, err
	}
	for i, p := range batch {
		events, err := s.applyOnRead(ctx, []Event{p.Event})
		if err != nil {
			return nil, err
		}
		batch[i].Event = events[0]
	}
	return batch, nil
}

// LastSequence returns the highest sequence number appended to the stream so
// far.
func (s *Store) LastSequence(ctx context.Context, streamType string, streamID uuid.UUID) (int64, error) {
	return s.engine.LastSequence(ctx, streamType, streamID)
}

func (s *Store) ListStreams(ctx context.Context, streamType string) ([]uuid.UUID, error) {
	return s.engine.ListStreams(ctx, streamType)
}

func (s *Store) applyOnRead(ctx context.Context, events []Event) ([]Event, error) {
	for i := range events {
		var err error
		for _, v := range slices.Backward(s.mw) {
			events[i], err = v.OnRead(ctx, events[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return events, nil
}
