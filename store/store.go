package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrConflict is returned when an append violates the (stream_id, sequence)
// uniqueness invariant — i.e. another writer has already produced an event at
// the given sequence number.
var ErrConflict = errors.New("pebbles: sequence conflict")

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
// Append takes an explicit tx so a caller can scope multiple appends —
// including across streams and repositories — to one database transaction.
// tx is nil when the caller has no transaction to share; backends without a
// real transaction concept (e.g. the in-memory engine) ignore it.
type Engine interface {
	Append(ctx context.Context, tx pgx.Tx, e Event) error
	Read(ctx context.Context, streamType string, streamID uuid.UUID, fromSequence int64) ([]Event, error)
	ReadAll(ctx context.Context, from int64, limit int) ([]Positioned, error)
	LastSequence(ctx context.Context, tx pgx.Tx, streamType string, streamID uuid.UUID) (int64, error)
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
// event (or passes it to the next middleware in the chain).
type AppendFunc func(ctx context.Context, tx pgx.Tx, e Event) error

// Middleware intercepts store operations. Append wraps the full write path:
// transform e, call next to persist it, then run any post-write logic (e.g.
// shred the key after a tombstone). OnRead transforms events on the way out of
// the store. Returning an error from either hook aborts the operation.
//
// tx is the same caller-supplied transaction passed to [Store.Append]; a
// middleware that itself touches the database (e.g. a per-stream key table)
// should use it so its writes land in the same transaction as the event
// insert.
type Middleware interface {
	Append(ctx context.Context, tx pgx.Tx, e Event, next AppendFunc) error
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

// Append persists e. tx is an optional caller-managed transaction — pass nil
// to let the store manage its own write, or a [pgx.Tx] to scope this append
// (and any others sharing the same tx) to one atomic transaction.
func (s *Store) Append(ctx context.Context, tx pgx.Tx, e Event) error {
	return s.buildChain(0)(ctx, tx, e)
}

func (s *Store) buildChain(i int) AppendFunc {
	if i >= len(s.mw) {
		return s.engine.Append
	}
	return func(ctx context.Context, tx pgx.Tx, e Event) error {
		return s.mw[i].Append(ctx, tx, e, s.buildChain(i+1))
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
// far. tx is an optional caller-managed transaction — see [Engine.LastSequence].
func (s *Store) LastSequence(ctx context.Context, tx pgx.Tx, streamType string, streamID uuid.UUID) (int64, error) {
	return s.engine.LastSequence(ctx, tx, streamType, streamID)
}

func (s *Store) ListStreams(ctx context.Context, streamType string) ([]uuid.UUID, error) {
	return s.engine.ListStreams(ctx, streamType)
}

func (s *Store) applyOnRead(ctx context.Context, events []Event) ([]Event, error) {
	for i := range events {
		var err error
		for j := len(s.mw) - 1; j >= 0; j-- {
			events[i], err = s.mw[j].OnRead(ctx, events[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return events, nil
}
