// stream provides single-stream access — folding one stream, addressed
// by id, into state. Repository is the write+read aggregate store; View is a
// read-only read-model loader. Both are synchronous and on demand; for read
// models maintained across many streams, see the projection package.
package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	"github.com/asger-noer/pebbles/internal/fold"
	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Repository serves a single aggregate type: Load folds a stream into state,
// Append writes new events to it.
type Repository[S any, E pebbles.Event] struct {
	streamType string
	codecs     codec.Registrar
	store      *store.Store
	folder     *fold.Folder[S, E]
	opts       options
}

// NewRepository constructs a Repository for aggregate A. The stream type and
// aggregate logic are derived from A's zero value, so neither needs to be
// passed explicitly. Only one should be created per aggregate, and it should be
// shared across the app.
func NewRepository[S any, E pebbles.Event, A pebbles.Aggregate[S, E]](
	codecs *codec.Registry[A],
	s *store.Store,
	opts ...Option,
) *Repository[S, E] {
	var agg A
	return &Repository[S, E]{
		streamType: codecs.StreamType(),
		codecs:     codecs,
		store:      s,
		folder:     fold.New(s, codecs, agg),
		opts:       resolveOptions(opts),
	}
}

// Load reads all events for streamID and folds them through Aggregate.Apply,
// returning the latest state. Returns Initial() if the stream is empty.
func (r *Repository[S, E]) Load(ctx context.Context, streamID uuid.UUID) (state S, err error) {
	ctx, finish := r.opts.recorder.Start(ctx, "stream.repository.load")
	defer func() { finish(err) }()
	return r.folder.Fold(ctx, streamID)
}

// ExecuteFunc is the type of the function passed to Repository.Execute. It
// takes the current state and returns the events to append, or an error if
// it fails. ctx is the context passed to Execute, so a command can honor
// cancellation/deadlines or thread request-scoped values (e.g. a logger)
// through to code it calls — it should not be used to reach the store or
// this repository directly, since Execute itself owns persistence.
type ExecuteFunc[S any, E pebbles.Event] func(ctx context.Context, state S) ([]E, error)

// Execute loads current state and its stream revision, calls fn, and appends
// events at that revision if fn succeeds. The revision check prevents a stale
// command from appending after a concurrent write to this stream. Cross-stream
// invariants should be handled asynchronously. Returns [store.ErrConflict] if a
// concurrent writer claimed the next sequence; callers should rerun Execute to
// reload state and revalidate the command.
func (r *Repository[S, E]) Execute(
	ctx context.Context,
	streamID uuid.UUID,
	fn ExecuteFunc[S, E],
) (result S, err error) {
	ctx, finish := r.opts.recorder.Start(ctx, "stream.repository.execute")
	defer func() { finish(err) }()

	var zero S
	state, revision, err := r.folder.FoldWithSequence(ctx, streamID)
	if err != nil {
		return zero, fmt.Errorf("loading state: %w", err)
	}

	events, err := fn(ctx, state)
	if err != nil {
		return zero, fmt.Errorf("executing function: %w", err)
	}

	if err := r.appendAtRevision(ctx, streamID, revision, events...); err != nil {
		return zero, fmt.Errorf("appending events: %w", err)
	}

	for _, ev := range events {
		state = r.folder.Apply(state, ev)
	}

	return state, nil
}

// Delete appends a [pebbles.Deleted] tombstone to the stream. Any middleware
// in the pipeline (e.g. CryptoShredMiddleware) handles post-write cleanup such
// as crypto-shredding the stream's key. Returns [store.ErrConflict] on a
// concurrent writer.
func (r *Repository[S, E]) Delete(ctx context.Context, streamID uuid.UUID) (err error) {
	ctx, finish := r.opts.recorder.Start(ctx, "stream.repository.delete")
	defer func() { finish(err) }()

	last, err := r.store.LastSequence(ctx, r.streamType, streamID)
	if err != nil {
		return fmt.Errorf("fetching last sequence: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generating event ID: %w", err)
	}

	data, err := proto.Marshal(&pebbles.Deleted{})
	if err != nil {
		return fmt.Errorf("marshaling tombstone: %w", err)
	}

	tombstone := store.Event{
		ID:         id,
		StreamType: r.streamType,
		StreamID:   streamID,
		Sequence:   last + 1,
		Name:       pebbles.DeletedName,
		At:         time.Now().UTC(),
		Headers:    map[string][]string{},
		Data:       data,
	}
	return r.store.Append(ctx, tombstone)
}

// Append writes events in order, auto-assigning sequence numbers starting at
// LastSequence+1. Returns [store.ErrConflict] if a concurrent writer beat us to
// a sequence — callers should reload state and retry.
func (r *Repository[S, E]) Append(ctx context.Context, streamID uuid.UUID, events ...E) (err error) {
	ctx, finish := r.opts.recorder.Start(ctx, "stream.repository.append")
	defer func() { finish(err) }()

	if len(events) == 0 {
		return nil
	}

	last, err := r.store.LastSequence(ctx, r.streamType, streamID)
	if err != nil {
		return fmt.Errorf("fetching last sequence: %w", err)
	}

	return r.appendAtRevision(ctx, streamID, last, events...)
}

func (r *Repository[S, E]) appendAtRevision(ctx context.Context, streamID uuid.UUID, revision int64, events ...E) error {
	if len(events) == 0 {
		return nil
	}

	now := time.Now().UTC()
	batch := make([]store.Event, 0, len(events))

	for i, ev := range events {
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generating event ID: %w", err)
		}

		data, name, err := r.codecs.Encode(ev)
		if err != nil {
			return fmt.Errorf("encoding event %d: %w", i, err)
		}

		event := store.Event{
			ID:         id,
			StreamType: r.streamType,
			StreamID:   streamID,
			Sequence:   revision + int64(i) + 1,
			Name:       name,
			At:         now,
			Headers:    map[string][]string{},
			Data:       data,
		}

		batch = append(batch, event)
	}

	if err := r.store.AppendBatch(ctx, batch...); err != nil {
		return fmt.Errorf("appending event batch: %w", err)
	}
	return nil
}
