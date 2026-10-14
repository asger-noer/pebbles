// Package fold holds the single-stream read-and-fold routine shared by
// stream.Repository.Load and stream.View.Load. It lives in internal/ rather than
// the core pebbles package because it depends on codec and store (and codec
// imports pebbles, which would otherwise cycle).
package fold

import (
	"context"
	"fmt"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
)

// StreamReader is the single-stream read slice of Store, for read-only callers
// that fold one stream (e.g. a single-stream projection).
type StreamReader interface {
	Read(ctx context.Context, streamType string, streamID uuid.UUID, fromSequence int64) ([]store.Event, error)
}

// Folder reads a single stream and folds its events into M through a
// [pebbles.Stream]. It captures the stable dependencies once, so Fold needs only
// the stream id. A [pebbles.Aggregate] satisfies [pebbles.Stream] (identical
// method set), so a Repository folds its aggregate through the same routine.
type Folder[M any, E pebbles.Event] struct {
	reader    StreamReader
	codecs    codec.Registrar
	projector pebbles.Stream[M, E]
}

// New builds a Folder for the given projector.
func New[M any, E pebbles.Event](
	reader StreamReader,
	codecs codec.Registrar,
	projector pebbles.Stream[M, E],
) *Folder[M, E] {
	return &Folder[M, E]{reader: reader, codecs: codecs, projector: projector}
}

// Apply delegates to the projector's Apply method.
func (f *Folder[M, E]) Apply(state M, ev E) M {
	return f.projector.Apply(state, ev)
}

// Fold reads the stream and folds it: it hydrates each stored event through the
// codecs, asserts it satisfies E, and applies it. An empty stream returns
// Initial().
func (f *Folder[M, E]) Fold(ctx context.Context, streamID uuid.UUID) (M, error) {
	state, _, err := f.FoldWithSequence(ctx, streamID)
	return state, err
}

// FoldWithSequence returns the folded state and the sequence of its last event.
// An empty stream has sequence 0.
func (f *Folder[M, E]) FoldWithSequence(ctx context.Context, streamID uuid.UUID) (M, int64, error) {
	state := f.projector.Initial()

	events, err := f.reader.Read(ctx, f.projector.StreamType(), streamID, 0)
	if err != nil {
		return state, 0, fmt.Errorf("reading events from store: %w", err)
	}

	var sequence int64
	for _, raw := range events {
		if raw.Name == pebbles.DeletedName {
			return state, sequence, pebbles.ErrDeleted
		}

		payload, err := f.codecs.Hydrate(raw.Name, raw.Data)
		if err != nil {
			return state, sequence, fmt.Errorf("hydrating event %s: %w", raw.Name, err)
		}

		ev, ok := payload.(E)
		if !ok {
			return state, sequence, fmt.Errorf("hydrated %s as %T, which does not satisfy %T", raw.Name, payload, new(E))
		}

		state = f.projector.Apply(state, ev)
		sequence = raw.Sequence
	}

	return state, sequence, nil
}
