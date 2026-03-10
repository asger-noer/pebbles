package stream

import (
	"context"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	"github.com/asger-noer/pebbles/internal/fold"
	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
)

// View folds a single stream into a read model on demand. It is the read-only
// counterpart to [Repository]: one Load that reads the stream and folds it
// through a [pebbles.Stream]. Use it for a read-model view of one stream.
type View[M any, E pebbles.Event] struct {
	folder *fold.Folder[M, E]
}

// NewView constructs a [View] for the given projector.
func NewView[M any, E pebbles.Event](
	p pebbles.Stream[M, E],
	codecs codec.Registrar,
	s *store.Store,
) *View[M, E] {
	return &View[M, E]{
		folder: fold.New(s, codecs, p),
	}
}

// Load reads the stream and folds it through the projector, returning the read
// model. An empty stream yields Initial().
func (v *View[M, E]) Load(ctx context.Context, streamID uuid.UUID) (M, error) {
	return v.folder.Fold(ctx, streamID)
}
