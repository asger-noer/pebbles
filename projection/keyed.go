package projection

import (
	"context"
	"errors"
	"fmt"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/codec"
	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
)

// routeFunc resolves the grouping key(s) an event applies to, using the index to
// learn and resolve keys. Both NewKeyed and NewRouted reduce to one of these.
type routeFunc[E pebbles.Event] func(p pebbles.Pebble[E], idx pebbles.Index) []uuid.UUID

// Keyed folds events into one model M per grouping key, resolving the
// key for each event through an index it learns as it goes. Build it with
// NewKeyed (managed Key) or NewRouted (explicit Route); read a key's model with
// Get. It reuses the shared catch-up driver.
type Keyed[M any, E pebbles.Event] struct {
	name       string
	subs       pebbles.Subscriptions
	initial    func() M
	apply      func(M, E) M
	route      routeFunc[E]
	registries map[string]codec.Registrar
	store      *store.Store
	snapshots  store.KeyedSnapshotStore[M]
	opts       options
}

// NewKeyed wires the managed form: the projector names the key on the events
// that carry it (typically creation) via Key, and the framework records
// streamID -> key and inherits it for the rest of that stream. It panics if the
// projector's Subscriptions are invalid, or if any subscribed stream type has no
// corresponding registry.
func NewKeyed[M any, E pebbles.Event](
	p pebbles.KeyedProjector[M, E],
	s *store.Store,
	snaps store.KeyedSnapshotStore[M],
	registries []codec.Registrar,
	opts ...Option,
) *Keyed[M, E] {
	route := func(peb pebbles.Pebble[E], idx pebbles.Index) []uuid.UUID {
		if key, ok := p.Key(peb.Event); ok {
			idx.Put(peb.StreamID, key)
			return []uuid.UUID{key}
		}
		if key, ok := idx.Get(peb.StreamID); ok {
			return []uuid.UUID{key}
		}
		return nil // not yet resolvable — skipped, cursor still advances
	}
	return newKeyed(p.Name(), p.Subscriptions(), p.Initial, p.Apply, route, s, snaps, registries, opts)
}

// NewRouted wires the explicit form: Route resolves the key(s) itself via the
// index (learning from creation events, looking up payload foreign keys from
// other streams). It panics if the projector's Subscriptions are invalid, or if
// any subscribed stream type has no corresponding registry.
func NewRouted[M any, E pebbles.Event](
	p pebbles.RoutedProjector[M, E],
	s *store.Store,
	snaps store.KeyedSnapshotStore[M],
	registries []codec.Registrar,
	opts ...Option,
) *Keyed[M, E] {
	return newKeyed(p.Name(), p.Subscriptions(), p.Initial, p.Apply, p.Route, s, snaps, registries, opts)
}

func newKeyed[M any, E pebbles.Event](
	name string,
	subs pebbles.Subscriptions,
	initial func() M,
	apply func(M, E) M,
	route routeFunc[E],
	s *store.Store,
	snaps store.KeyedSnapshotStore[M],
	registries []codec.Registrar,
	opts []Option,
) *Keyed[M, E] {
	if err := subs.Validate(); err != nil {
		panic(err)
	}
	return &Keyed[M, E]{
		name:       name,
		subs:       subs,
		initial:    initial,
		apply:      apply,
		route:      route,
		registries: buildRegistryMap(subs, registries),
		store:      s,
		snapshots:  snaps,
		opts:       resolveOptions(opts),
	}
}

// Run loads the cursor and the resolver index, then folds the log into one model
// per key. Each batch commits the touched models, newly learned index entries,
// and the advanced cursor atomically. It blocks until ctx is cancelled
// (returning nil) or a fatal error occurs.
func (k *Keyed[M, E]) Run(ctx context.Context) error {
	pos, err := k.snapshots.Position(ctx, k.name)
	if err != nil {
		return fmt.Errorf("loading position %q: %w", k.name, err)
	}
	index, err := k.snapshots.LoadIndex(ctx, k.name)
	if err != nil {
		return fmt.Errorf("loading index %q: %w", k.name, err)
	}
	if index == nil {
		index = map[uuid.UUID]uuid.UUID{}
	}

	pendingModels := map[uuid.UUID]M{}
	pendingIndex := map[uuid.UUID]uuid.UUID{}
	var pendingDeletions []uuid.UUID
	idx := &indexHandle{live: index, pending: pendingIndex}

	d := &driver{
		store:      k.store,
		registries: k.registries,
		subs:       k.subs,
		opts:       k.opts,
		load:       func(context.Context) (int64, error) { return pos, nil },
		apply: func(ctx context.Context, peb pebbles.Pebble[pebbles.Event]) error {
			if _, ok := peb.Event.(*pebbles.Deleted); ok {
				if key, found := idx.Get(peb.StreamID); found {
					pendingDeletions = append(pendingDeletions, key)
				}
				return nil
			}

			ev, ok := peb.Event.(E)
			if !ok {
				var zero E
				name := string(peb.Event.ProtoReflect().Descriptor().FullName())
				return fmt.Errorf("keyed projection %q: event %s does not satisfy %T", k.name, name, zero)
			}
			pe := pebbles.Pebble[E]{
				ID:         peb.ID,
				StreamType: peb.StreamType,
				StreamID:   peb.StreamID,
				Sequence:   peb.Sequence,
				At:         peb.At,
				Headers:    peb.Headers,
				Event:      ev,
			}

			for _, key := range k.route(pe, idx) {
				m, ok := pendingModels[key]
				if !ok {
					loaded, err := k.snapshots.Load(ctx, k.name, key)
					switch {
					case errors.Is(err, store.ErrSnapshotNotFound):
						m = k.initial()
					case err != nil:
						return fmt.Errorf("loading model %q/%s: %w", k.name, key, err)
					default:
						m = loaded
					}
				}
				pendingModels[key] = k.apply(m, ev)
			}
			return nil
		},
		commit: func(ctx context.Context, position int64) error {
			if err := k.snapshots.Commit(ctx, k.name, position, pendingModels, pendingIndex, pendingDeletions); err != nil {
				return err
			}
			clear(pendingModels)
			clear(pendingIndex)
			pendingDeletions = pendingDeletions[:0]
			return nil
		},
	}

	return d.run(ctx)
}

// Get returns the model materialized for key, or Initial() if the key has not
// been seen.
func (k *Keyed[M, E]) Get(ctx context.Context, key uuid.UUID) (M, error) {
	m, err := k.snapshots.Load(ctx, k.name, key)
	if errors.Is(err, store.ErrSnapshotNotFound) {
		return k.initial(), nil
	}
	if err != nil {
		return m, fmt.Errorf("loading model %q/%s: %w", k.name, key, err)
	}
	return m, nil
}

// indexHandle is the pebbles.Index given to a projector during a run. Put records
// a mapping both in the live index (so later events resolve it) and in pending
// (to be persisted on the next commit). Get reads the live index.
type indexHandle struct {
	live    map[uuid.UUID]uuid.UUID
	pending map[uuid.UUID]uuid.UUID
}

func (h *indexHandle) Get(id uuid.UUID) (uuid.UUID, bool) {
	key, ok := h.live[id]
	return key, ok
}

func (h *indexHandle) Put(id, key uuid.UUID) {
	h.live[id] = key
	h.pending[id] = key
}

var _ pebbles.Index = (*indexHandle)(nil)
