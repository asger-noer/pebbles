package memory

import (
	"context"
	"fmt"
	"maps"

	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
)

// CheckpointStore is an in-memory projection.CheckpointStore backed by the
// Store. It is safe for concurrent use.
type CheckpointStore struct {
	store *Engine
}

// NewCheckpointStore returns a CheckpointStore whose state lives in s.
func NewCheckpointStore(s *Engine) *CheckpointStore {
	return &CheckpointStore{store: s}
}

func (c *CheckpointStore) Load(ctx context.Context, name string) (int64, error) {
	c.store.mu.RLock()
	defer c.store.mu.RUnlock()
	return c.store.checkpoints[name], nil
}

func (c *CheckpointStore) Save(ctx context.Context, name string, position int64) error {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.checkpoints[name] = position
	return nil
}

// KeyedSnapshotStore is an in-memory store.KeyedSnapshotStore[M] backed by the
// Store. It is safe for concurrent use.
type KeyedSnapshotStore[M any] struct {
	store *Engine
}

// NewKeyedSnapshotStore returns a KeyedSnapshotStore whose state lives in s.
func NewKeyedSnapshotStore[M any](s *Engine) *KeyedSnapshotStore[M] {
	return &KeyedSnapshotStore[M]{store: s}
}

func (k *KeyedSnapshotStore[M]) Position(ctx context.Context, name string) (int64, error) {
	k.store.mu.RLock()
	defer k.store.mu.RUnlock()
	if st, ok := k.store.keyed[name]; ok {
		return st.position, nil
	}
	return 0, nil
}

func (k *KeyedSnapshotStore[M]) LoadIndex(ctx context.Context, name string) (map[uuid.UUID]uuid.UUID, error) {
	k.store.mu.RLock()
	defer k.store.mu.RUnlock()
	out := map[uuid.UUID]uuid.UUID{}
	if st, ok := k.store.keyed[name]; ok {
		maps.Copy(out, st.index)
	}
	return out, nil
}

func (k *KeyedSnapshotStore[M]) Load(ctx context.Context, name string, key uuid.UUID) (M, error) {
	k.store.mu.RLock()
	defer k.store.mu.RUnlock()

	var zero M
	st, ok := k.store.keyed[name]
	if !ok {
		return zero, store.ErrSnapshotNotFound
	}
	v, ok := st.models[key]
	if !ok {
		return zero, store.ErrSnapshotNotFound
	}
	model, ok := v.(M)
	if !ok {
		return zero, fmt.Errorf("memory: keyed snapshot %q/%s holds %T, not %T", name, key, v, zero)
	}
	return model, nil
}

func (k *KeyedSnapshotStore[M]) Commit(ctx context.Context, name string, position int64,
	models map[uuid.UUID]M, index map[uuid.UUID]uuid.UUID, deletedKeys []uuid.UUID,
) error {
	k.store.mu.Lock()
	defer k.store.mu.Unlock()

	st, ok := k.store.keyed[name]
	if !ok {
		st = &keyedState{
			models: map[uuid.UUID]any{},
			index:  map[uuid.UUID]uuid.UUID{},
		}
		k.store.keyed[name] = st
	}
	for key, m := range models {
		st.models[key] = m
	}
	maps.Copy(st.index, index)
	for _, key := range deletedKeys {
		delete(st.models, key)
	}
	st.position = position
	return nil
}

var (
	_ store.CheckpointStore         = (*CheckpointStore)(nil)
	_ store.KeyedSnapshotStore[int] = (*KeyedSnapshotStore[int])(nil)
)
