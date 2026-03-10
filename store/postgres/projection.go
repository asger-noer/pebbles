package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/postgres/queries"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckpointStore is a Postgres-backed projection.CheckpointStore, persisting a
// Handler-based projection's cursor in pebbles.projection_checkpoint.
type CheckpointStore struct {
	queries *queries.Queries
}

// NewCheckpointStore returns a CheckpointStore sharing the Store's pool. The
// backing table is created by the Store's migrations.
func NewCheckpointStore(s *Engine) *CheckpointStore {
	return &CheckpointStore{queries: s.queries}
}

func (c *CheckpointStore) Load(ctx context.Context, name string) (int64, error) {
	pos, err := c.queries.LoadCheckpoint(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pos, err
}

func (c *CheckpointStore) Save(ctx context.Context, name string, position int64) error {
	return c.queries.SaveCheckpoint(ctx, queries.SaveCheckpointParams{Name: name, Position: position})
}

// KeyedSnapshotStore is a Postgres-backed store.KeyedSnapshotStore[M]. Per-key
// models live in pebbles.projection_keyed_snapshot, the resolver index in
// pebbles.projection_keyed_index, and the cursor in pebbles.projection_checkpoint;
// Commit writes all three in one transaction.
type KeyedSnapshotStore[M any] struct {
	pool    *pgxpool.Pool
	queries *queries.Queries
}

// NewKeyedSnapshotStore returns a KeyedSnapshotStore sharing the Store's pool.
func NewKeyedSnapshotStore[M any](s *Engine) *KeyedSnapshotStore[M] {
	return &KeyedSnapshotStore[M]{pool: s.pool, queries: s.queries}
}

func (k *KeyedSnapshotStore[M]) Position(ctx context.Context, name string) (int64, error) {
	pos, err := k.queries.LoadCheckpoint(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return pos, err
}

func (k *KeyedSnapshotStore[M]) LoadIndex(ctx context.Context, name string) (map[uuid.UUID]uuid.UUID, error) {
	rows, err := k.queries.LoadKeyedIndex(ctx, name)
	if err != nil {
		return nil, err
	}
	out := make(map[uuid.UUID]uuid.UUID, len(rows))
	for _, row := range rows {
		out[row.ID] = row.Key
	}
	return out, nil
}

func (k *KeyedSnapshotStore[M]) Load(ctx context.Context, name string, key uuid.UUID) (M, error) {
	var model M
	data, err := k.queries.LoadKeyedSnapshot(ctx, queries.LoadKeyedSnapshotParams{Name: name, Key: key})
	if errors.Is(err, pgx.ErrNoRows) {
		return model, store.ErrSnapshotNotFound
	}
	if err != nil {
		return model, err
	}
	if err := json.Unmarshal(data, &model); err != nil {
		return model, err
	}
	return model, nil
}

func (k *KeyedSnapshotStore[M]) Commit(ctx context.Context, name string, position int64,
	models map[uuid.UUID]M, index map[uuid.UUID]uuid.UUID, deletedKeys []uuid.UUID,
) error {
	tx, err := k.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	q := k.queries.WithTx(tx)
	for key, m := range models {
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if err := q.SaveKeyedSnapshot(ctx, queries.SaveKeyedSnapshotParams{Name: name, Key: key, Model: data}); err != nil {
			return err
		}
	}
	for id, key := range index {
		if err := q.SaveKeyedIndex(ctx, queries.SaveKeyedIndexParams{Name: name, ID: id, Key: key}); err != nil {
			return err
		}
	}
	for _, key := range deletedKeys {
		if err := q.DeleteKeyedSnapshotsByKey(ctx, queries.DeleteKeyedSnapshotsByKeyParams{
			Name: name,
			Key:  key,
		}); err != nil {
			return err
		}
	}
	if err := q.SaveCheckpoint(ctx, queries.SaveCheckpointParams{Name: name, Position: position}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

var (
	_ store.CheckpointStore         = (*CheckpointStore)(nil)
	_ store.KeyedSnapshotStore[int] = (*KeyedSnapshotStore[int])(nil)
)
