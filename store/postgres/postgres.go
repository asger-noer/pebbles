package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/postgres/queries"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/tern/v2/migrate"
)

//go:generate go tool sqlc -f sqlc.yaml generate

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Engine is a Postgres-backed implementation of store.Engine, built on top of
// the sqlc-generated queries.
type Engine struct {
	pool    *pgxpool.Pool
	queries *queries.Queries
}

func New(ctx context.Context, pool *pgxpool.Pool) (*Engine, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	// The pebbles schema must exist before tern can create its version table
	// inside it; migration 001 also issues this (idempotently) for anyone
	// inspecting the migration history, but the version table needs it first.
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS pebbles"); err != nil {
		return nil, err
	}

	// pebbles.schema_version is a table of its own, distinct from any version
	// table an embedding application's own migrator uses, so the two don't
	// collide over a shared migration counter.
	migrator, err := migrate.NewMigrator(ctx, conn.Conn(), "pebbles.schema_version")
	if err != nil {
		return nil, err
	}
	migrationsSubFS, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, err
	}
	if err := migrator.LoadMigrations(migrationsSubFS); err != nil {
		return nil, err
	}
	if err := migrator.Migrate(ctx); err != nil {
		return nil, err
	}

	return &Engine{
		pool:    pool,
		queries: queries.New(pool),
	}, nil
}

// Append persists one event. AppendBatch writes all events in one transaction.
// An insert trigger on pebbles.event wakes listening projection runners.
func (s *Engine) Append(ctx context.Context, e store.Event) error {
	return s.AppendBatch(ctx, []store.Event{e})
}

// AppendBatch writes all events atomically.
func (s *Engine) AppendBatch(ctx context.Context, events []store.Event) error {
	if len(events) == 0 {
		return nil
	}
	first := events[0]
	for _, e := range events[1:] {
		if e.StreamType != first.StreamType || e.StreamID != first.StreamID {
			return store.ErrMixedStreams
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.queries.WithTx(tx)
	for i, e := range events {
		headers, err := json.Marshal(e.Headers)
		if err != nil {
			return fmt.Errorf("marshaling headers for event %d: %w", i, err)
		}
		err = q.AppendEvent(ctx, queries.AppendEventParams{
			ID:         e.ID,
			StreamType: e.StreamType,
			StreamID:   e.StreamID,
			Sequence:   e.Sequence,
			Name:       e.Name,
			At:         pgtype.Timestamptz{Time: e.At, Valid: true},
			Headers:    headers,
			Data:       e.Data,
		})
		if isUniqueViolation(err) {
			return store.ErrConflict
		}
		if err != nil {
			return fmt.Errorf("appending event %d: %w", i, err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Engine) Read(ctx context.Context, streamType string, streamID uuid.UUID, fromSequence int64) ([]store.Event, error) {
	rows, err := s.queries.ListStreamEvents(ctx, queries.ListStreamEventsParams{
		StreamType: streamType,
		StreamID:   streamID,
		Sequence:   fromSequence,
	})
	if err != nil {
		return nil, err
	}
	out := make([]store.Event, 0, len(rows))
	for _, row := range rows {
		headers, err := decodeHeaders(row.Headers)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Event{
			ID:         row.ID,
			StreamType: row.StreamType,
			StreamID:   row.StreamID,
			Sequence:   row.Sequence,
			Name:       row.Name,
			At:         row.At.Time,
			Headers:    headers,
			Data:       row.Data,
		})
	}
	return out, nil
}

func (s *Engine) ReadAll(ctx context.Context, from int64, limit int) ([]store.Positioned, error) {
	rows, err := s.queries.ReadAllEvents(ctx, queries.ReadAllEventsParams{
		FromPosition: pgtype.Int8{Int64: from, Valid: true},
		MaxEvents:    int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]store.Positioned, 0, len(rows))
	for _, row := range rows {
		headers, err := decodeHeaders(row.Headers)
		if err != nil {
			return nil, err
		}
		out = append(out, store.Positioned{
			Position: row.GlobalPosition.Int64,
			Event: store.Event{
				ID:         row.ID,
				StreamType: row.StreamType,
				StreamID:   row.StreamID,
				Sequence:   row.Sequence,
				Name:       row.Name,
				At:         row.At.Time,
				Headers:    headers,
				Data:       row.Data,
			},
		})
	}
	return out, nil
}

func (s *Engine) LastSequence(ctx context.Context, streamType string, streamID uuid.UUID) (int64, error) {
	seq, err := s.queries.LastSequence(ctx, queries.LastSequenceParams{
		StreamType: streamType,
		StreamID:   streamID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}

func (s *Engine) ListStreams(ctx context.Context, streamType string) ([]uuid.UUID, error) {
	return s.queries.ListStreams(ctx, streamType)
}

func decodeHeaders(raw []byte) (pebbles.Headers, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var headers pebbles.Headers
	if err := json.Unmarshal(raw, &headers); err != nil {
		return nil, err
	}
	return headers, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

var _ store.Engine = (*Engine)(nil)
