package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/internal/aesgcm"
	"github.com/asger-noer/pebbles/store"
	"github.com/asger-noer/pebbles/store/postgres/queries"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CryptoShredMiddleware implements [store.Middleware] using per-stream
// AES-256-GCM keys stored in pebbles.stream_key.
//
// Wire it with [store.WithMiddleware]:
//
//	enc     := postgres.NewCryptoShredMiddleware(st)
//	wrapped := store.WithMiddleware(st, enc)
//	repo    := stream.NewRepository(codecs, st, stream.WithMiddleware(enc))
type CryptoShredMiddleware struct {
	pool    *pgxpool.Pool
	queries *queries.Queries
}

// NewCryptoShredMiddleware returns an CryptoShredMiddleware backed by s.
func NewCryptoShredMiddleware(pool *pgxpool.Pool) *CryptoShredMiddleware {
	return &CryptoShredMiddleware{
		pool:    pool,
		queries: queries.New(pool),
	}
}

// Append is the write-path hook. It:
//  1. Auto-creates a key for new streams (ON CONFLICT DO NOTHING).
//  2. Encrypts e.Data if a key exists (no-op for the tombstone, which has no data).
//  3. Calls next to persist the event.
//  4. After a successful tombstone write, deletes the stream's key (crypto-shred).
func (m *CryptoShredMiddleware) Append(ctx context.Context, tx pgx.Tx, e store.Event, next store.AppendFunc) error {
	isTombstone := e.Name == pebbles.DeletedName

	q := m.queries
	if tx != nil {
		q = q.WithTx(tx)
	}

	if !isTombstone {
		if err := m.ensureKey(ctx, q, e.StreamID); err != nil {
			return err
		}

		if len(e.Data) > 0 {
			key, err := m.loadKey(ctx, q, e.StreamID)
			if err != nil {
				return err
			}
			if key != nil {
				encrypted, err := aesgcm.Encrypt(key, e.Data)
				if err != nil {
					return fmt.Errorf("encrypting event: %w", err)
				}
				e.Data = encrypted
			}
		}
	}

	if err := next(ctx, tx, e); err != nil {
		return err
	}

	if isTombstone {
		if err := q.DeleteStreamKey(ctx, e.StreamID); err != nil {
			return fmt.Errorf("deleting stream key: %w", err)
		}
	}

	return nil
}

// OnRead decrypts e.Data if a key exists. If the key has been deleted and the
// event carried data, it is converted to a [pebbles.Deleted] tombstone.
func (m *CryptoShredMiddleware) OnRead(ctx context.Context, e store.Event) (store.Event, error) {
	if e.Name == pebbles.DeletedName || len(e.Data) == 0 {
		return e, nil
	}
	key, err := m.loadKey(ctx, m.queries, e.StreamID)
	if err != nil {
		return e, err
	}
	if key == nil {
		e.Name = pebbles.DeletedName
		e.Data = nil
		return e, nil
	}
	plain, err := aesgcm.Decrypt(key, e.Data)
	if err != nil {
		return e, fmt.Errorf("decrypting event %s seq %d: %w", e.Name, e.Sequence, err)
	}
	e.Data = plain
	return e, nil
}

func (m *CryptoShredMiddleware) ensureKey(ctx context.Context, q *queries.Queries, streamID uuid.UUID) error {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return fmt.Errorf("generating key: %w", err)
	}
	return q.CreateStreamKey(ctx, queries.CreateStreamKeyParams{
		StreamID: streamID,
		Key:      key,
	})
}

func (m *CryptoShredMiddleware) loadKey(ctx context.Context, q *queries.Queries, streamID uuid.UUID) ([]byte, error) {
	key, err := q.GetStreamKey(ctx, streamID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return key, err
}

var _ store.Middleware = (*CryptoShredMiddleware)(nil)
