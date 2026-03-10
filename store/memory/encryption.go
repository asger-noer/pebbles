package memory

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"sync"

	"github.com/asger-noer/pebbles"
	"github.com/asger-noer/pebbles/internal/aesgcm"
	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CryptoShredMiddleware implements [store.Middleware] using per-stream
// AES-256-GCM keys. Keys are stored in memory and are independent of the
// backing [Engine].
//
// Wire it with [stream.WithMiddleware]:
//
//	enc  := memory.NewCryptoShredMiddleware()
//	repo := stream.NewRepository(codecs, st, stream.WithMiddleware(enc))
type CryptoShredMiddleware struct {
	mu   sync.RWMutex
	keys map[uuid.UUID][]byte
}

// NewCryptoShredMiddleware returns an CryptoShredMiddleware with an empty key store.
func NewCryptoShredMiddleware() *CryptoShredMiddleware {
	return &CryptoShredMiddleware{keys: make(map[uuid.UUID][]byte)}
}

// Append is the write-path hook. It:
//  1. Auto-creates a key for new streams (no key yet).
//  2. Encrypts e.Data before passing it to next (no-op for the tombstone, which has no data).
//  3. Calls next to persist the event.
//  4. After a successful tombstone write, deletes the stream's key (crypto-shred).
func (m *CryptoShredMiddleware) Append(ctx context.Context, tx pgx.Tx, e store.Event, next store.AppendFunc) error {
	isTombstone := e.Name == pebbles.DeletedName

	if !isTombstone {
		if err := m.ensureKey(e.StreamID); err != nil {
			return err
		}

		if len(e.Data) > 0 {
			encrypted, err := aesgcm.Encrypt(m.key(e.StreamID), e.Data)
			if err != nil {
				return fmt.Errorf("encrypting event: %w", err)
			}
			e.Data = encrypted
		}
	}

	if err := next(ctx, tx, e); err != nil {
		return err
	}

	if isTombstone {
		m.mu.Lock()
		delete(m.keys, e.StreamID)
		m.mu.Unlock()
	}

	return nil
}

// OnRead decrypts e.Data if a key exists. If the key has been deleted and the
// event carried data, it is converted to a [pebbles.Deleted] tombstone so
// projection drivers receive a clean deletion signal.
func (m *CryptoShredMiddleware) OnRead(_ context.Context, e store.Event) (store.Event, error) {
	if e.Name == pebbles.DeletedName || len(e.Data) == 0 {
		return e, nil
	}
	key := m.key(e.StreamID)
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

func (m *CryptoShredMiddleware) ensureKey(streamID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.keys[streamID]; ok {
		return nil
	}
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return fmt.Errorf("generating key: %w", err)
	}
	m.keys[streamID] = key
	return nil
}

func (m *CryptoShredMiddleware) key(streamID uuid.UUID) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keys[streamID]
}

var _ store.Middleware = (*CryptoShredMiddleware)(nil)
