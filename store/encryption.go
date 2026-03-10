package store

import (
	"context"

	"github.com/asger-noer/pebbles/internal/aesgcm"
	"github.com/jackc/pgx/v5"
)

// EncryptionMiddleware encrypts event data with AES-256-GCM using a key
// derived from the master key and stream ID via HKDF. No per-stream key
// storage is required; the same (masterKey, streamID) pair always produces
// the same derived key, so decryption is always possible as long as the
// master key is available. Unlike CryptoShredMiddleware, this middleware
// does not support crypto-shredding.
type EncryptionMiddleware struct {
	masterKey []byte
}

var _ Middleware = (*EncryptionMiddleware)(nil)

// NewEncryptionMiddleware returns an EncryptionMiddleware using masterKey.
// masterKey can be any length; 32 bytes of random material is recommended.
func NewEncryptionMiddleware(masterKey []byte) *EncryptionMiddleware {
	return &EncryptionMiddleware{masterKey: masterKey}
}

func (m *EncryptionMiddleware) Append(ctx context.Context, tx pgx.Tx, e Event, next AppendFunc) error {
	key, err := aesgcm.DeriveKey(m.masterKey, e.StreamID)
	if err != nil {
		return err
	}

	e.Data, err = aesgcm.Encrypt(key, e.Data)
	if err != nil {
		return err
	}

	return next(ctx, tx, e)
}

func (m *EncryptionMiddleware) OnRead(ctx context.Context, e Event) (Event, error) {
	if len(e.Data) == 0 {
		return e, nil
	}

	key, err := aesgcm.DeriveKey(m.masterKey, e.StreamID)
	if err != nil {
		return Event{}, err
	}

	e.Data, err = aesgcm.Decrypt(key, e.Data)
	if err != nil {
		return Event{}, err
	}

	return e, nil
}
