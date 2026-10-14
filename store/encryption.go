package store

import (
	"context"

	"github.com/asger-noer/pebbles/internal/aesgcm"
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

func (m *EncryptionMiddleware) Append(ctx context.Context, events []Event, next AppendFunc) error {
	for i := range events {
		key, err := aesgcm.DeriveKey(m.masterKey, events[i].StreamID)
		if err != nil {
			return err
		}

		events[i].Data, err = aesgcm.Encrypt(key, events[i].Data)
		if err != nil {
			return err
		}
	}

	return next(ctx, events)
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
