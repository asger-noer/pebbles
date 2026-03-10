package store_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestEncryptionMiddlewareRoundTrip(t *testing.T) {
	masterKey := bytes.Repeat([]byte("k"), 32)
	mw := store.NewEncryptionMiddleware(masterKey)

	streamID := uuid.New()
	plaintext := []byte("hello world")
	e := store.Event{StreamID: streamID, Data: plaintext}

	var stored store.Event
	err := mw.Append(context.Background(), nil, e, func(_ context.Context, _ pgx.Tx, ev store.Event) error {
		stored = ev
		return nil
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	decrypted, err := mw.OnRead(context.Background(), stored)
	if err != nil {
		t.Fatalf("OnRead: %v", err)
	}
	if !bytes.Equal(decrypted.Data, plaintext) {
		t.Fatalf("got %q, want %q", decrypted.Data, plaintext)
	}
}

func TestEncryptionMiddlewareStoredDataIsOpaque(t *testing.T) {
	masterKey := bytes.Repeat([]byte("k"), 32)
	mw := store.NewEncryptionMiddleware(masterKey)

	plaintext := []byte("secret data")
	e := store.Event{StreamID: uuid.New(), Data: plaintext}

	var stored store.Event
	_ = mw.Append(context.Background(), nil, e, func(_ context.Context, _ pgx.Tx, ev store.Event) error {
		stored = ev
		return nil
	})

	if bytes.Equal(stored.Data, plaintext) {
		t.Fatal("stored data must not equal plaintext")
	}
}

func TestEncryptionMiddlewareDifferentStreamsProduceDifferentCiphertext(t *testing.T) {
	masterKey := bytes.Repeat([]byte("k"), 32)
	mw := store.NewEncryptionMiddleware(masterKey)

	plaintext := []byte("same plaintext")

	var ct1, ct2 []byte
	for i, target := range []*[]byte{&ct1, &ct2} {
		e := store.Event{StreamID: uuid.New(), Data: plaintext}
		_ = i
		_ = mw.Append(context.Background(), nil, e, func(_ context.Context, _ pgx.Tx, ev store.Event) error {
			*target = ev.Data
			return nil
		})
	}

	if bytes.Equal(ct1, ct2) {
		t.Fatal("different stream IDs must produce different ciphertext")
	}
}

func TestEncryptionMiddlewareWrongMasterKeyFails(t *testing.T) {
	masterKey := bytes.Repeat([]byte("k"), 32)
	wrongKey := bytes.Repeat([]byte("x"), 32)

	mw := store.NewEncryptionMiddleware(masterKey)
	mwWrong := store.NewEncryptionMiddleware(wrongKey)

	streamID := uuid.New()
	e := store.Event{StreamID: streamID, Data: []byte("secret")}

	var stored store.Event
	_ = mw.Append(context.Background(), nil, e, func(_ context.Context, _ pgx.Tx, ev store.Event) error {
		stored = ev
		return nil
	})

	_, err := mwWrong.OnRead(context.Background(), stored)
	if err == nil {
		t.Fatal("expected decryption error with wrong master key")
	}
}

func TestEncryptionMiddlewareEmptyDataPassesThrough(t *testing.T) {
	masterKey := bytes.Repeat([]byte("k"), 32)
	mw := store.NewEncryptionMiddleware(masterKey)

	e := store.Event{StreamID: uuid.New(), Data: nil}
	result, err := mw.OnRead(context.Background(), e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Data) != 0 {
		t.Fatal("empty data must pass through unchanged")
	}
}
