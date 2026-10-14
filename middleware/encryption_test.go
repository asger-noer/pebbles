package middleware_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/asger-noer/pebbles/middleware"
	"github.com/asger-noer/pebbles/store"
	"github.com/google/uuid"
)

func appendThrough(mw store.Middleware, e store.Event) (store.Event, error) {
	var stored store.Event
	err := mw.Append(context.Background(), []store.Event{e}, func(_ context.Context, events []store.Event) error {
		stored = events[0]
		return nil
	})
	return stored, err
}

func TestEncryptionMiddlewareRoundTrip(t *testing.T) {
	masterKey := bytes.Repeat([]byte("k"), 32)
	mw := middleware.NewEncryptionMiddleware(masterKey)

	plaintext := []byte("hello world")
	stored, err := appendThrough(mw, store.Event{StreamID: uuid.New(), Data: plaintext})
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
	mw := middleware.NewEncryptionMiddleware(bytes.Repeat([]byte("k"), 32))
	plaintext := []byte("secret data")
	stored, err := appendThrough(mw, store.Event{StreamID: uuid.New(), Data: plaintext})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(stored.Data, plaintext) {
		t.Fatal("stored data must not equal plaintext")
	}
}

func TestEncryptionMiddlewareDifferentStreamsProduceDifferentCiphertext(t *testing.T) {
	mw := middleware.NewEncryptionMiddleware(bytes.Repeat([]byte("k"), 32))
	plaintext := []byte("same plaintext")

	var ciphertext [2][]byte
	for i := range ciphertext {
		stored, err := appendThrough(mw, store.Event{StreamID: uuid.New(), Data: plaintext})
		if err != nil {
			t.Fatal(err)
		}
		ciphertext[i] = stored.Data
	}
	if bytes.Equal(ciphertext[0], ciphertext[1]) {
		t.Fatal("different stream IDs must produce different ciphertext")
	}
}

func TestEncryptionMiddlewareWrongMasterKeyFails(t *testing.T) {
	mw := middleware.NewEncryptionMiddleware(bytes.Repeat([]byte("k"), 32))
	mwWrong := middleware.NewEncryptionMiddleware(bytes.Repeat([]byte("x"), 32))

	stored, err := appendThrough(mw, store.Event{StreamID: uuid.New(), Data: []byte("secret")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mwWrong.OnRead(context.Background(), stored); err == nil {
		t.Fatal("expected decryption error with wrong master key")
	}
}

func TestEncryptionMiddlewareEmptyDataPassesThrough(t *testing.T) {
	mw := middleware.NewEncryptionMiddleware(bytes.Repeat([]byte("k"), 32))
	e := store.Event{StreamID: uuid.New(), Data: nil}
	result, err := mw.OnRead(context.Background(), e)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Data) != 0 {
		t.Fatal("empty data must pass through unchanged")
	}
}
