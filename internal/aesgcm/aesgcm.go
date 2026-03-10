// Package aesgcm provides AES-256-GCM helpers shared by the memory and
// postgres encrypting stores.
package aesgcm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"github.com/google/uuid"
	"golang.org/x/crypto/hkdf"
)

// AES256KeySize is the required length of AES-256 keys.
const AES256KeySize = 32

// Encrypt seals plaintext with AES-256-GCM. The nonce is prepended to the
// ciphertext so Decrypt can recover it from the same []byte.
func Encrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// DeriveKey derives a 32-byte AES-256 key from masterKey using streamID as
// HKDF context. The same (masterKey, streamID) pair always yields the same key.
func DeriveKey(masterKey []byte, streamID uuid.UUID) ([]byte, error) {
	h := hkdf.New(sha256.New, masterKey, nil, streamID[:])

	key := make([]byte, AES256KeySize)
	if _, err := io.ReadFull(h, key); err != nil {
		return nil, err
	}

	return key, nil
}

// Decrypt recovers plaintext from the nonce-prefixed ciphertext produced by Encrypt.
func Decrypt(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ciphertext, nil)
}
