package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
)

type responseCipher struct {
	aead       cipher.AEAD
	keyVersion int
}

func newResponseCipher(encodedKey string, keyVersion int) (*responseCipher, error) {
	key, err := base64.RawStdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, errors.New("IDEMPOTENCY_KEY must be unpadded base64")
	}
	if len(key) != 32 {
		return nil, errors.New("IDEMPOTENCY_KEY must decode to exactly 32 bytes")
	}
	if keyVersion <= 0 {
		return nil, errors.New("IDEMPOTENCY_KEY_VERSION must be positive")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &responseCipher{aead: aead, keyVersion: keyVersion}, nil
}

func (c *responseCipher) encrypt(purpose string, recordID []byte, value any) ([]byte, []byte, error) {
	plaintext, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	ciphertext := c.aead.Seal(nil, nonce, plaintext, associatedData(purpose, recordID))
	return ciphertext, nonce, nil
}

func (c *responseCipher) decrypt(purpose string, recordID, nonce, ciphertext []byte, destination any) error {
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, associatedData(purpose, recordID))
	if err != nil {
		return err
	}
	return json.Unmarshal(plaintext, destination)
}

func associatedData(purpose string, recordID []byte) []byte {
	result := make([]byte, 0, len(purpose)+1+len(recordID))
	result = append(result, purpose...)
	result = append(result, 0)
	result = append(result, recordID...)
	return result
}
