package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type responseCipher struct {
	keys       map[int]cipher.AEAD
	keyVersion int
}

func newResponseCipher(encodedKey string, keyVersion int) (*responseCipher, error) {
	return newResponseCipherKeyring(encodedKey, keyVersion, "")
}

func newResponseCipherKeyring(encodedKey string, keyVersion int, encodedPreviousKeys string) (*responseCipher, error) {
	if keyVersion <= 0 {
		return nil, errors.New("IDEMPOTENCY_KEY_VERSION must be positive")
	}
	active, err := decodeResponseCipherKey(encodedKey)
	if err != nil {
		return nil, err
	}
	keys := map[int]cipher.AEAD{keyVersion: active}
	if strings.TrimSpace(encodedPreviousKeys) != "" {
		for _, entry := range strings.Split(encodedPreviousKeys, ",") {
			parts := strings.SplitN(strings.TrimSpace(entry), ":", 2)
			if len(parts) != 2 {
				return nil, errors.New("IDEMPOTENCY_PREVIOUS_KEYS entries must use version:key")
			}
			version, err := strconv.Atoi(parts[0])
			if err != nil || version <= 0 {
				return nil, errors.New("IDEMPOTENCY_PREVIOUS_KEYS versions must be positive integers")
			}
			if version == keyVersion {
				return nil, errors.New("active idempotency key must not be repeated in IDEMPOTENCY_PREVIOUS_KEYS")
			}
			if _, exists := keys[version]; exists {
				return nil, fmt.Errorf("duplicate idempotency key version %d", version)
			}
			previous, err := decodeResponseCipherKey(parts[1])
			if err != nil {
				return nil, fmt.Errorf("decode previous idempotency key version %d: %w", version, err)
			}
			keys[version] = previous
		}
	}
	return &responseCipher{keys: keys, keyVersion: keyVersion}, nil
}

func decodeResponseCipherKey(encodedKey string) (cipher.AEAD, error) {
	key, err := base64.RawStdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, errors.New("IDEMPOTENCY_KEY must be unpadded base64")
	}
	if len(key) != 32 {
		return nil, errors.New("IDEMPOTENCY_KEY must decode to exactly 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return aead, nil
}

func (c *responseCipher) encrypt(purpose string, recordID []byte, value any) ([]byte, []byte, error) {
	aead := c.keys[c.keyVersion]
	plaintext, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, associatedData(purpose, recordID))
	return ciphertext, nonce, nil
}

func (c *responseCipher) decrypt(
	keyVersion int,
	purpose string,
	recordID, nonce, ciphertext []byte,
	destination any,
) error {
	aead, found := c.keys[keyVersion]
	if !found {
		return fmt.Errorf("idempotency encryption key version %d is unavailable", keyVersion)
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, associatedData(purpose, recordID))
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
