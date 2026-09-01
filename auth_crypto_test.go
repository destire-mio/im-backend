package main

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestResponseCipherKeyringReadsPreviousVersionAndWritesActiveVersion(t *testing.T) {
	versionOneKey := base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32))
	versionTwoKey := base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	versionOne, err := newResponseCipher(versionOneKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"refreshToken": "new-token-after-lost-response"}
	ciphertext, nonce, err := versionOne.encrypt("refresh", []byte("41"), want)
	if err != nil {
		t.Fatal(err)
	}

	rotated, err := newResponseCipherKeyring(versionTwoKey, 2, "1:"+versionOneKey)
	if err != nil {
		t.Fatal(err)
	}
	var recovered map[string]string
	if err := rotated.decrypt(1, "refresh", []byte("41"), nonce, ciphertext, &recovered); err != nil {
		t.Fatalf("decrypt previous key version: %v", err)
	}
	if recovered["refreshToken"] != want["refreshToken"] {
		t.Fatalf("recovered payload = %#v", recovered)
	}

	activeCiphertext, activeNonce, err := rotated.encrypt("refresh", []byte("42"), want)
	if err != nil {
		t.Fatal(err)
	}
	if err := rotated.decrypt(2, "refresh", []byte("42"), activeNonce, activeCiphertext, &recovered); err != nil {
		t.Fatalf("decrypt active key version: %v", err)
	}
	withoutPrevious, err := newResponseCipher(versionTwoKey, 2)
	if err != nil {
		t.Fatal(err)
	}
	err = withoutPrevious.decrypt(1, "refresh", []byte("41"), nonce, ciphertext, &recovered)
	if err == nil || !strings.Contains(err.Error(), "version 1 is unavailable") {
		t.Fatalf("missing previous key error = %v", err)
	}
}

func TestResponseCipherKeyringRejectsAmbiguousVersions(t *testing.T) {
	key := base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))
	for name, previous := range map[string]string{
		"active repeated": "2:" + key,
		"duplicate":       "1:" + key + ",1:" + key,
		"invalid version": "old:" + key,
		"missing key":     "1",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := newResponseCipherKeyring(key, 2, previous); err == nil {
				t.Fatal("invalid keyring configuration was accepted")
			}
		})
	}
}
