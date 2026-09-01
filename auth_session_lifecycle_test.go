package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSessionAbsoluteExpiryRejectsAccessAndRefresh(t *testing.T) {
	db := openTestDatabase(t)
	app := newTestApplication(t, db)
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("abs_exp"), "Absolute Expiry")
	now := time.Now().UTC()
	createdAt := now.Add(-sessionAbsoluteLifetime - time.Hour)
	if _, err := db.Exec(
		context.Background(),
		`UPDATE sessions
		 SET created_at = $1, idle_expires_at = $2
		 WHERE id = $3`,
		createdAt,
		now.Add(time.Hour),
		account.Auth.SessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		context.Background(),
		`UPDATE access_tokens SET expires_at = $1 WHERE session_id = $2`,
		now.Add(time.Hour),
		account.Auth.SessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		context.Background(),
		`UPDATE refresh_tokens SET expires_at = $1 WHERE session_id = $2`,
		now.Add(time.Hour),
		account.Auth.SessionID,
	); err != nil {
		t.Fatal(err)
	}

	access := doRequest(t, http.MethodGet, server.URL+"/auth/sessions", account.Auth.AccessToken, "")
	access.Body.Close()
	if access.StatusCode != http.StatusUnauthorized {
		t.Fatalf("absolute-expired access status = %d, want 401", access.StatusCode)
	}
	refresh := refreshTestAccount(
		t,
		server.URL,
		account.Auth.RefreshToken,
		uniqueOpaqueID("absolute-expired-refresh"),
		http.StatusUnauthorized,
	)
	if refresh.AccessToken != "" || refresh.RefreshToken != "" {
		t.Fatalf("absolute-expired refresh returned tokens: %+v", refresh)
	}
}

func TestRefreshLifetimeIsCappedBySessionAbsoluteExpiry(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("abs_cap"), "Absolute Cap")
	now := time.Now().UTC()
	createdAt := now.Add(-sessionAbsoluteLifetime + 24*time.Hour)
	absoluteExpiresAt := createdAt.Add(sessionAbsoluteLifetime)
	if _, err := db.Exec(
		context.Background(),
		`UPDATE sessions
		 SET created_at = $1, idle_expires_at = $2
		 WHERE id = $3`,
		createdAt,
		now.Add(sessionIdleLifetime),
		account.Auth.SessionID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		context.Background(),
		`UPDATE refresh_tokens SET expires_at = $1 WHERE session_id = $2`,
		now.Add(sessionIdleLifetime),
		account.Auth.SessionID,
	); err != nil {
		t.Fatal(err)
	}

	rotated := refreshTestAccount(
		t,
		server.URL,
		account.Auth.RefreshToken,
		uniqueOpaqueID("absolute-capped-refresh"),
		http.StatusOK,
	)
	if !rotated.SessionExpiresAt.Equal(absoluteExpiresAt) {
		t.Fatalf("session absolute expiry = %s, want %s", rotated.SessionExpiresAt, absoluteExpiresAt)
	}
	if !rotated.RefreshTokenExpiresAt.Equal(absoluteExpiresAt) {
		t.Fatalf("refresh expiry = %s, want absolute cap %s", rotated.RefreshTokenExpiresAt, absoluteExpiresAt)
	}
}

func TestRefreshRecoverySurvivesResponseEncryptionKeyRotation(t *testing.T) {
	db := openTestDatabase(t)
	app := newTestApplication(t, db)
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("key_rot"), "Key Rotation")
	refreshKey := uniqueOpaqueID("before-key-rotation")
	first := refreshTestAccount(t, server.URL, account.Auth.RefreshToken, refreshKey, http.StatusOK)

	versionTwoKey := base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{22}, 32))
	rotatedCipher, err := newResponseCipherKeyring(versionTwoKey, 2, "1:"+testEncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	app.responseCipher = rotatedCipher
	recovered := refreshTestAccount(t, server.URL, account.Auth.RefreshToken, refreshKey, http.StatusOK)
	if recovered.AccessToken != first.AccessToken || recovered.RefreshToken != first.RefreshToken {
		t.Fatal("key rotation changed the recovered refresh result")
	}

	refreshTestAccount(
		t,
		server.URL,
		first.RefreshToken,
		uniqueOpaqueID("after-key-rotation"),
		http.StatusOK,
	)
	var newestVersion int
	if err := db.QueryRow(
		context.Background(),
		`SELECT key_version
		 FROM refresh_idempotency_results
		 ORDER BY refresh_token_id DESC
		 LIMIT 1`,
	).Scan(&newestVersion); err != nil {
		t.Fatal(err)
	}
	if newestVersion != 2 {
		t.Fatalf("new refresh result key version = %d, want 2", newestVersion)
	}
}
