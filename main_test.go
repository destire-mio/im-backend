package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testEncryptionKey = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"

type testAccount struct {
	User     user
	Username string
	Password string
	Auth     authResponse
}

func TestDatabasePoolConfigAppliesExplicitMaximum(t *testing.T) {
	config, err := databasePoolConfig(defaultDatabaseURL, "24")
	if err != nil {
		t.Fatal(err)
	}
	if config.MaxConns != 24 {
		t.Fatalf("MaxConns = %d, want 24", config.MaxConns)
	}
	attachDatabaseAcquireTracer(config, newApplicationMetrics(nil))
	if _, ok := config.ConnConfig.Tracer.(pgxpool.AcquireTracer); !ok {
		t.Fatal("database pool acquire tracer was not attached")
	}
	if _, err := databasePoolConfig(defaultDatabaseURL, "0"); err == nil {
		t.Fatal("zero DATABASE_MAX_CONNECTIONS was accepted")
	}
	if _, err := databasePoolConfig(defaultDatabaseURL, "many"); err == nil {
		t.Fatal("non-numeric DATABASE_MAX_CONNECTIONS was accepted")
	}
}

func openTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create test database pool: %v", err)
	}
	t.Cleanup(db.Close)
	if err := db.Ping(ctx); err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	return db
}

func newTestApplication(t *testing.T, db *pgxpool.Pool) *application {
	t.Helper()
	cipher, err := newResponseCipher(testEncryptionKey, 1)
	if err != nil {
		t.Fatalf("create response cipher: %v", err)
	}
	return &application{db: db, responseCipher: cipher}
}

func TestRegisterCreatesSeparatedHashedTokensAndAllowsDuplicateDisplayNames(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	suffix := time.Now().UnixNano()
	first := registerTestAccount(t, db, server.URL, fmt.Sprintf("alice_%d", suffix), "Same Name")
	second := registerTestAccount(t, db, server.URL, fmt.Sprintf("bob_%d", suffix), "Same Name")
	if first.User.ID == second.User.ID {
		t.Fatal("duplicate display names must have different ids")
	}

	var passwordHash string
	if err := db.QueryRow(context.Background(), "SELECT password_hash FROM users WHERE id = $1", first.User.ID).Scan(&passwordHash); err != nil {
		t.Fatalf("read password hash: %v", err)
	}
	if passwordHash == first.Password || !strings.HasPrefix(passwordHash, "$argon2id$") {
		t.Fatal("password was not stored as an Argon2id hash")
	}

	assertStoredTokenHash(t, db, "access_tokens", first.Auth.SessionID, first.Auth.AccessToken)
	assertStoredTokenHash(t, db, "refresh_tokens", first.Auth.SessionID, first.Auth.RefreshToken)
	if first.Auth.AccessTokenExpiresAt.Sub(time.Now()) > 16*time.Minute {
		t.Fatal("access token lifetime is longer than the 15-minute target")
	}
	if first.Auth.RefreshTokenExpiresAt.Sub(time.Now()) < 89*24*time.Hour {
		t.Fatal("refresh token did not receive the 90-day idle lifetime")
	}
}

func TestRegisterRejectsInvalidAndDuplicateIdentity(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	suffix := time.Now().UnixNano()
	username := fmt.Sprintf("taken_%d", suffix)
	registerTestAccount(t, db, server.URL, username, "First")
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "malformed JSON", body: `{"username":`, wantStatus: http.StatusBadRequest},
		{name: "unknown field", body: `{"username":"valid_name","displayName":"Alice","password":"password123","admin":true}`, wantStatus: http.StatusBadRequest},
		{name: "invalid username", body: `{"username":"A!","displayName":"Alice","password":"password123"}`, wantStatus: http.StatusBadRequest},
		{name: "blank display name", body: `{"username":"valid_name","displayName":"   ","password":"password123"}`, wantStatus: http.StatusBadRequest},
		{name: "short password", body: `{"username":"valid_name","displayName":"Alice","password":"short"}`, wantStatus: http.StatusBadRequest},
		{name: "multiple JSON values", body: `{"username":"valid_name","displayName":"Alice","password":"password123"}{}`, wantStatus: http.StatusBadRequest},
		{name: "duplicate username", body: fmt.Sprintf(`{"username":%q,"displayName":"Second","password":"password123"}`, username), wantStatus: http.StatusConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := doRequest(t, http.MethodPost, server.URL+"/auth/register", "", test.body)
			defer response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
		})
	}
}

func TestLoginIdempotencyAndIndependentDeviceSessions(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("login"), "Login")
	requestID := uniqueOpaqueID("login-request")
	first := loginTestAccount(t, server.URL, account.Username, account.Password, requestID)
	retry := loginTestAccount(t, server.URL, account.Username, account.Password, requestID)
	if first.SessionID != retry.SessionID || first.AccessToken != retry.AccessToken || first.RefreshToken != retry.RefreshToken {
		t.Fatal("same loginRequestId did not recover the original token response")
	}

	secondDevice := loginTestAccount(t, server.URL, account.Username, account.Password, uniqueOpaqueID("login-request"))
	if secondDevice.SessionID == first.SessionID || secondDevice.AccessToken == first.AccessToken {
		t.Fatal("a new login request did not create an independent device session")
	}
	var sessions int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM sessions WHERE user_id = $1`, account.User.ID).Scan(&sessions); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessions != 3 {
		t.Fatalf("sessions = %d, want register + idempotent login + second device = 3", sessions)
	}
}

func TestLoginRejectsWrongPasswordWithoutRevealingWhichCredentialFailed(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("wrong"), "Login")

	for _, credentials := range []loginRequest{
		{Username: account.Username, Password: "wrong password", LoginRequestID: uniqueOpaqueID("wrong-password")},
		{Username: "missing_user", Password: account.Password, LoginRequestID: uniqueOpaqueID("missing-user")},
	} {
		body, err := json.Marshal(credentials)
		if err != nil {
			t.Fatalf("encode login request: %v", err)
		}
		response := doRequest(t, http.MethodPost, server.URL+"/auth/login", "", string(body))
		var failure errorResponse
		if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
			response.Body.Close()
			t.Fatalf("decode login failure: %v", err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized || failure.Error != "invalid username or password" {
			t.Fatalf("login failure = status %d, body %+v", response.StatusCode, failure)
		}
	}
}

func TestConcurrentRefreshUsesOneSuccessorAndSameKeyRecoversResponse(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("refresh"), "Refresh")
	idempotencyKey := uniqueOpaqueID("refresh-key")
	bodyBytes, err := json.Marshal(refreshRequest{RefreshToken: account.Auth.RefreshToken, IdempotencyKey: idempotencyKey})
	if err != nil {
		t.Fatalf("encode refresh request: %v", err)
	}

	start := make(chan struct{})
	type result struct {
		status int
		auth   authResponse
		err    error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			response, err := http.Post(server.URL+"/auth/refresh", "application/json", bytes.NewReader(bodyBytes))
			if err != nil {
				results <- result{err: err}
				return
			}
			defer response.Body.Close()
			var auth authResponse
			err = json.NewDecoder(response.Body).Decode(&auth)
			results <- result{status: response.StatusCode, auth: auth, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var responses []authResponse
	for current := range results {
		if current.err != nil {
			t.Fatalf("concurrent refresh: %v", current.err)
		}
		if current.status != http.StatusOK {
			t.Fatalf("refresh status = %d, want 200", current.status)
		}
		responses = append(responses, current.auth)
	}
	if responses[0].AccessToken != responses[1].AccessToken || responses[0].RefreshToken != responses[1].RefreshToken {
		t.Fatal("same refresh idempotency key did not return the same token pair")
	}

	_, oldHash, err := decodeAndHashToken(account.Auth.RefreshToken)
	if err != nil {
		t.Fatalf("hash old refresh token: %v", err)
	}
	var oldID, childCount int64
	if err := db.QueryRow(context.Background(), `SELECT id FROM refresh_tokens WHERE token_hash = $1`, oldHash).Scan(&oldID); err != nil {
		t.Fatalf("find old refresh token: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM refresh_tokens WHERE parent_token_id = $1`, oldID).Scan(&childCount); err != nil {
		t.Fatalf("count refresh successors: %v", err)
	}
	if childCount != 1 {
		t.Fatalf("refresh successor count = %d, want 1", childCount)
	}
}

func TestRefreshReplayWithDifferentKeyRevokesOnlyThatSession(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("replay"), "Replay")
	otherDevice := loginTestAccount(t, server.URL, account.Username, account.Password, uniqueOpaqueID("other-device"))
	first := refreshTestAccount(t, server.URL, account.Auth.RefreshToken, uniqueOpaqueID("winning-key"), http.StatusOK)
	refreshTestAccount(t, server.URL, account.Auth.RefreshToken, uniqueOpaqueID("attacker-key"), http.StatusUnauthorized)

	revoked := doRequest(t, http.MethodGet, server.URL+"/messages?peerId=1", first.AccessToken, "")
	revoked.Body.Close()
	if revoked.StatusCode != http.StatusUnauthorized {
		t.Fatalf("replayed session access status = %d, want 401", revoked.StatusCode)
	}
	stillActive := doRequest(t, http.MethodGet, server.URL+"/messages?peerId=1", otherDevice.AccessToken, "")
	stillActive.Body.Close()
	if stillActive.StatusCode != http.StatusOK {
		t.Fatalf("other device status = %d, want 200", stillActive.StatusCode)
	}
}

func TestLogoutRevokesOnlyCurrentSessionAndPasswordChangeRevokesAll(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("logout"), "Logout")
	other := loginTestAccount(t, server.URL, account.Username, account.Password, uniqueOpaqueID("other-login"))

	logout := doRequest(t, http.MethodPost, server.URL+"/auth/logout", account.Auth.AccessToken, "")
	logout.Body.Close()
	if logout.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status = %d, want 204", logout.StatusCode)
	}
	assertTokenStatus(t, server.URL, account.Auth.AccessToken, http.StatusUnauthorized)
	assertTokenStatus(t, server.URL, other.AccessToken, http.StatusOK)

	newPassword := "a new correct horse battery staple"
	changeBody, _ := json.Marshal(changePasswordRequest{CurrentPassword: account.Password, NewPassword: newPassword})
	changed := doRequest(t, http.MethodPost, server.URL+"/auth/password", other.AccessToken, string(changeBody))
	changed.Body.Close()
	if changed.StatusCode != http.StatusNoContent {
		t.Fatalf("change password status = %d, want 204", changed.StatusCode)
	}
	assertTokenStatus(t, server.URL, other.AccessToken, http.StatusUnauthorized)
	loginTestAccount(t, server.URL, account.Username, newPassword, uniqueOpaqueID("new-password-login"))
}

func TestSessionListAndTargetedRevocationRespectOwnership(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("sessions"), "Sessions")
	other := loginTestAccount(t, server.URL, account.Username, account.Password, uniqueOpaqueID("listed-device"))
	stranger := registerTestAccount(t, db, server.URL, uniqueUsername("stranger"), "Stranger")

	listed := doRequest(t, http.MethodGet, server.URL+"/auth/sessions", account.Auth.AccessToken, "")
	defer listed.Body.Close()
	if listed.StatusCode != http.StatusOK {
		t.Fatalf("list sessions status = %d, want 200", listed.StatusCode)
	}
	var sessions []sessionResponse
	if err := json.NewDecoder(listed.Body).Decode(&sessions); err != nil {
		t.Fatalf("decode sessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("session count = %d, want 2", len(sessions))
	}

	foreign := doRequest(t, http.MethodDelete, fmt.Sprintf("%s/auth/sessions/%d", server.URL, stranger.Auth.SessionID), account.Auth.AccessToken, "")
	foreign.Body.Close()
	if foreign.StatusCode != http.StatusNotFound {
		t.Fatalf("foreign session revocation status = %d, want 404", foreign.StatusCode)
	}
	assertTokenStatus(t, server.URL, stranger.Auth.AccessToken, http.StatusOK)

	revoked := doRequest(t, http.MethodDelete, fmt.Sprintf("%s/auth/sessions/%d", server.URL, other.SessionID), account.Auth.AccessToken, "")
	revoked.Body.Close()
	if revoked.StatusCode != http.StatusNoContent {
		t.Fatalf("targeted revocation status = %d, want 204", revoked.StatusCode)
	}
	assertTokenStatus(t, server.URL, other.AccessToken, http.StatusUnauthorized)
	assertTokenStatus(t, server.URL, account.Auth.AccessToken, http.StatusOK)
}

func TestAuthenticatedMessageFlowPreventsSenderForgeryAndUnauthorizedReads(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	suffix := time.Now().UnixNano()
	alice := registerTestAccount(t, db, server.URL, fmt.Sprintf("alice_%d", suffix), "Alice")
	bob := registerTestAccount(t, db, server.URL, fmt.Sprintf("bob_%d", suffix), "Bob")
	eve := registerTestAccount(t, db, server.URL, fmt.Sprintf("eve_%d", suffix), "Eve")

	created := createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "hello")
	if created.SenderID != alice.User.ID {
		t.Fatalf("sender id = %d, want authenticated user %d", created.SenderID, alice.User.ID)
	}
	forgedBody := fmt.Sprintf(`{"senderId":%d,"receiverId":%d,"content":"forged"}`, eve.User.ID, bob.User.ID)
	forgedResponse := doRequest(t, http.MethodPost, server.URL+"/messages", alice.Auth.AccessToken, forgedBody)
	forgedResponse.Body.Close()
	if forgedResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("forged sender status = %d, want 400", forgedResponse.StatusCode)
	}

	bobMessages := listMessagesThroughAPI(t, server.URL, bob.Auth.AccessToken, alice.User.ID)
	if len(bobMessages) != 1 || bobMessages[0].ID != created.ID {
		t.Fatalf("Bob messages = %+v, want created message", bobMessages)
	}
	if messages := listMessagesThroughAPI(t, server.URL, eve.Auth.AccessToken, alice.User.ID); len(messages) != 0 {
		t.Fatalf("Eve read messages outside her conversation: %+v", messages)
	}
}

func TestCreateMessagePreservesWhitespaceAndRejectsEmptyContent(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("sender"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("receiver"), "Receiver")
	for _, content := range []string{" ", "   ", "  hello  ", "line one\n  line two"} {
		created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, content)
		if created.Content != content {
			t.Fatalf("content = %q, want %q", created.Content, content)
		}
	}
	for _, body := range []string{
		fmt.Sprintf(`{"receiverId":%d}`, receiver.User.ID),
		fmt.Sprintf(`{"receiverId":%d,"content":null}`, receiver.User.ID),
		fmt.Sprintf(`{"receiverId":%d,"content":""}`, receiver.User.ID),
	} {
		response := doRequest(t, http.MethodPost, server.URL+"/messages", sender.Auth.AccessToken, body)
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d, want 400", body, response.StatusCode)
		}
	}
}

func TestCreateMessageIdempotencyCreatesOneMessageAndOneOutboxEvent(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("isend"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("irecv"), "Receiver")
	clientMessageID := uniqueOpaqueID("client-message")
	content := "idempotent hello"
	body, _ := json.Marshal(createMessageRequest{
		ClientMessageID: clientMessageID,
		ReceiverID:      receiver.User.ID,
		Content:         &content,
	})

	firstResponse := doRequest(t, http.MethodPost, server.URL+"/messages", sender.Auth.AccessToken, string(body))
	var first message
	if err := json.NewDecoder(firstResponse.Body).Decode(&first); err != nil {
		firstResponse.Body.Close()
		t.Fatalf("decode first message: %v", err)
	}
	firstResponse.Body.Close()
	if firstResponse.StatusCode != http.StatusCreated {
		t.Fatalf("first status = %d, want 201", firstResponse.StatusCode)
	}

	retryResponse := doRequest(t, http.MethodPost, server.URL+"/messages", sender.Auth.AccessToken, string(body))
	var retry message
	if err := json.NewDecoder(retryResponse.Body).Decode(&retry); err != nil {
		retryResponse.Body.Close()
		t.Fatalf("decode retried message: %v", err)
	}
	retryResponse.Body.Close()
	if retryResponse.StatusCode != http.StatusOK || retry.ID != first.ID {
		t.Fatalf("retry = status %d message %d, want status 200 message %d", retryResponse.StatusCode, retry.ID, first.ID)
	}

	var messages, events int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM messages WHERE sender_id = $1 AND client_message_id = $2`, sender.User.ID, clientMessageID).Scan(&messages); err != nil {
		t.Fatalf("count idempotent messages: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE message_id = $1 AND event_type = 'message.created'`, first.ID).Scan(&events); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if messages != 1 || events != 1 {
		t.Fatalf("messages = %d, events = %d, want one each", messages, events)
	}

	differentContent := "different payload"
	conflictingBody, _ := json.Marshal(createMessageRequest{
		ClientMessageID: clientMessageID,
		ReceiverID:      receiver.User.ID,
		Content:         &differentContent,
	})
	conflict := doRequest(t, http.MethodPost, server.URL+"/messages", sender.Auth.AccessToken, string(conflictingBody))
	conflict.Body.Close()
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("conflicting reuse status = %d, want 409", conflict.StatusCode)
	}
}

func TestConcurrentDuplicateMessageRequestsConvergeToOneResult(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("csend"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("crecv"), "Receiver")
	content := "concurrent idempotency"
	clientMessageID := uniqueOpaqueID("concurrent-message")
	body, _ := json.Marshal(createMessageRequest{
		ClientMessageID: clientMessageID,
		ReceiverID:      receiver.User.ID,
		Content:         &content,
	})

	type result struct {
		status  int
		message message
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			request, err := http.NewRequest(http.MethodPost, server.URL+"/messages", strings.NewReader(string(body)))
			if err != nil {
				results <- result{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+sender.Auth.AccessToken)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				results <- result{err: err}
				return
			}
			defer response.Body.Close()
			var created message
			err = json.NewDecoder(response.Body).Decode(&created)
			results <- result{status: response.StatusCode, message: created, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var messageID int64
	statuses := map[int]int{}
	for current := range results {
		if current.err != nil {
			t.Fatalf("concurrent request: %v", current.err)
		}
		statuses[current.status]++
		if messageID == 0 {
			messageID = current.message.ID
		} else if current.message.ID != messageID {
			t.Fatalf("concurrent requests returned message IDs %d and %d", messageID, current.message.ID)
		}
	}
	if statuses[http.StatusCreated] != 1 || statuses[http.StatusOK] != 1 {
		t.Fatalf("statuses = %+v, want one 201 and one 200", statuses)
	}

	var messages, events int
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM messages WHERE sender_id = $1 AND client_message_id = $2`, sender.User.ID, clientMessageID).Scan(&messages); err != nil {
		t.Fatalf("count concurrent messages: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE message_id = $1`, messageID).Scan(&events); err != nil {
		t.Fatalf("count concurrent outbox events: %v", err)
	}
	if messages != 1 || events != 1 {
		t.Fatalf("messages = %d, events = %d, want one each", messages, events)
	}
}

func TestExpiredAccessTokenIsRejectedWithoutInvalidatingSession(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("expired"), "Expired")
	_, tokenHash, err := decodeAndHashToken(account.Auth.AccessToken)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE access_tokens SET created_at = CURRENT_TIMESTAMP - INTERVAL '2 hours', expires_at = CURRENT_TIMESTAMP - INTERVAL '1 hour' WHERE token_hash = $1`, tokenHash); err != nil {
		t.Fatalf("expire access token: %v", err)
	}
	assertTokenStatus(t, server.URL, account.Auth.AccessToken, http.StatusUnauthorized)
	refreshed := refreshTestAccount(t, server.URL, account.Auth.RefreshToken, uniqueOpaqueID("expired-refresh"), http.StatusOK)
	assertTokenStatus(t, server.URL, refreshed.AccessToken, http.StatusOK)
}

func TestDatabaseConstraintsProtectDirectWrites(t *testing.T) {
	db := openTestDatabase(t)
	_, err := db.Exec(context.Background(), `INSERT INTO users (username, display_name, password_hash) VALUES ('Bad Username', 'Display', 'hash')`)
	assertPostgresCode(t, err, "23514")
	_, err = db.Exec(context.Background(), `INSERT INTO access_tokens (session_id, token_hash, expires_at) VALUES (1, '\x01', CURRENT_TIMESTAMP + INTERVAL '1 hour')`)
	assertPostgresCode(t, err, "23514")
}

func TestMessageCreationDefersSyncProjectionToOutbox(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("async_s"), "Async Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("async_r"), "Async Receiver")

	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "project later")
	var syncEvents, recipients int
	var payloadVersion int
	var ready bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*),
		        (SELECT payload_version FROM outbox_events WHERE message_id = $1),
		        (SELECT ready_at IS NOT NULL FROM outbox_events WHERE message_id = $1),
		        (SELECT count(*)
		         FROM outbox_recipients AS recipient
		         JOIN outbox_events AS event USING (event_id)
		         WHERE event.message_id = $1)
		 FROM user_message_events
		 WHERE message_id = $1`,
		created.ID,
	).Scan(&syncEvents, &payloadVersion, &ready, &recipients); err != nil {
		t.Fatalf("read pending projection: %v", err)
	}
	if syncEvents != 0 || payloadVersion != 3 || ready || recipients != 0 {
		t.Fatalf(
			"before projection: sync events=%d payload version=%d ready=%v recipients=%d",
			syncEvents,
			payloadVersion,
			ready,
			recipients,
		)
	}

	projectPendingMessageEvents(t, db)
	var published bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*),
		        (SELECT payload_version FROM outbox_events WHERE message_id = $1),
		        (SELECT ready_at IS NOT NULL FROM outbox_events WHERE message_id = $1),
		        (SELECT published_at IS NOT NULL FROM outbox_events WHERE message_id = $1),
		        (SELECT count(*)
		         FROM outbox_recipients AS recipient
		         JOIN outbox_events AS event USING (event_id)
		         WHERE event.message_id = $1)
		 FROM user_message_events
		 WHERE message_id = $1`,
		created.ID,
	).Scan(&syncEvents, &payloadVersion, &ready, &published, &recipients); err != nil {
		t.Fatalf("read completed projection: %v", err)
	}
	if syncEvents != 2 || payloadVersion != 3 || !ready || !published || recipients != 2 {
		t.Fatalf(
			"after projection: sync events=%d payload version=%d ready=%v published=%v recipients=%d",
			syncEvents,
			payloadVersion,
			ready,
			published,
			recipients,
		)
	}
}

func TestStructuredRecipientsRecoverAfterProjectionCommitBeforePublish(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("recover_s"), "Recover Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("recover_r"), "Recover Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "recover projection")

	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	first := mustMessageTestWorker(t, db, &testPublisher{}, config)
	claimed, err := first.claim(context.Background())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim event = %+v, err %v", claimed, err)
	}
	prepared, err := first.preparer.PrepareBatch(context.Background(), claimed)
	if err != nil || len(prepared) != 1 {
		t.Fatalf("prepare event = %+v, err %v", prepared, err)
	}
	firstPayload, err := decodeMessageCreatedEvent(prepared[0])
	if err != nil {
		t.Fatalf("decode first prepared event: %v", err)
	}

	var storedPayloadVersion, syncEvents, recipients int
	var ready, published bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT event.payload_version,
		        event.ready_at IS NOT NULL,
		        event.published_at IS NOT NULL,
		        (SELECT count(*) FROM user_message_events WHERE message_id = event.message_id),
		        (SELECT count(*) FROM outbox_recipients WHERE event_id = event.event_id)
		 FROM outbox_events AS event
		 WHERE event.message_id = $1`,
		created.ID,
	).Scan(&storedPayloadVersion, &ready, &published, &syncEvents, &recipients); err != nil {
		t.Fatalf("read committed projection: %v", err)
	}
	if storedPayloadVersion != 3 || !ready || published || syncEvents != 2 || recipients != 2 {
		t.Fatalf(
			"committed projection = payload %d ready %v published %v sync %d recipients %d",
			storedPayloadVersion,
			ready,
			published,
			syncEvents,
			recipients,
		)
	}

	if _, err := db.Exec(
		context.Background(),
		`UPDATE outbox_events
		 SET locked_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
		 WHERE message_id = $1`,
		created.ID,
	); err != nil {
		t.Fatalf("expire projected event lease: %v", err)
	}
	publisher := &testPublisher{}
	restarted := mustMessageTestWorker(t, db, publisher, config)
	processed, err := restarted.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("restart worker processed %d, err %v", processed, err)
	}
	received := publisher.received()
	if len(received) != 1 {
		t.Fatalf("restart published %d events, want 1", len(received))
	}
	retriedPayload, err := decodeMessageCreatedEvent(received[0])
	if err != nil {
		t.Fatalf("decode retried event: %v", err)
	}
	if fmt.Sprint(retriedPayload.Recipients) != fmt.Sprint(firstPayload.Recipients) {
		t.Fatalf("retried recipients = %+v, want %+v", retriedPayload.Recipients, firstPayload.Recipients)
	}
	if err := db.QueryRow(
		context.Background(),
		`SELECT published_at IS NOT NULL,
		        (SELECT count(*) FROM user_message_events WHERE message_id = outbox_events.message_id),
		        (SELECT count(*) FROM outbox_recipients WHERE event_id = outbox_events.event_id)
		 FROM outbox_events
		 WHERE message_id = $1`,
		created.ID,
	).Scan(&published, &syncEvents, &recipients); err != nil {
		t.Fatalf("read restarted projection: %v", err)
	}
	if !published || syncEvents != 2 || recipients != 2 {
		t.Fatalf("restart state = published %v sync %d recipients %d", published, syncEvents, recipients)
	}
}

func TestJSONBProjectionStorageRemainsAvailableForRollback(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("jsonb_s"), "JSONB Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("jsonb_r"), "JSONB Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "jsonb rollback")

	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	config.ProjectionStorage = syncProjectionStorageJSONB
	worker := mustMessageTestWorker(t, db, &testPublisher{}, config)
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("JSONB worker processed %d, err %v", processed, err)
	}

	var payloadVersion, syncEvents, recipients int
	var ready, published bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT event.payload_version,
		        event.ready_at IS NOT NULL,
		        event.published_at IS NOT NULL,
		        (SELECT count(*) FROM user_message_events WHERE message_id = event.message_id),
		        (SELECT count(*) FROM outbox_recipients WHERE event_id = event.event_id)
		 FROM outbox_events AS event
		 WHERE event.message_id = $1`,
		created.ID,
	).Scan(&payloadVersion, &ready, &published, &syncEvents, &recipients); err != nil {
		t.Fatalf("read JSONB rollback state: %v", err)
	}
	if payloadVersion != 2 || !ready || !published || syncEvents != 2 || recipients != 0 {
		t.Fatalf(
			"JSONB rollback state = payload %d ready %v published %v sync %d recipients %d",
			payloadVersion,
			ready,
			published,
			syncEvents,
			recipients,
		)
	}
}

func TestStructuredProjectionStoresOneRecipientForSelfMessage(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("self"), "Self Sender")
	created := createMessageThroughAPI(t, server.URL, account.Auth.AccessToken, account.User.ID, "note to self")
	projectPendingMessageEvents(t, db)

	var syncEvents, recipients int
	if err := db.QueryRow(
		context.Background(),
		`SELECT (SELECT count(*) FROM user_message_events WHERE message_id = $1),
		        (SELECT count(*)
		         FROM outbox_recipients AS recipient
		         JOIN outbox_events AS event USING (event_id)
		         WHERE event.message_id = $1)`,
		created.ID,
	).Scan(&syncEvents, &recipients); err != nil {
		t.Fatalf("read self-message projection: %v", err)
	}
	if syncEvents != 1 || recipients != 1 {
		t.Fatalf("self-message projection = sync %d recipients %d, want 1 and 1", syncEvents, recipients)
	}
}

func TestStructuredProjectionRollsBackWhenLeaseIsLost(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("lease_s"), "Lease Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("lease_r"), "Lease Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "lose lease")

	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	worker := mustMessageTestWorker(t, db, &testPublisher{}, config)
	claimed, err := worker.claim(context.Background())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim event = %+v, err %v", claimed, err)
	}
	if _, err := db.Exec(
		context.Background(),
		`UPDATE outbox_events
		 SET lock_token = gen_random_uuid()
		 WHERE message_id = $1`,
		created.ID,
	); err != nil {
		t.Fatalf("replace event lease: %v", err)
	}
	if _, err := worker.preparer.PrepareBatch(context.Background(), claimed); !errors.Is(err, errOutboxLeaseLost) {
		t.Fatalf("prepare after lease loss = %v, want lease lost", err)
	}

	var syncEvents, recipients, counters int
	var ready bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT (SELECT count(*) FROM user_message_events WHERE message_id = $1),
		        (SELECT count(*)
		         FROM outbox_recipients AS recipient
		         JOIN outbox_events AS event USING (event_id)
		         WHERE event.message_id = $1),
		        (SELECT count(*) FROM user_sync_counters WHERE user_id = ANY($2::bigint[])),
		        ready_at IS NOT NULL
		 FROM outbox_events
		 WHERE message_id = $1`,
		created.ID,
		[]int64{sender.User.ID, receiver.User.ID},
	).Scan(&syncEvents, &recipients, &counters, &ready); err != nil {
		t.Fatalf("read rolled-back projection: %v", err)
	}
	if syncEvents != 0 || recipients != 0 || counters != 0 || ready {
		t.Fatalf("lease-lost projection persisted sync=%d recipients=%d counters=%d ready=%v", syncEvents, recipients, counters, ready)
	}
}

func TestMessageSyncPaginatesWithPerUserCursor(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("sync_s"), "Sync Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("sync_r"), "Sync Receiver")

	created := []message{
		createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "sync one"),
		createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "sync two"),
		createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "sync three"),
	}
	projectPendingMessageEvents(t, db)

	first := syncMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 0, 2)
	if len(first.Events) != 2 || !first.HasMore || first.NextCursor != 2 || first.SnapshotCursor != 3 {
		t.Fatalf("first sync page = %+v", first)
	}
	for index, event := range first.Events {
		if event.Cursor != int64(index+1) || event.Message.ID != created[index].ID {
			t.Fatalf("first sync event %d = %+v", index, event)
		}
	}

	second := syncMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, first.NextCursor, 2, first.SnapshotCursor)
	if len(second.Events) != 1 || second.HasMore || second.NextCursor != 3 || second.SnapshotCursor != 3 {
		t.Fatalf("second sync page = %+v", second)
	}
	if second.Events[0].Cursor != 3 || second.Events[0].Message.ID != created[2].ID {
		t.Fatalf("second sync event = %+v", second.Events[0])
	}

	senderPage := syncMessagesThroughAPI(t, server.URL, sender.Auth.AccessToken, 0, 10)
	if len(senderPage.Events) != 3 || senderPage.NextCursor != 3 {
		t.Fatalf("sender device sync page = %+v", senderPage)
	}
}

func TestMessageSyncKeepsSnapshotStableWhileNewMessagesArrive(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("snapshot_s"), "Snapshot Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("snapshot_r"), "Snapshot Receiver")

	for _, content := range []string{"snapshot one", "snapshot two", "snapshot three"} {
		createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, content)
	}
	projectPendingMessageEvents(t, db)
	first := syncMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 0, 1)
	if first.SnapshotCursor != 3 || first.NextCursor != 1 || !first.HasMore {
		t.Fatalf("first snapshot page = %+v", first)
	}

	for _, content := range []string{"live four", "live five"} {
		createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, content)
	}
	projectPendingMessageEvents(t, db)
	second := syncMessagesThroughAPI(
		t,
		server.URL,
		receiver.Auth.AccessToken,
		first.NextCursor,
		10,
		first.SnapshotCursor,
	)
	if second.SnapshotCursor != 3 || second.NextCursor != 3 || second.HasMore || len(second.Events) != 2 {
		t.Fatalf("stable snapshot second page = %+v", second)
	}
	for index, event := range second.Events {
		if event.Cursor != int64(index+2) {
			t.Fatalf("stable snapshot event %d cursor = %d, want %d", index, event.Cursor, index+2)
		}
	}

	nextCycle := syncMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, second.NextCursor, 10)
	if nextCycle.SnapshotCursor != 5 || nextCycle.NextCursor != 5 || nextCycle.HasMore || len(nextCycle.Events) != 2 {
		t.Fatalf("next sync cycle = %+v", nextCycle)
	}

	ahead := doRequest(
		t,
		http.MethodGet,
		fmt.Sprintf("%s/messages/sync?after=3&limit=10&snapshotCursor=6", server.URL),
		receiver.Auth.AccessToken,
		"",
	)
	ahead.Body.Close()
	if ahead.StatusCode != http.StatusConflict {
		t.Fatalf("future snapshot status = %d, want 409", ahead.StatusCode)
	}
}

func TestMessageAcknowledgementIsPerDeviceMonotonicAndBounded(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("ack_s"), "ACK Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("ack_r"), "ACK Receiver")
	desktopDeviceID := uniqueOpaqueID("desktop-device")
	desktop := loginTestAccountForDevice(
		t,
		server.URL,
		receiver.Username,
		receiver.Password,
		uniqueOpaqueID("desktop-login"),
		desktopDeviceID,
	)

	for _, content := range []string{"ack one", "ack two", "ack three"} {
		createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, content)
	}
	projectPendingMessageEvents(t, db)

	phoneState := acknowledgeMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 3, http.StatusOK)
	if phoneState.AppliedCursor != 3 {
		t.Fatalf("phone applied cursor = %d, want 3", phoneState.AppliedCursor)
	}
	phoneState = acknowledgeMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 1, http.StatusOK)
	if phoneState.AppliedCursor != 3 {
		t.Fatalf("late phone ACK moved cursor to %d, want 3", phoneState.AppliedCursor)
	}

	desktopState := acknowledgeMessagesThroughAPI(t, server.URL, desktop.AccessToken, 1, http.StatusOK)
	if desktopState.AppliedCursor != 1 {
		t.Fatalf("desktop applied cursor = %d, want 1", desktopState.AppliedCursor)
	}
	if desktop.DeviceID != desktopDeviceID {
		t.Fatalf("desktop session device ID = %q, want %q", desktop.DeviceID, desktopDeviceID)
	}

	acknowledgeMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 4, http.StatusConflict)
	invalid := doRequest(t, http.MethodPost, server.URL+"/messages/ack", receiver.Auth.AccessToken, `{"cursor":3,"deviceId":"spoofed-device"}`)
	invalid.Body.Close()
	if invalid.StatusCode != http.StatusBadRequest {
		t.Fatalf("spoofed ACK identity status = %d, want 400", invalid.StatusCode)
	}
	unauthenticated := doRequest(t, http.MethodPost, server.URL+"/messages/ack", "", `{"cursor":1}`)
	unauthenticated.Body.Close()
	if unauthenticated.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated ACK status = %d, want 401", unauthenticated.StatusCode)
	}

	rows, err := db.Query(
		context.Background(),
		`SELECT device_id, applied_seq
		 FROM device_sync_states
		 WHERE user_id = $1
		 ORDER BY applied_seq DESC`,
		receiver.User.ID,
	)
	if err != nil {
		t.Fatalf("read device sync states: %v", err)
	}
	defer rows.Close()
	want := map[string]int64{
		receiver.Auth.DeviceID: 3,
		desktopDeviceID:        1,
	}
	for rows.Next() {
		var deviceID string
		var cursor int64
		if err := rows.Scan(&deviceID, &cursor); err != nil {
			t.Fatalf("scan device sync state: %v", err)
		}
		if expected, found := want[deviceID]; !found || expected != cursor {
			t.Fatalf("unexpected device sync state %q = %d", deviceID, cursor)
		}
		delete(want, deviceID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate device sync states: %v", err)
	}
	if len(want) != 0 {
		t.Fatalf("missing device sync states: %v", want)
	}
}

func TestOppositeDirectionMessagesAllocateSyncCursorsWithoutDeadlock(t *testing.T) {
	for _, mode := range []syncProjectionMode{syncProjectionModePerUser, syncProjectionModeBulk} {
		t.Run(string(mode), func(t *testing.T) {
			testOppositeDirectionMessagesAllocateSyncCursorsWithoutDeadlock(t, mode)
		})
	}
}

func testOppositeDirectionMessagesAllocateSyncCursorsWithoutDeadlock(t *testing.T, mode syncProjectionMode) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	first := registerTestAccount(t, db, server.URL, uniqueUsername("cursor_a"), "Cursor A")
	second := registerTestAccount(t, db, server.URL, uniqueUsername("cursor_b"), "Cursor B")

	type requestResult struct {
		status int
		err    error
	}
	const messagesPerDirection = 10
	start := make(chan struct{})
	results := make(chan requestResult, messagesPerDirection*2)
	var wait sync.WaitGroup
	for index := range messagesPerDirection * 2 {
		sender := first
		receiverID := second.User.ID
		if index%2 == 1 {
			sender = second
			receiverID = first.User.ID
		}
		wait.Add(1)
		go func(current int, account testAccount, peerID int64) {
			defer wait.Done()
			<-start
			content := fmt.Sprintf("concurrent-%d", current)
			body, err := json.Marshal(createMessageRequest{
				ClientMessageID: uniqueOpaqueID(fmt.Sprintf("cursor-%d", current)),
				ReceiverID:      peerID,
				Content:         &content,
			})
			if err != nil {
				results <- requestResult{err: err}
				return
			}
			request, err := http.NewRequest(http.MethodPost, server.URL+"/messages", bytes.NewReader(body))
			if err != nil {
				results <- requestResult{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+account.Auth.AccessToken)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				results <- requestResult{err: err}
				return
			}
			response.Body.Close()
			results <- requestResult{status: response.StatusCode}
		}(index, sender, receiverID)
	}
	close(start)
	wait.Wait()
	close(results)
	for result := range results {
		if result.err != nil || result.status != http.StatusCreated {
			t.Fatalf("concurrent message result = status %d, err %v", result.status, result.err)
		}
	}
	config := defaultOutboxWorkerConfig()
	config.BatchSize = messagesPerDirection
	config.ProjectionMode = mode
	workers := []*outboxWorker{
		mustMessageTestWorker(t, db, &webSocketOutboxPublisher{router: discardRealtimeRouter{}}, config),
		mustMessageTestWorker(t, db, &webSocketOutboxPublisher{router: discardRealtimeRouter{}}, config),
	}
	projectStart := make(chan struct{})
	projectResults := make(chan error, len(workers))
	for _, worker := range workers {
		go func(current *outboxWorker) {
			<-projectStart
			processed, err := current.RunOnce(context.Background())
			if err == nil && processed != messagesPerDirection {
				err = fmt.Errorf("processed %d events, want %d", processed, messagesPerDirection)
			}
			projectResults <- err
		}(worker)
	}
	close(projectStart)
	for range workers {
		if err := <-projectResults; err != nil {
			t.Fatalf("run concurrent sync projector: %v", err)
		}
	}

	for _, userID := range []int64{first.User.ID, second.User.ID} {
		var count, minimum, maximum int64
		if err := db.QueryRow(
			context.Background(),
			`SELECT count(*), min(seq), max(seq)
			 FROM user_message_events
			 WHERE user_id = $1`,
			userID,
		).Scan(&count, &minimum, &maximum); err != nil {
			t.Fatalf("read sync cursor range for user %d: %v", userID, err)
		}
		if count != messagesPerDirection*2 || minimum != 1 || maximum != messagesPerDirection*2 {
			t.Fatalf("user %d sync cursor range = count %d, min %d, max %d", userID, count, minimum, maximum)
		}
	}
}

type discardRealtimeRouter struct{}

func (discardRealtimeRouter) Publish(context.Context, int64, []byte) (int, error) {
	return 0, nil
}

func projectPendingMessageEvents(t *testing.T, db *pgxpool.Pool, metricObservers ...*applicationMetrics) {
	t.Helper()
	config := defaultOutboxWorkerConfig()
	config.BatchSize = 128
	worker, err := newMessageOutboxWorker(
		db,
		&webSocketOutboxPublisher{router: discardRealtimeRouter{}},
		config,
		metricObservers...,
	)
	if err != nil {
		t.Fatalf("create message worker: %v", err)
	}
	for {
		processed, err := worker.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("project pending message events: %v", err)
		}
		if processed < config.BatchSize {
			return
		}
	}
}

func registerTestAccount(t *testing.T, db *pgxpool.Pool, serverURL, username, displayName string) testAccount {
	t.Helper()
	password := "correct horse battery staple"
	body, err := json.Marshal(registerRequest{Username: username, DisplayName: displayName, Password: password, DeviceID: "test-device"})
	if err != nil {
		t.Fatalf("encode register request: %v", err)
	}
	response := doRequest(t, http.MethodPost, serverURL+"/auth/register", "", string(body))
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d, want 201", response.StatusCode)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("register response missing Cache-Control: no-store")
	}
	var created authResponse
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode register response: %v", err)
	}
	if created.User.ID <= 0 || created.SessionID <= 0 || created.AccessToken == "" || created.RefreshToken == "" {
		t.Fatalf("register response = %+v", created)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(context.Background(), "DELETE FROM messages WHERE sender_id = $1 OR receiver_id = $1", created.User.ID); err != nil {
			t.Errorf("clean messages for user %d: %v", created.User.ID, err)
		}
		if _, err := db.Exec(context.Background(), "DELETE FROM users WHERE id = $1", created.User.ID); err != nil {
			t.Errorf("clean user %d: %v", created.User.ID, err)
		}
	})
	return testAccount{User: created.User, Username: username, Password: password, Auth: created}
}

func loginTestAccount(t *testing.T, serverURL, username, password, requestID string) authResponse {
	t.Helper()
	return loginTestAccountForDevice(t, serverURL, username, password, requestID, "test-device")
}

func loginTestAccountForDevice(t *testing.T, serverURL, username, password, requestID, deviceID string) authResponse {
	t.Helper()
	body, err := json.Marshal(loginRequest{Username: username, Password: password, LoginRequestID: requestID, DeviceID: deviceID})
	if err != nil {
		t.Fatalf("encode login request: %v", err)
	}
	response := doRequest(t, http.MethodPost, serverURL+"/auth/login", "", string(body))
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, want 200", response.StatusCode)
	}
	var authenticated authResponse
	if err := json.NewDecoder(response.Body).Decode(&authenticated); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	return authenticated
}

func acknowledgeMessagesThroughAPI(t *testing.T, serverURL, token string, cursor int64, wantStatus int) deviceSyncState {
	t.Helper()
	body, err := json.Marshal(acknowledgeMessagesRequest{Cursor: &cursor})
	if err != nil {
		t.Fatalf("encode message ACK: %v", err)
	}
	response := doRequest(t, http.MethodPost, serverURL+"/messages/ack", token, string(body))
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		t.Fatalf("message ACK status = %d, want %d", response.StatusCode, wantStatus)
	}
	if wantStatus != http.StatusOK {
		return deviceSyncState{}
	}
	var state deviceSyncState
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatalf("decode message ACK: %v", err)
	}
	return state
}

func refreshTestAccount(t *testing.T, serverURL, refreshToken, key string, wantStatus int) authResponse {
	t.Helper()
	body, err := json.Marshal(refreshRequest{RefreshToken: refreshToken, IdempotencyKey: key})
	if err != nil {
		t.Fatalf("encode refresh request: %v", err)
	}
	response := doRequest(t, http.MethodPost, serverURL+"/auth/refresh", "", string(body))
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		t.Fatalf("refresh status = %d, want %d", response.StatusCode, wantStatus)
	}
	if wantStatus != http.StatusOK {
		return authResponse{}
	}
	var authenticated authResponse
	if err := json.NewDecoder(response.Body).Decode(&authenticated); err != nil {
		t.Fatalf("decode refresh response: %v", err)
	}
	return authenticated
}

func createMessageThroughAPI(t *testing.T, serverURL, token string, receiverID int64, content string) message {
	t.Helper()
	requestContent := content
	body, _ := json.Marshal(createMessageRequest{
		ClientMessageID: uniqueOpaqueID("client-message"),
		ReceiverID:      receiverID,
		Content:         &requestContent,
	})
	response := doRequest(t, http.MethodPost, serverURL+"/messages", token, string(body))
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create message status = %d, want 201", response.StatusCode)
	}
	var created message
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode message response: %v", err)
	}
	return created
}

func listMessagesThroughAPI(t *testing.T, serverURL, token string, peerID int64) []message {
	t.Helper()
	response := doRequest(t, http.MethodGet, fmt.Sprintf("%s/messages?peerId=%d", serverURL, peerID), token, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list messages status = %d, want 200", response.StatusCode)
	}
	var messages []message
	if err := json.NewDecoder(response.Body).Decode(&messages); err != nil {
		t.Fatalf("decode messages: %v", err)
	}
	return messages
}

func syncMessagesThroughAPI(t *testing.T, serverURL, token string, after int64, limit int, snapshotCursor ...int64) messageSyncResponse {
	t.Helper()
	path := fmt.Sprintf("%s/messages/sync?after=%d&limit=%d", serverURL, after, limit)
	if len(snapshotCursor) > 1 {
		t.Fatal("sync helper accepts at most one snapshot cursor")
	}
	if len(snapshotCursor) == 1 {
		path += fmt.Sprintf("&snapshotCursor=%d", snapshotCursor[0])
	}
	response := doRequest(
		t,
		http.MethodGet,
		path,
		token,
		"",
	)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("sync messages status = %d, want 200", response.StatusCode)
	}
	var page messageSyncResponse
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode message sync page: %v", err)
	}
	return page
}

func doRequest(t *testing.T, method, url, token, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	return response
}

func assertStoredTokenHash(t *testing.T, db *pgxpool.Pool, table string, sessionID int64, raw string) {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	want := sha256.Sum256(decoded)
	var stored []byte
	query := fmt.Sprintf("SELECT token_hash FROM %s WHERE session_id = $1 ORDER BY id LIMIT 1", table)
	if err := db.QueryRow(context.Background(), query, sessionID).Scan(&stored); err != nil {
		t.Fatalf("read %s token hash: %v", table, err)
	}
	if !bytes.Equal(stored, want[:]) || bytes.Equal(stored, decoded) {
		t.Fatalf("%s did not store only the SHA-256 token hash", table)
	}
}

func assertTokenStatus(t *testing.T, serverURL, token string, want int) {
	t.Helper()
	response := doRequest(t, http.MethodGet, serverURL+"/messages?peerId=1", token, "")
	response.Body.Close()
	if response.StatusCode != want {
		t.Fatalf("token status = %d, want %d", response.StatusCode, want)
	}
}

func uniqueUsername(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

func uniqueOpaqueID(prefix string) string {
	return fmt.Sprintf("%s-%d-abcdefgh", prefix, time.Now().UnixNano())
}

func assertPostgresCode(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatal("direct SQL unexpectedly succeeded")
	}
	var databaseError *pgconn.PgError
	if !errors.As(err, &databaseError) || databaseError.Code != wantCode {
		t.Fatalf("database error = %v, want PostgreSQL code %s", err, wantCode)
	}
}
