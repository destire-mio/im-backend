package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pause after a real database query has completed, before its caller continues.
// This controls scheduling without changing application code or query results.
type reviewQueryGate struct {
	match   string
	armed   atomic.Bool
	used    atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}
type reviewGateKey struct{}

func (g *reviewQueryGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, reviewGateKey{}, strings.Contains(d.SQL, g.match))
}
func (g *reviewQueryGate) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	match, _ := ctx.Value(reviewGateKey{}).(bool)
	if match && d.Err == nil && g.armed.Load() && g.used.CompareAndSwap(false, true) {
		close(g.entered)
		<-g.release
	}
}
func (g *reviewQueryGate) resume() { g.once.Do(func() { close(g.release) }) }
func reviewPool(t *testing.T, match string) (*pgxpool.Pool, *reviewQueryGate) {
	t.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	g := &reviewQueryGate{match: match, entered: make(chan struct{}), release: make(chan struct{})}
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Tracer = g
	db, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db, g
}
func reviewWaitGate(t *testing.T, g *reviewQueryGate) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("query gate was not reached")
	}
}

type reviewHTTPResult struct {
	status int
	body   []byte
	err    error
}

func reviewPost(url, token string, body any) reviewHTTPResult {
	encoded, err := json.Marshal(body)
	if err != nil {
		return reviewHTTPResult{err: err}
	}
	r, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return reviewHTTPResult{err: err}
	}
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		return reviewHTTPResult{err: err}
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	return reviewHTTPResult{status: response.StatusCode, body: data, err: err}
}

func TestReviewPasswordChangeMustInvalidateInFlightOldPasswordLogin(t *testing.T) {
	db, g := reviewPool(t, "SELECT id, username, display_name, password_hash FROM users WHERE username")
	app := newTestApplication(t, db)
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	t.Cleanup(g.resume)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("rev_login"), "Review Login")
	g.armed.Store(true)
	result := make(chan reviewHTTPResult, 1)
	go func() {
		result <- reviewPost(server.URL+"/auth/login", "", loginRequest{Username: account.Username, Password: account.Password, LoginRequestID: uniqueOpaqueID("inflight"), DeviceID: "old-password-device"})
	}()
	reviewWaitGate(t, g)
	changed := reviewPost(server.URL+"/auth/password", account.Auth.AccessToken, changePasswordRequest{CurrentPassword: account.Password, NewPassword: "replacement password 5678"})
	if changed.err != nil || changed.status != 204 {
		t.Fatalf("password change: status=%d err=%v", changed.status, changed.err)
	}
	assertTokenStatus(t, server.URL, account.Auth.AccessToken, http.StatusUnauthorized)
	g.resume()
	login := <-result
	if login.err != nil {
		t.Fatal(login.err)
	}
	if login.status == http.StatusUnauthorized {
		return
	}
	if login.status != http.StatusOK {
		t.Fatalf("login returned unexpected status %d", login.status)
	}
	var auth authResponse
	if err := json.Unmarshal(login.body, &auth); err != nil {
		t.Fatal(err)
	}
	probe := doRequest(t, http.MethodGet, server.URL+"/auth/sessions", auth.AccessToken, "")
	defer probe.Body.Close()
	if probe.StatusCode == http.StatusOK {
		t.Fatalf("old-password login returned 200 after password-change 204; its new session remains authorized (protected endpoint=200)")
	}
}

func TestReviewLogoutMustRejectInFlightWebSocketHandshake(t *testing.T) {
	db, g := reviewPool(t, "FROM access_tokens AS a")
	app, stop := newWebSocketTestApplication(t, db)
	t.Cleanup(stop)
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	t.Cleanup(g.resume)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("rev_ws_s"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("rev_ws_r"), "Receiver")
	other := loginTestAccount(t, server.URL, receiver.Username, receiver.Password, uniqueOpaqueID("control-session"))
	control := dialAuthenticatedWebSocket(t, server.URL, other.AccessToken)
	t.Cleanup(func() { control.CloseNow() })
	g.armed.Store(true)
	type dialResult struct {
		conn     *websocket.Conn
		err      error
		response *http.Response
	}
	ready := make(chan dialResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, response, err := websocket.Dial(ctx, webSocketURL(server.URL), &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer " + receiver.Auth.AccessToken}}})
		ready <- dialResult{conn, err, response}
	}()
	reviewWaitGate(t, g)
	logout := doRequest(t, http.MethodPost, server.URL+"/auth/logout", receiver.Auth.AccessToken, "")
	logout.Body.Close()
	if logout.StatusCode != 204 {
		t.Fatalf("logout status=%d", logout.StatusCode)
	}
	assertTokenStatus(t, server.URL, receiver.Auth.AccessToken, http.StatusUnauthorized)
	assertTokenStatus(t, server.URL, other.AccessToken, http.StatusOK)
	g.resume()
	dial := <-ready
	if dial.err != nil {
		if dial.response != nil && dial.response.StatusCode == http.StatusUnauthorized {
			return
		}
		t.Fatalf("unexpected handshake failure: %v", dial.err)
	}
	t.Cleanup(func() { dial.conn.CloseNow() })
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "private message after logout")
	publisher := &webSocketOutboxPublisher{router: app.webSocketHub}
	if err := publisher.Publish(t.Context(), loadOutboxEventForMessage(t, db, created.ID)); err != nil {
		t.Fatal(err)
	}
	if got := readWebSocketEnvelope(t, control); got.Message.ID != created.ID {
		t.Fatal("unrevoked control session lost its message")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, body, err := dial.conn.Read(ctx)
	if err == nil {
		t.Fatalf("revoked session received a message after logout 204 and HTTP auth 401: %s", body)
	}
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("revoked socket was not closed with policy violation: %v", err)
	}
}

type reviewNamespacedLimiter struct {
	limiter   authRateLimiter
	namespace string
}

func (l reviewNamespacedLimiter) Allow(ctx context.Context, rules []rateLimitRule) (bool, time.Duration, error) {
	for i := range rules {
		rules[i].key = l.namespace + ":" + rules[i].key
	}
	return l.limiter.Allow(ctx, rules)
}

func TestReviewTrustedProxyMustIgnoreSpoofedForwardedPrefix(t *testing.T) {
	networks, err := parseTrustedProxyNetworks("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	db := openTestDatabase(t)
	redisClient, namespace := openTestRedis(t)
	app := newTestApplication(t, db)
	app.trustedProxyNetworks = networks
	app.rateLimiter = reviewNamespacedLimiter{limiter: &redisRateLimiter{client: redisClient}, namespace: namespace}
	handler := app.routes()
	register := func(forwarded string) int {
		body, _ := json.Marshal(registerRequest{Username: uniqueUsername("xff"), DisplayName: "Forwarded", Password: "register password 5678"})
		r := httptest.NewRequest(http.MethodPost, "http://im/auth/register", bytes.NewReader(body))
		r.RemoteAddr = "10.0.0.8:443"
		r.Header.Set("X-Forwarded-For", forwarded)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	for range 10 {
		if status := register("198.51.100.42"); status != 201 {
			t.Fatalf("control registration=%d", status)
		}
	}
	if status := register("198.51.100.42"); status != 429 {
		t.Fatalf("control eleventh registration=%d, want 429", status)
	}
	// An append-style trusted proxy appends the real public client address.
	if status := register("203.0.113.77, 198.51.100.42"); status != 429 {
		t.Fatalf("same client bypassed exhausted registration IP quota by prepending a spoofed XFF address: status=%d", status)
	}
}

func TestReviewHeadlessMustSyncValidLargeMessagePage(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("rev_big_s"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("rev_big_r"), "Receiver")
	var conversationID int64
	for range 100 {
		m := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, strings.Repeat("中", 4000))
		conversationID = m.ConversationID
	}
	response := doRequest(t, http.MethodGet, fmt.Sprintf("%s/conversations/%d/messages?after=0&limit=100", server.URL, conversationID), receiver.Auth.AccessToken, "")
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || len(data) <= 1<<20 {
		t.Fatalf("unexpected fixture status=%d bytes=%d", response.StatusCode, len(data))
	}
	t.Logf("real API returned a valid %d-byte page", len(data))
	store := newHeadlessStore(t, filepath.Join(t.TempDir(), "state"), bytes.Repeat([]byte{17}, 32))
	client := newHeadlessClient(t, server.URL, store, http.DefaultClient)
	if err := client.PersistAuth(toHeadlessAuth(receiver.Auth)); err != nil {
		t.Fatal(err)
	}
	if err := client.SyncConversation(t.Context(), conversationID, 100); err != nil {
		t.Fatalf("valid 100-message page cannot be synced: %v", err)
	}
}

func TestReviewOutboxMustNotStartQueuedPublishAfterReclaim(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("rev_lease_s"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("rev_lease_r"), "Receiver")
	eventType := uniqueOpaqueID("review-slow")
	for range 4 {
		m := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "slow delivery")
		if _, err := db.Exec(t.Context(), "UPDATE outbox_events SET event_type=$1 WHERE message_id=$2", eventType, m.ID); err != nil {
			t.Fatal(err)
		}
	}
	config := defaultOutboxWorkerConfig()
	config.EventTypes = []string{eventType}
	config.BatchSize = 4
	config.Concurrency = 1
	config.LeaseDuration = 300 * time.Millisecond
	config.AttemptTimeout = 200 * time.Millisecond
	var started, stale atomic.Int32
	firstStarted := make(chan struct{})
	publisher := &testPublisher{publish: func(ctx context.Context, event outboxEvent) error {
		var stillOwned bool
		if err := db.QueryRow(ctx, "SELECT lock_token=$2::uuid AND published_at IS NULL FROM outbox_events WHERE event_id=$1::uuid", event.EventID, event.LockToken).Scan(&stillOwned); err != nil {
			return err
		}
		if !stillOwned {
			stale.Add(1)
		}
		if started.Add(1) == 1 {
			close(firstStarted)
		}
		timer := time.NewTimer(140 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}}
	first := mustTestWorker(t, db, publisher, config)
	finished := make(chan error, 1)
	go func() { _, err := first.RunOnce(context.Background()); finished <- err }()
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first delivery did not start")
	}
	// Wait for the original lease, not an implementation-specific number of
	// queued calls. A worker that claims fewer events may have finished already.
	time.Sleep(config.LeaseDuration + 50*time.Millisecond)
	secondPublisher := &testPublisher{}
	second := mustTestWorker(t, db, secondPublisher, config)
	count, err := second.RunOnce(t.Context())
	if err != nil || count == 0 {
		t.Fatalf("replacement worker processed=%d err=%v", count, err)
	}
	oldErr := <-finished
	t.Logf("replacement published=%d; old worker completion=%v", len(secondPublisher.received()), oldErr)
	if stale.Load() != 0 {
		t.Fatalf("old worker started %d queued publication(s) after replacement had completed those events", stale.Load())
	}
	if oldErr != nil {
		t.Fatalf("first worker completion: %v", oldErr)
	}
	for {
		count, err = second.RunOnce(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
	}
	var published int
	if err := db.QueryRow(t.Context(), "SELECT count(*) FROM outbox_events WHERE event_type=$1 AND published_at IS NOT NULL", eventType).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != 4 {
		t.Fatalf("published=%d, want all four messages", published)
	}
}
