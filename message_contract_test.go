package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed openapi.yaml
var openAPIDocument []byte

type fixedAuthRateLimiter struct {
	allowed    bool
	retryAfter time.Duration
	err        error
}

func (limiter fixedAuthRateLimiter) Allow(context.Context, []rateLimitRule) (bool, time.Duration, error) {
	return limiter.allowed, limiter.retryAfter, limiter.err
}

func TestRegisterOpenAPIContractValidationAndDependencies(t *testing.T) {
	_, router := loadOpenAPIContract(t)

	tests := []struct {
		name            string
		app             *application
		body            string
		wantStatus      int
		wantCode        string
		validateRequest bool
	}{
		{
			name:       "malformed JSON",
			app:        &application{},
			body:       `{"username":`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_JSON",
		},
		{
			name:            "invalid username",
			app:             &application{},
			body:            `{"username":"A!","displayName":"Alice","password":"password123"}`,
			wantStatus:      http.StatusBadRequest,
			wantCode:        "INVALID_USERNAME",
			validateRequest: true,
		},
		{
			name: "rate limited",
			app: &application{rateLimiter: fixedAuthRateLimiter{
				allowed:    false,
				retryAfter: 1500 * time.Millisecond,
			}},
			body:            `{"username":"rate_limited","displayName":"Alice","password":"password123"}`,
			wantStatus:      http.StatusTooManyRequests,
			wantCode:        "AUTH_RATE_LIMITED",
			validateRequest: true,
		},
		{
			name: "rate limiter unavailable",
			app: &application{rateLimiter: fixedAuthRateLimiter{
				err: errors.New("redis unavailable"),
			}},
			body:            `{"username":"rate_unavailable","displayName":"Alice","password":"password123"}`,
			wantStatus:      http.StatusServiceUnavailable,
			wantCode:        "AUTH_RATE_LIMIT_UNAVAILABLE",
			validateRequest: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.app.routes())
			defer server.Close()
			body := performOpenAPIContractRequest(
				t,
				router,
				http.MethodPost,
				server.URL+"/auth/register",
				"",
				test.body,
				test.wantStatus,
				test.validateRequest,
			)
			assertAPIErrorCodeAndRequestID(t, body, test.wantCode)
		})
	}

	t.Run("database failure", func(t *testing.T) {
		pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
		if err != nil {
			t.Fatalf("create closed database pool: %v", err)
		}
		pool.Close()
		server := httptest.NewServer((&application{db: pool}).routes())
		defer server.Close()
		body := performOpenAPIContractRequest(
			t,
			router,
			http.MethodPost,
			server.URL+"/auth/register",
			"",
			`{"username":"database_failure","displayName":"Alice","password":"password123"}`,
			http.StatusInternalServerError,
			true,
		)
		assertAPIErrorCodeAndRequestID(t, body, "INTERNAL_ERROR")
	})
}

func TestRegisterOpenAPIContractIntegration(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	username := uniqueUsername("regc")
	body := fmt.Sprintf(
		`{"username":%q,"displayName":"Contract Alice","password":"password123","deviceId":"contract-device"}`,
		username,
	)

	createdBody := performOpenAPIContractRequest(
		t,
		router,
		http.MethodPost,
		server.URL+"/auth/register",
		"",
		body,
		http.StatusCreated,
		true,
	)
	var created authResponse
	if err := json.Unmarshal(createdBody, &created); err != nil {
		t.Fatalf("decode registration response: %v", err)
	}
	if created.User.ID <= 0 || created.SessionID <= 0 || created.AccessToken == "" || created.RefreshToken == "" {
		t.Fatalf("incomplete registration response: %+v", created)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, created.User.ID); err != nil {
			t.Errorf("clean contract registration user: %v", err)
		}
	})

	conflictBody := performOpenAPIContractRequest(
		t,
		router,
		http.MethodPost,
		server.URL+"/auth/register",
		"",
		body,
		http.StatusConflict,
		true,
	)
	assertAPIErrorCodeAndRequestID(t, conflictBody, "USERNAME_CONFLICT")
}

func TestLoginOpenAPIContractValidationAndDependencies(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	validBody := `{"username":"login_user","password":"password123","loginRequestId":"login-request-abcdefgh"}`
	tests := []struct {
		name            string
		app             *application
		body            string
		wantStatus      int
		wantCode        string
		validateRequest bool
	}{
		{
			name:       "malformed JSON",
			app:        &application{},
			body:       `{"username":`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "INVALID_JSON",
		},
		{
			name:            "invalid credentials shape",
			app:             &application{},
			body:            `{"username":"A!","password":"password123","loginRequestId":"login-request-abcdefgh"}`,
			wantStatus:      http.StatusUnauthorized,
			wantCode:        "INVALID_CREDENTIALS",
			validateRequest: true,
		},
		{
			name: "rate limited",
			app: &application{rateLimiter: fixedAuthRateLimiter{
				allowed:    false,
				retryAfter: 2 * time.Second,
			}},
			body:            validBody,
			wantStatus:      http.StatusTooManyRequests,
			wantCode:        "AUTH_RATE_LIMITED",
			validateRequest: true,
		},
		{
			name: "rate limiter unavailable",
			app: &application{rateLimiter: fixedAuthRateLimiter{
				err: errors.New("redis unavailable"),
			}},
			body:            validBody,
			wantStatus:      http.StatusServiceUnavailable,
			wantCode:        "AUTH_RATE_LIMIT_UNAVAILABLE",
			validateRequest: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.app.routes())
			defer server.Close()
			body := performOpenAPIContractRequest(
				t,
				router,
				http.MethodPost,
				server.URL+"/auth/login",
				"",
				test.body,
				test.wantStatus,
				test.validateRequest,
			)
			assertAPIErrorCodeAndRequestID(t, body, test.wantCode)
		})
	}

	t.Run("database failure", func(t *testing.T) {
		pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
		if err != nil {
			t.Fatalf("create closed database pool: %v", err)
		}
		pool.Close()
		server := httptest.NewServer((&application{db: pool}).routes())
		defer server.Close()
		body := performOpenAPIContractRequest(
			t,
			router,
			http.MethodPost,
			server.URL+"/auth/login",
			"",
			validBody,
			http.StatusInternalServerError,
			true,
		)
		assertAPIErrorCodeAndRequestID(t, body, "INTERNAL_ERROR")
	})
}

func TestLoginOpenAPIContractIntegration(t *testing.T) {
	db := openTestDatabase(t)
	app := newTestApplication(t, db)
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	alice := registerTestAccount(t, db, server.URL, uniqueUsername("logca"), "Contract Alice")
	bob := registerTestAccount(t, db, server.URL, uniqueUsername("logcb"), "Contract Bob")
	requestID := uniqueOpaqueID("login-contract")
	aliceBody := fmt.Sprintf(
		`{"username":%q,"password":%q,"loginRequestId":%q,"deviceId":"contract-device"}`,
		alice.Username,
		alice.Password,
		requestID,
	)

	firstBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/login", "", aliceBody, http.StatusOK, true)
	replayBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/login", "", aliceBody, http.StatusOK, true)
	var first, replay authResponse
	if err := json.Unmarshal(firstBody, &first); err != nil {
		t.Fatalf("decode first login response: %v", err)
	}
	if err := json.Unmarshal(replayBody, &replay); err != nil {
		t.Fatalf("decode replay login response: %v", err)
	}
	if first.SessionID != replay.SessionID || first.AccessToken != replay.AccessToken || first.RefreshToken != replay.RefreshToken {
		t.Fatal("idempotent login replay returned a different session or token pair")
	}

	wrongPasswordBody := fmt.Sprintf(
		`{"username":%q,"password":"wrong password","loginRequestId":%q}`,
		alice.Username,
		uniqueOpaqueID("wrong-password"),
	)
	invalidBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/login", "", wrongPasswordBody, http.StatusUnauthorized, true)
	assertAPIErrorCodeAndRequestID(t, invalidBody, "INVALID_CREDENTIALS")

	bobBody := fmt.Sprintf(
		`{"username":%q,"password":%q,"loginRequestId":%q}`,
		bob.Username,
		bob.Password,
		requestID,
	)
	conflictBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/login", "", bobBody, http.StatusConflict, true)
	assertAPIErrorCodeAndRequestID(t, conflictBody, "LOGIN_REQUEST_ID_CONFLICT")

	unavailableServer := httptest.NewServer((&application{db: db}).routes())
	defer unavailableServer.Close()
	unavailableBody := fmt.Sprintf(
		`{"username":%q,"password":%q,"loginRequestId":%q}`,
		alice.Username,
		alice.Password,
		uniqueOpaqueID("recovery-unavailable"),
	)
	recoveryBody := performOpenAPIContractRequest(t, router, http.MethodPost, unavailableServer.URL+"/auth/login", "", unavailableBody, http.StatusServiceUnavailable, true)
	assertAPIErrorCodeAndRequestID(t, recoveryBody, "LOGIN_RECOVERY_UNAVAILABLE")
}

func TestRefreshOpenAPIContractValidationAndDependencies(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	validToken := base64.RawURLEncoding.EncodeToString(make([]byte, tokenLength))
	validBody := fmt.Sprintf(`{"refreshToken":%q,"idempotencyKey":"refresh-key-abcdefgh"}`, validToken)
	tests := []struct {
		name            string
		app             *application
		body            string
		wantStatus      int
		wantCode        string
		validateRequest bool
	}{
		{name: "malformed JSON", app: &application{}, body: `{"refreshToken":`, wantStatus: http.StatusBadRequest, wantCode: "INVALID_JSON"},
		{name: "invalid idempotency key", app: &application{}, body: fmt.Sprintf(`{"refreshToken":%q,"idempotencyKey":"short"}`, validToken), wantStatus: http.StatusBadRequest, wantCode: "INVALID_IDEMPOTENCY_KEY"},
		{name: "invalid refresh token", app: &application{}, body: `{"refreshToken":"invalid","idempotencyKey":"refresh-key-abcdefgh"}`, wantStatus: http.StatusUnauthorized, wantCode: "REFRESH_TOKEN_INVALID", validateRequest: true},
		{name: "recovery unavailable", app: &application{}, body: validBody, wantStatus: http.StatusServiceUnavailable, wantCode: "REFRESH_RECOVERY_UNAVAILABLE", validateRequest: true},
		{
			name: "rate limited",
			app: &application{rateLimiter: fixedAuthRateLimiter{
				allowed:    false,
				retryAfter: 2 * time.Second,
			}},
			body: validBody, wantStatus: http.StatusTooManyRequests, wantCode: "AUTH_RATE_LIMITED", validateRequest: true,
		},
		{
			name: "rate limiter unavailable",
			app: &application{rateLimiter: fixedAuthRateLimiter{
				err: errors.New("redis unavailable"),
			}},
			body: validBody, wantStatus: http.StatusServiceUnavailable, wantCode: "AUTH_RATE_LIMIT_UNAVAILABLE", validateRequest: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.app.routes())
			defer server.Close()
			body := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/refresh", "", test.body, test.wantStatus, test.validateRequest)
			assertAPIErrorCodeAndRequestID(t, body, test.wantCode)
		})
	}
}

func TestRefreshOpenAPIContractIntegration(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("refc"), "Refresh Contract")
	key := uniqueOpaqueID("refresh-contract")
	body := fmt.Sprintf(`{"refreshToken":%q,"idempotencyKey":%q}`, account.Auth.RefreshToken, key)

	firstBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/refresh", "", body, http.StatusOK, true)
	replayBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/refresh", "", body, http.StatusOK, true)
	var first, replay authResponse
	if err := json.Unmarshal(firstBody, &first); err != nil {
		t.Fatalf("decode first refresh response: %v", err)
	}
	if err := json.Unmarshal(replayBody, &replay); err != nil {
		t.Fatalf("decode replay refresh response: %v", err)
	}
	if first.AccessToken != replay.AccessToken || first.RefreshToken != replay.RefreshToken {
		t.Fatal("same refresh idempotency key returned a different token pair")
	}

	replayAttack := fmt.Sprintf(
		`{"refreshToken":%q,"idempotencyKey":%q}`,
		account.Auth.RefreshToken,
		uniqueOpaqueID("different-refresh-key"),
	)
	invalidBody := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/refresh", "", replayAttack, http.StatusUnauthorized, true)
	assertAPIErrorCodeAndRequestID(t, invalidBody, "REFRESH_TOKEN_INVALID")
}

func TestSessionAndPasswordOpenAPIContractIntegration(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	owner := registerTestAccount(t, db, server.URL, uniqueUsername("sessc"), "Session Contract")
	otherDevice := loginTestAccountForDevice(t, server.URL, owner.Username, owner.Password, uniqueOpaqueID("session-contract"), "second-device")
	stranger := registerTestAccount(t, db, server.URL, uniqueUsername("sessx"), "Session Stranger")

	performOpenAPIContractRequest(t, router, http.MethodGet, server.URL+"/auth/sessions", owner.Auth.AccessToken, "", http.StatusOK, true)
	invalidSession := performOpenAPIContractRequest(t, router, http.MethodDelete, server.URL+"/auth/sessions/0", owner.Auth.AccessToken, "", http.StatusBadRequest, false)
	assertAPIErrorCodeAndRequestID(t, invalidSession, "INVALID_SESSION_ID")
	foreignSession := performOpenAPIContractRequest(t, router, http.MethodDelete, fmt.Sprintf("%s/auth/sessions/%d", server.URL, stranger.Auth.SessionID), owner.Auth.AccessToken, "", http.StatusNotFound, true)
	assertAPIErrorCodeAndRequestID(t, foreignSession, "SESSION_NOT_FOUND")
	performOpenAPIContractRequest(t, router, http.MethodDelete, fmt.Sprintf("%s/auth/sessions/%d", server.URL, otherDevice.SessionID), owner.Auth.AccessToken, "", http.StatusNoContent, true)
	performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/logout", owner.Auth.AccessToken, "", http.StatusNoContent, true)

	logoutAll := registerTestAccount(t, db, server.URL, uniqueUsername("logac"), "Logout All Contract")
	loginTestAccountForDevice(t, server.URL, logoutAll.Username, logoutAll.Password, uniqueOpaqueID("logout-all-contract"), "second-device")
	performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/logout-all", logoutAll.Auth.AccessToken, "", http.StatusNoContent, true)

	password := registerTestAccount(t, db, server.URL, uniqueUsername("pwdc"), "Password Contract")
	malformed := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/password", password.Auth.AccessToken, `{"currentPassword":`, http.StatusBadRequest, false)
	assertAPIErrorCodeAndRequestID(t, malformed, "INVALID_JSON")
	unchangedBody := fmt.Sprintf(`{"currentPassword":%q,"newPassword":%q}`, password.Password, password.Password)
	unchanged := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/password", password.Auth.AccessToken, unchangedBody, http.StatusBadRequest, true)
	assertAPIErrorCodeAndRequestID(t, unchanged, "PASSWORD_UNCHANGED")
	wrongBody := `{"currentPassword":"wrong password","newPassword":"a different password"}`
	wrong := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/password", password.Auth.AccessToken, wrongBody, http.StatusUnauthorized, true)
	assertAPIErrorCodeAndRequestID(t, wrong, "CURRENT_PASSWORD_INVALID")
	validBody := fmt.Sprintf(`{"currentPassword":%q,"newPassword":"a different password"}`, password.Password)
	if _, err := db.Exec(context.Background(), `
CREATE FUNCTION contract_skip_user_update() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER contract_skip_user_update
BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION contract_skip_user_update()`); err != nil {
		t.Fatalf("install password conflict trigger: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), `
DROP TRIGGER IF EXISTS contract_skip_user_update ON users;
DROP FUNCTION IF EXISTS contract_skip_user_update()`)
	})
	conflict := performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/password", password.Auth.AccessToken, validBody, http.StatusConflict, true)
	assertAPIErrorCodeAndRequestID(t, conflict, "PASSWORD_CHANGE_CONFLICT")
	if _, err := db.Exec(context.Background(), `
DROP TRIGGER IF EXISTS contract_skip_user_update ON users;
DROP FUNCTION IF EXISTS contract_skip_user_update()`); err != nil {
		t.Fatalf("remove password conflict trigger: %v", err)
	}
	performOpenAPIContractRequest(t, router, http.MethodPost, server.URL+"/auth/password", password.Auth.AccessToken, validBody, http.StatusNoContent, true)
}

func TestProtectedAuthEndpointsOpenAPIAuthenticationContracts(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	tests := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodPost, path: "/auth/logout"},
		{method: http.MethodPost, path: "/auth/logout-all"},
		{method: http.MethodGet, path: "/auth/sessions"},
		{method: http.MethodDelete, path: "/auth/sessions/1"},
		{method: http.MethodPost, path: "/auth/password", body: `{"currentPassword":"password123","newPassword":"different123"}`},
	}
	for _, test := range tests {
		name := test.method + " " + test.path
		t.Run(name+" requires authentication", func(t *testing.T) {
			server := httptest.NewServer((&application{}).routes())
			defer server.Close()
			body := performOpenAPIContractRequest(t, router, test.method, server.URL+test.path, "", test.body, http.StatusUnauthorized, true)
			assertAPIErrorCodeAndRequestID(t, body, "AUTHENTICATION_REQUIRED")
		})
		t.Run(name+" authentication unavailable", func(t *testing.T) {
			pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
			if err != nil {
				t.Fatalf("create closed database pool: %v", err)
			}
			pool.Close()
			server := httptest.NewServer((&application{db: pool}).routes())
			defer server.Close()
			body := performOpenAPIContractRequest(
				t,
				router,
				test.method,
				server.URL+test.path,
				base64.RawURLEncoding.EncodeToString(make([]byte, tokenLength)),
				test.body,
				http.StatusServiceUnavailable,
				true,
			)
			assertAPIErrorCodeAndRequestID(t, body, "AUTHENTICATION_UNAVAILABLE")
		})
	}
}

func TestProtectedAuthHandlersOpenAPIInternalErrorContracts(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
	if err != nil {
		t.Fatalf("create closed database pool: %v", err)
	}
	pool.Close()
	app := &application{db: pool}
	tests := []struct {
		name    string
		method  string
		path    string
		body    string
		handler http.HandlerFunc
	}{
		{name: "logout", method: http.MethodPost, path: "/auth/logout", handler: app.logout},
		{name: "logout all", method: http.MethodPost, path: "/auth/logout-all", handler: app.logoutAll},
		{name: "list sessions", method: http.MethodGet, path: "/auth/sessions", handler: app.listSessions},
		{name: "revoke session", method: http.MethodDelete, path: "/auth/sessions/1", handler: app.revokeSession},
		{name: "change password", method: http.MethodPost, path: "/auth/password", body: `{"currentPassword":"password123","newPassword":"different123"}`, handler: app.changePassword},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "http://api.example.test"+test.path, bytes.NewBufferString(test.body))
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			request.SetPathValue("sessionID", "1")
			ctx := context.WithValue(request.Context(), authenticatedUserIDKey, int64(1))
			ctx = context.WithValue(ctx, authenticatedSessionIDKey, int64(1))
			request = request.WithContext(ctx)
			recorder := httptest.NewRecorder()
			requestIDMiddleware(test.handler).ServeHTTP(recorder, request)
			validateOpenAPIResponse(t, router, request, recorder, http.StatusInternalServerError)
			assertAPIErrorCodeAndRequestID(t, recorder.Body.Bytes(), "INTERNAL_ERROR")
		})
	}
}

func TestMessageHistoryOpenAPIContractIntegration(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	alice := registerTestAccount(t, db, server.URL, uniqueUsername("mhca"), "History Contract Alice")
	bob := registerTestAccount(t, db, server.URL, uniqueUsername("mhcb"), "History Contract Bob")
	carol := registerTestAccount(t, db, server.URL, uniqueUsername("mhcc"), "History Contract Carol")
	createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "history contract")
	pageBody := performOpenAPIContractRequest(
		t,
		router,
		http.MethodGet,
		fmt.Sprintf("%s/messages?peerId=%d&limit=10", server.URL, bob.User.ID),
		alice.Auth.AccessToken,
		"",
		http.StatusOK,
		true,
	)
	var page messageHistoryPage
	if err := json.Unmarshal(pageBody, &page); err != nil {
		t.Fatalf("decode history contract page: %v", err)
	}
	if len(page.Messages) != 1 || page.BeforeCursor == "" {
		t.Fatalf("unexpected history contract page: %+v", page)
	}

	tests := []struct {
		name            string
		path            string
		wantCode        string
		validateRequest bool
	}{
		{name: "invalid peer", path: "/messages?peerId=0", wantCode: "INVALID_PEER_ID"},
		{name: "invalid limit", path: fmt.Sprintf("/messages?peerId=%d&limit=0", bob.User.ID), wantCode: "INVALID_LIMIT"},
		{name: "invalid cursor encoding", path: fmt.Sprintf("/messages?peerId=%d&before=not-a-cursor", bob.User.ID), wantCode: "INVALID_CURSOR", validateRequest: true},
		{name: "cursor from another conversation", path: fmt.Sprintf("/messages?peerId=%d&before=%s", carol.User.ID, page.BeforeCursor), wantCode: "INVALID_CURSOR", validateRequest: true},
		{name: "unknown query", path: fmt.Sprintf("/messages?peerId=%d&offset=10", bob.User.ID), wantCode: "INVALID_QUERY"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := performOpenAPIContractRequest(t, router, http.MethodGet, server.URL+test.path, alice.Auth.AccessToken, "", http.StatusBadRequest, test.validateRequest)
			assertAPIErrorCodeAndRequestID(t, body, test.wantCode)
		})
	}
}

func TestMessageHistoryOpenAPIInternalErrorContract(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
	if err != nil {
		t.Fatalf("create closed database pool: %v", err)
	}
	pool.Close()
	app := &application{db: pool}
	request := httptest.NewRequest(http.MethodGet, "http://api.example.test/messages?peerId=2", nil)
	request = request.WithContext(context.WithValue(request.Context(), authenticatedUserIDKey, int64(1)))
	recorder := httptest.NewRecorder()
	requestIDMiddleware(http.HandlerFunc(app.listMessages)).ServeHTTP(recorder, request)
	validateOpenAPIResponse(t, router, request, recorder, http.StatusInternalServerError)
	assertAPIErrorCodeAndRequestID(t, recorder.Body.Bytes(), "INTERNAL_ERROR")
}

func TestOperationalAndDeprecatedEndpointOpenAPIContracts(t *testing.T) {
	db := openTestDatabase(t)
	app := newTestApplication(t, db)
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("opca"), "Operational Contract")

	performOpenAPIContractRequest(t, router, http.MethodGet, server.URL+"/health", "", "", http.StatusOK, true)
	for _, endpoint := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/messages/sync"},
		{method: http.MethodPost, path: "/messages/ack"},
	} {
		body := performOpenAPIContractRequest(t, router, endpoint.method, server.URL+endpoint.path, account.Auth.AccessToken, "", http.StatusGone, true)
		assertAPIErrorCodeAndRequestID(t, body, "ENDPOINT_GONE")
	}

	realtimeBody := performOpenAPIContractRequest(t, router, http.MethodGet, server.URL+"/ws", account.Auth.AccessToken, "", http.StatusServiceUnavailable, true)
	assertAPIErrorCodeAndRequestID(t, realtimeBody, "REALTIME_UNAVAILABLE")

	upgradeApp := newTestApplication(t, db)
	upgradeApp.webSocketHub = newWebSocketHub()
	upgradeServer := httptest.NewServer(upgradeApp.routes())
	defer upgradeServer.Close()
	performOpenAPIContractRequest(t, router, http.MethodGet, upgradeServer.URL+"/ws", account.Auth.AccessToken, "", http.StatusUpgradeRequired, true)
}

func TestHealthAndWebSocketDependencyOpenAPIContracts(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
	if err != nil {
		t.Fatalf("create closed database pool: %v", err)
	}
	pool.Close()
	server := httptest.NewServer((&application{db: pool}).routes())
	defer server.Close()
	healthBody := performOpenAPIContractRequest(t, router, http.MethodGet, server.URL+"/health", "", "", http.StatusServiceUnavailable, true)
	assertAPIErrorCodeAndRequestID(t, healthBody, "DEPENDENCY_UNAVAILABLE")
	webSocketBody := performOpenAPIContractRequest(
		t,
		router,
		http.MethodGet,
		server.URL+"/ws",
		base64.RawURLEncoding.EncodeToString(make([]byte, tokenLength)),
		"",
		http.StatusServiceUnavailable,
		true,
	)
	assertAPIErrorCodeAndRequestID(t, webSocketBody, "AUTHENTICATION_UNAVAILABLE")
}

func assertAPIErrorCodeAndRequestID(t *testing.T, body []byte, wantCode string) {
	t.Helper()
	var response apiErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode API error: %v", err)
	}
	if response.Code != wantCode {
		t.Fatalf("error code = %q, want %q", response.Code, wantCode)
	}
	if response.Message == "" || response.RequestID == "" {
		t.Fatalf("incomplete API error: %+v", response)
	}
}

func TestCreateMessageOpenAPIContract(t *testing.T) {
	document, router := loadOpenAPIContract(t)
	createdAt := time.Date(2026, time.September, 1, 10, 0, 0, 0, time.UTC)
	created := message{
		ID:              42,
		ConversationID:  21,
		ConversationSeq: 1,
		ClientMessageID: "client-message-0001",
		SenderID:        7,
		ReceiverID:      8,
		Content:         "hello",
		CreatedAt:       createdAt,
	}

	tests := []struct {
		name            string
		body            string
		service         *fakeMessageSendingService
		wantStatus      int
		validateRequest bool
	}{
		{
			name:            "created",
			body:            `{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`,
			service:         &fakeMessageSendingService{result: sendMessageResult{Message: created, Created: true}},
			wantStatus:      http.StatusCreated,
			validateRequest: true,
		},
		{
			name:            "idempotent replay",
			body:            `{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`,
			service:         &fakeMessageSendingService{result: sendMessageResult{Message: created}},
			wantStatus:      http.StatusOK,
			validateRequest: true,
		},
		{
			name:            "invalid message",
			body:            `{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`,
			service:         &fakeMessageSendingService{err: invalidMessageError("content is too long")},
			wantStatus:      http.StatusBadRequest,
			validateRequest: true,
		},
		{
			name:       "malformed JSON",
			body:       `{"receiverId":`,
			service:    &fakeMessageSendingService{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:            "idempotency conflict",
			body:            `{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`,
			service:         &fakeMessageSendingService{err: &messageServiceError{Code: messageServiceErrorIdempotencyConflict}},
			wantStatus:      http.StatusConflict,
			validateRequest: true,
		},
		{
			name:            "dependency unavailable",
			body:            `{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`,
			service:         &fakeMessageSendingService{err: &messageServiceError{Code: messageServiceErrorUnavailable, Cause: errors.New("database unavailable")}},
			wantStatus:      http.StatusServiceUnavailable,
			validateRequest: true,
		},
		{
			name:            "internal error",
			body:            `{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`,
			service:         &fakeMessageSendingService{err: internalMessageError(errors.New("unexpected query failure"))},
			wantStatus:      http.StatusInternalServerError,
			validateRequest: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "http://api.example.test/messages", bytes.NewBufferString(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer contract-test-token")
			request = request.WithContext(context.WithValue(request.Context(), authenticatedUserIDKey, int64(7)))

			route, pathParams, err := router.FindRoute(request)
			if err != nil {
				t.Fatalf("match OpenAPI route: %v", err)
			}
			requestInput := &openapi3filter.RequestValidationInput{
				Request:    request,
				PathParams: pathParams,
				Route:      route,
				Options: &openapi3filter.Options{
					AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
				},
			}
			if test.validateRequest {
				if err := openapi3filter.ValidateRequest(request.Context(), requestInput); err != nil {
					t.Fatalf("request violates OpenAPI contract: %v", err)
				}
			}

			app := &application{messageSender: test.service}
			recorder := httptest.NewRecorder()
			requestIDMiddleware(http.HandlerFunc(app.createMessage)).ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
			responseInput := &openapi3filter.ResponseValidationInput{
				RequestValidationInput: requestInput,
				Status:                 recorder.Code,
				Header:                 recorder.Header(),
				Options: &openapi3filter.Options{
					IncludeResponseStatus: true,
				},
			}
			responseInput.SetBodyBytes(recorder.Body.Bytes())
			if err := openapi3filter.ValidateResponse(request.Context(), responseInput); err != nil {
				t.Fatalf("response violates OpenAPI contract: %v\nbody: %s", err, recorder.Body.String())
			}
		})
	}

	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("OpenAPI document is invalid after contract run: %v", err)
	}
}

func TestCreateMessageAuthenticationOpenAPIContract(t *testing.T) {
	_, router := loadOpenAPIContract(t)

	t.Run("authentication required", func(t *testing.T) {
		app := &application{}
		request := httptest.NewRequest(
			http.MethodPost,
			"http://api.example.test/messages",
			bytes.NewBufferString(`{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`),
		)
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()

		handler := requestIDMiddleware(app.requireAuthentication(http.HandlerFunc(app.createMessage)))
		handler.ServeHTTP(recorder, request)

		validateOpenAPIResponse(t, router, request, recorder, http.StatusUnauthorized)
	})

	t.Run("authentication dependency unavailable", func(t *testing.T) {
		pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
		if err != nil {
			t.Fatalf("create closed database pool: %v", err)
		}
		pool.Close()
		app := &application{db: pool}
		request := httptest.NewRequest(
			http.MethodPost,
			"http://api.example.test/messages",
			bytes.NewBufferString(`{"clientMessageId":"client-message-0001","receiverId":8,"content":"hello"}`),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+base64.RawURLEncoding.EncodeToString(make([]byte, tokenLength)))
		recorder := httptest.NewRecorder()

		handler := requestIDMiddleware(app.requireAuthentication(http.HandlerFunc(app.createMessage)))
		handler.ServeHTTP(recorder, request)

		validateOpenAPIResponse(t, router, request, recorder, http.StatusServiceUnavailable)
	})
}

func TestConversationRecoveryOpenAPIContract(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	_, router := loadOpenAPIContract(t)
	alice := registerTestAccount(t, db, server.URL, uniqueUsername("contract_a"), "Contract Alice")
	bob := registerTestAccount(t, db, server.URL, uniqueUsername("contract_b"), "Contract Bob")
	created := createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "contract hello")

	var missingConversationID int64
	if err := db.QueryRow(t.Context(), `SELECT COALESCE(max(id), 0) + 1 FROM conversations`).Scan(&missingConversationID); err != nil {
		t.Fatalf("choose missing conversation id: %v", err)
	}

	tests := []struct {
		name            string
		method          string
		path            string
		body            string
		wantStatus      int
		validateRequest bool
	}{
		{name: "list conversations", method: http.MethodGet, path: "/conversations?after=0&limit=10", wantStatus: http.StatusOK, validateRequest: true},
		{name: "invalid conversation pagination", method: http.MethodGet, path: "/conversations?limit=0", wantStatus: http.StatusBadRequest},
		{name: "conversation list cursor ahead", method: http.MethodGet, path: "/conversations?after=9223372036854775807", wantStatus: http.StatusConflict, validateRequest: true},
		{name: "sync conversation messages", method: http.MethodGet, path: fmt.Sprintf("/conversations/%d/messages?after=0&limit=10", created.ConversationID), wantStatus: http.StatusOK, validateRequest: true},
		{name: "invalid conversation id", method: http.MethodGet, path: "/conversations/0/messages?after=0&limit=10", wantStatus: http.StatusBadRequest},
		{name: "conversation not found", method: http.MethodGet, path: fmt.Sprintf("/conversations/%d/messages?after=0&limit=10", missingConversationID), wantStatus: http.StatusNotFound, validateRequest: true},
		{name: "conversation stream cursor ahead", method: http.MethodGet, path: fmt.Sprintf("/conversations/%d/messages?after=%d", created.ConversationID, created.ConversationSeq+1), wantStatus: http.StatusConflict, validateRequest: true},
		{name: "acknowledge conversation", method: http.MethodPost, path: fmt.Sprintf("/conversations/%d/ack", created.ConversationID), body: fmt.Sprintf(`{"cursor":%d}`, created.ConversationSeq), wantStatus: http.StatusOK, validateRequest: true},
		{name: "invalid acknowledgement", method: http.MethodPost, path: fmt.Sprintf("/conversations/%d/ack", created.ConversationID), body: `{"cursor":-1}`, wantStatus: http.StatusBadRequest},
		{name: "acknowledgement conversation not found", method: http.MethodPost, path: fmt.Sprintf("/conversations/%d/ack", missingConversationID), body: `{"cursor":0}`, wantStatus: http.StatusNotFound, validateRequest: true},
		{name: "acknowledgement cursor ahead", method: http.MethodPost, path: fmt.Sprintf("/conversations/%d/ack", created.ConversationID), body: fmt.Sprintf(`{"cursor":%d}`, created.ConversationSeq+1), wantStatus: http.StatusConflict, validateRequest: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			performOpenAPIContractRequest(
				t,
				router,
				test.method,
				server.URL+test.path,
				alice.Auth.AccessToken,
				test.body,
				test.wantStatus,
				test.validateRequest,
			)
		})
	}
}

func TestConversationRecoveryAuthenticationUnavailableContract(t *testing.T) {
	_, router := loadOpenAPIContract(t)
	pool, err := pgxpool.New(context.Background(), defaultDatabaseURL)
	if err != nil {
		t.Fatalf("create closed database pool: %v", err)
	}
	pool.Close()
	app := &application{db: pool}
	token := base64.RawURLEncoding.EncodeToString(make([]byte, tokenLength))

	for _, test := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/conversations"},
		{method: http.MethodGet, path: "/conversations/1/messages"},
		{method: http.MethodPost, path: "/conversations/1/ack", body: `{"cursor":0}`},
	} {
		request := httptest.NewRequest(test.method, "http://api.example.test"+test.path, bytes.NewBufferString(test.body))
		request.Header.Set("Authorization", "Bearer "+token)
		if test.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		recorder := httptest.NewRecorder()

		app.routes().ServeHTTP(recorder, request)

		validateOpenAPIResponse(t, router, request, recorder, http.StatusServiceUnavailable)
	}
}

func loadOpenAPIContract(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	document, err := openapi3.NewLoader().LoadFromData(openAPIDocument)
	if err != nil {
		t.Fatalf("load OpenAPI document: %v", err)
	}
	if err := document.Validate(context.Background()); err != nil {
		t.Fatalf("validate OpenAPI document: %v", err)
	}
	router, err := legacy.NewRouter(document)
	if err != nil {
		t.Fatalf("build OpenAPI router: %v", err)
	}
	return document, router
}

func validateOpenAPIResponse(
	t *testing.T,
	router routers.Router,
	request *http.Request,
	recorder *httptest.ResponseRecorder,
	wantStatus int,
) {
	t.Helper()
	validateOpenAPIHTTPResponse(
		t,
		router,
		request,
		recorder.Code,
		recorder.Header(),
		recorder.Body.Bytes(),
		wantStatus,
	)
}

func performOpenAPIContractRequest(
	t *testing.T,
	router routers.Router,
	method string,
	url string,
	token string,
	body string,
	wantStatus int,
	validateRequest bool,
) []byte {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("create contract request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	route, pathParams, err := router.FindRoute(request)
	if err != nil {
		t.Fatalf("match OpenAPI route: %v", err)
	}
	if validateRequest {
		requestInput := &openapi3filter.RequestValidationInput{
			Request:    request,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
			},
		}
		if err := openapi3filter.ValidateRequest(request.Context(), requestInput); err != nil {
			t.Fatalf("request violates OpenAPI contract: %v", err)
		}
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("send contract request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read contract response: %v", err)
	}
	validateOpenAPIHTTPResponse(
		t,
		router,
		request,
		response.StatusCode,
		response.Header,
		responseBody,
		wantStatus,
	)
	return responseBody
}

func validateOpenAPIHTTPResponse(
	t *testing.T,
	router routers.Router,
	request *http.Request,
	status int,
	header http.Header,
	body []byte,
	wantStatus int,
) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("status = %d, want %d", status, wantStatus)
	}
	route, pathParams, err := router.FindRoute(request)
	if err != nil {
		t.Fatalf("match OpenAPI route: %v", err)
	}
	requestInput := &openapi3filter.RequestValidationInput{
		Request:    request,
		PathParams: pathParams,
		Route:      route,
	}
	responseInput := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: requestInput,
		Status:                 status,
		Header:                 header,
		Options: &openapi3filter.Options{
			IncludeResponseStatus: true,
		},
	}
	responseInput.SetBodyBytes(body)
	if err := openapi3filter.ValidateResponse(request.Context(), responseInput); err != nil {
		t.Fatalf("response violates OpenAPI contract: %v\nbody: %s", err, body)
	}
}
