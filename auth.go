package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/argon2"
)

const (
	maxDisplayNameRunes = 100
	minPasswordRunes    = 8
	maxPasswordRunes    = 128
	accessTokenLifetime = 15 * time.Minute
	sessionIdleLifetime = 90 * 24 * time.Hour
	idempotencyLifetime = 10 * time.Minute

	argonMemory      = 19 * 1024
	argonIterations  = 2
	argonParallelism = 1
	argonSaltLength  = 16
	argonKeyLength   = 32
	tokenLength      = 32
)

var usernamePattern = regexp.MustCompile(`^[a-z0-9_]{3,32}$`)

var errIdempotencyConflict = errors.New("idempotency key belongs to another request")

type registerRequest struct {
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
	Password    string `json:"password"`
	DeviceID    string `json:"deviceId,omitempty"`
}

type loginRequest struct {
	Username       string `json:"username"`
	Password       string `json:"password"`
	LoginRequestID string `json:"loginRequestId"`
	DeviceID       string `json:"deviceId,omitempty"`
}

type refreshRequest struct {
	RefreshToken   string `json:"refreshToken"`
	IdempotencyKey string `json:"idempotencyKey"`
}

type changePasswordRequest struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
}

type authResponse struct {
	User                  user      `json:"user"`
	SessionID             int64     `json:"sessionId"`
	DeviceID              string    `json:"deviceId"`
	AccessToken           string    `json:"accessToken"`
	AccessTokenExpiresAt  time.Time `json:"accessTokenExpiresAt"`
	RefreshToken          string    `json:"refreshToken"`
	RefreshTokenExpiresAt time.Time `json:"refreshTokenExpiresAt"`
}

type sessionResponse struct {
	ID            int64      `json:"id"`
	DeviceID      string     `json:"deviceId"`
	CreatedAt     time.Time  `json:"createdAt"`
	IdleExpiresAt time.Time  `json:"idleExpiresAt"`
	RevokedAt     *time.Time `json:"revokedAt,omitempty"`
	Current       bool       `json:"current"`
}

type generatedToken struct {
	raw  string
	hash []byte
}

type authenticationContextKey string

const (
	authenticatedUserIDKey          authenticationContextKey = "authenticated-user-id"
	authenticatedSessionIDKey       authenticationContextKey = "authenticated-session-id"
	authenticatedDeviceIDKey        authenticationContextKey = "authenticated-device-id"
	authenticatedTokenExpiryTimeKey authenticationContextKey = "authenticated-token-expiry-time"
)

func (app *application) register(w http.ResponseWriter, r *http.Request) {
	var input registerRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	input.Username = strings.ToLower(strings.TrimSpace(input.Username))
	input.DisplayName = strings.TrimSpace(input.DisplayName)
	if !usernamePattern.MatchString(input.Username) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "username must contain 3-32 lowercase letters, numbers or underscores"})
		return
	}
	if input.DisplayName == "" || utf8.RuneCountInString(input.DisplayName) > maxDisplayNameRunes {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "displayName must contain 1-100 characters"})
		return
	}
	if !validPassword(input.Password) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "password must contain 8-128 characters"})
		return
	}
	if !validOptionalDeviceID(input.DeviceID) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "deviceId is too long"})
		return
	}
	if !app.enforceAuthRateLimit(w, r, "register", input.Username, input.DeviceID) {
		return
	}

	passwordHash, err := hashPassword(input.Password)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create user"})
		return
	}

	tx, err := app.db.Begin(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create user"})
		return
	}
	defer tx.Rollback(r.Context())

	var created user
	err = tx.QueryRow(
		r.Context(),
		`INSERT INTO users (username, display_name, password_hash)
		 VALUES ($1, $2, $3)
		 RETURNING id, username, display_name`,
		input.Username,
		input.DisplayName,
		passwordHash,
	).Scan(&created.ID, &created.Username, &created.DisplayName)
	if err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) && databaseError.Code == "23505" {
			writeJSON(w, http.StatusConflict, errorResponse{Error: "username is already registered"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create user"})
		return
	}

	response, err := app.createSessionResponse(r.Context(), tx, created, input.DeviceID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create user"})
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create user"})
		return
	}

	writePrivateJSON(w, http.StatusCreated, response)
}

func (app *application) login(w http.ResponseWriter, r *http.Request) {
	var input loginRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	input.Username = strings.ToLower(strings.TrimSpace(input.Username))
	if !validOpaqueID(input.LoginRequestID) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "loginRequestId must contain 16-128 characters"})
		return
	}
	if !validOptionalDeviceID(input.DeviceID) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "deviceId is too long"})
		return
	}
	if !app.enforceAuthRateLimit(w, r, "login", input.Username, input.DeviceID) {
		return
	}
	if !usernamePattern.MatchString(input.Username) || !validPassword(input.Password) {
		consumePasswordVerificationTime(input.Password)
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid username or password"})
		return
	}

	authenticated, passwordHash, err := app.loadUserCredentials(r.Context(), input.Username)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			consumePasswordVerificationTime(input.Password)
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid username or password"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not log in"})
		return
	}

	matches, err := verifyPassword(input.Password, passwordHash)
	if err != nil || !matches {
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid username or password"})
		return
	}
	if app.responseCipher == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "login recovery is unavailable"})
		return
	}

	requestHash := sha256.Sum256([]byte(input.LoginRequestID))
	if existing, found, err := app.loadLoginResult(r.Context(), requestHash[:], authenticated.ID); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "could not recover login"})
		return
	} else if found {
		writePrivateJSON(w, http.StatusOK, existing)
		return
	}

	response, err := app.createIdempotentLogin(r.Context(), authenticated, input.DeviceID, requestHash[:])
	if errors.Is(err, errIdempotencyConflict) {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "loginRequestId was already used"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not log in"})
		return
	}
	writePrivateJSON(w, http.StatusOK, response)
}

func (app *application) refresh(w http.ResponseWriter, r *http.Request) {
	var input refreshRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if !validOpaqueID(input.IdempotencyKey) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "idempotencyKey must contain 16-128 characters"})
		return
	}
	_, refreshHash, err := decodeAndHashToken(input.RefreshToken)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "valid refresh token is required"})
		return
	}
	if !app.enforceAuthRateLimit(w, r, "refresh", "", base64.RawURLEncoding.EncodeToString(refreshHash[:8])) {
		return
	}
	if app.responseCipher == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "refresh recovery is unavailable"})
		return
	}

	keyHash := sha256.Sum256([]byte(input.IdempotencyKey))
	response, status, err := app.rotateRefreshToken(r.Context(), refreshHash, keyHash[:])
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "could not refresh session"})
		return
	}
	if status != http.StatusOK {
		writeJSON(w, status, errorResponse{Error: "refresh token is invalid or has been replayed"})
		return
	}
	writePrivateJSON(w, http.StatusOK, response)
}

func (app *application) logout(w http.ResponseWriter, r *http.Request) {
	_, err := app.db.Exec(
		r.Context(),
		`UPDATE sessions
		 SET revoked_at = CURRENT_TIMESTAMP
		 WHERE id = $1 AND revoked_at IS NULL`,
		authenticatedSessionID(r.Context()),
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not log out"})
		return
	}
	app.disconnectSessionWebSockets(r.Context(), authenticatedUserID(r.Context()), authenticatedSessionID(r.Context()))
	w.WriteHeader(http.StatusNoContent)
}

func (app *application) logoutAll(w http.ResponseWriter, r *http.Request) {
	_, err := app.db.Exec(
		r.Context(),
		`UPDATE sessions
		 SET revoked_at = CURRENT_TIMESTAMP
		 WHERE user_id = $1 AND revoked_at IS NULL`,
		authenticatedUserID(r.Context()),
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not log out all devices"})
		return
	}
	app.disconnectUserWebSockets(r.Context(), authenticatedUserID(r.Context()))
	w.WriteHeader(http.StatusNoContent)
}

func (app *application) listSessions(w http.ResponseWriter, r *http.Request) {
	rows, err := app.db.Query(
		r.Context(),
		`SELECT id, device_id, created_at, idle_expires_at, revoked_at
		 FROM sessions
		 WHERE user_id = $1
		 ORDER BY created_at DESC, id DESC`,
		authenticatedUserID(r.Context()),
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list sessions"})
		return
	}
	defer rows.Close()

	currentID := authenticatedSessionID(r.Context())
	sessions := make([]sessionResponse, 0)
	for rows.Next() {
		var current sessionResponse
		if err := rows.Scan(&current.ID, &current.DeviceID, &current.CreatedAt, &current.IdleExpiresAt, &current.RevokedAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list sessions"})
			return
		}
		current.Current = current.ID == currentID
		sessions = append(sessions, current)
	}
	if rows.Err() != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list sessions"})
		return
	}
	writeJSON(w, http.StatusOK, sessions)
}

func (app *application) revokeSession(w http.ResponseWriter, r *http.Request) {
	sessionID, err := strconv.ParseInt(r.PathValue("sessionID"), 10, 64)
	if err != nil || sessionID <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "sessionID must be a positive integer"})
		return
	}
	command, err := app.db.Exec(
		r.Context(),
		`UPDATE sessions
		 SET revoked_at = CURRENT_TIMESTAMP
		 WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`,
		sessionID,
		authenticatedUserID(r.Context()),
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not revoke session"})
		return
	}
	if command.RowsAffected() == 0 {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "active session was not found"})
		return
	}
	app.disconnectSessionWebSockets(r.Context(), authenticatedUserID(r.Context()), sessionID)
	w.WriteHeader(http.StatusNoContent)
}

func (app *application) changePassword(w http.ResponseWriter, r *http.Request) {
	var input changePasswordRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if !validPassword(input.NewPassword) {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "newPassword must contain 8-128 characters"})
		return
	}
	if input.CurrentPassword == input.NewPassword {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "newPassword must be different"})
		return
	}

	var currentHash string
	if err := app.db.QueryRow(r.Context(), `SELECT password_hash FROM users WHERE id = $1`, authenticatedUserID(r.Context())).Scan(&currentHash); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not change password"})
		return
	}
	matches, err := verifyPassword(input.CurrentPassword, currentHash)
	if err != nil || !matches {
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "current password is invalid"})
		return
	}
	newHash, err := hashPassword(input.NewPassword)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not change password"})
		return
	}

	tx, err := app.db.Begin(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not change password"})
		return
	}
	defer tx.Rollback(r.Context())
	command, err := tx.Exec(
		r.Context(),
		`UPDATE users SET password_hash = $1 WHERE id = $2 AND password_hash = $3`,
		newHash,
		authenticatedUserID(r.Context()),
		currentHash,
	)
	if err != nil || command.RowsAffected() != 1 {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "password changed concurrently; retry"})
		return
	}
	if _, err := tx.Exec(
		r.Context(),
		`UPDATE sessions SET revoked_at = CURRENT_TIMESTAMP WHERE user_id = $1 AND revoked_at IS NULL`,
		authenticatedUserID(r.Context()),
	); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not change password"})
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not change password"})
		return
	}
	app.disconnectUserWebSockets(r.Context(), authenticatedUserID(r.Context()))
	w.WriteHeader(http.StatusNoContent)
}

func (app *application) requireAuthentication(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fields := strings.Fields(r.Header.Get("Authorization"))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "valid bearer token is required"})
			return
		}
		_, tokenHash, err := decodeAndHashToken(fields[1])
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "valid bearer token is required"})
			return
		}

		var userID, sessionID int64
		var deviceID string
		var tokenExpiresAt time.Time
		err = app.db.QueryRow(
			r.Context(),
			`SELECT s.user_id, s.id, s.device_id, a.expires_at
			 FROM access_tokens AS a
			 JOIN sessions AS s ON s.id = a.session_id
			 WHERE a.token_hash = $1
			   AND a.expires_at > CURRENT_TIMESTAMP
			   AND s.revoked_at IS NULL
			   AND s.idle_expires_at > CURRENT_TIMESTAMP`,
			tokenHash,
		).Scan(&userID, &sessionID, &deviceID, &tokenExpiresAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "valid bearer token is required"})
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "could not authenticate request"})
			return
		}

		ctx := context.WithValue(r.Context(), authenticatedUserIDKey, userID)
		ctx = context.WithValue(ctx, authenticatedSessionIDKey, sessionID)
		ctx = context.WithValue(ctx, authenticatedDeviceIDKey, deviceID)
		ctx = context.WithValue(ctx, authenticatedTokenExpiryTimeKey, tokenExpiresAt)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (app *application) createSessionResponse(ctx context.Context, tx pgx.Tx, account user, deviceID string) (authResponse, error) {
	now := app.currentTime()
	if deviceID == "" {
		generated, err := newToken()
		if err != nil {
			return authResponse{}, err
		}
		deviceID = generated.raw
	}
	accessToken, err := newToken()
	if err != nil {
		return authResponse{}, err
	}
	refreshToken, err := newToken()
	if err != nil {
		return authResponse{}, err
	}
	accessExpiresAt := now.Add(accessTokenLifetime)
	refreshExpiresAt := now.Add(sessionIdleLifetime)

	var sessionID int64
	if err := tx.QueryRow(
		ctx,
		`INSERT INTO sessions (user_id, device_id, idle_expires_at)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		account.ID,
		deviceID,
		refreshExpiresAt,
	).Scan(&sessionID); err != nil {
		return authResponse{}, err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO access_tokens (session_id, token_hash, expires_at) VALUES ($1, $2, $3)`,
		sessionID,
		accessToken.hash,
		accessExpiresAt,
	); err != nil {
		return authResponse{}, err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO refresh_tokens (session_id, token_hash, expires_at) VALUES ($1, $2, $3)`,
		sessionID,
		refreshToken.hash,
		refreshExpiresAt,
	); err != nil {
		return authResponse{}, err
	}

	return authResponse{
		User:                  account,
		SessionID:             sessionID,
		DeviceID:              deviceID,
		AccessToken:           accessToken.raw,
		AccessTokenExpiresAt:  accessExpiresAt,
		RefreshToken:          refreshToken.raw,
		RefreshTokenExpiresAt: refreshExpiresAt,
	}, nil
}

func (app *application) createIdempotentLogin(ctx context.Context, account user, deviceID string, requestHash []byte) (authResponse, error) {
	tx, err := app.db.Begin(ctx)
	if err != nil {
		return authResponse{}, err
	}
	defer tx.Rollback(ctx)

	response, err := app.createSessionResponse(ctx, tx, account, deviceID)
	if err != nil {
		return authResponse{}, err
	}
	encrypted, nonce, err := app.responseCipher.encrypt("login", requestHash, response)
	if err != nil {
		return authResponse{}, err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO login_idempotency_results
		 (request_id_hash, user_id, session_id, encrypted_response, nonce, key_version, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		requestHash,
		account.ID,
		response.SessionID,
		encrypted,
		nonce,
		app.responseCipher.keyVersion,
		app.currentTime().Add(idempotencyLifetime),
	)
	if err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) && databaseError.Code == "23505" {
			_ = tx.Rollback(ctx)
			existing, found, loadErr := app.loadLoginResult(ctx, requestHash, account.ID)
			if loadErr != nil {
				return authResponse{}, loadErr
			}
			if !found {
				return authResponse{}, errIdempotencyConflict
			}
			return existing, nil
		}
		return authResponse{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return authResponse{}, err
	}
	return response, nil
}

func (app *application) loadLoginResult(ctx context.Context, requestHash []byte, userID int64) (authResponse, bool, error) {
	var encrypted, nonce []byte
	var keyVersion int
	err := app.db.QueryRow(
		ctx,
		`SELECT result.encrypted_response, result.nonce, result.key_version
		 FROM login_idempotency_results AS result
		 JOIN sessions AS session ON session.id = result.session_id
		 WHERE request_id_hash = $1
		   AND result.user_id = $2
		   AND result.expires_at > CURRENT_TIMESTAMP
		   AND session.revoked_at IS NULL
		   AND session.idle_expires_at > CURRENT_TIMESTAMP`,
		requestHash,
		userID,
	).Scan(&encrypted, &nonce, &keyVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return authResponse{}, false, nil
	}
	if err != nil {
		return authResponse{}, false, err
	}
	if keyVersion != app.responseCipher.keyVersion {
		return authResponse{}, false, errors.New("idempotency encryption key version is unavailable")
	}
	var response authResponse
	if err := app.responseCipher.decrypt("login", requestHash, nonce, encrypted, &response); err != nil {
		return authResponse{}, false, err
	}
	return response, true, nil
}

func (app *application) rotateRefreshToken(ctx context.Context, oldTokenHash, keyHash []byte) (authResponse, int, error) {
	tx, err := app.db.Begin(ctx)
	if err != nil {
		return authResponse{}, 0, err
	}
	defer tx.Rollback(ctx)

	var oldTokenID, sessionID, userID int64
	var deviceID string
	err = tx.QueryRow(
		ctx,
		`UPDATE refresh_tokens AS rt
		 SET consumed_at = CURRENT_TIMESTAMP
		 FROM sessions AS s
		 WHERE rt.token_hash = $1
		   AND rt.session_id = s.id
		   AND rt.consumed_at IS NULL
		   AND rt.expires_at > CURRENT_TIMESTAMP
		   AND s.revoked_at IS NULL
		   AND s.idle_expires_at > CURRENT_TIMESTAMP
		 RETURNING rt.id, rt.session_id, s.user_id, s.device_id`,
		oldTokenHash,
	).Scan(&oldTokenID, &sessionID, &userID, &deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.handleUsedOrInvalidRefresh(ctx, tx, oldTokenHash, keyHash)
	}
	if err != nil {
		return authResponse{}, 0, err
	}

	var account user
	if err := tx.QueryRow(
		ctx,
		`SELECT id, username, display_name FROM users WHERE id = $1`,
		userID,
	).Scan(&account.ID, &account.Username, &account.DisplayName); err != nil {
		return authResponse{}, 0, err
	}

	now := app.currentTime()
	accessToken, err := newToken()
	if err != nil {
		return authResponse{}, 0, err
	}
	refreshToken, err := newToken()
	if err != nil {
		return authResponse{}, 0, err
	}
	accessExpiresAt := now.Add(accessTokenLifetime)
	refreshExpiresAt := now.Add(sessionIdleLifetime)
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO access_tokens (session_id, token_hash, expires_at) VALUES ($1, $2, $3)`,
		sessionID,
		accessToken.hash,
		accessExpiresAt,
	); err != nil {
		return authResponse{}, 0, err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO refresh_tokens (session_id, parent_token_id, token_hash, expires_at)
		 VALUES ($1, $2, $3, $4)`,
		sessionID,
		oldTokenID,
		refreshToken.hash,
		refreshExpiresAt,
	); err != nil {
		return authResponse{}, 0, err
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE sessions SET idle_expires_at = $1 WHERE id = $2`,
		refreshExpiresAt,
		sessionID,
	); err != nil {
		return authResponse{}, 0, err
	}

	response := authResponse{
		User:                  account,
		SessionID:             sessionID,
		DeviceID:              deviceID,
		AccessToken:           accessToken.raw,
		AccessTokenExpiresAt:  accessExpiresAt,
		RefreshToken:          refreshToken.raw,
		RefreshTokenExpiresAt: refreshExpiresAt,
	}
	encrypted, nonce, err := app.responseCipher.encrypt("refresh", int64Bytes(oldTokenID), response)
	if err != nil {
		return authResponse{}, 0, err
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO refresh_idempotency_results
		 (refresh_token_id, idempotency_key_hash, encrypted_response, nonce, key_version, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		oldTokenID,
		keyHash,
		encrypted,
		nonce,
		app.responseCipher.keyVersion,
		now.Add(idempotencyLifetime),
	); err != nil {
		return authResponse{}, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return authResponse{}, 0, err
	}
	return response, http.StatusOK, nil
}

func (app *application) handleUsedOrInvalidRefresh(ctx context.Context, tx pgx.Tx, tokenHash, keyHash []byte) (authResponse, int, error) {
	var tokenID, sessionID, userID int64
	var consumedAt *time.Time
	var revokedAt *time.Time
	var idleExpiresAt time.Time
	err := tx.QueryRow(
		ctx,
		`SELECT rt.id, rt.session_id, s.user_id, rt.consumed_at, s.revoked_at, s.idle_expires_at
		 FROM refresh_tokens AS rt
		 JOIN sessions AS s ON s.id = rt.session_id
		 WHERE rt.token_hash = $1`,
		tokenHash,
	).Scan(&tokenID, &sessionID, &userID, &consumedAt, &revokedAt, &idleExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return authResponse{}, http.StatusUnauthorized, nil
	}
	if err != nil {
		return authResponse{}, 0, err
	}
	if consumedAt == nil || revokedAt != nil || !idleExpiresAt.After(app.currentTime()) {
		return authResponse{}, http.StatusUnauthorized, nil
	}

	var storedKeyHash, encrypted, nonce []byte
	var keyVersion int
	var expiresAt time.Time
	err = tx.QueryRow(
		ctx,
		`SELECT idempotency_key_hash, encrypted_response, nonce, key_version, expires_at
		 FROM refresh_idempotency_results
		 WHERE refresh_token_id = $1`,
		tokenID,
	).Scan(&storedKeyHash, &encrypted, &nonce, &keyVersion, &expiresAt)
	if err == nil && subtle.ConstantTimeCompare(storedKeyHash, keyHash) == 1 && expiresAt.After(app.currentTime()) {
		if keyVersion != app.responseCipher.keyVersion {
			return authResponse{}, 0, errors.New("idempotency encryption key version is unavailable")
		}
		var response authResponse
		if err := app.responseCipher.decrypt("refresh", int64Bytes(tokenID), nonce, encrypted, &response); err != nil {
			return authResponse{}, 0, err
		}
		return response, http.StatusOK, nil
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return authResponse{}, 0, err
	}

	if _, err := tx.Exec(
		ctx,
		`UPDATE sessions SET revoked_at = COALESCE(revoked_at, CURRENT_TIMESTAMP) WHERE id = $1`,
		sessionID,
	); err != nil {
		return authResponse{}, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return authResponse{}, 0, err
	}
	app.disconnectSessionWebSockets(ctx, userID, sessionID)
	return authResponse{}, http.StatusUnauthorized, nil
}

func (app *application) loadUserCredentials(ctx context.Context, username string) (user, string, error) {
	var account user
	var passwordHash string
	err := app.db.QueryRow(
		ctx,
		`SELECT id, username, display_name, password_hash FROM users WHERE username = $1`,
		username,
	).Scan(&account.ID, &account.Username, &account.DisplayName, &passwordHash)
	return account, passwordHash, err
}

func hashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(password), salt, argonIterations, argonMemory, argonParallelism, argonKeyLength)
	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		argonMemory,
		argonIterations,
		argonParallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

func verifyPassword(password, encodedHash string) (bool, error) {
	parts := strings.Split(encodedHash, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("invalid password hash")
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil || version != argon2.Version {
		return false, errors.New("invalid argon2 version")
	}
	var memory, iterations uint32
	var parallelism uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism); err != nil {
		return false, errors.New("invalid argon2 parameters")
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}
	expectedHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}
	actualHash := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, uint32(len(expectedHash)))
	return subtle.ConstantTimeCompare(actualHash, expectedHash) == 1, nil
}

func consumePasswordVerificationTime(password string) {
	salt := make([]byte, argonSaltLength)
	_ = argon2.IDKey([]byte(password), salt, argonIterations, argonMemory, argonParallelism, argonKeyLength)
}

func newToken() (generatedToken, error) {
	rawToken := make([]byte, tokenLength)
	if _, err := rand.Read(rawToken); err != nil {
		return generatedToken{}, err
	}
	hash := sha256.Sum256(rawToken)
	return generatedToken{raw: base64.RawURLEncoding.EncodeToString(rawToken), hash: hash[:]}, nil
}

func decodeAndHashToken(encoded string) ([]byte, []byte, error) {
	rawToken, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(rawToken) != tokenLength {
		return nil, nil, errors.New("invalid token")
	}
	hash := sha256.Sum256(rawToken)
	return rawToken, hash[:], nil
}

func validPassword(password string) bool {
	length := utf8.RuneCountInString(password)
	return length >= minPasswordRunes && length <= maxPasswordRunes
}

func validOpaqueID(value string) bool {
	length := len(value)
	return length >= 16 && length <= 128
}

func validOptionalDeviceID(value string) bool {
	return len(value) <= 128
}

func writePrivateJSON(w http.ResponseWriter, status int, response authResponse) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, status, response)
}

func (app *application) currentTime() time.Time {
	if app.now != nil {
		return app.now().UTC()
	}
	return time.Now().UTC()
}

func int64Bytes(value int64) []byte {
	return []byte(strconv.FormatInt(value, 10))
}

func authenticatedUserID(ctx context.Context) int64 {
	return ctx.Value(authenticatedUserIDKey).(int64)
}

func authenticatedSessionID(ctx context.Context) int64 {
	return ctx.Value(authenticatedSessionIDKey).(int64)
}

func authenticatedDeviceID(ctx context.Context) string {
	return ctx.Value(authenticatedDeviceIDKey).(string)
}

func authenticatedAccessTokenExpiry(ctx context.Context) time.Time {
	return ctx.Value(authenticatedTokenExpiryTimeKey).(time.Time)
}

func (app *application) disconnectSessionWebSockets(ctx context.Context, userID, sessionID int64) {
	if app.webSocketHub == nil && app.realtimeRouter == nil {
		return
	}
	disconnectContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	var err error
	if app.realtimeRouter != nil {
		err = app.realtimeRouter.DisconnectSession(disconnectContext, userID, sessionID)
	} else {
		err = app.webSocketHub.DisconnectSession(disconnectContext, userID, sessionID)
	}
	if err != nil && !errors.Is(err, errWebSocketHubStopped) {
		log.Printf("disconnect websocket session %d: %v", sessionID, err)
	}
}

func (app *application) disconnectUserWebSockets(ctx context.Context, userID int64) {
	if app.webSocketHub == nil && app.realtimeRouter == nil {
		return
	}
	disconnectContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	var err error
	if app.realtimeRouter != nil {
		err = app.realtimeRouter.DisconnectUser(disconnectContext, userID)
	} else {
		err = app.webSocketHub.DisconnectUser(disconnectContext, userID)
	}
	if err != nil && !errors.Is(err, errWebSocketHubStopped) {
		log.Printf("disconnect user %d websockets: %v", userID, err)
	}
}
