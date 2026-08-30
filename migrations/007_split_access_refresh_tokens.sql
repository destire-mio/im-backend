BEGIN;

ALTER TABLE sessions
    RENAME COLUMN expires_at TO idle_expires_at;

ALTER TABLE sessions
    DROP CONSTRAINT sessions_token_hash_valid;

CREATE TABLE access_tokens (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    token_hash BYTEA NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT access_tokens_hash_valid CHECK (octet_length(token_hash) = 32),
    CONSTRAINT access_tokens_expiry_valid CHECK (expires_at > created_at)
);

-- Existing 30-day bearer tokens get only a short compatibility window.
INSERT INTO access_tokens (session_id, token_hash, expires_at)
SELECT id, token_hash, LEAST(idle_expires_at, CURRENT_TIMESTAMP + INTERVAL '15 minutes')
FROM sessions
WHERE revoked_at IS NULL
  AND idle_expires_at > CURRENT_TIMESTAMP;

ALTER TABLE sessions
    DROP COLUMN token_hash;

CREATE INDEX access_tokens_session_id_idx ON access_tokens (session_id);

CREATE TABLE refresh_tokens (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    parent_token_id BIGINT REFERENCES refresh_tokens(id) ON DELETE SET NULL,
    token_hash BYTEA NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT refresh_tokens_hash_valid CHECK (octet_length(token_hash) = 32),
    CONSTRAINT refresh_tokens_expiry_valid CHECK (expires_at > created_at),
    CONSTRAINT refresh_tokens_consumed_valid CHECK (consumed_at IS NULL OR consumed_at >= created_at)
);

CREATE INDEX refresh_tokens_session_id_created_at_idx ON refresh_tokens (session_id, created_at DESC);

CREATE TABLE login_idempotency_results (
    request_id_hash BYTEA PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id BIGINT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    encrypted_response BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    key_version INTEGER NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT login_idempotency_request_hash_valid CHECK (octet_length(request_id_hash) = 32),
    CONSTRAINT login_idempotency_expiry_valid CHECK (expires_at > created_at)
);

CREATE TABLE refresh_idempotency_results (
    refresh_token_id BIGINT PRIMARY KEY REFERENCES refresh_tokens(id) ON DELETE CASCADE,
    idempotency_key_hash BYTEA NOT NULL,
    encrypted_response BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    key_version INTEGER NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT refresh_idempotency_key_hash_valid CHECK (octet_length(idempotency_key_hash) = 32),
    CONSTRAINT refresh_idempotency_expiry_valid CHECK (expires_at > created_at)
);

COMMIT;
