BEGIN;

ALTER TABLE users
    RENAME COLUMN name TO display_name;

ALTER TABLE users
    RENAME CONSTRAINT users_name_valid TO users_display_name_valid;

ALTER TABLE users
    ADD COLUMN username TEXT,
    ADD COLUMN password_hash TEXT;

-- Existing development users are preserved but intentionally cannot log in.
UPDATE users
SET username = 'legacy_' || id,
    password_hash = '!legacy-disabled!';

ALTER TABLE users
    ALTER COLUMN username SET NOT NULL,
    ALTER COLUMN password_hash SET NOT NULL,
    ADD CONSTRAINT users_username_valid CHECK (
        username = lower(btrim(username))
        AND username ~ '^[a-z0-9_]{3,32}$'
    ),
    ADD CONSTRAINT users_password_hash_valid CHECK (
        char_length(password_hash) BETWEEN 1 AND 512
    );

CREATE UNIQUE INDEX users_username_unique ON users (username);

CREATE TABLE sessions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash BYTEA NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sessions_token_hash_valid CHECK (octet_length(token_hash) = 32),
    CONSTRAINT sessions_expiry_valid CHECK (expires_at > created_at),
    CONSTRAINT sessions_revocation_valid CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

CREATE INDEX sessions_user_id_created_at_idx ON sessions (user_id, created_at DESC);

COMMIT;
