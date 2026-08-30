BEGIN;

ALTER TABLE sessions
    ADD COLUMN device_id TEXT;

UPDATE sessions
SET device_id = 'legacy-session-' || id;

ALTER TABLE sessions
    ALTER COLUMN device_id SET NOT NULL,
    ADD CONSTRAINT sessions_device_id_valid CHECK (
        char_length(device_id) BETWEEN 1 AND 128
    );

CREATE INDEX sessions_user_id_device_id_idx
    ON sessions (user_id, device_id);

CREATE TABLE device_sync_states (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    applied_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, device_id),
    CONSTRAINT device_sync_states_device_id_valid CHECK (
        char_length(device_id) BETWEEN 1 AND 128
    ),
    CONSTRAINT device_sync_states_applied_seq_valid CHECK (applied_seq >= 0)
);

COMMIT;
