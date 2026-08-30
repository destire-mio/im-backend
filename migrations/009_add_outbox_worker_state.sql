BEGIN;

ALTER TABLE outbox_events
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN locked_until TIMESTAMPTZ,
    ADD COLUMN lock_token UUID,
    ADD COLUMN published_at TIMESTAMPTZ,
    ADD COLUMN dead_at TIMESTAMPTZ,
    ADD COLUMN last_error TEXT,
    ADD CONSTRAINT outbox_attempt_count_valid CHECK (attempt_count >= 0),
    ADD CONSTRAINT outbox_next_attempt_valid CHECK (next_attempt_at >= created_at),
    ADD CONSTRAINT outbox_lock_pair_valid CHECK ((locked_until IS NULL) = (lock_token IS NULL)),
    ADD CONSTRAINT outbox_published_time_valid CHECK (published_at IS NULL OR published_at >= created_at),
    ADD CONSTRAINT outbox_dead_time_valid CHECK (dead_at IS NULL OR dead_at >= created_at),
    ADD CONSTRAINT outbox_single_terminal_state CHECK (published_at IS NULL OR dead_at IS NULL),
    ADD CONSTRAINT outbox_last_error_valid CHECK (last_error IS NULL OR char_length(last_error) <= 2000);

CREATE INDEX outbox_pending_idx
    ON outbox_events (next_attempt_at, created_at, event_id)
    WHERE published_at IS NULL AND dead_at IS NULL;

COMMIT;
