BEGIN;

CREATE TABLE user_sync_counters (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    last_seq BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT user_sync_counters_last_seq_valid CHECK (last_seq >= 0)
);

CREATE TABLE user_message_events (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    seq BIGINT NOT NULL,
    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, seq),
    CONSTRAINT user_message_events_seq_valid CHECK (seq > 0),
    CONSTRAINT user_message_events_message_unique UNIQUE (user_id, message_id)
);

CREATE INDEX user_message_events_message_id_idx
    ON user_message_events (message_id);

WITH participant_messages AS (
    SELECT sender_id AS user_id, id AS message_id, created_at
    FROM messages
    UNION
    SELECT receiver_id AS user_id, id AS message_id, created_at
    FROM messages
), numbered AS (
    SELECT user_id,
           row_number() OVER (PARTITION BY user_id ORDER BY created_at, message_id) AS seq,
           message_id,
           created_at
    FROM participant_messages
)
INSERT INTO user_message_events (user_id, seq, message_id, created_at)
SELECT user_id, seq, message_id, created_at
FROM numbered;

INSERT INTO user_sync_counters (user_id, last_seq)
SELECT user_id, max(seq)
FROM user_message_events
GROUP BY user_id;

COMMIT;
