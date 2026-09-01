-- Expand/contract migration for conversation-scoped message recovery.
--
-- Run this migration with message writers stopped. The backfill rewrites the
-- existing messages table to assign a stable sequence inside every direct
-- conversation. Version-3 Outbox and user-level Sync tables are deliberately
-- retained so the new binary can drain pre-deployment events. This does not
-- make pre-015 data immediately visible in the conversation stream. The new
-- cursor columns intentionally remain nullable during the rollback window so a
-- pre-015 writer can still insert messages. Before the conversation-aware
-- binary is started again, reconcile those rows with im-migrate.

BEGIN;

CREATE TABLE conversations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind TEXT NOT NULL DEFAULT 'direct',
    direct_user_low_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    direct_user_high_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    last_seq BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT conversations_kind_valid CHECK (kind IN ('direct', 'group')),
    CONSTRAINT conversations_last_seq_valid CHECK (last_seq >= 0),
    CONSTRAINT conversations_updated_at_valid CHECK (updated_at >= created_at),
    CONSTRAINT conversations_direct_pair_valid CHECK (
        (
            kind = 'direct'
            AND direct_user_low_id IS NOT NULL
            AND direct_user_high_id IS NOT NULL
            AND direct_user_low_id <= direct_user_high_id
        )
        OR (
            kind = 'group'
            AND direct_user_low_id IS NULL
            AND direct_user_high_id IS NULL
        )
    ),
    CONSTRAINT conversations_direct_pair_unique UNIQUE (
        direct_user_low_id,
        direct_user_high_id
    )
);

CREATE TABLE conversation_members (
    conversation_id BIGINT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (conversation_id, user_id)
);

CREATE INDEX conversation_members_user_conversation_idx
    ON conversation_members (user_id, conversation_id);

ALTER TABLE messages
    ADD COLUMN conversation_id BIGINT,
    ADD COLUMN conversation_seq BIGINT;

INSERT INTO conversations (
    kind,
    direct_user_low_id,
    direct_user_high_id,
    created_at,
    updated_at
)
SELECT 'direct',
       LEAST(sender_id, receiver_id),
       GREATEST(sender_id, receiver_id),
       min(created_at),
       max(created_at)
FROM messages
GROUP BY LEAST(sender_id, receiver_id), GREATEST(sender_id, receiver_id);

INSERT INTO conversation_members (conversation_id, user_id, joined_at)
SELECT conversation.id, conversation.direct_user_low_id, conversation.created_at
FROM conversations AS conversation
WHERE conversation.kind = 'direct'
UNION ALL
SELECT conversation.id, conversation.direct_user_high_id, conversation.created_at
FROM conversations AS conversation
WHERE conversation.kind = 'direct'
  AND conversation.direct_user_high_id <> conversation.direct_user_low_id;

WITH ranked AS (
    SELECT message.id,
           conversation.id AS conversation_id,
           row_number() OVER (
               PARTITION BY conversation.id
               ORDER BY message.created_at, message.id
           ) AS conversation_seq
    FROM messages AS message
    JOIN conversations AS conversation
      ON conversation.kind = 'direct'
     AND conversation.direct_user_low_id = LEAST(message.sender_id, message.receiver_id)
     AND conversation.direct_user_high_id = GREATEST(message.sender_id, message.receiver_id)
)
UPDATE messages AS message
SET conversation_id = ranked.conversation_id,
    conversation_seq = ranked.conversation_seq
FROM ranked
WHERE message.id = ranked.id;

UPDATE conversations AS conversation
SET last_seq = summary.last_seq,
    updated_at = summary.updated_at
FROM (
    SELECT message.conversation_id,
           max(message.conversation_seq) AS last_seq,
           max(message.created_at) AS updated_at
    FROM messages AS message
    GROUP BY message.conversation_id
) AS summary
WHERE conversation.id = summary.conversation_id;

-- Unpublished legacy events may have been encoded before messages carried a
-- conversation cursor. Enrich their durable payload so a post-migration
-- WebSocket notification points clients at the same recovery stream.
UPDATE outbox_events AS event
SET payload = CASE event.payload_version
    WHEN 1 THEN jsonb_set(
        jsonb_set(event.payload, '{conversationId}', to_jsonb(message.conversation_id), true),
        '{conversationSeq}',
        to_jsonb(message.conversation_seq),
        true
    )
    WHEN 2 THEN jsonb_set(
        jsonb_set(event.payload, '{message,conversationId}', to_jsonb(message.conversation_id), true),
        '{message,conversationSeq}',
        to_jsonb(message.conversation_seq),
        true
    )
    WHEN 3 THEN jsonb_set(
        jsonb_set(event.payload, '{message,conversationId}', to_jsonb(message.conversation_id), true),
        '{message,conversationSeq}',
        to_jsonb(message.conversation_seq),
        true
    )
    ELSE event.payload
END
FROM messages AS message
WHERE event.message_id = message.id
  AND event.event_type = 'message.created'
  AND event.payload_version IN (1, 2, 3)
  AND event.published_at IS NULL
  AND event.dead_at IS NULL;

ALTER TABLE messages
    ADD CONSTRAINT messages_conversation_fk
        FOREIGN KEY (conversation_id) REFERENCES conversations(id),
    ADD CONSTRAINT messages_conversation_seq_valid
        CHECK (conversation_seq > 0),
    ADD CONSTRAINT messages_conversation_seq_unique
        UNIQUE (conversation_id, conversation_seq);

-- Keeps the application startup gate and rollback reconciliation bounded even
-- when the messages table is large. The index is empty during normal operation
-- and receives only rows written by a pre-015 binary.
CREATE INDEX messages_missing_conversation_cursor_idx
    ON messages (id)
    WHERE conversation_id IS NULL OR conversation_seq IS NULL;

CREATE TABLE device_conversation_sync_states (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL,
    conversation_id BIGINT NOT NULL,
    applied_seq BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, device_id, conversation_id),
    CONSTRAINT device_conversation_sync_states_membership_fk
        FOREIGN KEY (conversation_id, user_id)
        REFERENCES conversation_members(conversation_id, user_id)
        ON DELETE CASCADE,
    CONSTRAINT device_conversation_sync_states_device_id_valid CHECK (
        char_length(device_id) BETWEEN 1 AND 128
    ),
    CONSTRAINT device_conversation_sync_states_applied_seq_valid CHECK (applied_seq >= 0)
);

COMMIT;
