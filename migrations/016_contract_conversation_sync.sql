-- End the pre-conversation rollback window. Stop API writers and Outbox workers
-- before applying this migration to a populated database. Historical migration
-- files stay immutable; old binaries cannot run against the contracted schema.
BEGIN;

LOCK TABLE messages, conversations, conversation_members, outbox_events
    IN ACCESS EXCLUSIVE MODE;

DO $contract$
BEGIN
    IF EXISTS (
        SELECT 1 FROM messages
        WHERE (conversation_id IS NULL) <> (conversation_seq IS NULL)
    ) THEN
        RAISE EXCEPTION 'migration 016: partial conversation cursor requires manual repair';
    END IF;
    IF EXISTS (
        SELECT 1 FROM outbox_events
        WHERE event_type = 'message.created'
          AND published_at IS NULL AND dead_at IS NULL
          AND payload_version NOT IN (1, 2, 3, 4)
    ) THEN
        RAISE EXCEPTION 'migration 016: unsupported pending message payload version';
    END IF;
END
$contract$;

-- An old writer may have inserted messages after 015. Repair those rows once,
-- appending after existing conversation cursors without renumbering any of them.
INSERT INTO conversations (
    kind, direct_user_low_id, direct_user_high_id, created_at, updated_at
)
SELECT 'direct', LEAST(sender_id, receiver_id), GREATEST(sender_id, receiver_id),
       min(created_at), max(created_at)
FROM messages
WHERE conversation_id IS NULL
GROUP BY LEAST(sender_id, receiver_id), GREATEST(sender_id, receiver_id)
ON CONFLICT (direct_user_low_id, direct_user_high_id) DO NOTHING;

INSERT INTO conversation_members (conversation_id, user_id, joined_at)
SELECT DISTINCT conversation.id, participant.user_id,
       LEAST(conversation.created_at, message.created_at)
FROM messages AS message
JOIN conversations AS conversation
  ON conversation.kind = 'direct'
 AND conversation.direct_user_low_id = LEAST(message.sender_id, message.receiver_id)
 AND conversation.direct_user_high_id = GREATEST(message.sender_id, message.receiver_id)
CROSS JOIN LATERAL (
    SELECT message.sender_id AS user_id
    UNION
    SELECT message.receiver_id AS user_id
) AS participant
WHERE message.conversation_id IS NULL
ON CONFLICT (conversation_id, user_id) DO NOTHING;

WITH ranked AS (
    SELECT message.id, conversation.id AS conversation_id,
           conversation.last_seq + row_number() OVER (
               PARTITION BY conversation.id ORDER BY message.created_at, message.id
           ) AS conversation_seq
    FROM messages AS message
    JOIN conversations AS conversation
      ON conversation.kind = 'direct'
     AND conversation.direct_user_low_id = LEAST(message.sender_id, message.receiver_id)
     AND conversation.direct_user_high_id = GREATEST(message.sender_id, message.receiver_id)
    WHERE message.conversation_id IS NULL
)
UPDATE messages AS message
SET conversation_id = ranked.conversation_id,
    conversation_seq = ranked.conversation_seq
FROM ranked
WHERE message.id = ranked.id;

UPDATE conversations AS conversation
SET last_seq = GREATEST(conversation.last_seq, summary.last_seq),
    created_at = LEAST(conversation.created_at, summary.created_at),
    updated_at = GREATEST(conversation.updated_at, summary.updated_at)
FROM (
    SELECT conversation_id, max(conversation_seq) AS last_seq,
           min(created_at) AS created_at, max(created_at) AS updated_at
    FROM messages GROUP BY conversation_id
) AS summary
WHERE conversation.id = summary.conversation_id;

ALTER TABLE messages
    ALTER COLUMN conversation_id SET NOT NULL,
    ALTER COLUMN conversation_seq SET NOT NULL;

-- Rebuild pending legacy payloads from authoritative messages, not from partial
-- projection state. Preserve event identity, retry accounting and trace context.
-- Terminal events remain unchanged as historical records, not replayable work.
UPDATE outbox_events AS event
SET payload_version = 4,
    payload = jsonb_strip_nulls(jsonb_build_object(
        'message', jsonb_build_object(
            'id', message.id,
            'conversationId', message.conversation_id,
            'conversationSeq', message.conversation_seq,
            'clientMessageId', message.client_message_id,
            'senderId', message.sender_id,
            'receiverId', message.receiver_id,
            'content', message.content,
            'createdAt', message.created_at
        ),
        'recipients', CASE WHEN message.sender_id = message.receiver_id
            THEN jsonb_build_array(jsonb_build_object('userId', message.sender_id))
            ELSE jsonb_build_array(
                jsonb_build_object('userId', message.sender_id),
                jsonb_build_object('userId', message.receiver_id)
            )
        END,
        'traceParent', event.payload -> 'traceParent',
        'traceState', event.payload -> 'traceState'
    )),
    ready_at = COALESCE(event.ready_at, CURRENT_TIMESTAMP),
    locked_until = NULL,
    lock_token = NULL
FROM messages AS message
WHERE event.message_id = message.id
  AND event.event_type = 'message.created'
  AND event.payload_version IN (1, 2, 3)
  AND event.published_at IS NULL AND event.dead_at IS NULL;

ALTER TABLE outbox_events ADD CONSTRAINT outbox_message_pending_v4_ready CHECK (
    event_type <> 'message.created'
    OR published_at IS NOT NULL OR dead_at IS NOT NULL
    OR (payload_version = 4 AND ready_at IS NOT NULL)
);

DROP TABLE outbox_recipients;
DROP TABLE message_projection_jobs;
DROP TABLE device_sync_states;
DROP TABLE user_message_events;
DROP TABLE user_sync_counters;
DROP INDEX outbox_unready_message_projection_idx;
DROP INDEX messages_missing_conversation_cursor_idx;
DROP INDEX messages_direction_history_idx;
DROP STATISTICS messages_sender_receiver_stats;

COMMIT;
