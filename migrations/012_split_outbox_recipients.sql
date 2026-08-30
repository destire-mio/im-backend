BEGIN;

ALTER TABLE outbox_events
    ADD COLUMN ready_at TIMESTAMPTZ;

CREATE TABLE outbox_recipients (
    event_id UUID NOT NULL REFERENCES outbox_events(event_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    cursor BIGINT NOT NULL,
    PRIMARY KEY (event_id, user_id),
    CONSTRAINT outbox_recipients_cursor_valid CHECK (cursor > 0),
    CONSTRAINT outbox_recipients_user_cursor_unique UNIQUE (user_id, cursor),
    CONSTRAINT outbox_recipients_sync_event_fk
        FOREIGN KEY (user_id, cursor)
        REFERENCES user_message_events(user_id, seq)
        ON DELETE CASCADE
);

-- Existing version-1/version-2 events already have authoritative Sync rows
-- after migration 010. Backfill the structured realtime recipients from that
-- source instead of trusting duplicated JSONB payload data.
INSERT INTO outbox_recipients (event_id, user_id, cursor)
SELECT event.event_id, sync_event.user_id, sync_event.seq
FROM outbox_events AS event
JOIN user_message_events AS sync_event
  ON sync_event.message_id = event.message_id
WHERE event.event_type = 'message.created';

-- A message is ready only when every distinct participant has a Sync row.
-- Historical timestamps are not recoverable, so unpublished backfilled rows
-- use migration time while published rows use their durable publish time.
UPDATE outbox_events AS event
SET ready_at = COALESCE(event.published_at, CURRENT_TIMESTAMP)
FROM messages AS message
WHERE event.message_id = message.id
  AND event.event_type = 'message.created'
  AND (
      SELECT count(*)
      FROM outbox_recipients AS recipient
      WHERE recipient.event_id = event.event_id
  ) = CASE
      WHEN message.sender_id = message.receiver_id THEN 1
      ELSE 2
  END;

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_ready_time_valid
        CHECK (ready_at IS NULL OR ready_at >= created_at),
    ADD CONSTRAINT outbox_message_publish_requires_ready
        CHECK (
            event_type <> 'message.created'
            OR published_at IS NULL
            OR ready_at IS NOT NULL
        );

COMMIT;
