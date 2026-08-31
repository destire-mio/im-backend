BEGIN;

CREATE TABLE message_projection_jobs (
    event_id UUID NOT NULL REFERENCES outbox_events(event_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    shard SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    projected_at TIMESTAMPTZ,
    PRIMARY KEY (event_id, user_id),
    CONSTRAINT message_projection_jobs_shard_valid CHECK (shard BETWEEN 0 AND 255),
    CONSTRAINT message_projection_jobs_user_message_unique UNIQUE (user_id, message_id)
);

CREATE INDEX message_projection_jobs_pending_shard_idx
    ON message_projection_jobs (shard, created_at, message_id, user_id)
    WHERE projected_at IS NULL;

CREATE INDEX outbox_unready_message_projection_idx
    ON outbox_events (created_at, event_id)
    WHERE event_type = 'message.created'
      AND ready_at IS NULL
      AND published_at IS NULL
      AND dead_at IS NULL;

INSERT INTO message_projection_jobs (event_id, user_id, message_id, shard, created_at)
SELECT event.event_id,
       participant.user_id,
       event.message_id,
       mod(participant.user_id, 256)::smallint,
       event.created_at
FROM outbox_events AS event
JOIN messages AS message ON message.id = event.message_id
CROSS JOIN LATERAL (
    SELECT DISTINCT user_id
    FROM unnest(ARRAY[message.sender_id, message.receiver_id]) AS users(user_id)
) AS participant
WHERE event.event_type = 'message.created'
  AND event.ready_at IS NULL
  AND event.published_at IS NULL
  AND event.dead_at IS NULL
ON CONFLICT (event_id, user_id) DO NOTHING;

COMMIT;
