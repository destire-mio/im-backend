BEGIN;

ALTER TABLE messages
    ADD COLUMN client_message_id TEXT;

-- Historical development rows did not originate from an idempotent client request.
UPDATE messages
SET client_message_id = 'legacy-' || lpad(id::text, 16, '0');

ALTER TABLE messages
    ALTER COLUMN client_message_id SET NOT NULL,
    ADD CONSTRAINT messages_client_message_id_valid CHECK (
        char_length(client_message_id) BETWEEN 16 AND 128
    ),
    ADD CONSTRAINT messages_sender_client_id_unique UNIQUE (sender_id, client_message_id);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    payload_version SMALLINT NOT NULL,
    message_id BIGINT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT outbox_event_type_valid CHECK (char_length(event_type) BETWEEN 1 AND 100),
    CONSTRAINT outbox_payload_version_valid CHECK (payload_version > 0),
    CONSTRAINT outbox_message_event_unique UNIQUE (message_id, event_type)
);

COMMIT;
