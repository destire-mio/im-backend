-- Historical baseline: the project began with a single durable messages table.
-- Later migrations add users, validation, idempotency, Outbox, Sync, and
-- conversation-scoped recovery in their original order.

BEGIN;

CREATE TABLE messages (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sender_id BIGINT NOT NULL,
    receiver_id BIGINT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
