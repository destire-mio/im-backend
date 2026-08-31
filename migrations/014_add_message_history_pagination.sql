-- This migration intentionally has no BEGIN/COMMIT wrapper because PostgreSQL
-- does not allow CREATE INDEX CONCURRENTLY inside a transaction block.

CREATE INDEX CONCURRENTLY messages_direction_history_idx
    ON messages (sender_id, receiver_id, created_at DESC, id DESC);

CREATE STATISTICS messages_sender_receiver_stats (dependencies, mcv)
    ON sender_id, receiver_id
    FROM messages;

ALTER STATISTICS messages_sender_receiver_stats SET STATISTICS 1000;

ANALYZE messages;
