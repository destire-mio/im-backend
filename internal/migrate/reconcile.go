package migrate

import (
	"context"
	"errors"
	"fmt"
)

// ReconcileConversations repairs messages inserted by a pre-015 writer during
// the rollback window. Writers must be stopped: conversation sequence numbers
// are allocated after each conversation's existing cursor and become durable
// client cursors once committed.
func (runner *Runner) ReconcileConversations(ctx context.Context) (int64, error) {
	if runner.Conn == nil {
		return 0, errors.New("migration database connection is required")
	}
	if !runner.AllowMaintenance {
		return 0, ErrMaintenanceRequired
	}
	migrations, err := Load(runner.Files)
	if err != nil {
		return 0, err
	}

	if err := runner.acquireLock(ctx); err != nil {
		return 0, err
	}
	defer runner.releaseLock(ctx)

	var historyExists bool
	if err := runner.Conn.QueryRow(
		ctx,
		`SELECT to_regclass('public.schema_migrations') IS NOT NULL`,
	).Scan(&historyExists); err != nil {
		return 0, fmt.Errorf("inspect migration history: %w", err)
	}
	if !historyExists {
		return 0, fmt.Errorf("reconcile requires migration 015 history: %w", ErrSchemaNotReady)
	}
	applied, err := runner.loadApplied(ctx)
	if err != nil {
		return 0, err
	}
	if err := validateReady(migrations, applied); err != nil {
		return 0, fmt.Errorf("reconcile requires the current migration set: %w", err)
	}

	tx, err := runner.Conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin conversation reconciliation: %w", err)
	}
	defer tx.Rollback(ctx)

	var partialCursorExists bool
	if err := tx.QueryRow(
		ctx,
		`SELECT EXISTS (
		     SELECT 1
		     FROM messages
		     WHERE (conversation_id IS NULL) <> (conversation_seq IS NULL)
		 )`,
	).Scan(&partialCursorExists); err != nil {
		return 0, fmt.Errorf("inspect partial conversation cursors: %w", err)
	}
	if partialCursorExists {
		return 0, errors.New("message has only one conversation cursor field; manual repair is required")
	}

	var repaired int64
	if err := tx.QueryRow(
		ctx,
		`SELECT count(*) FROM messages WHERE conversation_id IS NULL`,
	).Scan(&repaired); err != nil {
		return 0, fmt.Errorf("count messages requiring reconciliation: %w", err)
	}
	if repaired == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("commit empty conversation reconciliation: %w", err)
		}
		return 0, nil
	}

	if _, err := tx.Exec(ctx, `
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
WHERE conversation_id IS NULL
GROUP BY LEAST(sender_id, receiver_id), GREATEST(sender_id, receiver_id)
ON CONFLICT (direct_user_low_id, direct_user_high_id) DO NOTHING`); err != nil {
		return 0, fmt.Errorf("create missing conversations: %w", err)
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO conversation_members (conversation_id, user_id, joined_at)
SELECT DISTINCT conversation.id,
       participant.user_id,
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
ON CONFLICT (conversation_id, user_id) DO NOTHING`); err != nil {
		return 0, fmt.Errorf("create missing conversation members: %w", err)
	}

	if _, err := tx.Exec(ctx, `
WITH ranked AS (
    SELECT message.id,
           conversation.id AS conversation_id,
           conversation.last_seq + row_number() OVER (
               PARTITION BY conversation.id
               ORDER BY message.created_at, message.id
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
WHERE message.id = ranked.id`); err != nil {
		return 0, fmt.Errorf("assign conversation cursors: %w", err)
	}

	if _, err := tx.Exec(ctx, `
UPDATE conversations AS conversation
SET last_seq = summary.last_seq,
    created_at = LEAST(conversation.created_at, summary.created_at),
    updated_at = GREATEST(conversation.updated_at, summary.updated_at)
FROM (
    SELECT message.conversation_id,
           max(message.conversation_seq) AS last_seq,
           min(message.created_at) AS created_at,
           max(message.created_at) AS updated_at
    FROM messages AS message
    GROUP BY message.conversation_id
) AS summary
WHERE conversation.id = summary.conversation_id`); err != nil {
		return 0, fmt.Errorf("advance conversation cursors: %w", err)
	}

	if _, err := tx.Exec(ctx, `
UPDATE outbox_events AS event
SET payload = CASE event.payload_version
    WHEN 1 THEN jsonb_set(
        jsonb_set(event.payload, '{conversationId}', to_jsonb(message.conversation_id), true),
        '{conversationSeq}', to_jsonb(message.conversation_seq), true
    )
    WHEN 2 THEN jsonb_set(
        jsonb_set(event.payload, '{message,conversationId}', to_jsonb(message.conversation_id), true),
        '{message,conversationSeq}', to_jsonb(message.conversation_seq), true
    )
    WHEN 3 THEN jsonb_set(
        jsonb_set(event.payload, '{message,conversationId}', to_jsonb(message.conversation_id), true),
        '{message,conversationSeq}', to_jsonb(message.conversation_seq), true
    )
    ELSE event.payload
END
FROM messages AS message
WHERE event.message_id = message.id
  AND event.event_type = 'message.created'
  AND event.payload_version IN (1, 2, 3)
  AND event.published_at IS NULL
  AND event.dead_at IS NULL`); err != nil {
		return 0, fmt.Errorf("enrich pending message events: %w", err)
	}

	var remaining int64
	if err := tx.QueryRow(
		ctx,
		`SELECT count(*)
		 FROM messages
		 WHERE conversation_id IS NULL OR conversation_seq IS NULL`,
	).Scan(&remaining); err != nil {
		return 0, fmt.Errorf("verify conversation reconciliation: %w", err)
	}
	if remaining != 0 {
		return 0, fmt.Errorf("conversation reconciliation left %d messages without cursors", remaining)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit conversation reconciliation: %w", err)
	}
	return repaired, nil
}
