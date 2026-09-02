package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errMessageReceiverNotFound = errors.New("message receiver not found")

type messageStore struct {
	db *pgxpool.Pool
}

type messageStoreTransaction struct {
	tx pgx.Tx
}

func (store *messageStore) begin(ctx context.Context) (*messageStoreTransaction, error) {
	tx, err := store.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &messageStoreTransaction{tx: tx}, nil
}

func (tx *messageStoreTransaction) commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *messageStoreTransaction) rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
}

func (tx *messageStoreTransaction) findIdempotentMessage(
	ctx context.Context,
	senderID int64,
	clientMessageID string,
) (message, bool, error) {
	return scanIdempotentMessage(tx.tx.QueryRow(ctx, idempotentMessageQuery, senderID, clientMessageID))
}

func (store *messageStore) findCommittedIdempotentMessage(
	ctx context.Context,
	senderID int64,
	clientMessageID string,
) (message, error) {
	found, exists, err := scanIdempotentMessage(store.db.QueryRow(ctx, idempotentMessageQuery, senderID, clientMessageID))
	if err != nil {
		return message{}, err
	}
	if !exists {
		return message{}, pgx.ErrNoRows
	}
	return found, nil
}

const idempotentMessageQuery = `SELECT id,
       conversation_id,
       conversation_seq,
       client_message_id,
       sender_id,
       receiver_id,
       content,
       created_at
FROM messages
WHERE sender_id = $1 AND client_message_id = $2`

type messageRow interface {
	Scan(dest ...any) error
}

func scanIdempotentMessage(row messageRow) (message, bool, error) {
	var found message
	err := row.Scan(
		&found.ID,
		&found.ConversationID,
		&found.ConversationSeq,
		&found.ClientMessageID,
		&found.SenderID,
		&found.ReceiverID,
		&found.Content,
		&found.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return message{}, false, nil
	}
	if err != nil {
		return message{}, false, err
	}
	return found, true, nil
}

func (tx *messageStoreTransaction) resolveDirectConversation(
	ctx context.Context,
	lowUserID int64,
	highUserID int64,
) (int64, bool, error) {
	conversationID, created, err := resolveDirectConversation(ctx, tx.tx, lowUserID, highUserID)
	if isForeignKeyViolation(err) {
		return 0, false, errMessageReceiverNotFound
	}
	return conversationID, created, err
}

func resolveDirectConversation(
	ctx context.Context,
	tx pgx.Tx,
	lowUserID int64,
	highUserID int64,
) (int64, bool, error) {
	var conversationID int64
	err := tx.QueryRow(
		ctx,
		`SELECT id
		 FROM conversations
		 WHERE kind = 'direct'
		   AND direct_user_low_id = $1
		   AND direct_user_high_id = $2`,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	if err == nil {
		return conversationID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}

	err = tx.QueryRow(
		ctx,
		`INSERT INTO conversations (kind, direct_user_low_id, direct_user_high_id)
		 VALUES ('direct', $1, $2)
		 ON CONFLICT (direct_user_low_id, direct_user_high_id) DO NOTHING
		 RETURNING id`,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	if err == nil {
		return conversationID, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}

	err = tx.QueryRow(
		ctx,
		`SELECT id
		 FROM conversations
		 WHERE kind = 'direct'
		   AND direct_user_low_id = $1
		   AND direct_user_high_id = $2`,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	if err != nil {
		return 0, false, err
	}
	return conversationID, false, nil
}

func (tx *messageStoreTransaction) insertConversationMembers(
	ctx context.Context,
	conversationID int64,
	senderID int64,
	receiverID int64,
) error {
	_, err := tx.tx.Exec(
		ctx,
		`INSERT INTO conversation_members (conversation_id, user_id)
		 SELECT $1, participant.user_id
		 FROM (
		     SELECT DISTINCT user_id
		     FROM unnest($2::bigint[]) AS users(user_id)
		 ) AS participant`,
		conversationID,
		[]int64{senderID, receiverID},
	)
	if isForeignKeyViolation(err) {
		return errMessageReceiverNotFound
	}
	return err
}

func (tx *messageStoreTransaction) insertMessageAndOutbox(
	ctx context.Context,
	conversationID int64,
	senderID int64,
	receiverID int64,
	clientMessageID string,
	content string,
) (message, bool, error) {
	var created message
	err := tx.tx.QueryRow(
		ctx,
		`WITH allocated AS (
		   UPDATE conversations
		   SET last_seq = last_seq + 1,
		       updated_at = GREATEST(updated_at, clock_timestamp())
		   WHERE id = $1
		   RETURNING last_seq, updated_at
		 ), inserted AS (
		   INSERT INTO messages (
		       conversation_id,
		       conversation_seq,
		       sender_id,
		       receiver_id,
		       client_message_id,
		       content,
		       created_at
		   )
		   SELECT $1, allocated.last_seq, $2, $3, $4, $5, allocated.updated_at
		   FROM allocated
		   ON CONFLICT (sender_id, client_message_id) DO NOTHING
		   RETURNING id,
		             conversation_id,
		             conversation_seq,
		             client_message_id,
		             sender_id,
		             receiver_id,
		             content,
		             created_at
		 ), created_event AS (
		   INSERT INTO outbox_events (
		       event_type,
		       payload_version,
		       message_id,
		       payload,
		       ready_at
		   )
		   SELECT 'message.created',
		          4,
		          inserted.id,
		          jsonb_build_object(
		              'message', jsonb_build_object(
		                  'id', inserted.id,
		                  'conversationId', inserted.conversation_id,
		                  'conversationSeq', inserted.conversation_seq,
		                  'clientMessageId', inserted.client_message_id,
		                  'senderId', inserted.sender_id,
		                  'receiverId', inserted.receiver_id,
		                  'content', inserted.content,
		                  'createdAt', inserted.created_at
		              ),
		              'recipients', CASE WHEN inserted.sender_id = inserted.receiver_id
		                  THEN jsonb_build_array(jsonb_build_object('userId', inserted.sender_id))
		                  ELSE jsonb_build_array(
		                      jsonb_build_object('userId', inserted.sender_id),
		                      jsonb_build_object('userId', inserted.receiver_id)
		                  )
		              END
		          ),
		          inserted.created_at
		   FROM inserted
		   RETURNING message_id
		 )
		 SELECT inserted.id,
		        inserted.conversation_id,
		        inserted.conversation_seq,
		        inserted.client_message_id,
		        inserted.sender_id,
		        inserted.receiver_id,
		        inserted.content,
		        inserted.created_at
		 FROM inserted
		 JOIN created_event ON created_event.message_id = inserted.id`,
		conversationID,
		senderID,
		receiverID,
		clientMessageID,
		content,
	).Scan(
		&created.ID,
		&created.ConversationID,
		&created.ConversationSeq,
		&created.ClientMessageID,
		&created.SenderID,
		&created.ReceiverID,
		&created.Content,
		&created.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return message{}, false, nil
	}
	if err != nil {
		return message{}, false, err
	}
	return created, true, nil
}

func isForeignKeyViolation(err error) bool {
	var databaseError *pgconn.PgError
	return errors.As(err, &databaseError) && databaseError.Code == "23503"
}
