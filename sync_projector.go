package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type syncProjectionMode string
type syncProjectionStorage string

const (
	syncProjectionModePerUser syncProjectionMode = "per_user"
	syncProjectionModeBulk    syncProjectionMode = "bulk"

	syncProjectionStorageJSONB      syncProjectionStorage = "jsonb"
	syncProjectionStorageRecipients syncProjectionStorage = "recipients"
	syncProjectionStorageSyncEvents syncProjectionStorage = "sync_events"
)

func normalizeSyncProjectionMode(mode syncProjectionMode) (syncProjectionMode, error) {
	switch mode {
	case "", syncProjectionModePerUser:
		return syncProjectionModePerUser, nil
	case syncProjectionModeBulk:
		return syncProjectionModeBulk, nil
	default:
		return "", fmt.Errorf("unsupported outbox projection mode %q", mode)
	}
}

func normalizeSyncProjectionStorage(storage syncProjectionStorage) (syncProjectionStorage, error) {
	switch storage {
	case "", syncProjectionStorageRecipients:
		return syncProjectionStorageRecipients, nil
	case syncProjectionStorageJSONB:
		return syncProjectionStorageJSONB, nil
	case syncProjectionStorageSyncEvents:
		return syncProjectionStorageSyncEvents, nil
	default:
		return "", fmt.Errorf("unsupported outbox projection storage %q", storage)
	}
}

// messageSyncProjector makes durable message.created events publishable only
// after their Sync rows and recoverable realtime routing state commit atomically.
type messageSyncProjector struct {
	db      *pgxpool.Pool
	metrics *applicationMetrics
	mode    syncProjectionMode
	storage syncProjectionStorage
}

type pendingProjection struct {
	event      outboxEvent
	message    message
	recipients []messageEventRecipient
}

type projectionItem struct {
	projectionIndex int
	messageID       int64
	createdUnixNano int64
}

func (projector *messageSyncProjector) PrepareBatch(ctx context.Context, events []outboxEvent) ([]outboxEvent, error) {
	if projector == nil || projector.db == nil {
		return events, errors.New("message sync projector requires a database")
	}
	storage, err := normalizeSyncProjectionStorage(projector.storage)
	if err != nil {
		return events, err
	}
	switch storage {
	case syncProjectionStorageJSONB:
		return projector.prepareJSONBBatch(ctx, events)
	case syncProjectionStorageRecipients, syncProjectionStorageSyncEvents:
		return projector.prepareStructuredBatch(ctx, events, storage)
	default:
		return events, fmt.Errorf("unsupported outbox projection storage %q", storage)
	}
}

func (projector *messageSyncProjector) prepareJSONBBatch(ctx context.Context, events []outboxEvent) ([]outboxEvent, error) {
	decodeStarted := time.Now()
	projections := make([]pendingProjection, 0, len(events))
	for _, event := range events {
		if event.EventType != "message.created" || event.PayloadVersion != 3 {
			continue
		}
		var payload messageCreatedPendingPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return events, fmt.Errorf("decode pending event %s: %w", event.EventID, err)
		}
		if payload.Message.ID <= 0 || payload.Message.ID != event.MessageID || payload.Message.SenderID <= 0 || payload.Message.ReceiverID <= 0 {
			return events, fmt.Errorf("pending event %s is incomplete", event.EventID)
		}
		projections = append(projections, pendingProjection{event: event, message: payload.Message})
	}
	if len(projections) == 0 {
		return events, nil
	}

	itemsByUser := make(map[int64][]projectionItem)
	for index, projection := range projections {
		userIDs := []int64{projection.message.SenderID}
		if projection.message.ReceiverID != projection.message.SenderID {
			userIDs = append(userIDs, projection.message.ReceiverID)
		}
		for _, userID := range userIDs {
			itemsByUser[userID] = append(itemsByUser[userID], projectionItem{
				projectionIndex: index,
				messageID:       projection.message.ID,
				createdUnixNano: projection.message.CreatedAt.UnixNano(),
			})
		}
	}

	// Every projector locks counter rows in the same user order. This keeps the
	// multi-instance path deadlock-safe while one update allocates a whole range.
	userIDs := make([]int64, 0, len(itemsByUser))
	for userID := range itemsByUser {
		userIDs = append(userIDs, userID)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	projector.metrics.ObserveOutboxStage(outboxStagePrepareDecode, time.Since(decodeStarted))

	beginStarted := time.Now()
	tx, err := projector.db.Begin(ctx)
	if err != nil {
		return events, err
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareBegin, time.Since(beginStarted))
	defer tx.Rollback(ctx)

	projectionMode, err := normalizeSyncProjectionMode(projector.mode)
	if err != nil {
		return events, err
	}
	projector.metrics.ObserveOutboxProjectionBatch(len(userIDs))
	projectUsersStarted := time.Now()
	switch projectionMode {
	case syncProjectionModePerUser:
		err = projector.projectPerUser(ctx, tx, projections, itemsByUser, userIDs)
	case syncProjectionModeBulk:
		err = projector.projectBulk(ctx, tx, projections, itemsByUser, userIDs)
	}
	if err != nil {
		return events, err
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareUsers, time.Since(projectUsersStarted))

	encodeStarted := time.Now()
	preparedByID := make(map[string]outboxEvent, len(projections))
	eventIDs := make([]string, 0, len(projections))
	lockTokens := make([]string, 0, len(projections))
	payloads := make([]string, 0, len(projections))
	for _, projection := range projections {
		sort.Slice(projection.recipients, func(i, j int) bool {
			return projection.recipients[i].UserID < projection.recipients[j].UserID
		})
		payload, err := json.Marshal(messageCreatedEventPayload{
			Message:    projection.message,
			Recipients: projection.recipients,
		})
		if err != nil {
			return events, fmt.Errorf("encode projected event %s: %w", projection.event.EventID, err)
		}
		eventIDs = append(eventIDs, projection.event.EventID)
		lockTokens = append(lockTokens, projection.event.LockToken)
		payloads = append(payloads, string(payload))
		prepared := projection.event
		prepared.PayloadVersion = 2
		prepared.Payload = payload
		preparedByID[prepared.EventID] = prepared
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareEncode, time.Since(encodeStarted))
	storeStarted := time.Now()
	command, err := tx.Exec(
		ctx,
		`UPDATE outbox_events AS event
		 SET payload_version = 2,
		     payload = projected.payload::jsonb,
		     ready_at = CURRENT_TIMESTAMP
		 FROM unnest($1::text[], $2::text[], $3::text[])
		      AS projected(event_id, lock_token, payload)
		 WHERE event.event_id = projected.event_id::uuid
		   AND event.lock_token = projected.lock_token::uuid
		   AND event.payload_version = 3
		   AND event.ready_at IS NULL
		   AND event.published_at IS NULL
		   AND event.dead_at IS NULL`,
		eventIDs,
		lockTokens,
		payloads,
	)
	if err != nil {
		return events, fmt.Errorf("store projected batch: %w", err)
	}
	if command.RowsAffected() != int64(len(projections)) {
		return events, fmt.Errorf("store projected batch: %w", errOutboxLeaseLost)
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareStore, time.Since(storeStarted))

	commitStarted := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return events, err
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareCommit, time.Since(commitStarted))
	preparedEvents := make([]outboxEvent, len(events))
	for index, event := range events {
		if prepared, found := preparedByID[event.EventID]; found {
			preparedEvents[index] = prepared
		} else {
			preparedEvents[index] = event
		}
	}
	return preparedEvents, nil
}

func (projector *messageSyncProjector) prepareStructuredBatch(
	ctx context.Context,
	events []outboxEvent,
	storage syncProjectionStorage,
) ([]outboxEvent, error) {
	decodeStarted := time.Now()
	projections := make([]pendingProjection, 0, len(events))
	for _, event := range events {
		if event.EventType != "message.created" {
			continue
		}
		projectedMessage, err := decodeProjectionMessage(event)
		if err != nil {
			return events, fmt.Errorf("decode event %s for structured recipients: %w", event.EventID, err)
		}
		if projectedMessage.ID <= 0 || projectedMessage.ID != event.MessageID || projectedMessage.SenderID <= 0 || projectedMessage.ReceiverID <= 0 {
			return events, fmt.Errorf("event %s is incomplete", event.EventID)
		}
		projections = append(projections, pendingProjection{event: event, message: projectedMessage})
	}
	if len(projections) == 0 {
		return events, nil
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareDecode, time.Since(decodeStarted))

	beginStarted := time.Now()
	tx, err := projector.db.Begin(ctx)
	if err != nil {
		return events, err
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareBegin, time.Since(beginStarted))
	defer tx.Rollback(ctx)

	readyEventIDs := make([]string, 0, len(projections))
	for _, projection := range projections {
		if projection.event.ReadyAt != nil {
			readyEventIDs = append(readyEventIDs, projection.event.EventID)
		}
	}
	switch storage {
	case syncProjectionStorageRecipients:
		if err := projector.loadStructuredRecipients(ctx, tx, projections, readyEventIDs); err != nil {
			return events, err
		}
		fallbackEventIDs := make([]string, 0, len(readyEventIDs))
		for _, projection := range projections {
			if projection.event.ReadyAt != nil && len(projection.recipients) == 0 {
				fallbackEventIDs = append(fallbackEventIDs, projection.event.EventID)
			}
		}
		if err := projector.loadSyncEventRecipients(ctx, tx, projections, fallbackEventIDs); err != nil {
			return events, err
		}
	case syncProjectionStorageSyncEvents:
		if err := projector.loadSyncEventRecipients(ctx, tx, projections, readyEventIDs); err != nil {
			return events, err
		}
	default:
		return events, fmt.Errorf("unsupported structured outbox projection storage %q", storage)
	}

	itemsByUser := make(map[int64][]projectionItem)
	for index, projection := range projections {
		if projection.event.ReadyAt != nil {
			continue
		}
		userIDs := []int64{projection.message.SenderID}
		if projection.message.ReceiverID != projection.message.SenderID {
			userIDs = append(userIDs, projection.message.ReceiverID)
		}
		for _, userID := range userIDs {
			itemsByUser[userID] = append(itemsByUser[userID], projectionItem{
				projectionIndex: index,
				messageID:       projection.message.ID,
				createdUnixNano: projection.message.CreatedAt.UnixNano(),
			})
		}
	}

	userIDs := make([]int64, 0, len(itemsByUser))
	for userID := range itemsByUser {
		userIDs = append(userIDs, userID)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	if len(userIDs) > 0 {
		projectionMode, err := normalizeSyncProjectionMode(projector.mode)
		if err != nil {
			return events, err
		}
		projector.metrics.ObserveOutboxProjectionBatch(len(userIDs))
		projectUsersStarted := time.Now()
		switch projectionMode {
		case syncProjectionModePerUser:
			err = projector.projectPerUser(ctx, tx, projections, itemsByUser, userIDs)
		case syncProjectionModeBulk:
			err = projector.projectBulk(ctx, tx, projections, itemsByUser, userIDs)
		}
		if err != nil {
			return events, err
		}
		projector.metrics.ObserveOutboxStage(outboxStagePrepareUsers, time.Since(projectUsersStarted))
	}

	encodeStarted := time.Now()
	preparedByID := make(map[string]outboxEvent, len(projections))
	for index := range projections {
		projection := &projections[index]
		if err := validateProjectionRecipients(*projection); err != nil {
			return events, err
		}
		sort.Slice(projection.recipients, func(i, j int) bool {
			return projection.recipients[i].UserID < projection.recipients[j].UserID
		})
		payload, err := json.Marshal(messageCreatedEventPayload{
			Message:    projection.message,
			Recipients: projection.recipients,
		})
		if err != nil {
			return events, fmt.Errorf("encode projected event %s: %w", projection.event.EventID, err)
		}
		prepared := projection.event
		prepared.PayloadVersion = 2
		prepared.Payload = payload
		preparedByID[prepared.EventID] = prepared
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareEncode, time.Since(encodeStarted))

	storeStarted := time.Now()
	switch storage {
	case syncProjectionStorageRecipients:
		if err := projector.storeStructuredProjection(ctx, tx, projections); err != nil {
			return events, err
		}
	case syncProjectionStorageSyncEvents:
		if err := projector.storeSyncEventProjection(ctx, tx, projections); err != nil {
			return events, err
		}
	default:
		return events, fmt.Errorf("unsupported structured outbox projection storage %q", storage)
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareStore, time.Since(storeStarted))

	commitStarted := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return events, err
	}
	projector.metrics.ObserveOutboxStage(outboxStagePrepareCommit, time.Since(commitStarted))

	preparedEvents := make([]outboxEvent, len(events))
	for index, event := range events {
		if prepared, found := preparedByID[event.EventID]; found {
			preparedEvents[index] = prepared
		} else {
			preparedEvents[index] = event
		}
	}
	return preparedEvents, nil
}

func decodeProjectionMessage(event outboxEvent) (message, error) {
	switch event.PayloadVersion {
	case 3:
		var payload messageCreatedPendingPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return message{}, fmt.Errorf("decode pending message.created payload: %w", err)
		}
		return payload.Message, nil
	case 1, 2:
		payload, err := decodeMessageCreatedEvent(event)
		if err != nil {
			return message{}, err
		}
		return payload.Message, nil
	default:
		return message{}, fmt.Errorf("unsupported message.created payload version %d", event.PayloadVersion)
	}
}

func (projector *messageSyncProjector) loadStructuredRecipients(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
	eventIDs []string,
) error {
	if len(eventIDs) == 0 {
		return nil
	}
	projectionByEventID := make(map[string]int, len(eventIDs))
	for index, projection := range projections {
		if projection.event.ReadyAt != nil {
			projectionByEventID[projection.event.EventID] = index
		}
	}
	rows, err := tx.Query(
		ctx,
		`SELECT recipient.event_id::text, recipient.user_id, recipient.cursor
		 FROM unnest($1::text[]) AS requested(event_id)
		 JOIN outbox_recipients AS recipient
		   ON recipient.event_id = requested.event_id::uuid
		 ORDER BY recipient.event_id, recipient.user_id`,
		eventIDs,
	)
	if err != nil {
		return fmt.Errorf("load structured outbox recipients: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		var recipient messageEventRecipient
		if err := rows.Scan(&eventID, &recipient.UserID, &recipient.Cursor); err != nil {
			return fmt.Errorf("scan structured outbox recipient: %w", err)
		}
		projectionIndex, found := projectionByEventID[eventID]
		if !found {
			return fmt.Errorf("loaded recipient for unexpected event %s", eventID)
		}
		projections[projectionIndex].recipients = append(projections[projectionIndex].recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate structured outbox recipients: %w", err)
	}
	return nil
}

// loadSyncEventRecipients rebuilds the realtime routing payload after a
// projection committed but the process stopped before publish. The Sync rows
// are authoritative; normal first-attempt delivery already has the same
// recipients in memory and does not execute this recovery query.
func (projector *messageSyncProjector) loadSyncEventRecipients(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
	eventIDs []string,
) error {
	if len(eventIDs) == 0 {
		return nil
	}
	projectionByEventID := make(map[string]int, len(eventIDs))
	for index, projection := range projections {
		if projection.event.ReadyAt != nil {
			projectionByEventID[projection.event.EventID] = index
		}
	}
	rows, err := tx.Query(
		ctx,
		`SELECT event.event_id::text, sync_event.user_id, sync_event.seq
		 FROM unnest($1::text[]) AS requested(event_id)
		 JOIN outbox_events AS event
		   ON event.event_id = requested.event_id::uuid
		 JOIN user_message_events AS sync_event
		   ON sync_event.message_id = event.message_id
		 WHERE event.event_type = 'message.created'
		   AND event.ready_at IS NOT NULL
		 ORDER BY event.event_id, sync_event.user_id`,
		eventIDs,
	)
	if err != nil {
		return fmt.Errorf("load sync event outbox recipients: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		var recipient messageEventRecipient
		if err := rows.Scan(&eventID, &recipient.UserID, &recipient.Cursor); err != nil {
			return fmt.Errorf("scan sync event outbox recipient: %w", err)
		}
		projectionIndex, found := projectionByEventID[eventID]
		if !found {
			return fmt.Errorf("loaded sync event recipient for unexpected event %s", eventID)
		}
		projections[projectionIndex].recipients = append(projections[projectionIndex].recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sync event outbox recipients: %w", err)
	}
	return nil
}

func (projector *messageSyncProjector) storeStructuredProjection(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
) error {
	eventIDs := make([]string, 0, len(projections))
	lockTokens := make([]string, 0, len(projections))
	recipientEventIDs := make([]string, 0, len(projections)*2)
	recipientUserIDs := make([]int64, 0, len(projections)*2)
	recipientCursors := make([]int64, 0, len(projections)*2)
	for _, projection := range projections {
		if projection.event.ReadyAt != nil {
			continue
		}
		eventIDs = append(eventIDs, projection.event.EventID)
		lockTokens = append(lockTokens, projection.event.LockToken)
		for _, recipient := range projection.recipients {
			recipientEventIDs = append(recipientEventIDs, projection.event.EventID)
			recipientUserIDs = append(recipientUserIDs, recipient.UserID)
			recipientCursors = append(recipientCursors, recipient.Cursor)
		}
	}
	if len(eventIDs) == 0 {
		return nil
	}

	var readyCount, recipientCount int64
	err := tx.QueryRow(
		ctx,
		`WITH ready AS (
		   UPDATE outbox_events AS event
		   SET ready_at = CURRENT_TIMESTAMP
		   FROM unnest($1::text[], $2::text[]) AS claimed(event_id, lock_token)
		   WHERE event.event_id = claimed.event_id::uuid
		     AND event.lock_token = claimed.lock_token::uuid
		     AND event.event_type = 'message.created'
		     AND event.ready_at IS NULL
		     AND event.published_at IS NULL
		     AND event.dead_at IS NULL
		   RETURNING event.event_id
		 ), inserted AS (
		   INSERT INTO outbox_recipients (event_id, user_id, cursor)
		   SELECT projected.event_id::uuid, projected.user_id, projected.cursor
		   FROM unnest($3::text[], $4::bigint[], $5::bigint[])
		        AS projected(event_id, user_id, cursor)
		   JOIN ready ON ready.event_id = projected.event_id::uuid
		   RETURNING event_id
		 )
		 SELECT (SELECT count(*) FROM ready),
		        (SELECT count(*) FROM inserted)`,
		eventIDs,
		lockTokens,
		recipientEventIDs,
		recipientUserIDs,
		recipientCursors,
	).Scan(&readyCount, &recipientCount)
	if err != nil {
		return fmt.Errorf("store structured outbox projection: %w", err)
	}
	if readyCount != int64(len(eventIDs)) {
		return fmt.Errorf("store structured outbox projection: %w", errOutboxLeaseLost)
	}
	if recipientCount != int64(len(recipientEventIDs)) {
		return fmt.Errorf(
			"store structured outbox projection: inserted %d recipients, want %d",
			recipientCount,
			len(recipientEventIDs),
		)
	}
	return nil
}

func (projector *messageSyncProjector) storeSyncEventProjection(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
) error {
	eventIDs := make([]string, 0, len(projections))
	lockTokens := make([]string, 0, len(projections))
	for _, projection := range projections {
		if projection.event.ReadyAt != nil {
			continue
		}
		eventIDs = append(eventIDs, projection.event.EventID)
		lockTokens = append(lockTokens, projection.event.LockToken)
	}
	if len(eventIDs) == 0 {
		return nil
	}

	command, err := tx.Exec(
		ctx,
		`UPDATE outbox_events AS event
		 SET ready_at = CURRENT_TIMESTAMP
		 FROM unnest($1::text[], $2::text[]) AS claimed(event_id, lock_token)
		 WHERE event.event_id = claimed.event_id::uuid
		   AND event.lock_token = claimed.lock_token::uuid
		   AND event.event_type = 'message.created'
		   AND event.ready_at IS NULL
		   AND event.published_at IS NULL
		   AND event.dead_at IS NULL`,
		eventIDs,
		lockTokens,
	)
	if err != nil {
		return fmt.Errorf("store sync event outbox projection: %w", err)
	}
	if command.RowsAffected() != int64(len(eventIDs)) {
		return fmt.Errorf("store sync event outbox projection: %w", errOutboxLeaseLost)
	}
	return nil
}

func validateProjectionRecipients(projection pendingProjection) error {
	expected := map[int64]struct{}{projection.message.SenderID: {}}
	expected[projection.message.ReceiverID] = struct{}{}
	if len(projection.recipients) != len(expected) {
		return fmt.Errorf(
			"event %s has %d recipients, want %d",
			projection.event.EventID,
			len(projection.recipients),
			len(expected),
		)
	}
	seen := make(map[int64]struct{}, len(projection.recipients))
	for _, recipient := range projection.recipients {
		if recipient.Cursor <= 0 {
			return fmt.Errorf("event %s has invalid cursor for user %d", projection.event.EventID, recipient.UserID)
		}
		if _, found := expected[recipient.UserID]; !found {
			return fmt.Errorf("event %s has unexpected recipient %d", projection.event.EventID, recipient.UserID)
		}
		if _, duplicate := seen[recipient.UserID]; duplicate {
			return fmt.Errorf("event %s has duplicate recipient %d", projection.event.EventID, recipient.UserID)
		}
		seen[recipient.UserID] = struct{}{}
	}
	return nil
}

func (projector *messageSyncProjector) projectPerUser(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
	itemsByUser map[int64][]projectionItem,
	userIDs []int64,
) error {
	for _, userID := range userIDs {
		if err := projector.projectOneUser(ctx, tx, projections, userID, itemsByUser[userID]); err != nil {
			return err
		}
	}
	return nil
}

func (projector *messageSyncProjector) projectOneUser(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
	userID int64,
	items []projectionItem,
) error {
	started := time.Now()
	defer func() { projector.metrics.ObserveOutboxProjectionQuery(time.Since(started)) }()

	sortProjectionItems(items)
	messageIDs := make([]int64, len(items))
	for index, item := range items {
		messageIDs[index] = item.messageID
	}

	rows, err := tx.Query(
		ctx,
		`WITH allocation AS (
		   INSERT INTO user_sync_counters (user_id, last_seq)
		   VALUES ($1, cardinality($2::bigint[]))
		   ON CONFLICT (user_id) DO UPDATE
		   SET last_seq = user_sync_counters.last_seq + EXCLUDED.last_seq
		   RETURNING last_seq
		 ), inserted AS (
		   INSERT INTO user_message_events (user_id, seq, message_id)
		   SELECT $1,
		          allocation.last_seq - cardinality($2::bigint[]) + input.ordinality,
		          input.message_id
		   FROM allocation
		   CROSS JOIN unnest($2::bigint[]) WITH ORDINALITY AS input(message_id, ordinality)
		   RETURNING seq, message_id
		 )
		 SELECT seq, message_id FROM inserted ORDER BY seq`,
		userID,
		messageIDs,
	)
	if err != nil {
		return fmt.Errorf("project %d messages for user %d: %w", len(items), userID, err)
	}
	defer rows.Close()

	projectionByMessageID := make(map[int64]int, len(items))
	for _, item := range items {
		projectionByMessageID[item.messageID] = item.projectionIndex
	}
	for rows.Next() {
		var cursor, messageID int64
		if err := rows.Scan(&cursor, &messageID); err != nil {
			return fmt.Errorf("scan projected event for user %d: %w", userID, err)
		}
		projectionIndex, found := projectionByMessageID[messageID]
		if !found {
			return fmt.Errorf("projected unexpected message %d for user %d", messageID, userID)
		}
		projections[projectionIndex].recipients = append(
			projections[projectionIndex].recipients,
			messageEventRecipient{UserID: userID, Cursor: cursor},
		)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate projected events for user %d: %w", userID, err)
	}
	return nil
}

type lockedSyncCounter struct {
	userID  int64
	lastSeq int64
}

func (projector *messageSyncProjector) projectBulk(
	ctx context.Context,
	tx pgx.Tx,
	projections []pendingProjection,
	itemsByUser map[int64][]projectionItem,
	userIDs []int64,
) error {
	if err := projector.ensureSyncCounters(ctx, tx, userIDs); err != nil {
		return err
	}
	counters, err := projector.lockSyncCounters(ctx, tx, userIDs)
	if err != nil {
		return err
	}
	if len(counters) != len(userIDs) {
		return fmt.Errorf("lock sync counters: got %d rows, want %d", len(counters), len(userIDs))
	}

	counterLastSeqs := make([]int64, 0, len(counters))
	eventUserIDs := make([]int64, 0, len(projections)*2)
	eventSeqs := make([]int64, 0, len(projections)*2)
	eventMessageIDs := make([]int64, 0, len(projections)*2)
	for index, counter := range counters {
		if counter.userID != userIDs[index] {
			return fmt.Errorf("lock sync counters: row %d is user %d, want %d", index, counter.userID, userIDs[index])
		}
		items := itemsByUser[counter.userID]
		sortProjectionItems(items)
		if counter.lastSeq > int64(^uint64(0)>>1)-int64(len(items)) {
			return fmt.Errorf("project %d messages for user %d: cursor overflow", len(items), counter.userID)
		}
		lastSeq := counter.lastSeq
		for _, item := range items {
			lastSeq++
			eventUserIDs = append(eventUserIDs, counter.userID)
			eventSeqs = append(eventSeqs, lastSeq)
			eventMessageIDs = append(eventMessageIDs, item.messageID)
			projections[item.projectionIndex].recipients = append(
				projections[item.projectionIndex].recipients,
				messageEventRecipient{UserID: counter.userID, Cursor: lastSeq},
			)
		}
		counterLastSeqs = append(counterLastSeqs, lastSeq)
	}

	inserted, err := projector.storeBulkProjection(
		ctx,
		tx,
		userIDs,
		counterLastSeqs,
		eventUserIDs,
		eventSeqs,
		eventMessageIDs,
	)
	if err != nil {
		return err
	}
	if inserted != int64(len(eventUserIDs)) {
		return fmt.Errorf("store bulk projection: inserted %d rows, want %d", inserted, len(eventUserIDs))
	}
	return nil
}

func (projector *messageSyncProjector) ensureSyncCounters(ctx context.Context, tx pgx.Tx, userIDs []int64) error {
	started := time.Now()
	defer func() { projector.metrics.ObserveOutboxProjectionQuery(time.Since(started)) }()

	_, err := tx.Exec(
		ctx,
		`INSERT INTO user_sync_counters (user_id, last_seq)
		 SELECT input.user_id, 0
		 FROM unnest($1::bigint[]) AS input(user_id)
		 ORDER BY input.user_id
		 ON CONFLICT (user_id) DO NOTHING`,
		userIDs,
	)
	if err != nil {
		return fmt.Errorf("ensure sync counters: %w", err)
	}
	return nil
}

func (projector *messageSyncProjector) lockSyncCounters(
	ctx context.Context,
	tx pgx.Tx,
	userIDs []int64,
) ([]lockedSyncCounter, error) {
	started := time.Now()
	defer func() { projector.metrics.ObserveOutboxProjectionQuery(time.Since(started)) }()

	rows, err := tx.Query(
		ctx,
		`SELECT counter.user_id, counter.last_seq
		 FROM user_sync_counters AS counter
		 WHERE counter.user_id = ANY($1::bigint[])
		 ORDER BY counter.user_id
		 FOR UPDATE`,
		userIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("lock sync counters: %w", err)
	}
	defer rows.Close()

	counters := make([]lockedSyncCounter, 0, len(userIDs))
	for rows.Next() {
		var counter lockedSyncCounter
		if err := rows.Scan(&counter.userID, &counter.lastSeq); err != nil {
			return nil, fmt.Errorf("scan locked sync counter: %w", err)
		}
		counters = append(counters, counter)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate locked sync counters: %w", err)
	}
	return counters, nil
}

func (projector *messageSyncProjector) storeBulkProjection(
	ctx context.Context,
	tx pgx.Tx,
	userIDs []int64,
	lastSeqs []int64,
	eventUserIDs []int64,
	eventSeqs []int64,
	eventMessageIDs []int64,
) (int64, error) {
	started := time.Now()
	defer func() { projector.metrics.ObserveOutboxProjectionQuery(time.Since(started)) }()

	command, err := tx.Exec(
		ctx,
		`WITH updated AS (
		   UPDATE user_sync_counters AS counter
		   SET last_seq = input.last_seq
		   FROM unnest($1::bigint[], $2::bigint[]) AS input(user_id, last_seq)
		   WHERE counter.user_id = input.user_id
		   RETURNING counter.user_id
		 )
		 INSERT INTO user_message_events (user_id, seq, message_id)
		 SELECT input.user_id, input.seq, input.message_id
		 FROM unnest($3::bigint[], $4::bigint[], $5::bigint[])
		      AS input(user_id, seq, message_id)
		 JOIN updated USING (user_id)`,
		userIDs,
		lastSeqs,
		eventUserIDs,
		eventSeqs,
		eventMessageIDs,
	)
	if err != nil {
		return 0, fmt.Errorf("store bulk projection: %w", err)
	}
	return command.RowsAffected(), nil
}

func sortProjectionItems(items []projectionItem) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].createdUnixNano == items[j].createdUnixNano {
			return items[i].messageID < items[j].messageID
		}
		return items[i].createdUnixNano < items[j].createdUnixNano
	})
}
