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

const (
	syncProjectionModePerUser syncProjectionMode = "per_user"
	syncProjectionModeBulk    syncProjectionMode = "bulk"
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

// messageSyncProjector converts durable version-3 message.created events into
// publishable version-2 events. All cursor rows and Outbox payloads are changed
// in one transaction, so realtime delivery cannot get ahead of Sync recovery.
type messageSyncProjector struct {
	db      *pgxpool.Pool
	metrics *applicationMetrics
	mode    syncProjectionMode
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
		     payload = projected.payload::jsonb
		 FROM unnest($1::text[], $2::text[], $3::text[])
		      AS projected(event_id, lock_token, payload)
		 WHERE event.event_id = projected.event_id::uuid
		   AND event.lock_token = projected.lock_token::uuid
		   AND event.payload_version = 3
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
