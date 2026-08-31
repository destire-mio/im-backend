package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type testPublisher struct {
	mu      sync.Mutex
	events  []outboxEvent
	publish func(context.Context, outboxEvent) error
}

type testBatchPublisher struct {
	prepareCalls atomic.Int32
	prepared     *testPublisher
	directCalls  atomic.Int32
}

func (publisher *testBatchPublisher) PreparePublishBatch(
	_ context.Context,
	_ []outboxEvent,
) (outboxPublisher, error) {
	publisher.prepareCalls.Add(1)
	return publisher.prepared, nil
}

func (publisher *testBatchPublisher) Publish(context.Context, outboxEvent) error {
	publisher.directCalls.Add(1)
	return errors.New("unprepared publisher was called")
}

func (publisher *testPublisher) Publish(ctx context.Context, event outboxEvent) error {
	publisher.mu.Lock()
	publisher.events = append(publisher.events, event)
	publisher.mu.Unlock()
	if publisher.publish != nil {
		return publisher.publish(ctx, event)
	}
	return nil
}

func (publisher *testPublisher) received() []outboxEvent {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	result := make([]outboxEvent, len(publisher.events))
	copy(result, publisher.events)
	return result
}

func testWorkerConfig() outboxWorkerConfig {
	config := defaultOutboxWorkerConfig()
	config.EventTypes = []string{"test.outbox"}
	config.BatchSize = 8
	config.Concurrency = 4
	config.LeaseDuration = 2 * time.Second
	config.AttemptTimeout = time.Second
	config.PollInterval = 10 * time.Millisecond
	config.MaxAttempts = 3
	config.BaseBackoff = 5 * time.Second
	config.MaxBackoff = 20 * time.Second
	config.jitter = func(delay time.Duration) time.Duration { return delay }
	return config
}

func TestOutboxWorkerPublishesAndMarksEventComplete(t *testing.T) {
	db := openTestDatabase(t)
	eventID := createPendingOutboxEvent(t, db)
	publisher := &testPublisher{}
	worker := mustTestWorker(t, db, publisher, testWorkerConfig())

	processed, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run worker: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed = %d, want 1", processed)
	}
	events := publisher.received()
	if len(events) != 1 || events[0].EventID != eventID || events[0].AttemptCount != 1 {
		t.Fatalf("published events = %+v", events)
	}

	var publishedAt *time.Time
	var attemptCount int
	var lockedUntil *time.Time
	var lockToken *string
	if err := db.QueryRow(
		context.Background(),
		`SELECT published_at, attempt_count, locked_until, lock_token::text
		 FROM outbox_events WHERE event_id = $1`,
		eventID,
	).Scan(&publishedAt, &attemptCount, &lockedUntil, &lockToken); err != nil {
		t.Fatalf("read published event: %v", err)
	}
	if publishedAt == nil || attemptCount != 1 || lockedUntil != nil || lockToken != nil {
		t.Fatalf("published=%v attempts=%d lockUntil=%v lockToken=%v", publishedAt, attemptCount, lockedUntil, lockToken)
	}
}

func TestOutboxWorkerPreparesPublisherOncePerBatch(t *testing.T) {
	db := openTestDatabase(t)
	eventIDs := map[string]bool{
		createPendingOutboxEvent(t, db): true,
		createPendingOutboxEvent(t, db): true,
	}
	prepared := &testPublisher{}
	publisher := &testBatchPublisher{prepared: prepared}
	worker := mustTestWorker(t, db, publisher, testWorkerConfig())

	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != len(eventIDs) {
		t.Fatalf("run batch-aware worker = processed %d, err %v", processed, err)
	}
	if calls := publisher.prepareCalls.Load(); calls != 1 {
		t.Fatalf("publisher prepare calls = %d, want 1", calls)
	}
	if calls := publisher.directCalls.Load(); calls != 0 {
		t.Fatalf("unprepared publisher calls = %d, want 0", calls)
	}
	received := prepared.received()
	if len(received) != len(eventIDs) {
		t.Fatalf("prepared publisher received %d events, want %d", len(received), len(eventIDs))
	}
	for _, event := range received {
		if !eventIDs[event.EventID] {
			t.Fatalf("prepared publisher received unexpected event %s", event.EventID)
		}
	}
}

func TestOutboxPipelinePreparesNextBatchWhileDeliveringCurrent(t *testing.T) {
	db := openTestDatabase(t)
	eventIDs := []string{
		createPendingOutboxEvent(t, db),
		createPendingOutboxEvent(t, db),
	}
	firstPublishStarted := make(chan struct{})
	secondBatchPrepared := make(chan struct{})
	releaseFirstPublish := make(chan struct{})
	var prepareCalls atomic.Int32
	var publishCalls atomic.Int32
	publisher := &testPublisher{publish: func(ctx context.Context, _ outboxEvent) error {
		if publishCalls.Add(1) != 1 {
			return nil
		}
		close(firstPublishStarted)
		select {
		case <-releaseFirstPublish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	config := testWorkerConfig()
	config.BatchSize = 1
	config.ExecutionMode = outboxExecutionModePipeline
	worker := mustTestWorker(t, db, publisher, config)
	worker.preparer = testBatchPreparerFunc(func(_ context.Context, events []outboxEvent) ([]outboxEvent, error) {
		if prepareCalls.Add(1) == 2 {
			close(secondBatchPrepared)
		}
		return events, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("pipeline worker did not stop")
		}
	}()

	select {
	case <-firstPublishStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first batch did not reach delivery")
	}
	select {
	case <-secondBatchPrepared:
		// The first publish is still blocked, so this can happen only if the
		// preparation and delivery stages overlap.
	case <-time.After(2 * time.Second):
		t.Fatal("second batch was not prepared while first delivery was blocked")
	}
	close(releaseFirstPublish)

	deadline := time.Now().Add(2 * time.Second)
	for {
		var published int
		if err := db.QueryRow(
			context.Background(),
			`SELECT count(*)
			 FROM outbox_events
			 WHERE event_id::text = ANY($1::text[])
			   AND published_at IS NOT NULL`,
			eventIDs,
		).Scan(&published); err != nil {
			t.Fatalf("count pipeline publications: %v", err)
		}
		if published == len(eventIDs) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("published %d pipeline events, want %d", published, len(eventIDs))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOutboxSerialRunWaitsForCurrentDeliveryBeforePreparingNextBatch(t *testing.T) {
	db := openTestDatabase(t)
	createPendingOutboxEvent(t, db)
	createPendingOutboxEvent(t, db)
	firstPublishStarted := make(chan struct{})
	secondBatchPrepared := make(chan struct{})
	releaseFirstPublish := make(chan struct{})
	var prepareCalls atomic.Int32
	var publishCalls atomic.Int32
	publisher := &testPublisher{publish: func(ctx context.Context, _ outboxEvent) error {
		if publishCalls.Add(1) != 1 {
			return nil
		}
		close(firstPublishStarted)
		select {
		case <-releaseFirstPublish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	config := testWorkerConfig()
	config.BatchSize = 1
	config.ExecutionMode = outboxExecutionModeSerial
	worker := mustTestWorker(t, db, publisher, config)
	worker.preparer = testBatchPreparerFunc(func(_ context.Context, events []outboxEvent) ([]outboxEvent, error) {
		if prepareCalls.Add(1) == 2 {
			close(secondBatchPrepared)
		}
		return events, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("serial worker did not stop")
		}
	}()

	select {
	case <-firstPublishStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first serial batch did not reach delivery")
	}
	select {
	case <-secondBatchPrepared:
		t.Fatal("serial mode prepared the next batch before current delivery completed")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirstPublish)
	select {
	case <-secondBatchPrepared:
	case <-time.After(2 * time.Second):
		t.Fatal("serial mode did not prepare the next batch after delivery completed")
	}
}

func TestOutboxPipelineShutdownReleasesPreparedBatchLeases(t *testing.T) {
	db := openTestDatabase(t)
	eventIDs := []string{
		createPendingOutboxEvent(t, db),
		createPendingOutboxEvent(t, db),
	}
	firstPublishStarted := make(chan struct{})
	secondBatchPrepared := make(chan struct{})
	var prepareCalls atomic.Int32
	var publishCalls atomic.Int32
	publisher := &testPublisher{publish: func(ctx context.Context, _ outboxEvent) error {
		if publishCalls.Add(1) == 1 {
			close(firstPublishStarted)
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	config := testWorkerConfig()
	config.BatchSize = 1
	config.ExecutionMode = outboxExecutionModePipeline
	worker := mustTestWorker(t, db, publisher, config)
	worker.preparer = testBatchPreparerFunc(func(_ context.Context, events []outboxEvent) ([]outboxEvent, error) {
		if prepareCalls.Add(1) == 2 {
			close(secondBatchPrepared)
		}
		return events, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-firstPublishStarted:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("first batch did not reach delivery")
	}
	select {
	case <-secondBatchPrepared:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("second batch was not prepared before shutdown")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stop pipeline worker: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline worker did not stop after cancellation")
	}

	var released int
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*)
		 FROM outbox_events
		 WHERE event_id::text = ANY($1::text[])
		   AND published_at IS NULL
		   AND dead_at IS NULL
		   AND locked_until IS NULL
		   AND lock_token IS NULL
		   AND last_error IS NOT NULL`,
		eventIDs,
	).Scan(&released); err != nil {
		t.Fatalf("count released pipeline leases: %v", err)
	}
	if released != len(eventIDs) {
		t.Fatalf("released %d pipeline leases, want %d", released, len(eventIDs))
	}
}

func TestTwoOutboxWorkersClaimDisjointBatches(t *testing.T) {
	db := openTestDatabase(t)
	wantEvents := make(map[string]bool)
	for range 4 {
		wantEvents[createPendingOutboxEvent(t, db)] = true
	}
	publisher := &testPublisher{}
	config := testWorkerConfig()
	config.BatchSize = 2
	first := mustTestWorker(t, db, publisher, config)
	second := mustTestWorker(t, db, publisher, config)

	start := make(chan struct{})
	errorsChannel := make(chan error, 2)
	var wait sync.WaitGroup
	for _, worker := range []*outboxWorker{first, second} {
		wait.Add(1)
		go func(current *outboxWorker) {
			defer wait.Done()
			<-start
			_, err := current.RunOnce(context.Background())
			errorsChannel <- err
		}(worker)
	}
	close(start)
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("run concurrent worker: %v", err)
		}
	}

	received := publisher.received()
	if len(received) != 4 {
		t.Fatalf("received %d events, want 4", len(received))
	}
	seen := make(map[string]bool)
	for _, event := range received {
		if !wantEvents[event.EventID] || seen[event.EventID] {
			t.Fatalf("unexpected or duplicate event %s", event.EventID)
		}
		seen[event.EventID] = true
	}
}

func TestOutboxWorkerSchedulesTransientFailureWithBackoff(t *testing.T) {
	db := openTestDatabase(t)
	eventID := createPendingOutboxEvent(t, db)
	publisher := &testPublisher{publish: func(context.Context, outboxEvent) error {
		return retryablePublishError(errors.New("temporary downstream failure"), 7*time.Second)
	}}
	worker := mustTestWorker(t, db, publisher, testWorkerConfig())
	startedAt := time.Now()

	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	var nextAttempt time.Time
	var attemptCount int
	var deadAt *time.Time
	var lastError string
	if err := db.QueryRow(
		context.Background(),
		`SELECT next_attempt_at, attempt_count, dead_at, last_error
		 FROM outbox_events WHERE event_id = $1`,
		eventID,
	).Scan(&nextAttempt, &attemptCount, &deadAt, &lastError); err != nil {
		t.Fatalf("read retried event: %v", err)
	}
	if nextAttempt.Before(startedAt.Add(6*time.Second)) || attemptCount != 1 || deadAt != nil || lastError == "" {
		t.Fatalf("next=%v attempts=%d dead=%v error=%q", nextAttempt, attemptCount, deadAt, lastError)
	}
}

func TestOutboxWorkerMarksPermanentFailureDead(t *testing.T) {
	db := openTestDatabase(t)
	eventID := createPendingOutboxEvent(t, db)
	publisher := &testPublisher{publish: func(context.Context, outboxEvent) error {
		return permanentPublishError(errors.New("unsupported payload version"))
	}}
	worker := mustTestWorker(t, db, publisher, testWorkerConfig())

	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("run worker: %v", err)
	}
	var deadAt *time.Time
	var publishedAt *time.Time
	var lastError string
	if err := db.QueryRow(
		context.Background(),
		`SELECT dead_at, published_at, last_error FROM outbox_events WHERE event_id = $1`,
		eventID,
	).Scan(&deadAt, &publishedAt, &lastError); err != nil {
		t.Fatalf("read dead event: %v", err)
	}
	if deadAt == nil || publishedAt != nil || lastError != "unsupported payload version" {
		t.Fatalf("dead=%v published=%v error=%q", deadAt, publishedAt, lastError)
	}
}

func TestExpiredLeaseCanBeReclaimedAndOldOwnerCannotComplete(t *testing.T) {
	db := openTestDatabase(t)
	eventID := createPendingOutboxEvent(t, db)
	worker := mustTestWorker(t, db, &testPublisher{}, testWorkerConfig())

	firstClaim, err := worker.claim(context.Background())
	if err != nil || len(firstClaim) != 1 {
		t.Fatalf("first claim = %+v, err=%v", firstClaim, err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE outbox_events SET locked_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE event_id = $1`, eventID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	secondClaim, err := worker.claim(context.Background())
	if err != nil || len(secondClaim) != 1 {
		t.Fatalf("second claim = %+v, err=%v", secondClaim, err)
	}
	if firstClaim[0].LockToken == secondClaim[0].LockToken {
		t.Fatal("reclaimed event received the old lock token")
	}
	if err := worker.markPublished(context.Background(), firstClaim[0]); !errors.Is(err, errOutboxLeaseLost) {
		t.Fatalf("old owner mark result = %v, want lease lost", err)
	}
	if err := worker.markPublished(context.Background(), secondClaim[0]); err != nil {
		t.Fatalf("new owner could not complete event: %v", err)
	}
}

func TestOutboxCleanupDeletesOnlyOnePublishedBatch(t *testing.T) {
	db := openTestDatabase(t)
	for range 2 {
		createPendingOutboxEvent(t, db)
	}
	worker := mustTestWorker(t, db, &testPublisher{}, testWorkerConfig())
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("publish cleanup fixtures: %v", err)
	}

	deleted, err := worker.cleanupPublished(context.Background(), time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("cleanup published: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
}

func mustTestWorker(t *testing.T, db *pgxpool.Pool, publisher outboxPublisher, config outboxWorkerConfig) *outboxWorker {
	t.Helper()
	worker, err := newOutboxWorker(db, publisher, config)
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	return worker
}

func mustMessageTestWorker(t *testing.T, db *pgxpool.Pool, publisher outboxPublisher, config outboxWorkerConfig) *outboxWorker {
	t.Helper()
	worker, err := newMessageOutboxWorker(db, publisher, config)
	if err != nil {
		t.Fatalf("create message worker: %v", err)
	}
	return worker
}

func TestMessageOutboxWorkerRejectsUnsupportedProjectionMode(t *testing.T) {
	config := defaultOutboxWorkerConfig()
	config.ProjectionMode = "unsupported"
	if _, err := newMessageOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config); err == nil {
		t.Fatal("unsupported projection mode was accepted")
	}
}

func TestOutboxWorkerRejectsUnsupportedExecutionMode(t *testing.T) {
	config := defaultOutboxWorkerConfig()
	config.ExecutionMode = "unsupported"
	if _, err := newOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config); err == nil {
		t.Fatal("unsupported execution mode was accepted")
	}
}

func TestMessageOutboxWorkerRejectsUnsupportedProjectionStorage(t *testing.T) {
	config := defaultOutboxWorkerConfig()
	config.ProjectionStorage = "unsupported"
	if _, err := newMessageOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config); err == nil {
		t.Fatal("unsupported projection storage was accepted")
	}
}

func TestMessageOutboxWorkerDefaultsEmptyProjectionStorageToSyncEvents(t *testing.T) {
	config := defaultOutboxWorkerConfig()
	config.ProjectionStorage = ""
	worker, err := newMessageOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config)
	if err != nil {
		t.Fatalf("create worker with empty projection storage: %v", err)
	}
	if worker.config.ProjectionStorage != syncProjectionStorageSyncEvents {
		t.Fatalf("projection storage = %q, want %q", worker.config.ProjectionStorage, syncProjectionStorageSyncEvents)
	}
}

func TestMessageOutboxWorkerConfiguresUserShardedPreparation(t *testing.T) {
	config := defaultOutboxWorkerConfig()
	config.PrepareMode = outboxPrepareModeUserSharded
	config.PrepareWorkers = 4
	worker, err := newMessageOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config)
	if err != nil {
		t.Fatalf("create user-sharded message worker: %v", err)
	}
	if !worker.config.claimReadyOnly || worker.config.PrepareWorkers != 4 {
		t.Fatalf("user-sharded config = %+v", worker.config)
	}
}

func TestMessageOutboxWorkerRejectsInvalidPreparationConfig(t *testing.T) {
	tests := []outboxWorkerConfig{
		func() outboxWorkerConfig {
			config := defaultOutboxWorkerConfig()
			config.PrepareMode = "unsupported"
			return config
		}(),
		func() outboxWorkerConfig {
			config := defaultOutboxWorkerConfig()
			config.PrepareWorkers = 4
			return config
		}(),
		func() outboxWorkerConfig {
			config := defaultOutboxWorkerConfig()
			config.PrepareMode = outboxPrepareModeUserSharded
			config.PrepareWorkers = messageProjectionVirtualShards + 1
			return config
		}(),
		func() outboxWorkerConfig {
			config := defaultOutboxWorkerConfig()
			config.PrepareMode = outboxPrepareModeUserSharded
			config.PrepareWorkers = 4
			config.ProjectionMode = syncProjectionModePerUser
			return config
		}(),
		func() outboxWorkerConfig {
			config := defaultOutboxWorkerConfig()
			config.PrepareMode = outboxPrepareModeUserSharded
			config.PrepareWorkers = 4
			config.ProjectionStorage = syncProjectionStorageJSONB
			return config
		}(),
	}
	for index, config := range tests {
		if _, err := newMessageOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config); err == nil {
			t.Fatalf("invalid preparation config %d was accepted", index)
		}
	}
}

func createPendingOutboxEvent(t *testing.T, db *pgxpool.Pool) string {
	t.Helper()
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	suffix := time.Now().UnixNano()
	sender := registerTestAccount(t, db, server.URL, fmt.Sprintf("os_%d", suffix), "Outbox Sender")
	receiver := registerTestAccount(t, db, server.URL, fmt.Sprintf("or_%d", suffix), "Outbox Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "outbox test")

	var eventID string
	if err := db.QueryRow(
		context.Background(),
		`SELECT event_id::text FROM outbox_events WHERE message_id = $1`,
		created.ID,
	).Scan(&eventID); err != nil {
		t.Fatalf("read pending event: %v", err)
	}
	if _, err := db.Exec(context.Background(), `UPDATE outbox_events SET event_type = 'test.outbox' WHERE event_id = $1`, eventID); err != nil {
		t.Fatalf("isolate test outbox event: %v", err)
	}
	return eventID
}
