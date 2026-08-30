package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type testPublisher struct {
	mu      sync.Mutex
	events  []outboxEvent
	publish func(context.Context, outboxEvent) error
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

func TestMessageOutboxWorkerRejectsUnsupportedProjectionStorage(t *testing.T) {
	config := defaultOutboxWorkerConfig()
	config.ProjectionStorage = "unsupported"
	if _, err := newMessageOutboxWorker(&pgxpool.Pool{}, &testPublisher{}, config); err == nil {
		t.Fatal("unsupported projection storage was accepted")
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
