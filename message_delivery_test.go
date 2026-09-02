package main

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
)

func TestMessageCommitIsImmediatelySyncableAndReadyForDelivery(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("ready_s"), "Ready Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("ready_r"), "Ready Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "ready at commit")
	event := loadOutboxEventForMessage(t, db, created.ID)
	if event.PayloadVersion != 4 || event.ReadyAt == nil {
		t.Fatalf("committed event is not ready v4: %+v", event)
	}
	payload, err := decodeMessageCreatedEvent(event)
	// PostgreSQL JSON and HTTP JSON may decode the same instant with different
	// time.Location values. Compare instants, not time.Time's internal pointers.
	want := created
	want.CreatedAt = payload.Message.CreatedAt
	if err != nil || payload.Message != want || !payload.Message.CreatedAt.Equal(created.CreatedAt) || len(payload.Recipients) != 2 {
		t.Fatalf("committed payload = %+v, err %v", payload, err)
	}
	// No worker has run: recovery must depend only on the send transaction.
	for _, token := range []string{sender.Auth.AccessToken, receiver.Auth.AccessToken} {
		page := syncConversationMessagesThroughAPI(t, server.URL, token, created.ConversationID, 0, 10)
		if len(page.Messages) != 1 || page.Messages[0].ID != created.ID || page.NextCursor != created.ConversationSeq {
			t.Fatalf("message not recoverable immediately after commit: %+v", page)
		}
	}
	publishPendingMessageEvents(t, db)
	published := loadOutboxEventForMessage(t, db, created.ID)
	if published.EventID != event.EventID || !bytes.Equal(published.Payload, event.Payload) {
		t.Fatal("delivery rewrote the durable event or its recipients")
	}
	var ready, complete bool
	if err := db.QueryRow(t.Context(),
		`SELECT ready_at IS NOT NULL, published_at IS NOT NULL FROM outbox_events WHERE event_id = $1`,
		event.EventID,
	).Scan(&ready, &complete); err != nil || !ready || !complete {
		t.Fatalf("ready=%t published=%t err=%v", ready, complete, err)
	}
}

func TestReadyMessageSurvivesWorkerRestartAndFencesStaleCompletion(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("restart_s"), "Restart Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("restart_r"), "Restart Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "restart before publish")
	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	first := mustTestWorker(t, db, &testPublisher{}, config)
	claimed, err := first.claim(t.Context())
	if err != nil || len(claimed) != 1 || claimed[0].MessageID != created.ID {
		t.Fatalf("claim = %+v err=%v", claimed, err)
	}
	// Simulate a crashed worker. A replacement can reclaim the same durable
	// payload after lease expiry without rebuilding any user projection.
	if _, err := db.Exec(t.Context(),
		`UPDATE outbox_events SET locked_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE event_id = $1`,
		claimed[0].EventID,
	); err != nil {
		t.Fatal(err)
	}
	publisher := &testPublisher{}
	restarted := mustTestWorker(t, db, publisher, config)
	if processed, err := restarted.RunOnce(t.Context()); err != nil || processed != 1 {
		t.Fatalf("restart processed=%d err=%v", processed, err)
	}
	received := publisher.received()
	if len(received) != 1 || received[0].EventID != claimed[0].EventID ||
		received[0].PayloadVersion != 4 || !bytes.Equal(received[0].Payload, claimed[0].Payload) {
		t.Fatalf("restarted delivery changed identity or payload: %+v", received)
	}
	if err := first.markPublished(context.Background(), claimed[0]); !errors.Is(err, errOutboxLeaseLost) {
		t.Fatalf("stale completion = %v, want lease lost", err)
	}
}

func TestMessagePublisherRejectsUnmigratedPayloadVersions(t *testing.T) {
	event := testMessageCreatedOutboxEvent(t, "unsupported-version", 1, []int64{7, 9})
	for _, version := range []int16{1, 2, 3, 5} {
		event.PayloadVersion = version
		if _, err := decodeMessageCreatedEvent(event); err == nil {
			t.Fatalf("accepted obsolete or unknown payload version %d", version)
		}
	}
}
