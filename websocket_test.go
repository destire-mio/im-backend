package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testBatchDeliveryRouter struct {
	preparedUsers  []int64
	publishedUsers []int64
	directCalls    int
}

func (router *testBatchDeliveryRouter) PrepareDeliveryBatch(
	_ context.Context,
	userIDs []int64,
) (webSocketDeliveryRouter, error) {
	router.preparedUsers = append([]int64(nil), userIDs...)
	return testPreparedDeliveryRouter{owner: router}, nil
}

func (router *testBatchDeliveryRouter) Publish(context.Context, int64, []byte) (int, error) {
	router.directCalls++
	return 0, errors.New("unprepared delivery router was called")
}

type testPreparedDeliveryRouter struct {
	owner *testBatchDeliveryRouter
}

func (router testPreparedDeliveryRouter) Publish(_ context.Context, userID int64, _ []byte) (int, error) {
	router.owner.publishedUsers = append(router.owner.publishedUsers, userID)
	return 0, nil
}

func TestWebSocketRequiresAuthentication(t *testing.T) {
	db := openTestDatabase(t)
	app, stopHub := newWebSocketTestApplication(t, db)
	defer stopHub()
	server := httptest.NewServer(app.routes())
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, response, err := websocket.Dial(ctx, webSocketURL(server.URL), nil)
	if err == nil {
		t.Fatal("websocket connection without authentication unexpectedly succeeded")
	}
	if response == nil || response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("websocket authentication response = %#v, want 401", response)
	}
}

func TestWebSocketPublisherDeliversToEveryOnlineDevice(t *testing.T) {
	db := openTestDatabase(t)
	app, stopHub := newWebSocketTestApplication(t, db)
	defer stopHub()
	server := httptest.NewServer(app.routes())
	defer server.Close()

	sender := registerTestAccount(t, db, server.URL, uniqueUsername("ws_sender"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("ws_receiver"), "Receiver")
	secondDevice := loginTestAccount(t, server.URL, receiver.Username, receiver.Password, uniqueOpaqueID("ws-device"))

	phone := dialAuthenticatedWebSocket(t, server.URL, receiver.Auth.AccessToken)
	defer phone.CloseNow()
	desktop := dialAuthenticatedWebSocket(t, server.URL, secondDevice.AccessToken)
	defer desktop.CloseNow()

	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "realtime message")
	event := loadOutboxEventForMessage(t, db, created.ID)
	publisher := &webSocketOutboxPublisher{router: app.webSocketHub}
	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	config.Concurrency = 1
	worker := mustMessageTestWorker(t, db, publisher, config)
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("run websocket outbox worker = processed %d, err %v", processed, err)
	}

	for name, connection := range map[string]*websocket.Conn{"phone": phone, "desktop": desktop} {
		t.Run(name, func(t *testing.T) {
			envelope := readWebSocketEnvelope(t, connection)
			if envelope.Type != "message.created" || envelope.EventID != event.EventID {
				t.Fatalf("envelope identity = %+v", envelope)
			}
			if envelope.Message.ID != created.ID || envelope.Cursor <= 0 {
				t.Fatalf("envelope message = %+v", envelope)
			}
		})
	}
}

func TestWebSocketPublisherDeduplicatesBatchPresenceUsers(t *testing.T) {
	router := &testBatchDeliveryRouter{}
	publisher := &webSocketOutboxPublisher{router: router, batchPresence: true}
	first := testMessageCreatedOutboxEvent(t, "event-1", 101, []int64{7, 9})
	second := testMessageCreatedOutboxEvent(t, "event-2", 102, []int64{9, 11})
	malformed := outboxEvent{
		EventID:        "event-malformed",
		EventType:      "message.created",
		PayloadVersion: 2,
		Payload:        json.RawMessage(`{"message":`),
	}

	prepared, err := publisher.PreparePublishBatch(context.Background(), []outboxEvent{first, malformed, second})
	if err != nil {
		t.Fatalf("prepare websocket publisher batch: %v", err)
	}
	if want := []int64{7, 9, 11}; !reflect.DeepEqual(router.preparedUsers, want) {
		t.Fatalf("prepared Presence users = %v, want %v", router.preparedUsers, want)
	}
	for _, event := range []outboxEvent{first, second} {
		if err := prepared.Publish(context.Background(), event); err != nil {
			t.Fatalf("publish prepared event %s: %v", event.EventID, err)
		}
	}
	if router.directCalls != 0 {
		t.Fatalf("unprepared delivery calls = %d, want 0", router.directCalls)
	}
	if want := []int64{7, 9, 9, 11}; !reflect.DeepEqual(router.publishedUsers, want) {
		t.Fatalf("prepared delivery users = %v, want %v", router.publishedUsers, want)
	}
}

func testMessageCreatedOutboxEvent(
	t *testing.T,
	eventID string,
	messageID int64,
	userIDs []int64,
) outboxEvent {
	t.Helper()
	recipients := make([]messageEventRecipient, 0, len(userIDs))
	for index, userID := range userIDs {
		recipients = append(recipients, messageEventRecipient{UserID: userID, Cursor: int64(index + 1)})
	}
	payload, err := json.Marshal(messageCreatedEventPayload{
		Message:    message{ID: messageID},
		Recipients: recipients,
	})
	if err != nil {
		t.Fatalf("encode test message.created payload: %v", err)
	}
	return outboxEvent{
		EventID:        eventID,
		EventType:      "message.created",
		PayloadVersion: 2,
		MessageID:      messageID,
		Payload:        payload,
	}
}

func TestOfflineRecipientCanSyncAfterOutboxCompletes(t *testing.T) {
	db := openTestDatabase(t)
	app, stopHub := newWebSocketTestApplication(t, db)
	defer stopHub()
	server := httptest.NewServer(app.routes())
	defer server.Close()

	sender := registerTestAccount(t, db, server.URL, uniqueUsername("ws_off_s"), "Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("ws_off_r"), "Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "offline message")

	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	config.Concurrency = 1
	worker := mustMessageTestWorker(t, db, &webSocketOutboxPublisher{router: app.webSocketHub}, config)
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("run offline outbox worker = processed %d, err %v", processed, err)
	}

	var published bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT published_at IS NOT NULL FROM outbox_events WHERE message_id = $1`,
		created.ID,
	).Scan(&published); err != nil || !published {
		t.Fatalf("offline outbox published = %v, err %v", published, err)
	}
	page := syncMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 0, 10)
	if len(page.Events) != 1 || page.Events[0].Message.ID != created.ID {
		t.Fatalf("offline sync page = %+v", page)
	}
}

func TestLogoutClosesOnlyCurrentSessionWebSocket(t *testing.T) {
	db := openTestDatabase(t)
	app, stopHub := newWebSocketTestApplication(t, db)
	defer stopHub()
	server := httptest.NewServer(app.routes())
	defer server.Close()

	account := registerTestAccount(t, db, server.URL, uniqueUsername("ws_logout"), "Logout")
	otherSession := loginTestAccount(t, server.URL, account.Username, account.Password, uniqueOpaqueID("ws-other"))
	current := dialAuthenticatedWebSocket(t, server.URL, account.Auth.AccessToken)
	defer current.CloseNow()
	other := dialAuthenticatedWebSocket(t, server.URL, otherSession.AccessToken)
	defer other.CloseNow()

	response := doRequest(t, http.MethodPost, server.URL+"/auth/logout", account.Auth.AccessToken, "")
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status = %d, want 204", response.StatusCode)
	}

	readContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, err := current.Read(readContext); err == nil {
		t.Fatal("revoked session websocket remained open")
	}

	payload := []byte(`{"type":"still-online"}`)
	delivered, err := app.webSocketHub.Publish(context.Background(), account.User.ID, payload)
	if err != nil || delivered != 1 {
		t.Fatalf("publish to remaining session = delivered %d, err %v", delivered, err)
	}
	otherContext, otherCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer otherCancel()
	_, received, err := other.Read(otherContext)
	if err != nil || string(received) != string(payload) {
		t.Fatalf("remaining session received %q, err %v", received, err)
	}
}

func TestFullClientQueueRemovesOnlySlowClient(t *testing.T) {
	hub := newWebSocketHub()
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	defer func() {
		cancel()
		<-hub.done
	}()

	slow := &webSocketClient{
		userID:       7,
		sessionID:    9,
		connectionID: "slow",
		send:         make(chan []byte, 1),
	}
	if err := hub.Register(context.Background(), slow); err != nil {
		t.Fatalf("register slow client: %v", err)
	}
	if delivered, err := hub.Publish(context.Background(), 7, []byte("first")); err != nil || delivered != 1 {
		t.Fatalf("first publish = delivered %d, err %v", delivered, err)
	}
	if delivered, err := hub.Publish(context.Background(), 7, []byte("second")); err != nil || delivered != 0 {
		t.Fatalf("full queue publish = delivered %d, err %v", delivered, err)
	}
	if delivered, err := hub.Publish(context.Background(), 7, []byte("third")); err != nil || delivered != 0 {
		t.Fatalf("removed client publish = delivered %d, err %v", delivered, err)
	}
}

func TestLegacyMessageCreatedPayloadRemainsReadable(t *testing.T) {
	event := outboxEvent{
		PayloadVersion: 1,
		Payload: json.RawMessage(`{
			"messageId": 88,
			"clientMessageId": "legacy-client-id-88",
			"senderId": 3,
			"receiverId": 4,
			"content": "legacy",
			"createdAt": "2026-08-29T00:00:00Z"
		}`),
	}
	payload, err := decodeMessageCreatedEvent(event)
	if err != nil {
		t.Fatalf("decode legacy payload: %v", err)
	}
	if payload.Message.ID != 88 || payload.Message.ReceiverID != 4 || len(payload.Recipients) != 1 {
		t.Fatalf("legacy payload = %+v", payload)
	}
}

func newWebSocketTestApplication(t *testing.T, db *pgxpool.Pool) (*application, func()) {
	t.Helper()
	app := newTestApplication(t, db)
	hub := newWebSocketHub()
	hubContext, cancel := context.WithCancel(context.Background())
	go hub.Run(hubContext)
	app.webSocketHub = hub
	return app, func() {
		cancel()
		select {
		case <-hub.done:
		case <-time.After(2 * time.Second):
			t.Error("websocket hub did not stop")
		}
	}
}

func dialAuthenticatedWebSocket(t *testing.T, serverURL, accessToken string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(ctx, webSocketURL(serverURL), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + accessToken}},
	})
	if err != nil {
		if response != nil {
			t.Fatalf("dial websocket: %v (status %d)", err, response.StatusCode)
		}
		t.Fatalf("dial websocket: %v", err)
	}
	return connection
}

func readWebSocketEnvelope(t *testing.T, connection *websocket.Conn) webSocketEnvelope {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	messageType, payload, err := connection.Read(ctx)
	if err != nil {
		t.Fatalf("read websocket event: %v", err)
	}
	if messageType != websocket.MessageText {
		t.Fatalf("websocket message type = %v, want text", messageType)
	}
	var envelope webSocketEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode websocket event %q: %v", payload, err)
	}
	return envelope
}

func loadOutboxEventForMessage(t *testing.T, db *pgxpool.Pool, messageID int64) outboxEvent {
	t.Helper()
	var event outboxEvent
	if err := db.QueryRow(
		context.Background(),
		`SELECT event_id::text, event_type, payload_version, message_id, payload
		 FROM outbox_events
		 WHERE message_id = $1`,
		messageID,
	).Scan(&event.EventID, &event.EventType, &event.PayloadVersion, &event.MessageID, &event.Payload); err != nil {
		t.Fatalf("load outbox event: %v", err)
	}
	return event
}

func webSocketURL(serverURL string) string {
	return fmt.Sprintf("ws%s/ws", strings.TrimPrefix(serverURL, "http"))
}
