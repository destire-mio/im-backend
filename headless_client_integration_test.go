package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/destire-mio/im-backend/internal/headlessclient"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHeadlessClientRecoversRefreshSyncAndACKAfterLostResponses(t *testing.T) {
	db := openTestDatabase(t)
	app, stopHub := newWebSocketTestApplication(t, db)
	defer stopHub()
	server := httptest.NewServer(app.routes())
	defer server.Close()

	sender := registerTestAccount(t, db, server.URL, uniqueUsername("hc_s"), "Headless Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("hc_r"), "Headless Receiver")
	secondDevice := loginTestAccountForDevice(
		t,
		server.URL,
		receiver.Username,
		receiver.Password,
		uniqueOpaqueID("headless-second-login"),
		"headless-second-device",
	)

	statePath := filepath.Join(t.TempDir(), "phone.state")
	stateKey := bytes.Repeat([]byte{17}, 32)
	store := newHeadlessStore(t, statePath, stateKey)
	phone := newHeadlessClient(t, server.URL, store, http.DefaultClient)
	if err := phone.PersistAuth(toHeadlessAuth(receiver.Auth)); err != nil {
		t.Fatal(err)
	}

	refreshKey := uniqueOpaqueID("headless-refresh")
	lostRefresh := &loseSuccessfulResponseTransport{
		base: http.DefaultTransport,
		path: "/auth/refresh",
	}
	phone = newHeadlessClient(t, server.URL, store, &http.Client{Transport: lostRefresh})
	if err := phone.Refresh(context.Background(), refreshKey); err == nil {
		t.Fatal("refresh with a lost response unexpectedly succeeded locally")
	}
	afterLostRefresh, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if afterLostRefresh.PendingRefresh == nil ||
		afterLostRefresh.PendingRefresh.IdempotencyKey != refreshKey ||
		afterLostRefresh.Auth.RefreshToken != receiver.Auth.RefreshToken {
		t.Fatalf("durable refresh recovery state = %+v", afterLostRefresh)
	}

	store = newHeadlessStore(t, statePath, stateKey)
	phone = newHeadlessClient(t, server.URL, store, http.DefaultClient)
	if err := phone.Refresh(context.Background(), refreshKey); err != nil {
		t.Fatalf("recover refresh after restart: %v", err)
	}
	afterRefresh, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if afterRefresh.PendingRefresh != nil ||
		afterRefresh.Auth.RefreshToken == receiver.Auth.RefreshToken ||
		afterRefresh.Auth.AccessToken == receiver.Auth.AccessToken {
		t.Fatalf("recovered token state = %+v", afterRefresh)
	}

	webSocket := dialAuthenticatedWebSocket(t, server.URL, afterRefresh.Auth.AccessToken)
	defer webSocket.CloseNow()
	created := make([]message, 0, 3)
	for _, content := range []string{"headless one", "headless two", "headless three"} {
		created = append(created, createMessageThroughAPI(
			t,
			server.URL,
			sender.Auth.AccessToken,
			receiver.User.ID,
			content,
		))
	}
	conversationID := created[0].ConversationID
	worker := mustTestWorker(t, db, &webSocketOutboxPublisher{router: app.webSocketHub}, defaultOutboxWorkerConfig())
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 3 {
		t.Fatalf("publish realtime messages = %d, %v", processed, err)
	}
	envelopes := make([]webSocketEnvelope, 0, 3)
	for range created {
		envelopes = append(envelopes, readWebSocketEnvelope(t, webSocket))
	}
	sort.Slice(envelopes, func(first, second int) bool {
		return envelopes[first].ConversationSeq < envelopes[second].ConversationSeq
	})
	for index, envelope := range envelopes {
		if envelope.ConversationSeq != int64(index+1) {
			t.Fatalf("realtime envelope %d cursor = %d, want %d", index, envelope.ConversationSeq, index+1)
		}
	}
	if _, err := phone.ApplyRealtime(toHeadlessEnvelope(envelopes[1])); !errors.Is(err, headlessclient.ErrMessageGap) {
		t.Fatalf("out-of-order realtime error = %v", err)
	}
	beforeSync, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if conversation := beforeSync.Conversations[fmt.Sprint(conversationID)]; conversation != nil {
		t.Fatalf("gapped realtime advanced local state: %+v", conversation)
	}

	if err := phone.SyncConversation(context.Background(), conversationID, 2); err != nil {
		t.Fatalf("repair realtime gap through Sync: %v", err)
	}
	afterSync, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	conversation := afterSync.Conversations[fmt.Sprint(conversationID)]
	if conversation == nil || conversation.AppliedCursor != 3 || conversation.PendingACK != 3 || len(conversation.Messages) != 3 {
		t.Fatalf("durable synced conversation = %+v", conversation)
	}
	if result, err := phone.ApplyRealtime(toHeadlessEnvelope(envelopes[1])); err != nil || result != headlessclient.Duplicate {
		t.Fatalf("late realtime duplicate = %q, %v", result, err)
	}

	store = newHeadlessStore(t, statePath, stateKey)
	lostACK := &loseSuccessfulResponseTransport{
		base: http.DefaultTransport,
		path: fmt.Sprintf("/conversations/%d/ack", conversationID),
	}
	phone = newHeadlessClient(t, server.URL, store, &http.Client{Transport: lostACK})
	if err := phone.FlushACK(context.Background(), conversationID); err == nil {
		t.Fatal("ACK with a lost response unexpectedly succeeded locally")
	}
	assertServerDeviceCursor(t, db, receiver.User.ID, receiver.Auth.DeviceID, conversationID, 3)
	afterLostACK, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if afterLostACK.Conversations[fmt.Sprint(conversationID)].PendingACK != 3 {
		t.Fatalf("lost ACK did not remain pending: %+v", afterLostACK.Conversations[fmt.Sprint(conversationID)])
	}

	store = newHeadlessStore(t, statePath, stateKey)
	phone = newHeadlessClient(t, server.URL, store, http.DefaultClient)
	if err := phone.FlushACK(context.Background(), conversationID); err != nil {
		t.Fatalf("retry ACK after restart: %v", err)
	}
	afterACK, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	conversation = afterACK.Conversations[fmt.Sprint(conversationID)]
	if conversation.PendingACK != 0 || conversation.AckedCursor != 3 {
		t.Fatalf("ACK recovery state = %+v", conversation)
	}

	secondPath := filepath.Join(t.TempDir(), "second-device.state")
	secondStore := newHeadlessStore(t, secondPath, bytes.Repeat([]byte{23}, 32))
	second := newHeadlessClient(t, server.URL, secondStore, http.DefaultClient)
	if err := second.PersistAuth(toHeadlessAuth(secondDevice)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.ApplyRealtime(toHeadlessEnvelope(envelopes[0])); err != nil {
		t.Fatalf("second device apply first realtime message: %v", err)
	}
	if err := second.FlushACK(context.Background(), conversationID); err != nil {
		t.Fatalf("second device ACK: %v", err)
	}
	assertServerDeviceCursor(t, db, receiver.User.ID, receiver.Auth.DeviceID, conversationID, 3)
	assertServerDeviceCursor(t, db, receiver.User.ID, secondDevice.DeviceID, conversationID, 1)

	logout := doRequest(t, http.MethodPost, server.URL+"/auth/logout", afterACK.Auth.AccessToken, "")
	logout.Body.Close()
	if logout.StatusCode != http.StatusNoContent {
		t.Fatalf("logout status = %d, want 204", logout.StatusCode)
	}
	err = phone.SyncConversation(context.Background(), conversationID, 10)
	var apiError *headlessclient.APIError
	if !errors.As(err, &apiError) || apiError.Status != http.StatusUnauthorized {
		t.Fatalf("sync with revoked client state error = %v", err)
	}
}

type loseSuccessfulResponseTransport struct {
	base http.RoundTripper
	path string
	mu   sync.Mutex
	lost bool
}

func (transport *loseSuccessfulResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	transport.mu.Lock()
	shouldLose := !transport.lost && request.URL.Path == transport.path && response.StatusCode >= 200 && response.StatusCode < 300
	if shouldLose {
		transport.lost = true
	}
	transport.mu.Unlock()
	if !shouldLose {
		return response, nil
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return nil, errors.New("simulated successful response loss")
}

func newHeadlessStore(t *testing.T, path string, key []byte) *headlessclient.FileStore {
	t.Helper()
	store, err := headlessclient.NewFileStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newHeadlessClient(
	t *testing.T,
	serverURL string,
	store *headlessclient.FileStore,
	httpClient *http.Client,
) *headlessclient.Client {
	t.Helper()
	client, err := headlessclient.New(serverURL, store, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func toHeadlessAuth(auth authResponse) headlessclient.AuthState {
	return headlessclient.AuthState{
		SessionID:             auth.SessionID,
		DeviceID:              auth.DeviceID,
		SessionExpiresAt:      auth.SessionExpiresAt.Format(time.RFC3339Nano),
		AccessToken:           auth.AccessToken,
		AccessTokenExpiresAt:  auth.AccessTokenExpiresAt.Format(time.RFC3339Nano),
		RefreshToken:          auth.RefreshToken,
		RefreshTokenExpiresAt: auth.RefreshTokenExpiresAt.Format(time.RFC3339Nano),
	}
}

func toHeadlessEnvelope(envelope webSocketEnvelope) headlessclient.RealtimeEnvelope {
	return headlessclient.RealtimeEnvelope{
		Type:            envelope.Type,
		ConversationID:  envelope.ConversationID,
		ConversationSeq: envelope.ConversationSeq,
		Message: headlessclient.Message{
			ID:              envelope.Message.ID,
			ConversationID:  envelope.Message.ConversationID,
			ConversationSeq: envelope.Message.ConversationSeq,
			ClientMessageID: envelope.Message.ClientMessageID,
			SenderID:        envelope.Message.SenderID,
			ReceiverID:      envelope.Message.ReceiverID,
			Content:         envelope.Message.Content,
			CreatedAt:       envelope.Message.CreatedAt.Format(time.RFC3339Nano),
		},
	}
}

func assertServerDeviceCursor(
	t *testing.T,
	db *pgxpool.Pool,
	userID int64,
	deviceID string,
	conversationID int64,
	want int64,
) {
	t.Helper()
	var cursor int64
	if err := db.QueryRow(
		context.Background(),
		`SELECT applied_seq
		 FROM device_conversation_sync_states
		 WHERE user_id = $1 AND device_id = $2 AND conversation_id = $3`,
		userID,
		deviceID,
		conversationID,
	).Scan(&cursor); err != nil {
		t.Fatal(err)
	}
	if cursor != want {
		t.Fatalf("server device cursor for %q = %d, want %d", deviceID, cursor, want)
	}
}
