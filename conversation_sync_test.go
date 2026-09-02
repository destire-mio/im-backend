package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConversationListUsesStableMembershipSnapshot(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	alice := registerTestAccount(t, db, server.URL, uniqueUsername("list_a"), "List Alice")
	bob := registerTestAccount(t, db, server.URL, uniqueUsername("list_b"), "List Bob")
	carol := registerTestAccount(t, db, server.URL, uniqueUsername("list_c"), "List Carol")
	dave := registerTestAccount(t, db, server.URL, uniqueUsername("list_d"), "List Dave")
	outsider := registerTestAccount(t, db, server.URL, uniqueUsername("list_o"), "List Outsider")

	bobMessage := createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "hello Bob")
	carolMessage := createMessageThroughAPI(t, server.URL, carol.Auth.AccessToken, alice.User.ID, "hello Alice")

	first := listConversationsThroughAPI(t, server.URL, alice.Auth.AccessToken, 0, 1)
	if len(first.Conversations) != 1 || !first.HasMore {
		t.Fatalf("first conversation page = %+v", first)
	}
	if current := first.Conversations[0]; current.ID != bobMessage.ConversationID || current.Peer.ID != bob.User.ID || current.LastSeq != 1 {
		t.Fatalf("first conversation = %+v", current)
	}

	daveMessage := createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, dave.User.ID, "hello Dave")
	second := listConversationsThroughAPI(
		t,
		server.URL,
		alice.Auth.AccessToken,
		first.NextCursor,
		1,
		first.SnapshotCursor,
	)
	if len(second.Conversations) != 1 || second.HasMore {
		t.Fatalf("second conversation page = %+v", second)
	}
	if current := second.Conversations[0]; current.ID != carolMessage.ConversationID || current.Peer.ID != carol.User.ID {
		t.Fatalf("second conversation = %+v", current)
	}

	nextCycle := listConversationsThroughAPI(
		t,
		server.URL,
		alice.Auth.AccessToken,
		second.NextCursor,
		10,
	)
	if len(nextCycle.Conversations) != 1 || nextCycle.Conversations[0].ID != daveMessage.ConversationID {
		t.Fatalf("next conversation scan = %+v", nextCycle)
	}

	foreign := listConversationsThroughAPI(t, server.URL, outsider.Auth.AccessToken, 0, 10)
	if len(foreign.Conversations) != 0 || foreign.SnapshotCursor != 0 {
		t.Fatalf("outsider conversation page = %+v", foreign)
	}
}

func TestLegacyUserSyncEndpointsDirectClientsToConversationSync(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("gone"), "Gone Client")

	for _, request := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/messages/sync?after=0&limit=10"},
		{method: http.MethodPost, path: "/messages/ack", body: `{"cursor":0}`},
	} {
		response := doRequest(t, request.method, server.URL+request.path, account.Auth.AccessToken, request.body)
		response.Body.Close()
		if response.StatusCode != http.StatusGone {
			t.Fatalf("%s %s status = %d, want 410", request.method, request.path, response.StatusCode)
		}
	}
}

func TestSelfConversationHasOneMemberAndOneRealtimeRecipient(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("self_v4"), "Self V4")
	created := createMessageThroughAPI(t, server.URL, account.Auth.AccessToken, account.User.ID, "note to self")

	page := listConversationsThroughAPI(t, server.URL, account.Auth.AccessToken, 0, 10)
	if len(page.Conversations) != 1 || page.Conversations[0].Peer.ID != account.User.ID || page.Conversations[0].LastSeq != 1 {
		t.Fatalf("self conversation page = %+v", page)
	}
	publisher := &testPublisher{}
	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	worker := mustTestWorker(t, db, publisher, config)
	processed, err := worker.RunOnce(t.Context())
	if err != nil || processed != 1 {
		t.Fatalf("publish self v4 event = %d, err %v", processed, err)
	}
	received := publisher.received()
	if len(received) != 1 || received[0].PayloadVersion != 4 {
		t.Fatalf("self v4 events = %+v", received)
	}
	payload, err := decodeMessageCreatedEvent(received[0])
	if err != nil {
		t.Fatalf("decode self v4 event: %v", err)
	}
	if len(payload.Recipients) != 1 || payload.Recipients[0].UserID != account.User.ID {
		t.Fatalf("self v4 recipients = %+v", payload.Recipients)
	}
	var members int
	if err := db.QueryRow(
		t.Context(),
		`SELECT count(*) FROM conversation_members WHERE conversation_id = $1`,
		created.ConversationID,
	).Scan(&members); err != nil {
		t.Fatalf("count self conversation members: %v", err)
	}
	if members != 1 {
		t.Fatalf("self conversation members = %d, want 1", members)
	}
}

func listConversationsThroughAPI(
	t *testing.T,
	serverURL string,
	token string,
	after int64,
	limit int,
	snapshotCursor ...int64,
) conversationListResponse {
	t.Helper()
	path := fmt.Sprintf("%s/conversations?after=%d&limit=%d", serverURL, after, limit)
	if len(snapshotCursor) > 1 {
		t.Fatal("conversation list helper accepts at most one snapshot cursor")
	}
	if len(snapshotCursor) == 1 {
		path += fmt.Sprintf("&snapshotCursor=%d", snapshotCursor[0])
	}
	response := doRequest(t, http.MethodGet, path, token, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list conversations status = %d, want 200", response.StatusCode)
	}
	var page conversationListResponse
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode conversation page: %v", err)
	}
	return page
}
