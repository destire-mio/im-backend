package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestMessageHistoryPaginatesBothDirectionsWithStableConversationCursor(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	alice := registerTestAccount(t, db, server.URL, uniqueUsername("history_a"), "History Alice")
	bob := registerTestAccount(t, db, server.URL, uniqueUsername("history_b"), "History Bob")

	created := []message{
		createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "one"),
		createMessageThroughAPI(t, server.URL, bob.Auth.AccessToken, alice.User.ID, "two"),
		createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "three"),
		createMessageThroughAPI(t, server.URL, bob.Auth.AccessToken, alice.User.ID, "four"),
	}
	fixed := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	for index, current := range created {
		createdAt := fixed
		if index == len(created)-1 {
			createdAt = fixed.Add(time.Second)
		}
		if _, err := db.Exec(
			t.Context(),
			`UPDATE messages SET created_at = $1 WHERE id = $2`,
			createdAt,
			current.ID,
		); err != nil {
			t.Fatalf("set message %d timestamp: %v", current.ID, err)
		}
	}

	latest := messageHistoryThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "", "", 2)
	assertMessageIDs(t, latest.Messages, created[2].ID, created[3].ID)
	if !latest.HasMoreBefore || latest.HasMoreAfter || latest.BeforeCursor == "" || latest.AfterCursor == "" {
		t.Fatalf("latest page metadata = %+v", latest)
	}
	oldestBoundary, err := decodeMessageHistoryCursor(latest.BeforeCursor)
	if err != nil ||
		oldestBoundary.ConversationID != created[2].ConversationID ||
		oldestBoundary.Sequence != created[2].ConversationSeq {
		t.Fatalf("latest before cursor = %+v, err=%v", oldestBoundary, err)
	}

	newest := createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "five")
	older := messageHistoryThroughAPI(
		t,
		server.URL,
		alice.Auth.AccessToken,
		bob.User.ID,
		latest.BeforeCursor,
		"",
		2,
	)
	assertMessageIDs(t, older.Messages, created[0].ID, created[1].ID)
	if older.HasMoreBefore || !older.HasMoreAfter {
		t.Fatalf("older page metadata = %+v", older)
	}
	sameTimestampNewer := messageHistoryThroughAPI(
		t,
		server.URL,
		alice.Auth.AccessToken,
		bob.User.ID,
		"",
		older.BeforeCursor,
		2,
	)
	assertMessageIDs(t, sameTimestampNewer.Messages, created[1].ID, created[2].ID)
	if !sameTimestampNewer.HasMoreBefore || !sameTimestampNewer.HasMoreAfter {
		t.Fatalf("same-timestamp newer page metadata = %+v", sameTimestampNewer)
	}

	newer := messageHistoryThroughAPI(
		t,
		server.URL,
		alice.Auth.AccessToken,
		bob.User.ID,
		"",
		latest.AfterCursor,
		2,
	)
	assertMessageIDs(t, newer.Messages, newest.ID)
	if !newer.HasMoreBefore || newer.HasMoreAfter {
		t.Fatalf("newer page metadata = %+v", newer)
	}
}

func TestMessageHistoryValidatesPaginationContract(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	alice := registerTestAccount(t, db, server.URL, uniqueUsername("hist_ca"), "History Contract Alice")
	bob := registerTestAccount(t, db, server.URL, uniqueUsername("hist_cb"), "History Contract Bob")
	carol := registerTestAccount(t, db, server.URL, uniqueUsername("hist_cc"), "History Contract Carol")
	createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "cursor source")
	page := messageHistoryThroughAPI(t, server.URL, alice.Auth.AccessToken, bob.User.ID, "", "", 1)
	createMessageThroughAPI(t, server.URL, alice.Auth.AccessToken, carol.User.ID, "other conversation")

	paths := []string{
		fmt.Sprintf("/messages?peerId=%d&before=%s&after=%s", bob.User.ID, page.BeforeCursor, page.AfterCursor),
		fmt.Sprintf("/messages?peerId=%d&before=not-a-cursor", bob.User.ID),
		fmt.Sprintf("/messages?peerId=%d&limit=0", bob.User.ID),
		fmt.Sprintf("/messages?peerId=%d&limit=201", bob.User.ID),
		fmt.Sprintf("/messages?peerId=%d&peerId=%d", bob.User.ID, bob.User.ID),
		fmt.Sprintf("/messages?peerId=%d&offset=10", bob.User.ID),
		fmt.Sprintf("/messages?peerId=%d&before=%s", carol.User.ID, page.BeforeCursor),
	}
	for _, path := range paths {
		response := doRequest(t, http.MethodGet, server.URL+path, alice.Auth.AccessToken, "")
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("GET %s status = %d, want 400", path, response.StatusCode)
		}
	}
}

func TestMessageHistoryDoesNotDuplicateSelfMessages(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	account := registerTestAccount(t, db, server.URL, uniqueUsername("history_self"), "History Self")
	created := createMessageThroughAPI(t, server.URL, account.Auth.AccessToken, account.User.ID, "note to self")

	page := messageHistoryThroughAPI(t, server.URL, account.Auth.AccessToken, account.User.ID, "", "", 10)
	assertMessageIDs(t, page.Messages, created.ID)
}

func messageHistoryThroughAPI(
	t *testing.T,
	serverURL string,
	token string,
	peerID int64,
	before string,
	after string,
	limit int,
) messageHistoryPage {
	t.Helper()
	query := url.Values{"peerId": {fmt.Sprintf("%d", peerID)}}
	if before != "" {
		query.Set("before", before)
	}
	if after != "" {
		query.Set("after", after)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	response := doRequest(t, http.MethodGet, serverURL+"/messages?"+query.Encode(), token, "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list messages status = %d, want 200", response.StatusCode)
	}
	var page messageHistoryPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode message history: %v", err)
	}
	return page
}

func assertMessageIDs(t *testing.T, messages []message, want ...int64) {
	t.Helper()
	if len(messages) != len(want) {
		t.Fatalf("message count = %d, want %d: %+v", len(messages), len(want), messages)
	}
	for index, expected := range want {
		if messages[index].ID != expected {
			t.Fatalf("message %d id = %d, want %d", index, messages[index].ID, expected)
		}
	}
}
