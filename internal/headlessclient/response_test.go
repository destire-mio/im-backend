package headlessclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResponseLimitAcceptsMaximumEscapedSyncPage(t *testing.T) {
	page := messagePage{ConversationID: 1, NextCursor: 200, SnapshotCursor: 200}
	for i := int64(1); i <= 200; i++ {
		page.Messages = append(page.Messages, Message{ID: i, ConversationID: 1, ConversationSeq: i, ClientMessageID: strings.Repeat("x", 128), Content: strings.Repeat("<", 4000)})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(page) }))
	defer server.Close()
	client := &Client{baseURL: server.URL, httpClient: server.Client()}
	var got messagePage
	if err := client.doJSON(context.Background(), http.MethodGet, "/", "", nil, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 200 || got.Messages[199].Content != page.Messages[199].Content {
		t.Fatal("valid maximum page was truncated")
	}
}

func TestResponseLimitRejectsOversizedBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"data": strings.Repeat("x", maxResponseBodyBytes)})
	}))
	defer server.Close()
	client := &Client{baseURL: server.URL, httpClient: server.Client()}
	var output map[string]string
	if err := client.doJSON(context.Background(), http.MethodGet, "/", "", nil, &output); err == nil || !strings.Contains(err.Error(), "exceeds the size limit") {
		t.Fatalf("oversized response error=%v", err)
	}
}
