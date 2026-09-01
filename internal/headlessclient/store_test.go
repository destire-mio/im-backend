package headlessclient

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreEncryptsAuthenticationState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.state")
	key := bytes.Repeat([]byte{7}, 32)
	store, err := NewFileStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	accessToken := "secret-access-token"
	refreshToken := "secret-refresh-token"
	if err := store.Update(func(state *State) error {
		state.Auth = &AuthState{
			SessionID:    41,
			DeviceID:     "phone",
			AccessToken:  accessToken,
			RefreshToken: refreshToken,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(accessToken)) || bytes.Contains(raw, []byte(refreshToken)) {
		t.Fatal("client state contains a plaintext token")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("client state permissions = %o, want 600", info.Mode().Perm())
	}

	reopened, err := NewFileStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Auth == nil || state.Auth.AccessToken != accessToken || state.Auth.RefreshToken != refreshToken {
		t.Fatalf("reopened authentication state = %+v", state.Auth)
	}
}

func TestFileStoreFailedAtomicReplacePreservesPreviousState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.state")
	key := bytes.Repeat([]byte{9}, 32)
	store, err := NewFileStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(state *State) error {
		state.Conversations["11"] = &ConversationState{
			AppliedCursor: 1,
			Messages: map[string]Message{
				"1": {ID: 101, ConversationID: 11, ConversationSeq: 1},
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	injected := errors.New("simulated crash before atomic replace")
	failing, err := NewFileStore(path, key, WithBeforeRename(func() error { return injected }))
	if err != nil {
		t.Fatal(err)
	}
	err = failing.Update(func(state *State) error {
		conversation := state.Conversations["11"]
		conversation.Messages["2"] = Message{ID: 102, ConversationID: 11, ConversationSeq: 2}
		conversation.AppliedCursor = 2
		return nil
	})
	if !errors.Is(err, injected) {
		t.Fatalf("failed update error = %v", err)
	}

	reopened, err := NewFileStore(path, key)
	if err != nil {
		t.Fatal(err)
	}
	state, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	conversation := state.Conversations["11"]
	if conversation.AppliedCursor != 1 || len(conversation.Messages) != 1 {
		t.Fatalf("state after interrupted replace = %+v", conversation)
	}
}

func TestRealtimeApplyRejectsGapAndDeduplicatesCommittedMessage(t *testing.T) {
	store, err := NewFileStore(filepath.Join(t.TempDir(), "client.state"), bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	client, err := New("http://im.example", store, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := RealtimeEnvelope{
		Type:            "message.created",
		ConversationID:  5,
		ConversationSeq: 1,
		Message:         Message{ID: 51, ConversationID: 5, ConversationSeq: 1},
	}
	result, err := client.ApplyRealtime(first)
	if err != nil || result != Applied {
		t.Fatalf("first realtime apply = %q, %v", result, err)
	}
	result, err = client.ApplyRealtime(first)
	if err != nil || result != Duplicate {
		t.Fatalf("duplicate realtime apply = %q, %v", result, err)
	}
	gapped := RealtimeEnvelope{
		Type:            "message.created",
		ConversationID:  5,
		ConversationSeq: 3,
		Message:         Message{ID: 53, ConversationID: 5, ConversationSeq: 3},
	}
	if _, err := client.ApplyRealtime(gapped); !errors.Is(err, ErrMessageGap) {
		t.Fatalf("gapped realtime apply error = %v", err)
	}
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Conversations["5"].AppliedCursor != 1 || len(state.Conversations["5"].Messages) != 1 {
		t.Fatalf("gapped message changed durable state: %+v", state.Conversations["5"])
	}
}
