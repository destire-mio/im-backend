package headlessclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrMessageGap = errors.New("realtime message has a cursor gap")

type APIError struct {
	Status int
	Code   string
}

func (err *APIError) Error() string {
	return fmt.Sprintf("IM API returned status %d (%s)", err.Status, err.Code)
}

type RealtimeEnvelope struct {
	Type            string  `json:"type"`
	ConversationID  int64   `json:"conversationId"`
	ConversationSeq int64   `json:"conversationSeq"`
	Message         Message `json:"message"`
}

type ApplyResult string

const (
	Applied   ApplyResult = "applied"
	Duplicate ApplyResult = "duplicate"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	store      *FileStore
}

func New(baseURL string, store *FileStore, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("valid IM base URL is required")
	}
	if store == nil {
		return nil, errors.New("client state store is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
		store:      store,
	}, nil
}

func (client *Client) PersistAuth(auth AuthState) error {
	if auth.AccessToken == "" || auth.RefreshToken == "" || auth.SessionID <= 0 || auth.DeviceID == "" {
		return errors.New("complete authentication state is required")
	}
	return client.store.Update(func(state *State) error {
		copy := auth
		state.Auth = &copy
		state.PendingRefresh = nil
		return nil
	})
}

func (client *Client) Refresh(ctx context.Context, idempotencyKey string) error {
	if idempotencyKey == "" {
		return errors.New("refresh idempotency key is required")
	}
	var pending PendingRefresh
	if err := client.store.Update(func(state *State) error {
		if state.Auth == nil || state.Auth.RefreshToken == "" {
			return errors.New("client is not authenticated")
		}
		if state.PendingRefresh == nil {
			state.PendingRefresh = &PendingRefresh{
				RefreshToken:   state.Auth.RefreshToken,
				IdempotencyKey: idempotencyKey,
			}
		}
		if state.PendingRefresh.IdempotencyKey != idempotencyKey {
			return errors.New("a different refresh operation is already pending")
		}
		pending = *state.PendingRefresh
		return nil
	}); err != nil {
		return err
	}

	var response AuthState
	if err := client.doJSON(ctx, http.MethodPost, "/auth/refresh", "", map[string]string{
		"refreshToken":   pending.RefreshToken,
		"idempotencyKey": pending.IdempotencyKey,
	}, &response); err != nil {
		return err
	}
	return client.store.Update(func(state *State) error {
		if state.PendingRefresh == nil || *state.PendingRefresh != pending {
			return errors.New("pending refresh changed before commit")
		}
		state.Auth = &response
		state.PendingRefresh = nil
		return nil
	})
}

func (client *Client) ApplyRealtime(envelope RealtimeEnvelope) (ApplyResult, error) {
	if envelope.Type != "message.created" || envelope.ConversationID <= 0 {
		return "", errors.New("invalid realtime message envelope")
	}
	if envelope.Message.ConversationID != envelope.ConversationID ||
		envelope.Message.ConversationSeq != envelope.ConversationSeq {
		return "", errors.New("realtime envelope and message cursor disagree")
	}
	return client.applyMessage(envelope.Message)
}

func (client *Client) SyncConversation(ctx context.Context, conversationID int64, pageLimit int) error {
	if conversationID <= 0 || pageLimit <= 0 || pageLimit > 200 {
		return errors.New("valid conversation and page limit are required")
	}
	var snapshotCursor int64
	for {
		state, err := client.store.Snapshot()
		if err != nil {
			return err
		}
		if state.Auth == nil {
			return errors.New("client is not authenticated")
		}
		conversation := state.Conversations[strconv.FormatInt(conversationID, 10)]
		var after int64
		if conversation != nil {
			after = conversation.AppliedCursor
		}
		query := url.Values{
			"after": []string{strconv.FormatInt(after, 10)},
			"limit": []string{strconv.Itoa(pageLimit)},
		}
		if snapshotCursor > 0 {
			query.Set("snapshotCursor", strconv.FormatInt(snapshotCursor, 10))
		}
		var page messagePage
		path := fmt.Sprintf("/conversations/%d/messages?%s", conversationID, query.Encode())
		if err := client.doJSON(ctx, http.MethodGet, path, state.Auth.AccessToken, nil, &page); err != nil {
			return err
		}
		if page.ConversationID != conversationID || page.NextCursor < after {
			return errors.New("sync page cursor is invalid")
		}
		if snapshotCursor == 0 {
			snapshotCursor = page.SnapshotCursor
		} else if page.SnapshotCursor != snapshotCursor {
			return errors.New("sync snapshot changed between pages")
		}
		for _, message := range page.Messages {
			if _, err := client.applyMessage(message); err != nil {
				return err
			}
		}
		state, err = client.store.Snapshot()
		if err != nil {
			return err
		}
		conversation = state.Conversations[strconv.FormatInt(conversationID, 10)]
		if conversation == nil || conversation.AppliedCursor != page.NextCursor {
			return errors.New("sync page did not advance to the advertised cursor")
		}
		if !page.HasMore {
			return nil
		}
	}
}

func (client *Client) FlushACK(ctx context.Context, conversationID int64) error {
	state, err := client.store.Snapshot()
	if err != nil {
		return err
	}
	if state.Auth == nil {
		return errors.New("client is not authenticated")
	}
	conversationKey := strconv.FormatInt(conversationID, 10)
	conversation := state.Conversations[conversationKey]
	if conversation == nil || conversation.PendingACK <= conversation.AckedCursor {
		return nil
	}
	pending := conversation.PendingACK
	var response struct {
		AppliedCursor int64 `json:"appliedCursor"`
	}
	path := fmt.Sprintf("/conversations/%d/ack", conversationID)
	if err := client.doJSON(ctx, http.MethodPost, path, state.Auth.AccessToken, map[string]int64{
		"cursor": pending,
	}, &response); err != nil {
		return err
	}
	if response.AppliedCursor < pending {
		return errors.New("server acknowledged an older cursor")
	}
	return client.store.Update(func(current *State) error {
		conversation := current.Conversations[conversationKey]
		if conversation == nil {
			return errors.New("conversation disappeared before ACK commit")
		}
		if response.AppliedCursor > conversation.AckedCursor {
			conversation.AckedCursor = response.AppliedCursor
		}
		if conversation.PendingACK <= response.AppliedCursor {
			conversation.PendingACK = 0
		}
		return nil
	})
}

func (client *Client) applyMessage(message Message) (ApplyResult, error) {
	if message.ID <= 0 || message.ConversationID <= 0 || message.ConversationSeq <= 0 {
		return "", errors.New("message identity and cursor must be positive")
	}
	result := Applied
	err := client.store.Update(func(state *State) error {
		key := strconv.FormatInt(message.ConversationID, 10)
		conversation := state.Conversations[key]
		if conversation == nil {
			conversation = &ConversationState{Messages: make(map[string]Message)}
			state.Conversations[key] = conversation
		}
		messageKey := strconv.FormatInt(message.ConversationSeq, 10)
		if message.ConversationSeq <= conversation.AppliedCursor {
			existing, found := conversation.Messages[messageKey]
			if !found || existing.ID != message.ID {
				return errors.New("message cursor conflicts with durable local state")
			}
			result = Duplicate
			return nil
		}
		if message.ConversationSeq != conversation.AppliedCursor+1 {
			return ErrMessageGap
		}
		conversation.Messages[messageKey] = message
		conversation.AppliedCursor = message.ConversationSeq
		if conversation.AppliedCursor > conversation.PendingACK {
			conversation.PendingACK = conversation.AppliedCursor
		}
		return nil
	})
	return result, err
}

func (client *Client) doJSON(
	ctx context.Context,
	method string,
	path string,
	accessToken string,
	input any,
	output any,
) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode IM request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("create IM request: %w", err)
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if accessToken != "" {
		request.Header.Set("Authorization", "Bearer "+accessToken)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("send IM request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var apiError struct {
			Code string `json:"code"`
		}
		_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&apiError)
		return &APIError{Status: response.StatusCode, Code: apiError.Code}
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(output); err != nil {
		return fmt.Errorf("decode IM response: %w", err)
	}
	return nil
}

type messagePage struct {
	ConversationID int64     `json:"conversationId"`
	Messages       []Message `json:"messages"`
	NextCursor     int64     `json:"nextCursor"`
	SnapshotCursor int64     `json:"snapshotCursor"`
	HasMore        bool      `json:"hasMore"`
}
