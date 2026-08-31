package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"
)

const (
	messageHistoryCursorVersion = 2
	defaultMessageHistoryLimit  = 50
	maxMessageHistoryLimit      = 200
	maxMessageHistoryCursorSize = 512
)

type messageHistoryDirection uint8

const (
	messageHistoryLatest messageHistoryDirection = iota
	messageHistoryBefore
	messageHistoryAfter
)

type messageHistoryCursor struct {
	Version        int   `json:"v"`
	ConversationID int64 `json:"conversationId"`
	Sequence       int64 `json:"seq"`
}

type messageHistoryPage struct {
	ConversationID int64     `json:"conversationId,omitempty"`
	Messages       []message `json:"messages"`
	BeforeCursor   string    `json:"beforeCursor,omitempty"`
	AfterCursor    string    `json:"afterCursor,omitempty"`
	HasMoreBefore  bool      `json:"hasMoreBefore"`
	HasMoreAfter   bool      `json:"hasMoreAfter"`
}

const latestMessageHistoryQuery = `
	SELECT id,
	       conversation_id,
	       conversation_seq,
	       client_message_id,
	       sender_id,
	       receiver_id,
	       content,
	       created_at
	FROM messages
	WHERE conversation_id = $1
	ORDER BY conversation_seq DESC
	LIMIT $2`

const beforeMessageHistoryQuery = `
	SELECT id,
	       conversation_id,
	       conversation_seq,
	       client_message_id,
	       sender_id,
	       receiver_id,
	       content,
	       created_at
	FROM messages
	WHERE conversation_id = $1
	  AND conversation_seq < $2
	ORDER BY conversation_seq DESC
	LIMIT $3`

const afterMessageHistoryQuery = `
	SELECT id,
	       conversation_id,
	       conversation_seq,
	       client_message_id,
	       sender_id,
	       receiver_id,
	       content,
	       created_at
	FROM messages
	WHERE conversation_id = $1
	  AND conversation_seq > $2
	ORDER BY conversation_seq ASC
	LIMIT $3`

func (app *application) listMessages(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	for key, values := range query {
		if key != "peerId" && key != "before" && key != "after" && key != "limit" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "only peerId, before, after and limit are allowed"})
			return
		}
		if len(values) != 1 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: key + " must be provided exactly once"})
			return
		}
	}

	peerValues, found := query["peerId"]
	if !found || len(peerValues) != 1 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "peerId is required exactly once"})
		return
	}
	peerID, err := strconv.ParseInt(peerValues[0], 10, 64)
	if err != nil || peerID <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "peerId must be a positive integer"})
		return
	}

	_, hasBefore := query["before"]
	_, hasAfter := query["after"]
	if hasBefore && hasAfter {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "before and after are mutually exclusive"})
		return
	}

	limit := defaultMessageHistoryLimit
	if values, found := query["limit"]; found {
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed <= 0 || parsed > maxMessageHistoryLimit {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "limit must be between 1 and 200"})
			return
		}
		limit = parsed
	}

	direction := messageHistoryLatest
	var cursor messageHistoryCursor
	if hasBefore {
		direction = messageHistoryBefore
		cursor, err = decodeMessageHistoryCursor(query.Get("before"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "before cursor is invalid"})
			return
		}
	} else if hasAfter {
		direction = messageHistoryAfter
		cursor, err = decodeMessageHistoryCursor(query.Get("after"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "after cursor is invalid"})
			return
		}
	}

	userID := authenticatedUserID(r.Context())
	conversationID, err := app.resolveDirectConversation(r.Context(), userID, peerID)
	if errors.Is(err, pgx.ErrNoRows) {
		if direction != messageHistoryLatest {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "message cursor does not belong to this conversation"})
			return
		}
		writeJSON(w, http.StatusOK, messageHistoryPage{Messages: make([]message, 0)})
		return
	}
	if err != nil {
		log.Printf("resolve direct conversation: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list messages"})
		return
	}
	if direction != messageHistoryLatest && cursor.ConversationID != conversationID {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "message cursor does not belong to this conversation"})
		return
	}

	queryLimit := limit + 1
	var rows pgx.Rows
	switch direction {
	case messageHistoryLatest:
		rows, err = app.db.Query(r.Context(), latestMessageHistoryQuery, conversationID, queryLimit)
	case messageHistoryBefore:
		rows, err = app.db.Query(r.Context(), beforeMessageHistoryQuery, conversationID, cursor.Sequence, queryLimit)
	case messageHistoryAfter:
		rows, err = app.db.Query(r.Context(), afterMessageHistoryQuery, conversationID, cursor.Sequence, queryLimit)
	}
	if err != nil {
		log.Printf("list messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list messages"})
		return
	}
	defer rows.Close()

	messages := make([]message, 0, queryLimit)
	for rows.Next() {
		var current message
		if err := scanMessage(rows, &current); err != nil {
			log.Printf("scan message: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list messages"})
			return
		}
		messages = append(messages, current)
	}
	if err := rows.Err(); err != nil {
		log.Printf("iterate messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list messages"})
		return
	}

	hasMore := len(messages) > limit
	if hasMore {
		messages = messages[:limit]
	}
	if direction != messageHistoryAfter {
		reverseMessages(messages)
	}

	page := messageHistoryPage{ConversationID: conversationID, Messages: messages}
	switch direction {
	case messageHistoryLatest:
		page.HasMoreBefore = hasMore
	case messageHistoryBefore:
		page.HasMoreBefore = hasMore
		page.HasMoreAfter = true
	case messageHistoryAfter:
		page.HasMoreBefore = true
		page.HasMoreAfter = hasMore
	}
	if len(messages) > 0 {
		page.BeforeCursor, err = encodeMessageHistoryCursor(messages[0])
		if err == nil {
			page.AfterCursor, err = encodeMessageHistoryCursor(messages[len(messages)-1])
		}
		if err != nil {
			log.Printf("encode message history cursor: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list messages"})
			return
		}
	}

	writeJSON(w, http.StatusOK, page)
}

func (app *application) resolveDirectConversation(
	ctx context.Context,
	userID int64,
	peerID int64,
) (int64, error) {
	lowUserID, highUserID := userID, peerID
	if lowUserID > highUserID {
		lowUserID, highUserID = highUserID, lowUserID
	}
	var conversationID int64
	err := app.db.QueryRow(
		ctx,
		`SELECT conversation.id
		 FROM conversations AS conversation
		 JOIN conversation_members AS member
		   ON member.conversation_id = conversation.id
		  AND member.user_id = $1
		 WHERE conversation.kind = 'direct'
		   AND conversation.direct_user_low_id = $2
		   AND conversation.direct_user_high_id = $3`,
		userID,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	return conversationID, err
}

func encodeMessageHistoryCursor(current message) (string, error) {
	payload, err := json.Marshal(messageHistoryCursor{
		Version:        messageHistoryCursorVersion,
		ConversationID: current.ConversationID,
		Sequence:       current.ConversationSeq,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeMessageHistoryCursor(raw string) (messageHistoryCursor, error) {
	if raw == "" || len(raw) > maxMessageHistoryCursorSize {
		return messageHistoryCursor{}, errors.New("invalid cursor size")
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return messageHistoryCursor{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cursor messageHistoryCursor
	if err := decoder.Decode(&cursor); err != nil {
		return messageHistoryCursor{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return messageHistoryCursor{}, errors.New("cursor must contain one JSON value")
		}
		return messageHistoryCursor{}, err
	}
	if cursor.Version != messageHistoryCursorVersion || cursor.ConversationID <= 0 || cursor.Sequence <= 0 {
		return messageHistoryCursor{}, errors.New("invalid cursor contents")
	}
	return cursor, nil
}

func reverseMessages(messages []message) {
	for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
		messages[left], messages[right] = messages[right], messages[left]
	}
}
