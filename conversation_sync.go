package main

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultConversationPageLimit = 100
	maxConversationPageLimit     = 200
)

type conversationSummary struct {
	ID        int64     `json:"id"`
	Kind      string    `json:"kind"`
	Peer      user      `json:"peer"`
	LastSeq   int64     `json:"lastSeq"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type conversationListResponse struct {
	Conversations  []conversationSummary `json:"conversations"`
	NextCursor     int64                 `json:"nextCursor"`
	SnapshotCursor int64                 `json:"snapshotCursor"`
	HasMore        bool                  `json:"hasMore"`
}

type conversationMessagePage struct {
	ConversationID int64     `json:"conversationId"`
	Messages       []message `json:"messages"`
	NextCursor     int64     `json:"nextCursor"`
	SnapshotCursor int64     `json:"snapshotCursor"`
	HasMore        bool      `json:"hasMore"`
}

func (app *application) listConversations(w http.ResponseWriter, r *http.Request) {
	after, limit, rawSnapshot, ok := parseSnapshotPagination(w, r)
	if !ok {
		return
	}

	userID := authenticatedUserID(r.Context())
	var currentCursor int64
	if err := app.db.QueryRow(
		r.Context(),
		`SELECT COALESCE(max(conversation.id), 0)
		 FROM conversation_members AS member
		 JOIN conversations AS conversation ON conversation.id = member.conversation_id
		 WHERE member.user_id = $1
		   AND conversation.kind = 'direct'`,
		userID,
	).Scan(&currentCursor); err != nil {
		log.Printf("read conversation list snapshot: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list conversations"})
		return
	}
	if after > currentCursor {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "after is ahead of the user's current conversation list"})
		return
	}

	snapshotCursor, ok := validateSnapshotCursor(w, rawSnapshot, after, currentCursor, "conversation list")
	if !ok {
		return
	}

	rows, err := app.db.Query(
		r.Context(),
		`SELECT conversation.id,
		        conversation.kind,
		        peer.id,
		        peer.username,
		        peer.display_name,
		        conversation.last_seq,
		        conversation.updated_at
		 FROM conversation_members AS member
		 JOIN conversations AS conversation ON conversation.id = member.conversation_id
		 JOIN users AS peer
		   ON peer.id = CASE
		       WHEN conversation.direct_user_low_id = $1
		        AND conversation.direct_user_high_id <> $1
		       THEN conversation.direct_user_high_id
		       ELSE conversation.direct_user_low_id
		   END
		 WHERE member.user_id = $1
		   AND conversation.kind = 'direct'
		   AND conversation.id > $2
		   AND conversation.id <= $3
		 ORDER BY conversation.id
		 LIMIT $4`,
		userID,
		after,
		snapshotCursor,
		limit+1,
	)
	if err != nil {
		log.Printf("list conversations: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list conversations"})
		return
	}
	defer rows.Close()

	conversations := make([]conversationSummary, 0, limit)
	hasMore := false
	for rows.Next() {
		var current conversationSummary
		if err := rows.Scan(
			&current.ID,
			&current.Kind,
			&current.Peer.ID,
			&current.Peer.Username,
			&current.Peer.DisplayName,
			&current.LastSeq,
			&current.UpdatedAt,
		); err != nil {
			log.Printf("scan conversation: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list conversations"})
			return
		}
		if len(conversations) == limit {
			hasMore = true
			break
		}
		conversations = append(conversations, current)
	}
	if err := rows.Err(); err != nil {
		log.Printf("iterate conversations: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list conversations"})
		return
	}

	nextCursor := after
	if len(conversations) > 0 {
		nextCursor = conversations[len(conversations)-1].ID
	}
	writeJSON(w, http.StatusOK, conversationListResponse{
		Conversations:  conversations,
		NextCursor:     nextCursor,
		SnapshotCursor: snapshotCursor,
		HasMore:        hasMore,
	})
}

func (app *application) syncConversationMessages(w http.ResponseWriter, r *http.Request) {
	conversationID, ok := parseConversationID(w, r)
	if !ok {
		return
	}
	after, limit, rawSnapshot, ok := parseSnapshotPagination(w, r)
	if !ok {
		return
	}

	userID := authenticatedUserID(r.Context())
	var currentCursor int64
	err := app.db.QueryRow(
		r.Context(),
		`SELECT conversation.last_seq
		 FROM conversation_members AS member
		 JOIN conversations AS conversation ON conversation.id = member.conversation_id
		 WHERE member.user_id = $1
		   AND conversation.id = $2`,
		userID,
		conversationID,
	).Scan(&currentCursor)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "conversation not found"})
		return
	}
	if err != nil {
		log.Printf("read conversation message snapshot: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync conversation"})
		return
	}
	if after > currentCursor {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "after is ahead of the conversation's current stream"})
		return
	}

	snapshotCursor, ok := validateSnapshotCursor(w, rawSnapshot, after, currentCursor, "conversation stream")
	if !ok {
		return
	}

	rows, err := app.db.Query(
		r.Context(),
		`SELECT id,
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
		   AND conversation_seq <= $3
		 ORDER BY conversation_seq
		 LIMIT $4`,
		conversationID,
		after,
		snapshotCursor,
		limit+1,
	)
	if err != nil {
		log.Printf("sync conversation messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync conversation"})
		return
	}
	defer rows.Close()

	messages := make([]message, 0, limit)
	hasMore := false
	for rows.Next() {
		var current message
		if err := scanMessage(rows, &current); err != nil {
			log.Printf("scan conversation message: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync conversation"})
			return
		}
		if len(messages) == limit {
			hasMore = true
			break
		}
		messages = append(messages, current)
	}
	if err := rows.Err(); err != nil {
		log.Printf("iterate conversation messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync conversation"})
		return
	}

	nextCursor := after
	if len(messages) > 0 {
		nextCursor = messages[len(messages)-1].ConversationSeq
	}
	if app.metrics != nil {
		app.metrics.syncPages.WithLabelValues(strconv.FormatBool(hasMore)).Inc()
		app.metrics.syncEvents.Add(float64(len(messages)))
	}
	writeJSON(w, http.StatusOK, conversationMessagePage{
		ConversationID: conversationID,
		Messages:       messages,
		NextCursor:     nextCursor,
		SnapshotCursor: snapshotCursor,
		HasMore:        hasMore,
	})
}

func parseSnapshotPagination(w http.ResponseWriter, r *http.Request) (int64, int, *int64, bool) {
	query := r.URL.Query()
	for key, values := range query {
		if key != "after" && key != "limit" && key != "snapshotCursor" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "only after, limit and snapshotCursor are allowed"})
			return 0, 0, nil, false
		}
		if len(values) != 1 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: key + " must be provided exactly once"})
			return 0, 0, nil, false
		}
	}

	after := int64(0)
	if values, found := query["after"]; found {
		parsed, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || parsed < 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "after must be a non-negative integer"})
			return 0, 0, nil, false
		}
		after = parsed
	}

	limit := defaultConversationPageLimit
	if values, found := query["limit"]; found {
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed <= 0 || parsed > maxConversationPageLimit {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "limit must be between 1 and 200"})
			return 0, 0, nil, false
		}
		limit = parsed
	}

	var snapshot *int64
	if values, found := query["snapshotCursor"]; found {
		parsed, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || parsed < 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "snapshotCursor must be a non-negative integer"})
			return 0, 0, nil, false
		}
		snapshot = &parsed
	}
	return after, limit, snapshot, true
}

func validateSnapshotCursor(
	w http.ResponseWriter,
	rawSnapshot *int64,
	after int64,
	current int64,
	streamName string,
) (int64, bool) {
	if rawSnapshot == nil {
		return current, true
	}
	if *rawSnapshot < after {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "snapshotCursor must be greater than or equal to after"})
		return 0, false
	}
	if *rawSnapshot > current {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "snapshotCursor is ahead of the user's current " + streamName})
		return 0, false
	}
	return *rawSnapshot, true
}

func parseConversationID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	conversationID, err := strconv.ParseInt(r.PathValue("conversationID"), 10, 64)
	if err != nil || conversationID <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "conversationID must be a positive integer"})
		return 0, false
	}
	return conversationID, true
}

type messageScanner interface {
	Scan(destinations ...any) error
}

func scanMessage(scanner messageScanner, current *message) error {
	return scanner.Scan(
		&current.ID,
		&current.ConversationID,
		&current.ConversationSeq,
		&current.ClientMessageID,
		&current.SenderID,
		&current.ReceiverID,
		&current.Content,
		&current.CreatedAt,
	)
}
