package main

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
)

type acknowledgeMessagesRequest struct {
	Cursor *int64 `json:"cursor"`
}

func (app *application) acknowledgeMessages(w http.ResponseWriter, r *http.Request) {
	app.observeACK("gone")
	writeJSON(w, http.StatusGone, errorResponse{
		Error: "user-level ACK was replaced by POST /conversations/{conversationID}/ack",
	})
}

type deviceConversationSyncState struct {
	ConversationID int64     `json:"conversationId"`
	AppliedCursor  int64     `json:"appliedCursor"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

func (app *application) acknowledgeConversation(w http.ResponseWriter, r *http.Request) {
	conversationID, ok := parseConversationID(w, r)
	if !ok {
		app.observeACK("invalid")
		return
	}

	var input acknowledgeMessagesRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		app.observeACK("invalid")
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if input.Cursor == nil || *input.Cursor < 0 {
		app.observeACK("invalid")
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "cursor must be a non-negative integer"})
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
		app.observeACK("not_found")
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "conversation not found"})
		return
	}
	if err != nil {
		app.observeACK("error")
		log.Printf("read conversation for acknowledgement: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not acknowledge conversation"})
		return
	}
	if *input.Cursor > currentCursor {
		app.observeACK("ahead")
		writeJSON(w, http.StatusConflict, errorResponse{Error: "cursor is ahead of the conversation's current stream"})
		return
	}

	state := deviceConversationSyncState{ConversationID: conversationID}
	err = app.db.QueryRow(
		r.Context(),
		`INSERT INTO device_conversation_sync_states (
		     user_id,
		     device_id,
		     conversation_id,
		     applied_seq
		 )
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (user_id, device_id, conversation_id) DO UPDATE
		 SET applied_seq = GREATEST(
		         device_conversation_sync_states.applied_seq,
		         EXCLUDED.applied_seq
		     ),
		     updated_at = GREATEST(
		         device_conversation_sync_states.updated_at,
		         clock_timestamp()
		     )
		 RETURNING applied_seq, updated_at`,
		userID,
		authenticatedDeviceID(r.Context()),
		conversationID,
		*input.Cursor,
	).Scan(&state.AppliedCursor, &state.UpdatedAt)
	if err != nil {
		app.observeACK("error")
		log.Printf("acknowledge conversation: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not acknowledge conversation"})
		return
	}

	app.observeACK("accepted")
	writeJSON(w, http.StatusOK, state)
}

func (app *application) observeACK(result string) {
	if app.metrics != nil {
		app.metrics.ackRequests.WithLabelValues(result).Inc()
	}
}
