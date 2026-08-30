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

type deviceSyncState struct {
	AppliedCursor int64     `json:"appliedCursor"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

func (app *application) acknowledgeMessages(w http.ResponseWriter, r *http.Request) {
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

	var state deviceSyncState
	err := app.db.QueryRow(
		r.Context(),
		`INSERT INTO device_sync_states (user_id, device_id, applied_seq)
		 SELECT $1, $2, $3
		 WHERE $3 <= COALESCE(
		     (SELECT last_seq FROM user_sync_counters WHERE user_id = $1),
		     0
		 )
		 ON CONFLICT (user_id, device_id) DO UPDATE
		 SET applied_seq = GREATEST(device_sync_states.applied_seq, EXCLUDED.applied_seq),
		     updated_at = CURRENT_TIMESTAMP
		 RETURNING applied_seq, updated_at`,
		authenticatedUserID(r.Context()),
		authenticatedDeviceID(r.Context()),
		*input.Cursor,
	).Scan(&state.AppliedCursor, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		app.observeACK("ahead")
		writeJSON(w, http.StatusConflict, errorResponse{Error: "cursor is ahead of the user's current sync stream"})
		return
	}
	if err != nil {
		app.observeACK("error")
		log.Printf("acknowledge messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not acknowledge messages"})
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
