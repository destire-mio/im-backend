package reliabilitylab

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
)

// PacketHandler is an isolated claim endpoint. Identity is supplied by the
// caller's authentication boundary, never by a user_id in the request body.
// It is deliberately not wired into the IM application or its production API.
func PacketHandler(store *PacketStore, authenticate func(*http.Request) (string, bool)) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /packets/{packet}/claims", func(w http.ResponseWriter, r *http.Request) {
		user, ok := authenticate(r)
		if !ok || user == "" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		receipt, err := store.Claim(r.Context(), r.PathValue("packet"), user)
		switch {
		case errors.Is(err, ErrExhausted):
			http.Error(w, "exhausted", http.StatusConflict)
		case errors.Is(err, pgx.ErrNoRows):
			http.Error(w, "packet not found", http.StatusNotFound)
		case err != nil:
			http.Error(w, "claim outcome unresolved; retry same packet and user", http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(receipt)
		}
	})
	return mux
}
