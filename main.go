package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const defaultDatabaseURL = "postgres://im:im@localhost:5433/im?sslmode=disable"

const (
	maxRequestBodyBytes    = 1 << 20
	maxMessageContentRunes = 4000
)

type application struct {
	db                   *pgxpool.Pool
	responseCipher       *responseCipher
	rateLimiter          authRateLimiter
	rateLimitFailOpen    bool
	trustedProxyNetworks []*net.IPNet
	webSocketHub         *webSocketHub
	webSocketPresence    *redisWebSocketPresence
	realtimeRouter       *redisRealtimeRouter
	metrics              *applicationMetrics
	now                  func() time.Time
}

type user struct {
	ID          int64  `json:"id"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
}

type createMessageRequest struct {
	ClientMessageID string  `json:"clientMessageId"`
	ReceiverID      int64   `json:"receiverId"`
	Content         *string `json:"content"`
}

type message struct {
	ID              int64     `json:"id"`
	ClientMessageID string    `json:"clientMessageId"`
	SenderID        int64     `json:"senderId"`
	ReceiverID      int64     `json:"receiverId"`
	Content         string    `json:"content"`
	CreatedAt       time.Time `json:"createdAt"`
}

type messageSyncEvent struct {
	Cursor  int64   `json:"cursor"`
	Message message `json:"message"`
}

type messageSyncResponse struct {
	Events         []messageSyncEvent `json:"events"`
	NextCursor     int64              `json:"nextCursor"`
	SnapshotCursor int64              `json:"snapshotCursor"`
	HasMore        bool               `json:"hasMore"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = defaultDatabaseURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	databaseConfig, err := databasePoolConfig(databaseURL, os.Getenv("DATABASE_MAX_CONNECTIONS"))
	if err != nil {
		log.Fatalf("configure database pool: %v", err)
	}
	db, err := pgxpool.NewWithConfig(ctx, databaseConfig)
	if err != nil {
		log.Fatalf("create database pool: %v", err)
	}
	defer db.Close()
	log.Printf("database pool max connections %d", databaseConfig.MaxConns)

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("connect to database: %v", err)
	}

	cipher, err := newResponseCipher(os.Getenv("IDEMPOTENCY_KEY"), environmentInt("IDEMPOTENCY_KEY_VERSION", 1))
	if err != nil {
		log.Fatalf("configure idempotency encryption: %v", err)
	}

	redisOptions, err := redis.ParseURL(environmentString("REDIS_URL", "redis://localhost:6379/0"))
	if err != nil {
		log.Fatalf("configure Redis: %v", err)
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("warning: Redis rate limiter is unavailable: %v", err)
	}

	trustedProxyNetworks, err := parseTrustedProxyNetworks(os.Getenv("TRUSTED_PROXY_CIDRS"))
	if err != nil {
		log.Fatalf("configure trusted proxies: %v", err)
	}

	app := &application{
		db:                   db,
		responseCipher:       cipher,
		rateLimiter:          &redisRateLimiter{client: redisClient},
		rateLimitFailOpen:    environmentBool("AUTH_RATE_LIMIT_FAIL_OPEN", false),
		trustedProxyNetworks: trustedProxyNetworks,
	}
	app.metrics = newApplicationMetrics(db)

	hub := newWebSocketHub(app.metrics)
	hubContext, cancelHub := context.WithCancel(context.Background())
	app.webSocketHub = hub
	go hub.Run(hubContext)
	instanceID, err := newRealtimeInstanceID(os.Getenv("REALTIME_INSTANCE_NAME"))
	if err != nil {
		cancelHub()
		log.Fatalf("create realtime instance identity: %v", err)
	}
	presence, err := newRedisWebSocketPresence(
		redisClient,
		environmentString("REALTIME_REDIS_NAMESPACE", defaultWebSocketPresenceNamespace),
		time.Duration(environmentInt("WEBSOCKET_PRESENCE_TTL_SECONDS", 30))*time.Second,
		time.Duration(environmentInt("WEBSOCKET_PRESENCE_RENEW_SECONDS", 10))*time.Second,
	)
	if err != nil {
		cancelHub()
		log.Fatalf("configure websocket presence: %v", err)
	}
	router, err := newRedisRealtimeRouter(redisClient, presence, hub, instanceID, app.metrics)
	if err != nil {
		cancelHub()
		log.Fatalf("configure realtime router: %v", err)
	}
	app.webSocketPresence = presence
	app.realtimeRouter = router
	routerContext, cancelRouter := context.WithCancel(context.Background())
	go router.Run(routerContext)
	log.Printf("realtime instance id %s", instanceID)

	workerConfig := defaultOutboxWorkerConfig()
	workerConfig.BatchSize = environmentInt("OUTBOX_BATCH_SIZE", workerConfig.BatchSize)
	workerConfig.Concurrency = environmentInt("OUTBOX_CONCURRENCY", workerConfig.Concurrency)
	workerConfig.ProjectionMode = syncProjectionMode(environmentString("OUTBOX_PROJECTION_MODE", string(workerConfig.ProjectionMode)))
	workerConfig.ProjectionStorage = syncProjectionStorage(environmentString("OUTBOX_PROJECTION_STORAGE", string(workerConfig.ProjectionStorage)))
	worker, err := newMessageOutboxWorker(db, &webSocketOutboxPublisher{router: router}, workerConfig, app.metrics)
	if err != nil {
		cancelRouter()
		cancelHub()
		log.Fatalf("configure outbox worker: %v", err)
	}
	app.metrics.SetOutboxWorkerConfig(workerConfig)
	log.Printf(
		"outbox worker batch size %d concurrency %d projection mode %s storage %s",
		workerConfig.BatchSize,
		workerConfig.Concurrency,
		workerConfig.ProjectionMode,
		workerConfig.ProjectionStorage,
	)
	workerContext, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if err := worker.Run(workerContext); err != nil {
			log.Printf("outbox worker stopped: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              environmentString("HTTP_ADDR", ":8080"),
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServer := &http.Server{
		Addr:              environmentString("METRICS_ADDR", "127.0.0.1:9090"),
		Handler:           app.metrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("server listening on http://localhost%s", server.Addr)
	log.Printf("metrics listening on http://%s/metrics", metricsServer.Addr)
	serverError := make(chan error, 1)
	metricsServerError := make(chan error, 1)
	go func() { serverError <- server.ListenAndServe() }()
	go func() { metricsServerError <- metricsServer.ListenAndServe() }()

	shutdownSignal, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case <-shutdownSignal.Done():
		log.Printf("server shutdown requested")
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server stopped: %v", err)
		}
	case err := <-metricsServerError:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server stopped: %v", err)
		}
	}

	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if err := server.Shutdown(shutdownContext); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := metricsServer.Shutdown(shutdownContext); err != nil {
		log.Printf("metrics shutdown: %v", err)
	}
	cancelWorker()
	select {
	case <-workerDone:
	case <-shutdownContext.Done():
		log.Printf("outbox worker shutdown timed out")
	}
	cancelRouter()
	select {
	case <-router.done:
	case <-shutdownContext.Done():
		log.Printf("realtime router shutdown timed out")
	}
	cancelHub()
	select {
	case <-hub.done:
	case <-shutdownContext.Done():
		log.Printf("websocket hub shutdown timed out")
	}
}

func (app *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", app.health)
	mux.HandleFunc("POST /auth/register", app.register)
	mux.HandleFunc("POST /auth/login", app.login)
	mux.HandleFunc("POST /auth/refresh", app.refresh)
	mux.Handle("POST /auth/logout", app.requireAuthentication(http.HandlerFunc(app.logout)))
	mux.Handle("POST /auth/logout-all", app.requireAuthentication(http.HandlerFunc(app.logoutAll)))
	mux.Handle("GET /auth/sessions", app.requireAuthentication(http.HandlerFunc(app.listSessions)))
	mux.Handle("DELETE /auth/sessions/{sessionID}", app.requireAuthentication(http.HandlerFunc(app.revokeSession)))
	mux.Handle("POST /auth/password", app.requireAuthentication(http.HandlerFunc(app.changePassword)))
	mux.Handle("POST /messages", app.requireAuthentication(http.HandlerFunc(app.createMessage)))
	mux.Handle("GET /messages", app.requireAuthentication(http.HandlerFunc(app.listMessages)))
	mux.Handle("GET /messages/sync", app.requireAuthentication(http.HandlerFunc(app.syncMessages)))
	mux.Handle("POST /messages/ack", app.requireAuthentication(http.HandlerFunc(app.acknowledgeMessages)))
	mux.Handle("GET /ws", app.requireAuthentication(http.HandlerFunc(app.serveWebSocket)))
	if app.metrics != nil {
		return app.metrics.InstrumentHTTP(mux)
	}
	return mux
}

func decodeSingleJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return err
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}

	return nil
}

func (app *application) createMessage(w http.ResponseWriter, r *http.Request) {
	var input createMessageRequest
	if err := decodeSingleJSON(w, r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	if !validOpaqueID(input.ClientMessageID) || input.ReceiverID <= 0 || input.Content == nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "clientMessageId, receiverId and content are required"})
		return
	}
	if utf8.RuneCountInString(*input.Content) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "content must contain at least one character"})
		return
	}
	if utf8.RuneCountInString(*input.Content) > maxMessageContentRunes {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "content is too long"})
		return
	}

	tx, err := app.db.Begin(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}
	defer tx.Rollback(r.Context())

	const insertMessage = `
		INSERT INTO messages (sender_id, receiver_id, client_message_id, content)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (sender_id, client_message_id) DO NOTHING
		RETURNING id, client_message_id, sender_id, receiver_id, content, created_at`

	senderID := authenticatedUserID(r.Context())
	var created message
	err = tx.QueryRow(
		r.Context(),
		insertMessage,
		senderID,
		input.ReceiverID,
		input.ClientMessageID,
		*input.Content,
	).Scan(
		&created.ID,
		&created.ClientMessageID,
		&created.SenderID,
		&created.ReceiverID,
		&created.Content,
		&created.CreatedAt,
	)
	createdNow := true
	var eventID string
	if errors.Is(err, pgx.ErrNoRows) {
		createdNow = false
		err = tx.QueryRow(
			r.Context(),
			`SELECT id, client_message_id, sender_id, receiver_id, content, created_at
			 FROM messages
			 WHERE sender_id = $1 AND client_message_id = $2`,
			senderID,
			input.ClientMessageID,
		).Scan(
			&created.ID,
			&created.ClientMessageID,
			&created.SenderID,
			&created.ReceiverID,
			&created.Content,
			&created.CreatedAt,
		)
		if err != nil {
			log.Printf("load idempotent message: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
			return
		}
		if created.ReceiverID != input.ReceiverID || created.Content != *input.Content {
			writeJSON(w, http.StatusConflict, errorResponse{Error: "clientMessageId was already used with different message data"})
			return
		}
	} else if err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) && databaseError.Code == "23503" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "receiverId does not exist"})
			return
		}

		log.Printf("create message: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}

	if createdNow {
		payload, err := json.Marshal(messageCreatedPendingPayload{Message: created})
		if err != nil {
			log.Printf("encode message outbox event: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
			return
		}
		err = tx.QueryRow(
			r.Context(),
			`INSERT INTO outbox_events (event_type, payload_version, message_id, payload)
			 VALUES ('message.created', 3, $1, $2::jsonb)
			 RETURNING event_id::text`,
			created.ID,
			payload,
		).Scan(&eventID)
		if err != nil {
			log.Printf("create message outbox event: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
			return
		}
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}
	if createdNow {
		writeJSON(w, http.StatusCreated, created)
		return
	}
	writeJSON(w, http.StatusOK, created)
}

func (app *application) listMessages(w http.ResponseWriter, r *http.Request) {
	if len(r.URL.Query()) != 1 || !r.URL.Query().Has("peerId") {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "only peerId is allowed"})
		return
	}

	peerID, err := strconv.ParseInt(r.URL.Query().Get("peerId"), 10, 64)
	if err != nil || peerID <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "peerId must be a positive integer"})
		return
	}

	const query = `
		SELECT id, client_message_id, sender_id, receiver_id, content, created_at
		FROM messages
		WHERE (sender_id = $1 AND receiver_id = $2)
		   OR (sender_id = $2 AND receiver_id = $1)
		ORDER BY created_at ASC, id ASC`

	rows, err := app.db.Query(r.Context(), query, authenticatedUserID(r.Context()), peerID)
	if err != nil {
		log.Printf("list messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not list messages"})
		return
	}
	defer rows.Close()

	messages := make([]message, 0)
	for rows.Next() {
		var current message
		if err := rows.Scan(
			&current.ID,
			&current.ClientMessageID,
			&current.SenderID,
			&current.ReceiverID,
			&current.Content,
			&current.CreatedAt,
		); err != nil {
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

	writeJSON(w, http.StatusOK, messages)
}

func (app *application) syncMessages(w http.ResponseWriter, r *http.Request) {
	for key := range r.URL.Query() {
		if key != "after" && key != "limit" && key != "snapshotCursor" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "only after, limit and snapshotCursor are allowed"})
			return
		}
	}

	after := int64(0)
	if raw := r.URL.Query().Get("after"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "after must be a non-negative integer"})
			return
		}
		after = parsed
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 200 {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "limit must be between 1 and 200"})
			return
		}
		limit = parsed
	}

	userID := authenticatedUserID(r.Context())
	var currentCursor int64
	err := app.db.QueryRow(
		r.Context(),
		`SELECT COALESCE(
		    (SELECT last_seq FROM user_sync_counters WHERE user_id = $1),
		    0
		)`,
		userID,
	).Scan(&currentCursor)
	if err != nil {
		log.Printf("read message sync snapshot: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync messages"})
		return
	}
	if after > currentCursor {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "after is ahead of the user's current sync stream"})
		return
	}

	snapshotCursor := currentCursor
	if r.URL.Query().Has("snapshotCursor") {
		raw := r.URL.Query().Get("snapshotCursor")
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < after {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "snapshotCursor must be an integer greater than or equal to after"})
			return
		}
		if parsed > currentCursor {
			writeJSON(w, http.StatusConflict, errorResponse{Error: "snapshotCursor is ahead of the user's current sync stream"})
			return
		}
		snapshotCursor = parsed
	}

	rows, err := app.db.Query(
		r.Context(),
		`SELECT event.seq,
		        message.id,
		        message.client_message_id,
		        message.sender_id,
		        message.receiver_id,
		        message.content,
		        message.created_at
		 FROM user_message_events AS event
		 JOIN messages AS message ON message.id = event.message_id
		 WHERE event.user_id = $1
		   AND event.seq > $2
		   AND event.seq <= $3
		 ORDER BY event.seq
		 LIMIT $4`,
		userID,
		after,
		snapshotCursor,
		limit+1,
	)
	if err != nil {
		log.Printf("sync messages: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync messages"})
		return
	}
	defer rows.Close()

	events := make([]messageSyncEvent, 0, limit)
	hasMore := false
	for rows.Next() {
		var event messageSyncEvent
		if err := rows.Scan(
			&event.Cursor,
			&event.Message.ID,
			&event.Message.ClientMessageID,
			&event.Message.SenderID,
			&event.Message.ReceiverID,
			&event.Message.Content,
			&event.Message.CreatedAt,
		); err != nil {
			log.Printf("scan message sync event: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync messages"})
			return
		}
		if len(events) == limit {
			hasMore = true
			break
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		log.Printf("iterate message sync events: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not sync messages"})
		return
	}

	nextCursor := after
	if len(events) > 0 {
		nextCursor = events[len(events)-1].Cursor
	}
	if app.metrics != nil {
		app.metrics.syncPages.WithLabelValues(strconv.FormatBool(hasMore)).Inc()
		app.metrics.syncEvents.Add(float64(len(events)))
	}
	writeJSON(w, http.StatusOK, messageSyncResponse{
		Events:         events,
		NextCursor:     nextCursor,
		SnapshotCursor: snapshotCursor,
		HasMore:        hasMore,
	})
}

func (app *application) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var result int
	if err := app.db.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
		http.Error(w, `{"status":"error"}`, http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func environmentString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func environmentInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("%s must be an integer", name)
	}
	return parsed
}

func environmentBool(name string, fallback bool) bool {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatalf("%s must be true or false", name)
	}
	return parsed
}

func databasePoolConfig(databaseURL, maximumConnections string) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	if maximumConnections == "" {
		return config, nil
	}
	parsed, err := strconv.ParseInt(maximumConnections, 10, 32)
	if err != nil || parsed <= 0 {
		return nil, errors.New("DATABASE_MAX_CONNECTIONS must be a positive integer")
	}
	config.MaxConns = int32(parsed)
	return config, nil
}
