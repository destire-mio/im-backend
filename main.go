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

const (
	defaultDatabaseURL                  = "postgres://im:im@localhost:5433/im?sslmode=disable"
	defaultOutboxDatabaseMaxConnections = 0
)

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
	ConversationID  int64     `json:"conversationId"`
	ConversationSeq int64     `json:"conversationSeq"`
	ClientMessageID string    `json:"clientMessageId"`
	SenderID        int64     `json:"senderId"`
	ReceiverID      int64     `json:"receiverId"`
	Content         string    `json:"content"`
	CreatedAt       time.Time `json:"createdAt"`
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
	appMetrics := newApplicationMetrics(nil)
	attachDatabaseAcquireTracer(databaseConfig, appMetrics)
	db, err := pgxpool.NewWithConfig(ctx, databaseConfig)
	if err != nil {
		log.Fatalf("create database pool: %v", err)
	}
	defer db.Close()
	log.Printf("database pool max connections %d", databaseConfig.MaxConns)

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("connect to database: %v", err)
	}
	appMetrics.registerDatabaseCollectors(db)

	outboxDB := db
	outboxDatabaseMaxConnections := environmentInt(
		"OUTBOX_DATABASE_MAX_CONNECTIONS",
		defaultOutboxDatabaseMaxConnections,
	)
	if outboxDatabaseMaxConnections < 0 {
		log.Fatal("OUTBOX_DATABASE_MAX_CONNECTIONS must be zero or a positive integer")
	}
	if outboxDatabaseMaxConnections == 0 {
		log.Printf("outbox database pool shared with API")
	} else {
		outboxDatabaseConfig, err := databasePoolConfig(
			databaseURL,
			strconv.Itoa(outboxDatabaseMaxConnections),
		)
		if err != nil {
			log.Fatalf("configure Outbox database pool: %v", err)
		}
		attachDatabaseAcquireTracer(outboxDatabaseConfig, appMetrics)
		isolatedOutboxDB, err := pgxpool.NewWithConfig(ctx, outboxDatabaseConfig)
		if err != nil {
			log.Fatalf("create Outbox database pool: %v", err)
		}
		defer isolatedOutboxDB.Close()
		if err := isolatedOutboxDB.Ping(ctx); err != nil {
			log.Fatalf("connect Outbox database pool: %v", err)
		}
		outboxDB = isolatedOutboxDB
		appMetrics.registerOutboxDatabasePoolCollector(outboxDB)
		log.Printf("outbox database pool isolated with max connections %d", outboxDatabaseConfig.MaxConns)
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
	app.metrics = appMetrics

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
	workerConfig.ExecutionMode = outboxExecutionMode(environmentString("OUTBOX_EXECUTION_MODE", string(workerConfig.ExecutionMode)))
	workerConfig.PrepareMode = outboxPrepareMode(environmentString("OUTBOX_PREPARE_MODE", string(workerConfig.PrepareMode)))
	workerConfig.PrepareWorkers = environmentInt("OUTBOX_PREPARE_WORKERS", workerConfig.PrepareWorkers)
	workerConfig.ProjectionMode = syncProjectionMode(environmentString("OUTBOX_PROJECTION_MODE", string(workerConfig.ProjectionMode)))
	workerConfig.ProjectionStorage = syncProjectionStorage(environmentString("OUTBOX_PROJECTION_STORAGE", string(workerConfig.ProjectionStorage)))
	batchPresence := environmentBool("OUTBOX_BATCH_PRESENCE_LOOKUP", true)
	worker, err := newMessageOutboxWorker(outboxDB, &webSocketOutboxPublisher{
		router:        router,
		batchPresence: batchPresence,
	}, workerConfig, app.metrics)
	if err != nil {
		cancelRouter()
		cancelHub()
		log.Fatalf("configure outbox worker: %v", err)
	}
	workerConfig = worker.config
	var projectionPool *messageProjectionPool
	if workerConfig.PrepareMode == outboxPrepareModeUserSharded {
		projectionPool, err = newMessageProjectionPool(outboxDB, workerConfig, app.metrics)
		if err != nil {
			cancelRouter()
			cancelHub()
			log.Fatalf("configure message projection workers: %v", err)
		}
	}
	app.metrics.SetOutboxWorkerConfig(workerConfig)
	app.metrics.SetOutboxBatchPresenceEnabled(batchPresence)
	log.Printf(
		"outbox worker batch size %d concurrency %d execution mode %s prepare mode %s prepare workers %d projection mode %s storage %s batch presence %t",
		workerConfig.BatchSize,
		workerConfig.Concurrency,
		workerConfig.ExecutionMode,
		workerConfig.PrepareMode,
		workerConfig.PrepareWorkers,
		workerConfig.ProjectionMode,
		workerConfig.ProjectionStorage,
		batchPresence,
	)
	workerContext, cancelWorker := context.WithCancel(context.Background())
	projectionDone := make(chan struct{})
	if projectionPool == nil {
		close(projectionDone)
	} else {
		go func() {
			defer close(projectionDone)
			if err := projectionPool.Run(workerContext); err != nil {
				log.Printf("message projection workers stopped: %v", err)
			}
		}()
	}
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
	case <-projectionDone:
	case <-shutdownContext.Done():
		log.Printf("message projection workers shutdown timed out")
	}
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
	mux.Handle("GET /conversations", app.requireAuthentication(http.HandlerFunc(app.listConversations)))
	mux.Handle("GET /conversations/{conversationID}/messages", app.requireAuthentication(http.HandlerFunc(app.syncConversationMessages)))
	mux.Handle("POST /conversations/{conversationID}/ack", app.requireAuthentication(http.HandlerFunc(app.acknowledgeConversation)))
	mux.Handle("GET /messages/sync", app.requireAuthentication(http.HandlerFunc(app.syncMessages)))
	mux.Handle("POST /messages/ack", app.requireAuthentication(http.HandlerFunc(app.acknowledgeMessages)))
	mux.Handle("GET /ws", app.requireAuthentication(http.HandlerFunc(app.serveWebSocket)))
	var handler http.Handler = mux
	if app.metrics != nil {
		handler = app.metrics.InstrumentHTTP(handler)
	}
	return databaseWorkloadHandler(databaseWorkloadAPI, handler)
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

	senderID := authenticatedUserID(r.Context())
	var created message
	err = tx.QueryRow(
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
		 WHERE sender_id = $1 AND client_message_id = $2`,
		senderID,
		input.ClientMessageID,
	).Scan(
		&created.ID,
		&created.ConversationID,
		&created.ConversationSeq,
		&created.ClientMessageID,
		&created.SenderID,
		&created.ReceiverID,
		&created.Content,
		&created.CreatedAt,
	)
	if err == nil {
		if created.ReceiverID != input.ReceiverID || created.Content != *input.Content {
			_ = tx.Rollback(r.Context())
			writeJSON(w, http.StatusConflict, errorResponse{Error: "clientMessageId was already used with different message data"})
			return
		}
		_ = tx.Rollback(r.Context())
		writeJSON(w, http.StatusOK, created)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		log.Printf("load idempotent message: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}

	lowUserID, highUserID := senderID, input.ReceiverID
	if lowUserID > highUserID {
		lowUserID, highUserID = highUserID, lowUserID
	}
	conversationID, conversationCreated, err := resolveDirectConversation(
		r.Context(),
		tx,
		lowUserID,
		highUserID,
	)
	if err != nil {
		var databaseError *pgconn.PgError
		if errors.As(err, &databaseError) && databaseError.Code == "23503" {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: "receiverId does not exist"})
			return
		}

		log.Printf("create message: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}

	if conversationCreated {
		_, err = tx.Exec(
			r.Context(),
			`INSERT INTO conversation_members (conversation_id, user_id)
			 SELECT $1, participant.user_id
			 FROM (
			     SELECT DISTINCT user_id
			     FROM unnest($2::bigint[]) AS users(user_id)
			 ) AS participant`,
			conversationID,
			[]int64{senderID, input.ReceiverID},
		)
		if err != nil {
			log.Printf("create direct conversation members: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
			return
		}
	}

	err = tx.QueryRow(
		r.Context(),
		`WITH allocated AS (
		   UPDATE conversations
		   SET last_seq = last_seq + 1,
		       updated_at = GREATEST(updated_at, clock_timestamp())
		   WHERE id = $1
		   RETURNING last_seq, updated_at
		 ), inserted AS (
		   INSERT INTO messages (
		       conversation_id,
		       conversation_seq,
		       sender_id,
		       receiver_id,
		       client_message_id,
		       content,
		       created_at
		   )
		   SELECT $1, allocated.last_seq, $2, $3, $4, $5, allocated.updated_at
		   FROM allocated
		   ON CONFLICT (sender_id, client_message_id) DO NOTHING
		   RETURNING id,
		             conversation_id,
		             conversation_seq,
		             client_message_id,
		             sender_id,
		             receiver_id,
		             content,
		             created_at
		 ), created_event AS (
		   INSERT INTO outbox_events (
		       event_type,
		       payload_version,
		       message_id,
		       payload,
		       ready_at
		   )
		   SELECT 'message.created',
		          4,
		          inserted.id,
		          jsonb_build_object(
		              'message', jsonb_build_object(
		                  'id', inserted.id,
		                  'conversationId', inserted.conversation_id,
		                  'conversationSeq', inserted.conversation_seq,
		                  'clientMessageId', inserted.client_message_id,
		                  'senderId', inserted.sender_id,
		                  'receiverId', inserted.receiver_id,
		                  'content', inserted.content,
		                  'createdAt', inserted.created_at
		              ),
		              'recipients', CASE
		                  WHEN inserted.sender_id = inserted.receiver_id THEN
		                      jsonb_build_array(
		                          jsonb_build_object('userId', inserted.sender_id)
		                      )
		                  ELSE
		                      jsonb_build_array(
		                          jsonb_build_object('userId', inserted.sender_id),
		                          jsonb_build_object('userId', inserted.receiver_id)
		                      )
		              END
		          ),
		          CURRENT_TIMESTAMP
		   FROM inserted
		   RETURNING message_id
		 )
		 SELECT inserted.id,
		        inserted.conversation_id,
		        inserted.conversation_seq,
		        inserted.client_message_id,
		        inserted.sender_id,
		        inserted.receiver_id,
		        inserted.content,
		        inserted.created_at
		 FROM inserted
		 JOIN created_event ON created_event.message_id = inserted.id`,
		conversationID,
		senderID,
		input.ReceiverID,
		input.ClientMessageID,
		*input.Content,
	).Scan(
		&created.ID,
		&created.ConversationID,
		&created.ConversationSeq,
		&created.ClientMessageID,
		&created.SenderID,
		&created.ReceiverID,
		&created.Content,
		&created.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent retry may have inserted the same idempotency key after
		// our first lookup. Roll back the provisional sequence allocation, then
		// return the committed winner without leaving a sequence gap.
		if rollbackErr := tx.Rollback(r.Context()); rollbackErr != nil {
			log.Printf("roll back duplicate message allocation: %v", rollbackErr)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
			return
		}
		err = app.db.QueryRow(
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
			 WHERE sender_id = $1 AND client_message_id = $2`,
			senderID,
			input.ClientMessageID,
		).Scan(
			&created.ID,
			&created.ConversationID,
			&created.ConversationSeq,
			&created.ClientMessageID,
			&created.SenderID,
			&created.ReceiverID,
			&created.Content,
			&created.CreatedAt,
		)
		if err != nil {
			log.Printf("load concurrent idempotent message: %v", err)
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
			return
		}
		if created.ReceiverID != input.ReceiverID || created.Content != *input.Content {
			writeJSON(w, http.StatusConflict, errorResponse{Error: "clientMessageId was already used with different message data"})
			return
		}
		writeJSON(w, http.StatusOK, created)
		return
	}
	if err != nil {
		log.Printf("create message: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "could not create message"})
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func resolveDirectConversation(
	ctx context.Context,
	tx pgx.Tx,
	lowUserID int64,
	highUserID int64,
) (int64, bool, error) {
	var conversationID int64
	err := tx.QueryRow(
		ctx,
		`SELECT id
		 FROM conversations
		 WHERE kind = 'direct'
		   AND direct_user_low_id = $1
		   AND direct_user_high_id = $2`,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	if err == nil {
		return conversationID, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}

	err = tx.QueryRow(
		ctx,
		`INSERT INTO conversations (kind, direct_user_low_id, direct_user_high_id)
		 VALUES ('direct', $1, $2)
		 ON CONFLICT (direct_user_low_id, direct_user_high_id) DO NOTHING
		 RETURNING id`,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	if err == nil {
		return conversationID, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}

	err = tx.QueryRow(
		ctx,
		`SELECT id
		 FROM conversations
		 WHERE kind = 'direct'
		   AND direct_user_low_id = $1
		   AND direct_user_high_id = $2`,
		lowUserID,
		highUserID,
	).Scan(&conversationID)
	if err != nil {
		return 0, false, err
	}
	return conversationID, false, nil
}

func (app *application) syncMessages(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusGone, errorResponse{
		Error: "user-level message sync was replaced by GET /conversations and GET /conversations/{conversationID}/messages",
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
