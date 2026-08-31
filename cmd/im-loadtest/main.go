package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"golang.org/x/crypto/argon2"
)

const (
	defaultDatabaseURL = "postgres://im:im@localhost:5433/im?sslmode=disable"
	trafficPatternRing = "ring"
	trafficPatternHot  = "hot"
	passwordMemory     = 19 * 1024
	passwordIterations = 2
	passwordParallel   = 1
	passwordSaltLength = 16
	passwordKeyLength  = 32
)

type config struct {
	baseURL              string
	metricsURL           string
	databaseURL          string
	allowedDB            string
	users                int
	devicesPerUser       int
	messages             int
	concurrency          int
	targetRate           int
	trafficPattern       string
	requestTimeout       time.Duration
	deliveryWait         time.Duration
	metricSampleInterval time.Duration
	reportPath           string
	allowWrite           bool
}

type apiUser struct {
	ID          int64  `json:"id"`
	Username    string `json:"username"`
	DisplayName string `json:"displayName"`
}

type authResponse struct {
	User        apiUser `json:"user"`
	SessionID   int64   `json:"sessionId"`
	DeviceID    string  `json:"deviceId"`
	AccessToken string  `json:"accessToken"`
}

type apiMessage struct {
	ID              int64     `json:"id"`
	ConversationID  int64     `json:"conversationId"`
	ConversationSeq int64     `json:"conversationSeq"`
	ClientMessageID string    `json:"clientMessageId"`
	SenderID        int64     `json:"senderId"`
	ReceiverID      int64     `json:"receiverId"`
	Content         string    `json:"content"`
	CreatedAt       time.Time `json:"createdAt"`
}

type conversationSummary struct {
	ID      int64 `json:"id"`
	LastSeq int64 `json:"lastSeq"`
}

type conversationListResponse struct {
	Conversations  []conversationSummary `json:"conversations"`
	NextCursor     int64                 `json:"nextCursor"`
	SnapshotCursor int64                 `json:"snapshotCursor"`
	HasMore        bool                  `json:"hasMore"`
}

type conversationSyncResponse struct {
	ConversationID int64        `json:"conversationId"`
	Messages       []apiMessage `json:"messages"`
	NextCursor     int64        `json:"nextCursor"`
	SnapshotCursor int64        `json:"snapshotCursor"`
	HasMore        bool         `json:"hasMore"`
}

type wsEnvelope struct {
	Type            string     `json:"type"`
	EventID         string     `json:"eventId"`
	ConversationID  int64      `json:"conversationId"`
	ConversationSeq int64      `json:"conversationSeq"`
	Message         apiMessage `json:"message"`
}

type testUser struct {
	ID       int64
	Username string
	Password string
	Sessions []authResponse
}

type liveConnection struct {
	label string
	conn  *websocket.Conn
}

type sendResult struct {
	clientMessageID string
	message         apiMessage
	latency         time.Duration
	status          int
	attempts        int
	recovered       bool
	dropped         bool
	err             string
}

const trackerShardCount = 64

type trackerShard struct {
	mu       sync.Mutex
	started  map[string]time.Time
	expected map[string]map[string]struct{}
	received map[string]map[string]time.Duration
}

type tracker struct {
	shards       [trackerShardCount]trackerShard
	unexpected   atomic.Int64
	duplicate    atomic.Int64
	errorsMu     sync.Mutex
	readerErrors []string
	change       chan struct{}
}

type durationStats struct {
	Count int     `json:"count"`
	P50MS float64 `json:"p50Ms"`
	P95MS float64 `json:"p95Ms"`
	P99MS float64 `json:"p99Ms"`
	MaxMS float64 `json:"maxMs"`
}

type verificationReport struct {
	Expected int      `json:"expected"`
	Observed int      `json:"observed"`
	Missing  []string `json:"missing,omitempty"`
	Passed   bool     `json:"passed"`
}

type histogramSnapshot struct {
	Count   uint64
	Sum     float64
	Buckets map[float64]uint64
}

type metricsSnapshot struct {
	Values                   map[string]float64
	OutboxStageDurations     map[string]histogramSnapshot
	DatabaseAcquireDurations map[string]map[string]histogramSnapshot
}

type histogramDeltaReport struct {
	Count       uint64  `json:"count"`
	AverageMS   float64 `json:"averageMs"`
	P50BucketMS float64 `json:"p50BucketMs"`
	P95BucketMS float64 `json:"p95BucketMs"`
	P99BucketMS float64 `json:"p99BucketMs"`
}

type metricSamplingReport struct {
	Interval string             `json:"interval"`
	Samples  int                `json:"samples"`
	Errors   int                `json:"errors"`
	Peaks    map[string]float64 `json:"peaks"`
}

type report struct {
	RunID                    string                                     `json:"runId"`
	StartedAt                time.Time                                  `json:"startedAt"`
	FinishedAt               time.Time                                  `json:"finishedAt"`
	Database                 string                                     `json:"database"`
	Users                    int                                        `json:"users"`
	DevicesPerUser           int                                        `json:"devicesPerUser"`
	WebSocketCount           int                                        `json:"webSocketCount"`
	Concurrency              int                                        `json:"concurrency"`
	LoadModel                string                                     `json:"loadModel"`
	TargetRateRPS            int                                        `json:"targetRateRps,omitempty"`
	TrafficPattern           string                                     `json:"trafficPattern"`
	RequestTimeout           string                                     `json:"requestTimeout"`
	DeliveryWait             string                                     `json:"deliveryWait"`
	LoadDurationMS           float64                                    `json:"loadDurationMs"`
	MessagesAttempted        int                                        `json:"messagesAttempted"`
	MessagesSucceeded        int                                        `json:"messagesSucceeded"`
	MessagesFailed           int                                        `json:"messagesFailed"`
	DroppedStarts            int                                        `json:"droppedStarts"`
	IdempotentRecovered      int                                        `json:"idempotentRecovered"`
	HTTPThroughputRPS        float64                                    `json:"httpThroughputRps"`
	HTTPLatency              durationStats                              `json:"httpLatency"`
	RealtimeLatency          durationStats                              `json:"realtimeLatency"`
	Realtime                 verificationReport                         `json:"realtime"`
	Sync                     verificationReport                         `json:"sync"`
	DuplicateRealtime        int                                        `json:"duplicateRealtime"`
	UnexpectedRealtime       int                                        `json:"unexpectedRealtime"`
	RequestErrors            []string                                   `json:"requestErrors,omitempty"`
	ReaderErrors             []string                                   `json:"readerErrors,omitempty"`
	MetricDeltas             map[string]float64                         `json:"metricDeltas,omitempty"`
	MetricEnd                map[string]float64                         `json:"metricEnd,omitempty"`
	OutboxStageDurations     map[string]histogramDeltaReport            `json:"outboxStageDurations,omitempty"`
	DatabaseAcquireDurations map[string]map[string]histogramDeltaReport `json:"databaseAcquireDurations,omitempty"`
	MetricSampling           metricSamplingReport                       `json:"metricSampling"`
}

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startedAt := time.Now().UTC()
	runID, err := newRunID(startedAt)
	if err != nil {
		fatalf("create run id: %v", err)
	}

	httpClient := newLoadHTTPClient(cfg.requestTimeout, cfg.concurrency)
	defer httpClient.CloseIdleConnections()
	if err := requireHealthy(ctx, httpClient, cfg.baseURL); err != nil {
		fatalf("preflight health check failed: %v", err)
	}

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		fatalf("open database: %v", err)
	}
	defer pool.Close()
	databaseName, err := validateDatabase(ctx, pool, cfg.allowedDB)
	if err != nil {
		fatalf("database safety check failed: %v", err)
	}

	fmt.Printf("run=%s database=%s\n", runID, databaseName)
	fmt.Printf("setup: seed %d users, login %d devices, open %d WebSockets (not timed)\n",
		cfg.users, cfg.users*cfg.devicesPerUser, cfg.users*cfg.devicesPerUser)
	fmt.Printf("traffic: pattern=%s\n", cfg.trafficPattern)

	users, err := seedUsers(ctx, pool, runID, cfg.users)
	if err != nil {
		fatalf("seed load-test users: %v", err)
	}
	if err := loginDevices(ctx, httpClient, cfg, runID, users); err != nil {
		fatalf("create test sessions: %v", err)
	}

	loadTracker := newTracker()
	connections, err := connectAll(ctx, cfg, users, loadTracker)
	if err != nil {
		fatalf("open WebSocket connections: %v", err)
	}
	defer closeConnections(connections)

	metricsBefore, metricsErr := fetchMetrics(ctx, httpClient, cfg.metricsURL)
	if metricsErr != nil {
		fatalf("metrics preflight failed: %v", metricsErr)
	}
	metricSampler := startMetricPeakSampler(ctx, httpClient, cfg.metricsURL, cfg.metricSampleInterval, metricsBefore.Values)

	loadModel := "closed"
	if cfg.targetRate > 0 {
		loadModel = "fixed-rate"
		fmt.Printf("load: %d scheduled starts at %d req/s with max-inflight=%d\n", cfg.messages, cfg.targetRate, cfg.concurrency)
	} else {
		fmt.Printf("load: %d messages with concurrency=%d\n", cfg.messages, cfg.concurrency)
	}
	loadStarted := time.Now()
	results := sendLoad(ctx, httpClient, cfg, runID, users, loadTracker)
	loadDuration := time.Since(loadStarted)

	successful, recovered, droppedStarts, requestErrors, httpLatencies := summarizeSends(results)
	if successful > 0 {
		fmt.Printf("HTTP completed: success=%d failed=%d; waiting up to %s for realtime delivery\n",
			successful, cfg.messages-successful, cfg.deliveryWait)
		loadTracker.wait(cfg.deliveryWait)
	}

	realtimeExpected, realtimeObserved, realtimeMissing, realtimeLatencies,
		duplicateRealtime, unexpectedRealtime, readerErrors := loadTracker.snapshot()

	syncExpected, syncObserved, syncMissing, syncErr := verifySync(ctx, httpClient, cfg, runID, users, results)
	if syncErr != nil {
		requestErrors = append(requestErrors, "sync verification: "+syncErr.Error())
	}

	metricsAfter, metricsErr := fetchMetrics(ctx, httpClient, cfg.metricsURL)
	if metricsErr != nil {
		requestErrors = append(requestErrors, "metrics after load: "+metricsErr.Error())
	}
	metricSampling := metricSampler.Stop(metricsAfter.Values)

	result := report{
		RunID:                    runID,
		StartedAt:                startedAt,
		FinishedAt:               time.Now().UTC(),
		Database:                 databaseName,
		Users:                    len(users),
		DevicesPerUser:           cfg.devicesPerUser,
		WebSocketCount:           len(connections),
		Concurrency:              cfg.concurrency,
		LoadModel:                loadModel,
		TargetRateRPS:            cfg.targetRate,
		TrafficPattern:           cfg.trafficPattern,
		RequestTimeout:           cfg.requestTimeout.String(),
		DeliveryWait:             cfg.deliveryWait.String(),
		LoadDurationMS:           durationMilliseconds(loadDuration),
		MessagesAttempted:        cfg.messages,
		MessagesSucceeded:        successful,
		MessagesFailed:           cfg.messages - successful,
		DroppedStarts:            droppedStarts,
		IdempotentRecovered:      recovered,
		HTTPThroughputRPS:        float64(successful) / loadDuration.Seconds(),
		HTTPLatency:              calculateDurationStats(httpLatencies),
		RealtimeLatency:          calculateDurationStats(realtimeLatencies),
		Realtime:                 verificationReport{Expected: realtimeExpected, Observed: realtimeObserved, Missing: capStrings(realtimeMissing, 100), Passed: realtimeExpected == realtimeObserved},
		Sync:                     verificationReport{Expected: syncExpected, Observed: syncObserved, Missing: capStrings(syncMissing, 100), Passed: syncErr == nil && syncExpected == syncObserved},
		DuplicateRealtime:        duplicateRealtime,
		UnexpectedRealtime:       unexpectedRealtime,
		RequestErrors:            capStrings(requestErrors, 100),
		ReaderErrors:             capStrings(readerErrors, 100),
		MetricDeltas:             metricDelta(metricsBefore.Values, metricsAfter.Values),
		MetricEnd:                selectedMetricEnd(metricsAfter.Values),
		OutboxStageDurations:     outboxStageDurationDelta(metricsBefore.OutboxStageDurations, metricsAfter.OutboxStageDurations),
		DatabaseAcquireDurations: databaseAcquireDurationDelta(metricsBefore.DatabaseAcquireDurations, metricsAfter.DatabaseAcquireDurations),
		MetricSampling:           metricSampling,
	}

	printReport(result)
	if cfg.reportPath != "" {
		if err := writeReport(cfg.reportPath, result); err != nil {
			fatalf("write JSON report: %v", err)
		}
		fmt.Printf("JSON report: %s\n", cfg.reportPath)
	}
	if result.MessagesFailed > 0 || !result.Sync.Passed || !result.Realtime.Passed ||
		len(result.RequestErrors) > 0 || len(result.ReaderErrors) > 0 || result.UnexpectedRealtime > 0 {
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.baseURL, "base-url", "http://127.0.0.1:8080", "IM HTTP API base URL")
	flag.StringVar(&cfg.metricsURL, "metrics-url", "http://127.0.0.1:9090/metrics", "Prometheus metrics URL")
	flag.StringVar(&cfg.databaseURL, "database-url", envOr("DATABASE_URL", defaultDatabaseURL), "administrator database URL used only to seed test users")
	flag.StringVar(&cfg.allowedDB, "allow-database", "", "required exact database name that may receive test data")
	flag.IntVar(&cfg.users, "users", 10, "number of test users (2-10)")
	flag.IntVar(&cfg.devicesPerUser, "devices", 2, "WebSocket devices per user (1-3)")
	flag.IntVar(&cfg.messages, "messages", 500, "number of messages to send")
	flag.IntVar(&cfg.concurrency, "concurrency", 20, "concurrent HTTP senders")
	flag.IntVar(&cfg.targetRate, "rate", 0, "fixed request-start rate per second; 0 keeps the closed-loop model")
	flag.StringVar(&cfg.trafficPattern, "traffic-pattern", trafficPatternRing, "message distribution: ring spreads traffic across user pairs; hot keeps all traffic in one conversation")
	flag.DurationVar(&cfg.requestTimeout, "request-timeout", 10*time.Second, "timeout for each HTTP request")
	flag.DurationVar(&cfg.deliveryWait, "delivery-wait", 30*time.Second, "maximum wait for WebSocket deliveries")
	flag.DurationVar(&cfg.metricSampleInterval, "metrics-sample-interval", 250*time.Millisecond, "interval for recording peak Outbox backlog metrics")
	flag.StringVar(&cfg.reportPath, "report", "", "optional JSON report path")
	flag.BoolVar(&cfg.allowWrite, "allow-write", false, "required confirmation that the test may write users and messages")
	flag.Parse()
	return cfg
}

func validateConfig(cfg config) error {
	if !cfg.allowWrite {
		return errors.New("refusing to write without -allow-write")
	}
	if strings.TrimSpace(cfg.allowedDB) == "" {
		return errors.New("-allow-database is required")
	}
	if cfg.users < 2 || cfg.users > 10 {
		return errors.New("-users must be between 2 and 10")
	}
	if cfg.devicesPerUser < 1 || cfg.devicesPerUser > 3 {
		return errors.New("-devices must be between 1 and 3")
	}
	if cfg.users*cfg.devicesPerUser > 30 {
		return errors.New("users * devices must not exceed the current login IP limit of 30/minute")
	}
	if cfg.messages < 1 || cfg.messages > 100_000 {
		return errors.New("-messages must be between 1 and 100000")
	}
	if cfg.concurrency < 1 || cfg.concurrency > 1_000 {
		return errors.New("-concurrency must be between 1 and 1000")
	}
	if cfg.targetRate < 0 || cfg.targetRate > 100_000 {
		return errors.New("-rate must be between 0 and 100000")
	}
	if cfg.trafficPattern != trafficPatternRing && cfg.trafficPattern != trafficPatternHot {
		return errors.New("-traffic-pattern must be ring or hot")
	}
	if cfg.requestTimeout <= 0 || cfg.deliveryWait <= 0 || cfg.metricSampleInterval <= 0 {
		return errors.New("timeouts must be positive")
	}
	for name, raw := range map[string]string{"base-url": cfg.baseURL, "metrics-url": cfg.metricsURL, "database-url": cfg.databaseURL} {
		if _, err := url.ParseRequestURI(raw); err != nil {
			return fmt.Errorf("invalid -%s: %w", name, err)
		}
	}
	return nil
}

func validateDatabase(ctx context.Context, pool *pgxpool.Pool, allowed string) (string, error) {
	var actual string
	if err := pool.QueryRow(ctx, `SELECT current_database()`).Scan(&actual); err != nil {
		return "", err
	}
	if actual != allowed {
		return actual, fmt.Errorf("connected to %q, but -allow-database is %q", actual, allowed)
	}
	var usersTableExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.users') IS NOT NULL`).Scan(&usersTableExists); err != nil {
		return actual, err
	}
	if !usersTableExists {
		return actual, errors.New("users table is missing; initialize the IM schema first")
	}
	return actual, nil
}

func newLoadHTTPClient(timeout time.Duration, maxConnections int) *http.Client {
	if maxConnections < 1 {
		maxConnections = 1
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxConnections
	transport.MaxIdleConnsPerHost = maxConnections
	transport.MaxConnsPerHost = maxConnections
	return &http.Client{Transport: transport, Timeout: timeout}
}

func requireHealthy(ctx context.Context, client *http.Client, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET /health returned %s", resp.Status)
	}
	return nil
}

func seedUsers(ctx context.Context, pool *pgxpool.Pool, runID string, count int) ([]testUser, error) {
	users := make([]testUser, 0, count)
	for index := 0; index < count; index++ {
		username := fmt.Sprintf("lt_%s_%02d", runID, index+1)
		password := fmt.Sprintf("Load_%s_%02d!", runID, index+1)
		hash, err := hashPassword(password)
		if err != nil {
			return nil, err
		}
		var id int64
		err = pool.QueryRow(ctx,
			`INSERT INTO users (username, display_name, password_hash)
			 VALUES ($1, $2, $3)
			 RETURNING id`,
			username, "Load Test "+strconv.Itoa(index+1), hash,
		).Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("insert %s: %w", username, err)
		}
		users = append(users, testUser{ID: id, Username: username, Password: password})
	}
	return users, nil
}

func loginDevices(ctx context.Context, client *http.Client, cfg config, runID string, users []testUser) error {
	for userIndex := range users {
		for deviceIndex := 0; deviceIndex < cfg.devicesPerUser; deviceIndex++ {
			deviceID := fmt.Sprintf("load-%s-u%02d-d%02d", runID, userIndex+1, deviceIndex+1)
			payload := map[string]string{
				"username":       users[userIndex].Username,
				"password":       users[userIndex].Password,
				"loginRequestId": fmt.Sprintf("login_%s_u%02d_d%02d", runID, userIndex+1, deviceIndex+1),
				"deviceId":       deviceID,
			}
			var authenticated authResponse
			status, retryAfter, err := doJSON(ctx, client, http.MethodPost, strings.TrimRight(cfg.baseURL, "/")+"/auth/login", "", payload, &authenticated)
			if status == http.StatusTooManyRequests {
				return fmt.Errorf("login %s returned 429; Retry-After=%s (use an isolated Redis database or wait for the limit window)", deviceID, retryAfter)
			}
			if err != nil {
				return fmt.Errorf("login %s: %w", deviceID, err)
			}
			if status != http.StatusOK {
				return fmt.Errorf("login %s returned HTTP %d", deviceID, status)
			}
			users[userIndex].Sessions = append(users[userIndex].Sessions, authenticated)
		}
	}
	return nil
}

func connectAll(ctx context.Context, cfg config, users []testUser, loadTracker *tracker) ([]liveConnection, error) {
	webSocketURL, err := toWebSocketURL(cfg.baseURL)
	if err != nil {
		return nil, err
	}
	connections := make([]liveConnection, 0, len(users)*cfg.devicesPerUser)
	for userIndex, user := range users {
		for deviceIndex, session := range user.Sessions {
			label := fmt.Sprintf("u%02d-d%02d", userIndex+1, deviceIndex+1)
			headers := http.Header{}
			headers.Set("Authorization", "Bearer "+session.AccessToken)
			conn, response, err := websocket.Dial(ctx, webSocketURL, &websocket.DialOptions{HTTPHeader: headers})
			if err != nil {
				closeConnections(connections)
				if response != nil {
					return nil, fmt.Errorf("connect %s: %w (HTTP %s)", label, err, response.Status)
				}
				return nil, fmt.Errorf("connect %s: %w", label, err)
			}
			connection := liveConnection{label: label, conn: conn}
			connections = append(connections, connection)
			go readWebSocket(ctx, connection, loadTracker)
		}
	}
	return connections, nil
}

func sendLoad(ctx context.Context, client *http.Client, cfg config, runID string, users []testUser, loadTracker *tracker) []sendResult {
	results := make(chan sendResult, cfg.messages)
	slots := make(chan struct{}, cfg.concurrency)
	var requests sync.WaitGroup
	loadStart := time.Now()
	for index := 0; index < cfg.messages; index++ {
		if cfg.targetRate > 0 {
			plannedStart := loadStart.Add(time.Duration(index) * time.Second / time.Duration(cfg.targetRate))
			if wait := time.Until(plannedStart); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					results <- droppedSend(runID, index, "load context cancelled before scheduled start")
					continue
				case <-timer.C:
				}
			}
			select {
			case slots <- struct{}{}:
			default:
				results <- droppedSend(runID, index, "max in-flight requests reached")
				continue
			}
		} else {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				results <- droppedSend(runID, index, "load context cancelled before request start")
				continue
			}
		}

		requests.Add(1)
		go func(index int) {
			defer requests.Done()
			defer func() { <-slots }()
			results <- sendOne(ctx, client, cfg, runID, users, loadTracker, index)
		}(index)
	}
	requests.Wait()
	close(results)

	collected := make([]sendResult, 0, cfg.messages)
	for result := range results {
		collected = append(collected, result)
	}
	sort.Slice(collected, func(i, j int) bool { return collected[i].clientMessageID < collected[j].clientMessageID })
	return collected
}

func droppedSend(runID string, index int, reason string) sendResult {
	return sendResult{
		clientMessageID: fmt.Sprintf("msg_%s_%08d", runID, index+1),
		dropped:         true,
		err:             "not started: " + reason,
	}
}

func sendOne(ctx context.Context, client *http.Client, cfg config, runID string, users []testUser, loadTracker *tracker, index int) sendResult {
	senderIndex, receiverIndex := messageParticipants(cfg.trafficPattern, index, len(users))
	clientID := fmt.Sprintf("msg_%s_%08d", runID, index+1)
	labels := make([]string, 0, cfg.devicesPerUser*2)
	for device := 0; device < cfg.devicesPerUser; device++ {
		labels = append(labels,
			fmt.Sprintf("u%02d-d%02d", senderIndex+1, device+1),
			fmt.Sprintf("u%02d-d%02d", receiverIndex+1, device+1),
		)
	}
	started := time.Now()
	loadTracker.expect(clientID, started, labels)
	payload := map[string]any{
		"clientMessageId": clientID,
		"receiverId":      users[receiverIndex].ID,
		"content":         fmt.Sprintf("load test %s message %d", runID, index+1),
	}
	result := sendResult{clientMessageID: clientID}
	for attempt := 1; attempt <= 2; attempt++ {
		var created apiMessage
		status, _, requestErr := doJSON(ctx, client, http.MethodPost, strings.TrimRight(cfg.baseURL, "/")+"/messages",
			users[senderIndex].Sessions[0].AccessToken, payload, &created)
		result.attempts = attempt
		result.status = status
		if requestErr == nil && (status == http.StatusCreated || status == http.StatusOK) {
			result.message = created
			result.recovered = attempt > 1 || status == http.StatusOK
			break
		}
		if attempt == 1 && (status == 0 || status >= 500) {
			continue
		}
		if requestErr != nil {
			result.err = requestErr.Error()
		} else {
			result.err = fmt.Sprintf("HTTP %d", status)
		}
	}
	result.latency = time.Since(started)
	if result.message.ID == 0 {
		if result.err == "" {
			result.err = fmt.Sprintf("HTTP %d returned no message", result.status)
		}
		loadTracker.drop(clientID)
	}
	return result
}

func messageParticipants(pattern string, index, userCount int) (int, int) {
	if pattern == trafficPatternHot {
		sender := index % 2
		return sender, 1 - sender
	}
	sender := index % userCount
	return sender, (sender + 1) % userCount
}

func verifySync(ctx context.Context, client *http.Client, cfg config, runID string, users []testUser, results []sendResult) (int, int, []string, error) {
	expected := make(map[int64]map[int64]struct{}, len(users))
	for _, user := range users {
		expected[user.ID] = make(map[int64]struct{})
	}
	for _, result := range results {
		if result.err != "" {
			continue
		}
		expected[result.message.SenderID][result.message.ID] = struct{}{}
		expected[result.message.ReceiverID][result.message.ID] = struct{}{}
	}

	observed := 0
	missing := make([]string, 0)
	for _, user := range users {
		seen, err := syncUser(ctx, client, cfg.baseURL, user.Sessions[0].AccessToken, "msg_"+runID+"_")
		if err != nil {
			return countExpected(expected), observed, missing, fmt.Errorf("user %d: %w", user.ID, err)
		}
		for messageID := range expected[user.ID] {
			if _, ok := seen[messageID]; ok {
				observed++
			} else {
				missing = append(missing, fmt.Sprintf("user=%d message=%d", user.ID, messageID))
			}
		}
	}
	sort.Strings(missing)
	return countExpected(expected), observed, missing, nil
}

func syncUser(ctx context.Context, client *http.Client, baseURL, token, clientIDPrefix string) (map[int64]struct{}, error) {
	seen := make(map[int64]struct{})
	conversationIDs, err := listConversationIDs(ctx, client, baseURL, token)
	if err != nil {
		return nil, err
	}
	for _, conversationID := range conversationIDs {
		messages, err := syncOneConversation(ctx, client, baseURL, token, conversationID)
		if err != nil {
			return nil, fmt.Errorf("conversation %d: %w", conversationID, err)
		}
		for _, message := range messages {
			if strings.HasPrefix(message.ClientMessageID, clientIDPrefix) {
				seen[message.ID] = struct{}{}
			}
		}
	}
	return seen, nil
}

func listConversationIDs(ctx context.Context, client *http.Client, baseURL, token string) ([]int64, error) {
	conversationIDs := make([]int64, 0)
	var after, snapshot int64
	first := true
	for pageNumber := 0; pageNumber < 10_000; pageNumber++ {
		path := fmt.Sprintf("%s/conversations?after=%d&limit=200", strings.TrimRight(baseURL, "/"), after)
		if !first {
			path += "&snapshotCursor=" + strconv.FormatInt(snapshot, 10)
		}
		var page conversationListResponse
		status, _, err := doJSON(ctx, client, http.MethodGet, path, token, nil, &page)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d", status)
		}
		if first {
			snapshot = page.SnapshotCursor
			first = false
		}
		lastConversationID := after
		for _, conversation := range page.Conversations {
			if conversation.ID <= lastConversationID || conversation.ID > snapshot {
				return nil, fmt.Errorf("invalid conversation list cursor %d", conversation.ID)
			}
			conversationIDs = append(conversationIDs, conversation.ID)
			lastConversationID = conversation.ID
		}
		if !page.HasMore {
			return conversationIDs, nil
		}
		if page.NextCursor <= after {
			return nil, errors.New("conversation list cursor did not advance")
		}
		after = page.NextCursor
	}
	return nil, errors.New("conversation list exceeded 10000 pages")
}

func syncOneConversation(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	token string,
	conversationID int64,
) ([]apiMessage, error) {
	messages := make([]apiMessage, 0)
	var after, snapshot int64
	first := true
	for pageNumber := 0; pageNumber < 10_000; pageNumber++ {
		path := fmt.Sprintf(
			"%s/conversations/%d/messages?after=%d&limit=200",
			strings.TrimRight(baseURL, "/"),
			conversationID,
			after,
		)
		if !first {
			path += "&snapshotCursor=" + strconv.FormatInt(snapshot, 10)
		}
		var page conversationSyncResponse
		status, _, err := doJSON(ctx, client, http.MethodGet, path, token, nil, &page)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d", status)
		}
		if page.ConversationID != conversationID {
			return nil, fmt.Errorf("response conversation %d does not match request", page.ConversationID)
		}
		if first {
			snapshot = page.SnapshotCursor
			first = false
		}
		lastSequence := after
		for _, message := range page.Messages {
			if message.ConversationID != conversationID ||
				message.ConversationSeq != lastSequence+1 ||
				message.ConversationSeq > snapshot {
				return nil, fmt.Errorf("invalid message cursor %d/%d", message.ConversationID, message.ConversationSeq)
			}
			messages = append(messages, message)
			lastSequence = message.ConversationSeq
		}
		if !page.HasMore {
			return messages, nil
		}
		if page.NextCursor <= after {
			return nil, errors.New("conversation message cursor did not advance")
		}
		after = page.NextCursor
	}
	return nil, errors.New("conversation sync exceeded 10000 pages")
}

func newTracker() *tracker {
	tracked := &tracker{change: make(chan struct{}, 1)}
	for index := range tracked.shards {
		tracked.shards[index].started = make(map[string]time.Time)
		tracked.shards[index].expected = make(map[string]map[string]struct{})
		tracked.shards[index].received = make(map[string]map[string]time.Duration)
	}
	return tracked
}

func (t *tracker) shard(clientID string) *trackerShard {
	hash := uint32(2166136261)
	for index := 0; index < len(clientID); index++ {
		hash ^= uint32(clientID[index])
		hash *= 16777619
	}
	return &t.shards[hash%trackerShardCount]
}

func (t *tracker) expect(clientID string, started time.Time, labels []string) {
	shard := t.shard(clientID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.started[clientID] = started
	shard.expected[clientID] = make(map[string]struct{}, len(labels))
	for _, label := range labels {
		shard.expected[clientID][label] = struct{}{}
	}
}

func (t *tracker) drop(clientID string) {
	shard := t.shard(clientID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.started, clientID)
	delete(shard.expected, clientID)
	delete(shard.received, clientID)
	t.signal()
}

func (t *tracker) observe(label string, envelope wsEnvelope) {
	clientID := envelope.Message.ClientMessageID
	shard := t.shard(clientID)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	expectedLabels, exists := shard.expected[clientID]
	if !exists {
		t.unexpected.Add(1)
		return
	}
	if _, expected := expectedLabels[label]; !expected {
		t.unexpected.Add(1)
		return
	}
	if shard.received[clientID] == nil {
		shard.received[clientID] = make(map[string]time.Duration)
	}
	if _, duplicate := shard.received[clientID][label]; duplicate {
		t.duplicate.Add(1)
		return
	}
	shard.received[clientID][label] = time.Since(shard.started[clientID])
	t.signal()
}

func (t *tracker) readerError(label string, err error) {
	t.errorsMu.Lock()
	defer t.errorsMu.Unlock()
	t.readerErrors = append(t.readerErrors, label+": "+err.Error())
	t.signal()
}

func (t *tracker) signal() {
	select {
	case t.change <- struct{}{}:
	default:
	}
}

func (t *tracker) wait(timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		expected, received := t.counts()
		if expected == received {
			return
		}
		select {
		case <-t.change:
		case <-timer.C:
			return
		}
	}
}

func (t *tracker) snapshot() (int, int, []string, []time.Duration, int, int, []string) {
	expectedCount, receivedCount := 0, 0
	missing := make([]string, 0)
	latencies := make([]time.Duration, 0)
	for index := range t.shards {
		shard := &t.shards[index]
		shard.mu.Lock()
		for clientID, labels := range shard.expected {
			expectedCount += len(labels)
			for label := range labels {
				latency, ok := shard.received[clientID][label]
				if !ok {
					missing = append(missing, clientID+"@"+label)
					continue
				}
				receivedCount++
				latencies = append(latencies, latency)
			}
		}
		shard.mu.Unlock()
	}
	sort.Strings(missing)
	t.errorsMu.Lock()
	readerErrors := append([]string(nil), t.readerErrors...)
	t.errorsMu.Unlock()
	return expectedCount, receivedCount, missing, latencies, int(t.duplicate.Load()), int(t.unexpected.Load()), readerErrors
}

func (t *tracker) counts() (int, int) {
	expected, received := 0, 0
	for index := range t.shards {
		shard := &t.shards[index]
		shard.mu.Lock()
		for _, labels := range shard.expected {
			expected += len(labels)
		}
		for _, labels := range shard.received {
			received += len(labels)
		}
		shard.mu.Unlock()
	}
	return expected, received
}

func readWebSocket(ctx context.Context, connection liveConnection, loadTracker *tracker) {
	for {
		_, payload, err := connection.conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				status := websocket.CloseStatus(err)
				if status != websocket.StatusNormalClosure && status != websocket.StatusGoingAway {
					loadTracker.readerError(connection.label, err)
				}
			}
			return
		}
		var envelope wsEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			loadTracker.readerError(connection.label, fmt.Errorf("decode event: %w", err))
			continue
		}
		if envelope.Type == "message.created" {
			if envelope.ConversationID <= 0 ||
				envelope.ConversationSeq <= 0 ||
				envelope.Message.ConversationID != envelope.ConversationID ||
				envelope.Message.ConversationSeq != envelope.ConversationSeq {
				loadTracker.readerError(connection.label, errors.New("message.created has an invalid conversation cursor"))
				continue
			}
			loadTracker.observe(connection.label, envelope)
		}
	}
}

func summarizeSends(results []sendResult) (int, int, int, []string, []time.Duration) {
	successful := 0
	recovered := 0
	dropped := 0
	errorsFound := make([]string, 0)
	latencies := make([]time.Duration, 0, len(results))
	for _, result := range results {
		if result.dropped {
			dropped++
		} else {
			latencies = append(latencies, result.latency)
		}
		if result.err == "" {
			successful++
			if result.recovered {
				recovered++
			}
			continue
		}
		errorsFound = append(errorsFound, result.clientMessageID+": "+result.err)
	}
	return successful, recovered, dropped, errorsFound, latencies
}

func doJSON(ctx context.Context, client *http.Client, method, endpoint, token string, input, output any) (int, string, error) {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return 0, "", err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return 0, "", err
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, resp.Header.Get("Retry-After"), err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && output != nil {
		if err := json.Unmarshal(payload, output); err != nil {
			return resp.StatusCode, resp.Header.Get("Retry-After"), fmt.Errorf("decode response: %w", err)
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiError struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(payload, &apiError) == nil && apiError.Error != "" {
			return resp.StatusCode, resp.Header.Get("Retry-After"), errors.New(apiError.Error)
		}
	}
	return resp.StatusCode, resp.Header.Get("Retry-After"), nil
}

func fetchMetrics(ctx context.Context, client *http.Client, endpoint string) (metricsSnapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return metricsSnapshot{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return metricsSnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return metricsSnapshot{}, fmt.Errorf("GET metrics returned %s", resp.Status)
	}
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return metricsSnapshot{}, err
	}
	values := make(map[string]float64)
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if index := strings.IndexByte(name, '{'); index >= 0 {
			name = name[:index]
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err == nil {
			values[name] += value
		}
	}
	if err := scanner.Err(); err != nil {
		return metricsSnapshot{}, err
	}
	stageDurations, err := parseOutboxStageDurations(payload)
	if err != nil {
		return metricsSnapshot{}, err
	}
	databaseAcquireDurations, err := parseDatabaseAcquireDurations(payload)
	if err != nil {
		return metricsSnapshot{}, err
	}
	return metricsSnapshot{
		Values:                   values,
		OutboxStageDurations:     stageDurations,
		DatabaseAcquireDurations: databaseAcquireDurations,
	}, nil
}

func parseOutboxStageDurations(payload []byte) (map[string]histogramSnapshot, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	family := families["im_backend_outbox_stage_duration_seconds"]
	if family == nil {
		return map[string]histogramSnapshot{}, nil
	}
	result := make(map[string]histogramSnapshot)
	for _, metric := range family.GetMetric() {
		stage := ""
		for _, label := range metric.GetLabel() {
			if label.GetName() == "stage" {
				stage = label.GetValue()
				break
			}
		}
		histogram := metric.GetHistogram()
		if stage == "" || histogram == nil {
			continue
		}
		buckets := make(map[float64]uint64, len(histogram.GetBucket()))
		for _, bucket := range histogram.GetBucket() {
			buckets[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
		}
		result[stage] = histogramSnapshot{
			Count:   histogram.GetSampleCount(),
			Sum:     histogram.GetSampleSum(),
			Buckets: buckets,
		}
	}
	return result, nil
}

func parseDatabaseAcquireDurations(payload []byte) (map[string]map[string]histogramSnapshot, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("parse Prometheus metrics: %w", err)
	}
	family := families["im_backend_database_pool_acquire_duration_seconds"]
	if family == nil {
		return map[string]map[string]histogramSnapshot{}, nil
	}
	result := make(map[string]map[string]histogramSnapshot)
	for _, metric := range family.GetMetric() {
		workload := ""
		acquireResult := ""
		for _, label := range metric.GetLabel() {
			switch label.GetName() {
			case "workload":
				workload = label.GetValue()
			case "result":
				acquireResult = label.GetValue()
			}
		}
		histogram := metric.GetHistogram()
		if workload == "" || acquireResult == "" || histogram == nil {
			continue
		}
		buckets := make(map[float64]uint64, len(histogram.GetBucket()))
		for _, bucket := range histogram.GetBucket() {
			buckets[bucket.GetUpperBound()] = bucket.GetCumulativeCount()
		}
		if result[workload] == nil {
			result[workload] = make(map[string]histogramSnapshot)
		}
		result[workload][acquireResult] = histogramSnapshot{
			Count:   histogram.GetSampleCount(),
			Sum:     histogram.GetSampleSum(),
			Buckets: buckets,
		}
	}
	return result, nil
}

type metricPeakSampler struct {
	cancel   context.CancelFunc
	done     chan struct{}
	interval string
	mu       sync.Mutex
	peaks    map[string]float64
	samples  int
	errors   int
}

var peakMetricNames = []string{
	"im_backend_outbox_pending_events",
	"im_backend_outbox_oldest_pending_age_seconds",
	"im_backend_outbox_projection_pending_jobs",
	"im_backend_outbox_projection_oldest_pending_job_age_seconds",
}

func startMetricPeakSampler(
	parent context.Context,
	client *http.Client,
	endpoint string,
	interval time.Duration,
	initial map[string]float64,
) *metricPeakSampler {
	ctx, cancel := context.WithCancel(parent)
	sampler := &metricPeakSampler{
		cancel:   cancel,
		done:     make(chan struct{}),
		interval: interval.String(),
		peaks:    make(map[string]float64),
	}
	sampler.observe(initial)
	go func() {
		defer close(sampler.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snapshot, err := fetchMetrics(ctx, client, endpoint)
				if err != nil {
					if ctx.Err() == nil {
						sampler.recordError()
					}
					continue
				}
				sampler.observe(snapshot.Values)
			}
		}
	}()
	return sampler
}

func (sampler *metricPeakSampler) observe(values map[string]float64) {
	if sampler == nil || values == nil {
		return
	}
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	sampler.samples++
	for _, name := range peakMetricNames {
		if value, exists := values[name]; exists && value > sampler.peaks[name] {
			sampler.peaks[name] = value
		}
	}
}

func (sampler *metricPeakSampler) recordError() {
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	sampler.errors++
}

func (sampler *metricPeakSampler) Stop(final map[string]float64) metricSamplingReport {
	if sampler == nil {
		return metricSamplingReport{}
	}
	sampler.cancel()
	<-sampler.done
	sampler.observe(final)
	sampler.mu.Lock()
	defer sampler.mu.Unlock()
	peaks := make(map[string]float64, len(sampler.peaks))
	for name, value := range sampler.peaks {
		peaks[name] = value
	}
	return metricSamplingReport{Interval: sampler.interval, Samples: sampler.samples, Errors: sampler.errors, Peaks: peaks}
}

var deltaMetricNames = []string{
	"im_backend_http_requests_total",
	"im_backend_outbox_publish_total",
	"im_backend_outbox_publish_duration_seconds_count",
	"im_backend_outbox_publish_duration_seconds_sum",
	"im_backend_outbox_projection_batches_total",
	"im_backend_outbox_projection_users_total",
	"im_backend_outbox_projection_query_duration_seconds_count",
	"im_backend_outbox_projection_query_duration_seconds_sum",
	"im_backend_outbox_batch_presence_batches_total",
	"im_backend_outbox_batch_presence_users_total",
	"im_backend_realtime_routing_total",
	"im_backend_websocket_deliveries_total",
	"im_backend_websocket_disconnects_total",
	"im_backend_websocket_io_total",
	"im_backend_websocket_write_duration_seconds_count",
	"im_backend_websocket_write_duration_seconds_sum",
	"im_backend_websocket_send_queue_depth_count",
	"im_backend_websocket_send_queue_depth_sum",
	"im_backend_sync_events_total",
	"im_backend_database_pool_acquires_total",
	"im_backend_database_pool_empty_acquires_total",
	"im_backend_database_pool_canceled_acquires_total",
	"im_backend_database_pool_acquire_duration_seconds_total",
	"im_backend_database_pool_empty_acquire_wait_seconds_total",
	"im_backend_outbox_database_pool_acquires_total",
	"im_backend_outbox_database_pool_empty_acquires_total",
	"im_backend_outbox_database_pool_canceled_acquires_total",
	"im_backend_outbox_database_pool_acquire_duration_seconds_total",
	"im_backend_outbox_database_pool_empty_acquire_wait_seconds_total",
}

var endMetricNames = []string{
	"im_backend_websocket_connections",
	"im_backend_outbox_pending_events",
	"im_backend_outbox_oldest_pending_age_seconds",
	"im_backend_outbox_dead_events",
	"im_backend_outbox_projection_pending_jobs",
	"im_backend_outbox_projection_oldest_pending_job_age_seconds",
	"im_backend_database_metrics_collection_success",
	"im_backend_database_pool_acquired_connections",
	"im_backend_database_pool_idle_connections",
	"im_backend_database_pool_constructing_connections",
	"im_backend_database_pool_total_connections",
	"im_backend_database_pool_max_connections",
	"im_backend_outbox_database_pool_acquired_connections",
	"im_backend_outbox_database_pool_idle_connections",
	"im_backend_outbox_database_pool_constructing_connections",
	"im_backend_outbox_database_pool_total_connections",
	"im_backend_outbox_database_pool_max_connections",
	"im_backend_outbox_worker_concurrency",
	"im_backend_outbox_worker_batch_size",
	"im_backend_outbox_prepare_workers",
	"im_backend_outbox_user_sharded_prepare_enabled",
	"im_backend_outbox_pipeline_enabled",
	"im_backend_outbox_batch_presence_enabled",
	"im_backend_outbox_projection_bulk_enabled",
	"im_backend_outbox_projection_recipients_enabled",
	"im_backend_outbox_projection_sync_events_enabled",
	"im_backend_websocket_send_queue_high_watermark",
	"go_goroutines",
	"process_resident_memory_bytes",
}

func metricDelta(before, after map[string]float64) map[string]float64 {
	result := make(map[string]float64)
	for _, name := range deltaMetricNames {
		if _, exists := after[name]; exists {
			result[name] = after[name] - before[name]
		}
	}
	return result
}

func outboxStageDurationDelta(
	before map[string]histogramSnapshot,
	after map[string]histogramSnapshot,
) map[string]histogramDeltaReport {
	result := make(map[string]histogramDeltaReport)
	for stage, current := range after {
		previous := before[stage]
		if current.Count < previous.Count {
			continue
		}
		count := current.Count - previous.Count
		if count == 0 {
			continue
		}
		sum := current.Sum - previous.Sum
		if sum < 0 {
			continue
		}
		result[stage] = histogramDeltaReport{
			Count:       count,
			AverageMS:   sum * 1000 / float64(count),
			P50BucketMS: histogramQuantileBucketMS(previous, current, count, 0.50),
			P95BucketMS: histogramQuantileBucketMS(previous, current, count, 0.95),
			P99BucketMS: histogramQuantileBucketMS(previous, current, count, 0.99),
		}
	}
	return result
}

func databaseAcquireDurationDelta(
	before map[string]map[string]histogramSnapshot,
	after map[string]map[string]histogramSnapshot,
) map[string]map[string]histogramDeltaReport {
	result := make(map[string]map[string]histogramDeltaReport)
	for workload, currentByResult := range after {
		for acquireResult, current := range currentByResult {
			previous := before[workload][acquireResult]
			if current.Count < previous.Count {
				continue
			}
			count := current.Count - previous.Count
			if count == 0 {
				continue
			}
			sum := current.Sum - previous.Sum
			if sum < 0 {
				continue
			}
			if result[workload] == nil {
				result[workload] = make(map[string]histogramDeltaReport)
			}
			result[workload][acquireResult] = histogramDeltaReport{
				Count:       count,
				AverageMS:   sum * 1000 / float64(count),
				P50BucketMS: histogramQuantileBucketMS(previous, current, count, 0.50),
				P95BucketMS: histogramQuantileBucketMS(previous, current, count, 0.95),
				P99BucketMS: histogramQuantileBucketMS(previous, current, count, 0.99),
			}
		}
	}
	return result
}

func histogramQuantileBucketMS(before, after histogramSnapshot, count uint64, quantile float64) float64 {
	if count == 0 {
		return 0
	}
	bounds := make([]float64, 0, len(after.Buckets))
	for bound := range after.Buckets {
		bounds = append(bounds, bound)
	}
	sort.Float64s(bounds)
	target := uint64(math.Ceil(float64(count) * quantile))
	for _, bound := range bounds {
		current := after.Buckets[bound]
		previous := before.Buckets[bound]
		if current >= previous && current-previous >= target {
			return bound * 1000
		}
	}
	return -1
}

func selectedMetricEnd(values map[string]float64) map[string]float64 {
	result := make(map[string]float64)
	for _, name := range endMetricNames {
		if value, exists := values[name]; exists {
			result[name] = value
		}
	}
	return result
}

func calculateDurationStats(values []time.Duration) durationStats {
	if len(values) == 0 {
		return durationStats{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return durationStats{
		Count: len(sorted),
		P50MS: durationMilliseconds(percentile(sorted, 0.50)),
		P95MS: durationMilliseconds(percentile(sorted, 0.95)),
		P99MS: durationMilliseconds(percentile(sorted, 0.99)),
		MaxMS: durationMilliseconds(sorted[len(sorted)-1]),
	}
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1)*quantile + 0.5)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func durationMilliseconds(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000
}

func printReport(result report) {
	fmt.Println("\n=== IM load-test report ===")
	fmt.Printf("Load: model=%s target=%d req/s max_inflight=%d dropped_starts=%d\n",
		result.LoadModel, result.TargetRateRPS, result.Concurrency, result.DroppedStarts)
	fmt.Printf("HTTP: success=%d failed=%d idempotent_recovered=%d achieved=%.1f req/s p50=%.2fms p95=%.2fms p99=%.2fms max=%.2fms\n",
		result.MessagesSucceeded, result.MessagesFailed, result.IdempotentRecovered, result.HTTPThroughputRPS,
		result.HTTPLatency.P50MS, result.HTTPLatency.P95MS, result.HTTPLatency.P99MS, result.HTTPLatency.MaxMS)
	fmt.Printf("Realtime: %s observed=%d/%d p95=%.2fms missing=%d duplicate=%d unexpected=%d\n",
		passLabel(result.Realtime.Passed), result.Realtime.Observed, result.Realtime.Expected,
		result.RealtimeLatency.P95MS, result.Realtime.Expected-result.Realtime.Observed, result.DuplicateRealtime, result.UnexpectedRealtime)
	fmt.Printf("Sync durability: %s observed=%d/%d missing=%d\n",
		passLabel(result.Sync.Passed), result.Sync.Observed, result.Sync.Expected, result.Sync.Expected-result.Sync.Observed)
	for _, stage := range []string{"claim", "prepare", "publish", "mark_published"} {
		if value, exists := result.OutboxStageDurations[stage]; exists {
			fmt.Printf("Outbox stage %-14s batches=%d avg=%.2fms p50<=%.2fms p95<=%.2fms p99<=%.2fms\n",
				stage, value.Count, value.AverageMS, value.P50BucketMS, value.P95BucketMS, value.P99BucketMS)
		}
	}
	for _, workload := range []string{"api", "outbox"} {
		for _, acquireResult := range []string{"success", "error"} {
			value, exists := result.DatabaseAcquireDurations[workload][acquireResult]
			if !exists {
				continue
			}
			fmt.Printf("DB acquire %-6s %-7s count=%d avg=%.2fms p50<=%.2fms p95<=%.2fms p99<=%.2fms\n",
				workload, acquireResult, value.Count, value.AverageMS, value.P50BucketMS, value.P95BucketMS, value.P99BucketMS)
		}
	}
	fmt.Printf("Outbox metric sampling: interval=%s samples=%d errors=%d peak_pending=%.0f peak_oldest_age=%.3fs\n",
		result.MetricSampling.Interval,
		result.MetricSampling.Samples,
		result.MetricSampling.Errors,
		result.MetricSampling.Peaks["im_backend_outbox_pending_events"],
		result.MetricSampling.Peaks["im_backend_outbox_oldest_pending_age_seconds"],
	)
	fmt.Printf("Projection jobs: peak_pending=%.0f peak_oldest_age=%.3fs\n",
		result.MetricSampling.Peaks["im_backend_outbox_projection_pending_jobs"],
		result.MetricSampling.Peaks["im_backend_outbox_projection_oldest_pending_job_age_seconds"],
	)
	for _, name := range endMetricNames {
		if value, exists := result.MetricEnd[name]; exists {
			fmt.Printf("metric end %-55s %.0f\n", name, value)
		}
	}
	if len(result.RequestErrors) > 0 {
		fmt.Println("first errors:")
		for _, item := range result.RequestErrors[:min(5, len(result.RequestErrors))] {
			fmt.Println(" -", item)
		}
	}
	if len(result.ReaderErrors) > 0 {
		fmt.Println("first WebSocket reader errors:")
		for _, item := range result.ReaderErrors[:min(5, len(result.ReaderErrors))] {
			fmt.Println(" -", item)
		}
	}
	if len(result.Realtime.Missing) > 0 {
		fmt.Println("first realtime misses:")
		for _, item := range result.Realtime.Missing[:min(5, len(result.Realtime.Missing))] {
			fmt.Println(" -", item)
		}
	}
	if len(result.Sync.Missing) > 0 {
		fmt.Println("first sync misses:")
		for _, item := range result.Sync.Missing[:min(5, len(result.Sync.Missing))] {
			fmt.Println(" -", item)
		}
	}
}

func writeReport(path string, result report) error {
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(payload, '\n'), 0o600)
}

func hashPassword(password string) (string, error) {
	salt := make([]byte, passwordSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(password), salt, passwordIterations, passwordMemory, passwordParallel, passwordKeyLength)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, passwordMemory, passwordIterations, passwordParallel,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(hash)), nil
}

func newRunID(now time.Time) (string, error) {
	random := make([]byte, 3)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return strings.ToLower(now.Format("060102150405") + hex.EncodeToString(random)), nil
}

func toWebSocketURL(base string) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	default:
		return "", fmt.Errorf("unsupported API scheme %q", parsed.Scheme)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/ws"
	parsed.RawQuery = ""
	return parsed.String(), nil
}

func countExpected(values map[int64]map[int64]struct{}) int {
	total := 0
	for _, items := range values {
		total += len(items)
	}
	return total
}

func closeConnections(connections []liveConnection) {
	for _, connection := range connections {
		_ = connection.conn.Close(websocket.StatusNormalClosure, "load test complete")
	}
}

func capStrings(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return append(append([]string(nil), values[:limit]...), fmt.Sprintf("... %d more", len(values)-limit))
}

func passLabel(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
