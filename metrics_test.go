package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type testBatchPreparerFunc func(context.Context, []outboxEvent) ([]outboxEvent, error)

func (prepare testBatchPreparerFunc) PrepareBatch(ctx context.Context, events []outboxEvent) ([]outboxEvent, error) {
	return prepare(ctx, events)
}

func TestDatabaseAcquireTracerPartitionsAPIAndOutboxWait(t *testing.T) {
	metrics := newApplicationMetrics(nil)
	now := time.Unix(100, 0)
	tracer := &databaseAcquireTracer{metrics: metrics, now: func() time.Time { return now }}

	apiContext := tracer.TraceAcquireStart(
		withDatabaseWorkload(context.Background(), databaseWorkloadAPI),
		nil,
		pgxpool.TraceAcquireStartData{},
	)
	now = now.Add(7 * time.Millisecond)
	tracer.TraceAcquireEnd(apiContext, nil, pgxpool.TraceAcquireEndData{})

	outboxContext := tracer.TraceAcquireStart(
		withDatabaseWorkload(context.Background(), databaseWorkloadOutbox),
		nil,
		pgxpool.TraceAcquireStartData{},
	)
	now = now.Add(11 * time.Millisecond)
	tracer.TraceAcquireEnd(outboxContext, nil, pgxpool.TraceAcquireEndData{Err: errors.New("pool timeout")})

	untagged := tracer.TraceAcquireStart(context.Background(), nil, pgxpool.TraceAcquireStartData{})
	now = now.Add(time.Second)
	tracer.TraceAcquireEnd(untagged, nil, pgxpool.TraceAcquireEndData{})

	exposition := scrapeMetrics(t, metrics)
	for _, expected := range []string{
		`im_backend_database_pool_acquire_duration_seconds_count{result="success",workload="api"} 1`,
		`im_backend_database_pool_acquire_duration_seconds_sum{result="success",workload="api"} 0.007`,
		`im_backend_database_pool_acquire_duration_seconds_count{result="error",workload="outbox"} 1`,
		`im_backend_database_pool_acquire_duration_seconds_sum{result="error",workload="outbox"} 0.011`,
	} {
		if !strings.Contains(exposition, expected) {
			t.Fatalf("metrics exposition missing %q\n%s", expected, exposition)
		}
	}
	if strings.Contains(exposition, `workload="unknown"`) {
		t.Fatalf("metrics exposition contains an unbounded workload label\n%s", exposition)
	}
}

func TestMetricsExposeHTTPOutboxSyncAndACKBoundaries(t *testing.T) {
	db := openTestDatabase(t)
	app := newTestApplication(t, db)
	app.metrics = newApplicationMetrics(db)
	workerConfig := defaultOutboxWorkerConfig()
	app.metrics.SetOutboxWorkerConfig(workerConfig)
	app.metrics.SetOutboxBatchPresenceEnabled(true)
	app.metrics.ObserveOutboxBatchPresence(2, "success")
	server := httptest.NewServer(app.routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("metrics_s"), "Metrics Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("metrics_r"), "Metrics Receiver")

	createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "observable message")
	projectPendingMessageEvents(t, db, app.metrics)
	page := syncMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, 0, 10)
	acknowledgeMessagesThroughAPI(t, server.URL, receiver.Auth.AccessToken, page.NextCursor, http.StatusOK)

	exposition := scrapeMetrics(t, app.metrics)
	for _, expected := range []string{
		"im_backend_database_metrics_collection_success 1",
		"im_backend_database_pool_max_connections",
		"im_backend_database_pool_acquires_total",
		"im_backend_database_pool_empty_acquire_wait_seconds_total",
		"im_backend_outbox_pending_events",
		"im_backend_outbox_projection_pending_jobs 0",
		"im_backend_outbox_worker_concurrency 16",
		"im_backend_outbox_worker_batch_size 64",
		"im_backend_outbox_prepare_workers 1",
		"im_backend_outbox_user_sharded_prepare_enabled 0",
		"im_backend_outbox_pipeline_enabled 1",
		"im_backend_outbox_batch_presence_enabled 1",
		`im_backend_outbox_batch_presence_batches_total{result="success"} 1`,
		"im_backend_outbox_batch_presence_users_total 2",
		"im_backend_outbox_projection_bulk_enabled 1",
		"im_backend_outbox_projection_recipients_enabled 0",
		"im_backend_outbox_projection_sync_events_enabled 1",
		"im_backend_outbox_projection_batches_total 1",
		"im_backend_outbox_projection_users_total 2",
		"im_backend_outbox_projection_query_duration_seconds_count 3",
		"im_backend_http_requests_total",
		`route="POST /messages"`,
		`status="201"`,
		"im_backend_sync_events_total 1",
		`im_backend_ack_requests_total{result="accepted"} 1`,
		"im_backend_device_sync_max_ack_lag 0",
	} {
		if !strings.Contains(exposition, expected) {
			t.Fatalf("metrics exposition missing %q\n%s", expected, exposition)
		}
	}
	for _, forbidden := range []string{sender.Username, receiver.Username, receiver.Auth.DeviceID} {
		if strings.Contains(exposition, forbidden) {
			t.Fatalf("metrics exposition contains high-cardinality identity %q", forbidden)
		}
	}
}

func TestMetricsExposeRecipientProjectionStorageForRollback(t *testing.T) {
	metrics := newApplicationMetrics(nil)
	config := defaultOutboxWorkerConfig()
	config.ProjectionStorage = syncProjectionStorageRecipients
	metrics.SetOutboxWorkerConfig(config)

	exposition := scrapeMetrics(t, metrics)
	for _, expected := range []string{
		"im_backend_outbox_projection_recipients_enabled 1",
		"im_backend_outbox_projection_sync_events_enabled 0",
	} {
		if !strings.Contains(exposition, expected) {
			t.Fatalf("metrics exposition missing %q\n%s", expected, exposition)
		}
	}
}

func TestOutboxAndSlowClientResultsAreObservable(t *testing.T) {
	db := openTestDatabase(t)
	metrics := newApplicationMetrics(db)
	createPendingOutboxEvent(t, db)
	worker, err := newOutboxWorker(db, &testPublisher{}, testWorkerConfig(), metrics)
	if err != nil {
		t.Fatalf("create observed worker: %v", err)
	}
	worker.preparer = testBatchPreparerFunc(func(_ context.Context, events []outboxEvent) ([]outboxEvent, error) {
		return events, nil
	})
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatalf("run observed worker: %v", err)
	}

	hub := newWebSocketHub(metrics)
	ctx, cancel := context.WithCancel(context.Background())
	go hub.Run(ctx)
	slow := &webSocketClient{
		userID:       901,
		sessionID:    902,
		connectionID: "metrics-slow-client",
		send:         make(chan []byte, 1),
	}
	if err := hub.Register(context.Background(), slow); err != nil {
		t.Fatalf("register observed slow client: %v", err)
	}
	if _, err := hub.Publish(context.Background(), slow.userID, []byte("first")); err != nil {
		t.Fatalf("fill observed slow client: %v", err)
	}
	if _, err := hub.Publish(context.Background(), slow.userID, []byte("second")); err != nil {
		t.Fatalf("overflow observed slow client: %v", err)
	}
	metrics.ObserveWebSocketWrite(2*time.Millisecond, "success")
	cancel()
	<-hub.done

	exposition := scrapeMetrics(t, metrics)
	for _, expected := range []string{
		`im_backend_outbox_publish_total{event_type="test.outbox",result="published"} 1`,
		`im_backend_outbox_publish_duration_seconds_count{event_type="test.outbox"} 1`,
		`im_backend_outbox_stage_duration_seconds_count{stage="claim"} 1`,
		`im_backend_outbox_stage_duration_seconds_count{stage="prepare"} 1`,
		`im_backend_outbox_stage_duration_seconds_count{stage="publish"} 1`,
		`im_backend_outbox_stage_duration_seconds_count{stage="mark_published"} 1`,
		`im_backend_websocket_deliveries_total{result="slow_client"} 1`,
		`im_backend_websocket_disconnects_total{reason="slow_client"} 1`,
		"im_backend_websocket_connections 0",
		"im_backend_websocket_send_queue_depth_count 2",
		"im_backend_websocket_send_queue_high_watermark 1",
		`im_backend_websocket_write_duration_seconds_count{result="success"} 1`,
	} {
		if !strings.Contains(exposition, expected) {
			t.Fatalf("metrics exposition missing %q\n%s", expected, exposition)
		}
	}
}

func TestOutboxFailureResultsAreObservable(t *testing.T) {
	db := openTestDatabase(t)
	metrics := newApplicationMetrics(db)
	createPendingOutboxEvent(t, db)
	retryWorker, err := newOutboxWorker(
		db,
		&testPublisher{publish: func(context.Context, outboxEvent) error {
			return retryablePublishError(errors.New("temporary metric failure"), 0)
		}},
		testWorkerConfig(),
		metrics,
	)
	if err != nil {
		t.Fatalf("create observed retry worker: %v", err)
	}
	if _, err := retryWorker.RunOnce(context.Background()); err != nil {
		t.Fatalf("run observed retry worker: %v", err)
	}

	createPendingOutboxEvent(t, db)
	deadWorker, err := newOutboxWorker(
		db,
		&testPublisher{publish: func(context.Context, outboxEvent) error {
			return permanentPublishError(errors.New("permanent metric failure"))
		}},
		testWorkerConfig(),
		metrics,
	)
	if err != nil {
		t.Fatalf("create observed dead worker: %v", err)
	}
	if _, err := deadWorker.RunOnce(context.Background()); err != nil {
		t.Fatalf("run observed dead worker: %v", err)
	}

	exposition := scrapeMetrics(t, metrics)
	for _, expected := range []string{
		`im_backend_outbox_publish_total{event_type="test.outbox",result="retry_scheduled"} 1`,
		`im_backend_outbox_publish_total{event_type="test.outbox",result="dead"} 1`,
	} {
		if !strings.Contains(exposition, expected) {
			t.Fatalf("metrics exposition missing %q\n%s", expected, exposition)
		}
	}
}

func TestHTTPMetricsPreserveWebSocketUpgrade(t *testing.T) {
	db := openTestDatabase(t)
	app := newTestApplication(t, db)
	app.metrics = newApplicationMetrics(db)
	hub := newWebSocketHub(app.metrics)
	hubContext, cancelHub := context.WithCancel(context.Background())
	go hub.Run(hubContext)
	app.webSocketHub = hub
	server := httptest.NewServer(app.routes())
	t.Cleanup(func() {
		server.Close()
		cancelHub()
		<-hub.done
	})
	account := registerTestAccount(t, db, server.URL, uniqueUsername("metrics_ws"), "Metrics WebSocket")
	connection := dialAuthenticatedWebSocket(t, server.URL, account.Auth.AccessToken)
	_ = connection.CloseNow()

	deadline := time.Now().Add(2 * time.Second)
	for {
		exposition := scrapeMetrics(t, app.metrics)
		if strings.Contains(exposition, `route="GET /ws"`) && strings.Contains(exposition, `status="101"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("WebSocket upgrade metric was not observed\n%s", exposition)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func scrapeMetrics(t *testing.T, metrics *applicationMetrics) string {
	t.Helper()
	server := httptest.NewServer(metrics.Handler())
	defer server.Close()
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", response.StatusCode)
	}
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	return string(payload)
}
