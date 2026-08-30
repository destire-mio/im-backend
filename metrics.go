package main

import (
	"context"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type applicationMetrics struct {
	registry                   *prometheus.Registry
	httpRequests               *prometheus.CounterVec
	httpDuration               *prometheus.HistogramVec
	databaseAcquireDuration    *prometheus.HistogramVec
	outboxPublish              *prometheus.CounterVec
	outboxPublishDuration      *prometheus.HistogramVec
	outboxStageDuration        *prometheus.HistogramVec
	realtimeRouting            *prometheus.CounterVec
	webSocketConnections       prometheus.Gauge
	webSocketDeliveries        *prometheus.CounterVec
	webSocketDisconnects       *prometheus.CounterVec
	webSocketIO                *prometheus.CounterVec
	webSocketWriteDuration     *prometheus.HistogramVec
	webSocketQueueDepth        prometheus.Histogram
	webSocketQueueHighWater    prometheus.Gauge
	webSocketQueueMaximum      atomic.Int64
	syncPages                  *prometheus.CounterVec
	syncEvents                 prometheus.Counter
	ackRequests                *prometheus.CounterVec
	outboxWorkerConcurrency    prometheus.Gauge
	outboxWorkerBatchSize      prometheus.Gauge
	outboxPipelineEnabled      prometheus.Gauge
	outboxBatchPresenceEnabled prometheus.Gauge
	outboxBatchPresenceBatches *prometheus.CounterVec
	outboxBatchPresenceUsers   prometheus.Counter
	outboxProjectionBulk       prometheus.Gauge
	outboxProjectionRecipients prometheus.Gauge
	outboxProjectionBatches    prometheus.Counter
	outboxProjectionUsers      prometheus.Counter
	outboxProjectionQueries    prometheus.Histogram
}

func newApplicationMetrics(db *pgxpool.Pool) *applicationMetrics {
	registry := prometheus.NewRegistry()
	metrics := &applicationMetrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "http_requests_total",
			Help:      "HTTP requests handled, partitioned by stable route, method and status.",
		}, []string{"route", "method", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration by stable route and method.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"route", "method"}),
		databaseAcquireDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "database_pool_acquire_duration_seconds",
			Help:      "Client-observed duration of acquiring a shared database pool connection by bounded workload and result.",
			Buckets:   []float64{0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"workload", "result"}),
		outboxPublish: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "outbox_publish_total",
			Help:      "Outbox publish attempts by event type and durable result.",
		}, []string{"event_type", "result"}),
		outboxPublishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "outbox_publish_duration_seconds",
			Help:      "Time spent inside the publisher for one outbox attempt, excluding durable state updates.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"event_type"}),
		outboxStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "outbox_stage_duration_seconds",
			Help:      "Wall-clock duration of one non-empty Outbox batch stage.",
			Buckets:   []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60},
		}, []string{"stage"}),
		realtimeRouting: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "realtime_routing_total",
			Help:      "Realtime routing operations by bounded stage and result.",
		}, []string{"stage", "result"}),
		webSocketConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "websocket_connections",
			Help:      "WebSocket connections currently registered in this instance Hub.",
		}),
		webSocketDeliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "websocket_deliveries_total",
			Help:      "Hub delivery outcomes for local WebSocket connections.",
		}, []string{"result"}),
		webSocketDisconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "websocket_disconnects_total",
			Help:      "WebSocket disconnections by bounded reason.",
		}, []string{"reason"}),
		webSocketIO: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "websocket_io_total",
			Help:      "WebSocket data and heartbeat I/O outcomes.",
		}, []string{"operation", "result"}),
		webSocketWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "websocket_write_duration_seconds",
			Help:      "Time spent writing one business event to a WebSocket connection.",
			Buckets:   []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"result"}),
		webSocketQueueDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "websocket_send_queue_depth",
			Help:      "Observed per-connection send queue depth after enqueue or overflow.",
			Buckets:   []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 192, 224, 240, 248, 252, 256},
		}),
		webSocketQueueHighWater: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "websocket_send_queue_high_watermark",
			Help:      "Highest observed per-connection send queue depth since process start.",
		}),
		syncPages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "sync_pages_total",
			Help:      "Sync API pages returned, partitioned by whether the snapshot has more pages.",
		}, []string{"has_more"}),
		syncEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "sync_events_total",
			Help:      "Events returned by the Sync API.",
		}),
		ackRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "ack_requests_total",
			Help:      "Device ACK requests by bounded result.",
		}, []string{"result"}),
		outboxWorkerConcurrency: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "outbox_worker_concurrency",
			Help:      "Configured maximum number of concurrently processed Outbox events.",
		}),
		outboxWorkerBatchSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "outbox_worker_batch_size",
			Help:      "Configured maximum number of Outbox events claimed per batch.",
		}),
		outboxPipelineEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "outbox_pipeline_enabled",
			Help:      "Whether preparation of the next Outbox batch overlaps delivery of the current batch.",
		}),
		outboxBatchPresenceEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "outbox_batch_presence_enabled",
			Help:      "Whether each Outbox batch resolves duplicate recipient Presence records once.",
		}),
		outboxBatchPresenceBatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "outbox_batch_presence_batches_total",
			Help:      "Outbox batch Presence snapshot attempts by bounded result.",
		}, []string{"result"}),
		outboxBatchPresenceUsers: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "outbox_batch_presence_users_total",
			Help:      "Distinct recipient users included in Outbox batch Presence snapshot attempts.",
		}),
		outboxProjectionBulk: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "outbox_projection_bulk_enabled",
			Help:      "Whether the set-based bulk sync projection implementation is enabled.",
		}),
		outboxProjectionRecipients: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "im_backend",
			Name:      "outbox_projection_recipients_enabled",
			Help:      "Whether structured Outbox recipients replace the projected JSONB payload rewrite.",
		}),
		outboxProjectionBatches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "outbox_projection_batches_total",
			Help:      "Attempts to project one non-empty Outbox batch.",
		}),
		outboxProjectionUsers: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "im_backend",
			Name:      "outbox_projection_users_total",
			Help:      "Distinct user streams included in attempted Outbox projection batches.",
		}),
		outboxProjectionQueries: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "im_backend",
			Name:      "outbox_projection_query_duration_seconds",
			Help:      "Client-observed duration of one SQL query in the sync projection stage.",
			Buckets:   []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
	}

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		metrics.httpRequests,
		metrics.httpDuration,
		metrics.databaseAcquireDuration,
		metrics.outboxPublish,
		metrics.outboxPublishDuration,
		metrics.outboxStageDuration,
		metrics.realtimeRouting,
		metrics.webSocketConnections,
		metrics.webSocketDeliveries,
		metrics.webSocketDisconnects,
		metrics.webSocketIO,
		metrics.webSocketWriteDuration,
		metrics.webSocketQueueDepth,
		metrics.webSocketQueueHighWater,
		metrics.syncPages,
		metrics.syncEvents,
		metrics.ackRequests,
		metrics.outboxWorkerConcurrency,
		metrics.outboxWorkerBatchSize,
		metrics.outboxPipelineEnabled,
		metrics.outboxBatchPresenceEnabled,
		metrics.outboxBatchPresenceBatches,
		metrics.outboxBatchPresenceUsers,
		metrics.outboxProjectionBulk,
		metrics.outboxProjectionRecipients,
		metrics.outboxProjectionBatches,
		metrics.outboxProjectionUsers,
		metrics.outboxProjectionQueries,
	)
	if db != nil {
		metrics.registerDatabaseCollectors(db)
	}
	return metrics
}

func (metrics *applicationMetrics) registerDatabaseCollectors(db *pgxpool.Pool) {
	if metrics == nil || db == nil {
		return
	}
	metrics.registry.MustRegister(newDatabaseStateCollector(db), newDatabasePoolCollector(db))
}

func (metrics *applicationMetrics) ObserveWebSocketQueueDepth(depth int) {
	if metrics == nil {
		return
	}
	metrics.webSocketQueueDepth.Observe(float64(depth))
	current := int64(depth)
	for {
		previous := metrics.webSocketQueueMaximum.Load()
		if current <= previous {
			return
		}
		if metrics.webSocketQueueMaximum.CompareAndSwap(previous, current) {
			metrics.webSocketQueueHighWater.Set(float64(current))
			return
		}
	}
}

func (metrics *applicationMetrics) ObserveWebSocketWrite(duration time.Duration, result string) {
	if metrics != nil {
		metrics.webSocketWriteDuration.WithLabelValues(result).Observe(duration.Seconds())
	}
}

func (metrics *applicationMetrics) ObserveDatabaseAcquire(workload, result string, duration time.Duration) {
	if metrics == nil || duration < 0 {
		return
	}
	switch workload {
	case databaseWorkloadAPI, databaseWorkloadOutbox:
	default:
		return
	}
	switch result {
	case databaseAcquireSuccess, databaseAcquireError:
	default:
		return
	}
	metrics.databaseAcquireDuration.WithLabelValues(workload, result).Observe(duration.Seconds())
}

func (metrics *applicationMetrics) ObserveOutboxStage(stage string, duration time.Duration) {
	if metrics != nil {
		metrics.outboxStageDuration.WithLabelValues(stage).Observe(duration.Seconds())
	}
}

func (metrics *applicationMetrics) ObserveOutboxProjectionBatch(users int) {
	if metrics == nil {
		return
	}
	metrics.outboxProjectionBatches.Inc()
	metrics.outboxProjectionUsers.Add(float64(users))
}

func (metrics *applicationMetrics) ObserveOutboxBatchPresence(users int, result string) {
	if metrics == nil {
		return
	}
	metrics.outboxBatchPresenceBatches.WithLabelValues(result).Inc()
	metrics.outboxBatchPresenceUsers.Add(float64(users))
}

func (metrics *applicationMetrics) ObserveOutboxProjectionQuery(duration time.Duration) {
	if metrics != nil {
		metrics.outboxProjectionQueries.Observe(duration.Seconds())
	}
}

func (metrics *applicationMetrics) SetOutboxWorkerConfig(config outboxWorkerConfig) {
	if metrics == nil {
		return
	}
	metrics.outboxWorkerConcurrency.Set(float64(config.Concurrency))
	metrics.outboxWorkerBatchSize.Set(float64(config.BatchSize))
	if config.ExecutionMode == outboxExecutionModePipeline {
		metrics.outboxPipelineEnabled.Set(1)
	} else {
		metrics.outboxPipelineEnabled.Set(0)
	}
	if config.ProjectionMode == syncProjectionModeBulk {
		metrics.outboxProjectionBulk.Set(1)
	} else {
		metrics.outboxProjectionBulk.Set(0)
	}
	if config.ProjectionStorage == syncProjectionStorageRecipients {
		metrics.outboxProjectionRecipients.Set(1)
	} else {
		metrics.outboxProjectionRecipients.Set(0)
	}
}

func (metrics *applicationMetrics) SetOutboxBatchPresenceEnabled(enabled bool) {
	if metrics == nil {
		return
	}
	if enabled {
		metrics.outboxBatchPresenceEnabled.Set(1)
	} else {
		metrics.outboxBatchPresenceEnabled.Set(0)
	}
}

func (metrics *applicationMetrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
		Timeout:       2 * time.Second,
	}))
	return mux
}

func (metrics *applicationMetrics) InstrumentHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		capture := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(capture, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		metrics.httpRequests.WithLabelValues(route, r.Method, strconv.Itoa(capture.status)).Inc()
		metrics.httpDuration.WithLabelValues(route, r.Method).Observe(time.Since(started).Seconds())
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (writer *statusResponseWriter) WriteHeader(status int) {
	if writer.wroteHeader {
		return
	}
	writer.status = status
	writer.wroteHeader = true
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *statusResponseWriter) Write(payload []byte) (int, error) {
	if !writer.wroteHeader {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(payload)
}

func (writer *statusResponseWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

func (writer *statusResponseWriter) ObserveStatus(status int) {
	writer.status = status
}

func observeHTTPStatus(writer http.ResponseWriter, status int) {
	if observer, ok := writer.(interface{ ObserveStatus(int) }); ok {
		observer.ObserveStatus(status)
	}
}

type databaseStateCollector struct {
	db                *pgxpool.Pool
	collectionSuccess *prometheus.Desc
	outboxPending     *prometheus.Desc
	outboxOldestAge   *prometheus.Desc
	outboxDead        *prometheus.Desc
	ackDevices        *prometheus.Desc
	ackMaxLag         *prometheus.Desc
}

type databasePoolCollector struct {
	pool                    *pgxpool.Pool
	acquiredConnections     *prometheus.Desc
	idleConnections         *prometheus.Desc
	constructingConnections *prometheus.Desc
	totalConnections        *prometheus.Desc
	maximumConnections      *prometheus.Desc
	acquires                *prometheus.Desc
	emptyAcquires           *prometheus.Desc
	canceledAcquires        *prometheus.Desc
	acquireDuration         *prometheus.Desc
	emptyAcquireWait        *prometheus.Desc
}

func newDatabasePoolCollector(pool *pgxpool.Pool) *databasePoolCollector {
	return &databasePoolCollector{
		pool: pool,
		acquiredConnections: prometheus.NewDesc(
			"im_backend_database_pool_acquired_connections",
			"Database connections currently checked out from the application pool.", nil, nil,
		),
		idleConnections: prometheus.NewDesc(
			"im_backend_database_pool_idle_connections",
			"Database connections currently idle in the application pool.", nil, nil,
		),
		constructingConnections: prometheus.NewDesc(
			"im_backend_database_pool_constructing_connections",
			"Database connections currently being constructed by the application pool.", nil, nil,
		),
		totalConnections: prometheus.NewDesc(
			"im_backend_database_pool_total_connections",
			"Database connections currently present in the application pool.", nil, nil,
		),
		maximumConnections: prometheus.NewDesc(
			"im_backend_database_pool_max_connections",
			"Configured maximum number of database connections in the application pool.", nil, nil,
		),
		acquires: prometheus.NewDesc(
			"im_backend_database_pool_acquires_total",
			"Successful database connection acquisitions from the application pool.", nil, nil,
		),
		emptyAcquires: prometheus.NewDesc(
			"im_backend_database_pool_empty_acquires_total",
			"Successful acquisitions that had to wait because the application pool was empty.", nil, nil,
		),
		canceledAcquires: prometheus.NewDesc(
			"im_backend_database_pool_canceled_acquires_total",
			"Database connection acquisitions canceled while waiting.", nil, nil,
		),
		acquireDuration: prometheus.NewDesc(
			"im_backend_database_pool_acquire_duration_seconds_total",
			"Cumulative duration of successful database connection acquisitions.", nil, nil,
		),
		emptyAcquireWait: prometheus.NewDesc(
			"im_backend_database_pool_empty_acquire_wait_seconds_total",
			"Cumulative time spent waiting because the application pool was empty.", nil, nil,
		),
	}
}

func (collector *databasePoolCollector) Describe(destination chan<- *prometheus.Desc) {
	for _, description := range []*prometheus.Desc{
		collector.acquiredConnections,
		collector.idleConnections,
		collector.constructingConnections,
		collector.totalConnections,
		collector.maximumConnections,
		collector.acquires,
		collector.emptyAcquires,
		collector.canceledAcquires,
		collector.acquireDuration,
		collector.emptyAcquireWait,
	} {
		destination <- description
	}
}

func (collector *databasePoolCollector) Collect(destination chan<- prometheus.Metric) {
	statistics := collector.pool.Stat()
	destination <- prometheus.MustNewConstMetric(collector.acquiredConnections, prometheus.GaugeValue, float64(statistics.AcquiredConns()))
	destination <- prometheus.MustNewConstMetric(collector.idleConnections, prometheus.GaugeValue, float64(statistics.IdleConns()))
	destination <- prometheus.MustNewConstMetric(collector.constructingConnections, prometheus.GaugeValue, float64(statistics.ConstructingConns()))
	destination <- prometheus.MustNewConstMetric(collector.totalConnections, prometheus.GaugeValue, float64(statistics.TotalConns()))
	destination <- prometheus.MustNewConstMetric(collector.maximumConnections, prometheus.GaugeValue, float64(statistics.MaxConns()))
	destination <- prometheus.MustNewConstMetric(collector.acquires, prometheus.CounterValue, float64(statistics.AcquireCount()))
	destination <- prometheus.MustNewConstMetric(collector.emptyAcquires, prometheus.CounterValue, float64(statistics.EmptyAcquireCount()))
	destination <- prometheus.MustNewConstMetric(collector.canceledAcquires, prometheus.CounterValue, float64(statistics.CanceledAcquireCount()))
	destination <- prometheus.MustNewConstMetric(collector.acquireDuration, prometheus.CounterValue, statistics.AcquireDuration().Seconds())
	destination <- prometheus.MustNewConstMetric(collector.emptyAcquireWait, prometheus.CounterValue, statistics.EmptyAcquireWaitTime().Seconds())
}

func newDatabaseStateCollector(db *pgxpool.Pool) *databaseStateCollector {
	return &databaseStateCollector{
		db: db,
		collectionSuccess: prometheus.NewDesc(
			"im_backend_database_metrics_collection_success",
			"Whether the latest database-backed metric collection succeeded.", nil, nil,
		),
		outboxPending: prometheus.NewDesc(
			"im_backend_outbox_pending_events",
			"Outbox events that are neither published nor dead.", nil, nil,
		),
		outboxOldestAge: prometheus.NewDesc(
			"im_backend_outbox_oldest_pending_age_seconds",
			"Age in seconds of the oldest pending outbox event.", nil, nil,
		),
		outboxDead: prometheus.NewDesc(
			"im_backend_outbox_dead_events",
			"Outbox events in the dead terminal state.", nil, nil,
		),
		ackDevices: prometheus.NewDesc(
			"im_backend_device_sync_states",
			"Number of devices with a recorded ACK state.", nil, nil,
		),
		ackMaxLag: prometheus.NewDesc(
			"im_backend_device_sync_max_ack_lag",
			"Maximum difference between the user stream cursor and a recorded device ACK.", nil, nil,
		),
	}
}

func (collector *databaseStateCollector) Describe(destination chan<- *prometheus.Desc) {
	destination <- collector.collectionSuccess
	destination <- collector.outboxPending
	destination <- collector.outboxOldestAge
	destination <- collector.outboxDead
	destination <- collector.ackDevices
	destination <- collector.ackMaxLag
}

func (collector *databaseStateCollector) Collect(destination chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var pending, dead float64
	var oldestAge float64
	err := collector.db.QueryRow(
		ctx,
		`SELECT count(*) FILTER (WHERE published_at IS NULL AND dead_at IS NULL)::double precision,
		        COALESCE(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - min(created_at)
		            FILTER (WHERE published_at IS NULL AND dead_at IS NULL)), 0)::double precision,
		        count(*) FILTER (WHERE dead_at IS NOT NULL)::double precision
		 FROM outbox_events`,
	).Scan(&pending, &oldestAge, &dead)
	if err != nil {
		destination <- prometheus.MustNewConstMetric(collector.collectionSuccess, prometheus.GaugeValue, 0)
		return
	}

	var devices, maxLag float64
	err = collector.db.QueryRow(
		ctx,
		`SELECT count(*)::double precision,
		        COALESCE(max(GREATEST(COALESCE(counter.last_seq, 0) - state.applied_seq, 0)), 0)::double precision
		 FROM device_sync_states AS state
		 LEFT JOIN user_sync_counters AS counter ON counter.user_id = state.user_id`,
	).Scan(&devices, &maxLag)
	if err != nil {
		destination <- prometheus.MustNewConstMetric(collector.collectionSuccess, prometheus.GaugeValue, 0)
		return
	}

	destination <- prometheus.MustNewConstMetric(collector.collectionSuccess, prometheus.GaugeValue, 1)
	destination <- prometheus.MustNewConstMetric(collector.outboxPending, prometheus.GaugeValue, pending)
	destination <- prometheus.MustNewConstMetric(collector.outboxOldestAge, prometheus.GaugeValue, oldestAge)
	destination <- prometheus.MustNewConstMetric(collector.outboxDead, prometheus.GaugeValue, dead)
	destination <- prometheus.MustNewConstMetric(collector.ackDevices, prometheus.GaugeValue, devices)
	destination <- prometheus.MustNewConstMetric(collector.ackMaxLag, prometheus.GaugeValue, maxLag)
}
