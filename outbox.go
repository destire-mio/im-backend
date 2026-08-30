package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var errOutboxLeaseLost = errors.New("outbox lease is no longer owned by this worker")

type outboxEvent struct {
	EventID        string
	EventType      string
	PayloadVersion int16
	MessageID      int64
	Payload        json.RawMessage
	AttemptCount   int
	LockToken      string
	ReadyAt        *time.Time
}

type outboxPublisher interface {
	Publish(context.Context, outboxEvent) error
}

type outboxBatchPreparer interface {
	PrepareBatch(context.Context, []outboxEvent) ([]outboxEvent, error)
}

type outboxExecutionMode string

const (
	outboxExecutionModeSerial   outboxExecutionMode = "serial"
	outboxExecutionModePipeline outboxExecutionMode = "pipeline"
)

type outboxWorkerConfig struct {
	EventTypes        []string
	BatchSize         int
	Concurrency       int
	ExecutionMode     outboxExecutionMode
	ProjectionMode    syncProjectionMode
	ProjectionStorage syncProjectionStorage
	LeaseDuration     time.Duration
	AttemptTimeout    time.Duration
	PollInterval      time.Duration
	MaxAttempts       int
	BaseBackoff       time.Duration
	MaxBackoff        time.Duration
	jitter            func(time.Duration) time.Duration
}

type outboxWorker struct {
	db        *pgxpool.Pool
	publisher outboxPublisher
	config    outboxWorkerConfig
	metrics   *applicationMetrics
	preparer  outboxBatchPreparer
}

type outboxPublishOutcome struct {
	event outboxEvent
	err   error
}

const (
	outboxStageClaim         = "claim"
	outboxStagePrepare       = "prepare"
	outboxStagePrepareDecode = "prepare_decode"
	outboxStagePrepareBegin  = "prepare_begin"
	outboxStagePrepareUsers  = "prepare_project_users"
	outboxStagePrepareEncode = "prepare_encode"
	outboxStagePrepareStore  = "prepare_store"
	outboxStagePrepareCommit = "prepare_commit"
	outboxStagePublish       = "publish"
	outboxStageMarkPublished = "mark_published"
)

type publishFailure struct {
	err        error
	permanent  bool
	retryAfter time.Duration
}

func (failure *publishFailure) Error() string { return failure.err.Error() }
func (failure *publishFailure) Unwrap() error { return failure.err }

func permanentPublishError(err error) error {
	return &publishFailure{err: err, permanent: true}
}

func retryablePublishError(err error, retryAfter time.Duration) error {
	return &publishFailure{err: err, retryAfter: retryAfter}
}

func normalizeOutboxExecutionMode(mode outboxExecutionMode) (outboxExecutionMode, error) {
	switch mode {
	case "", outboxExecutionModePipeline:
		return outboxExecutionModePipeline, nil
	case outboxExecutionModeSerial:
		return outboxExecutionModeSerial, nil
	default:
		return "", fmt.Errorf("unsupported outbox execution mode %q", mode)
	}
}

func defaultOutboxWorkerConfig() outboxWorkerConfig {
	return outboxWorkerConfig{
		EventTypes:        []string{"message.created"},
		BatchSize:         64,
		Concurrency:       16,
		ExecutionMode:     outboxExecutionModePipeline,
		ProjectionMode:    syncProjectionModeBulk,
		ProjectionStorage: syncProjectionStorageRecipients,
		LeaseDuration:     30 * time.Second,
		AttemptTimeout:    10 * time.Second,
		PollInterval:      500 * time.Millisecond,
		MaxAttempts:       10,
		BaseBackoff:       time.Second,
		MaxBackoff:        5 * time.Minute,
		jitter:            equalJitter,
	}
}

func newOutboxWorker(
	db *pgxpool.Pool,
	publisher outboxPublisher,
	config outboxWorkerConfig,
	metricObservers ...*applicationMetrics,
) (*outboxWorker, error) {
	if db == nil || publisher == nil {
		return nil, errors.New("outbox worker requires a database and publisher")
	}
	if len(config.EventTypes) == 0 {
		return nil, errors.New("outbox worker requires at least one event type")
	}
	if config.BatchSize <= 0 || config.Concurrency <= 0 || config.MaxAttempts <= 0 {
		return nil, errors.New("outbox batch size, concurrency and max attempts must be positive")
	}
	if config.LeaseDuration <= 0 || config.AttemptTimeout <= 0 || config.PollInterval <= 0 || config.BaseBackoff <= 0 || config.MaxBackoff <= 0 {
		return nil, errors.New("outbox durations must be positive")
	}
	if config.AttemptTimeout >= config.LeaseDuration {
		return nil, errors.New("outbox attempt timeout must be shorter than its lease")
	}
	if config.BaseBackoff > config.MaxBackoff {
		return nil, errors.New("outbox base backoff cannot exceed max backoff")
	}
	executionMode, err := normalizeOutboxExecutionMode(config.ExecutionMode)
	if err != nil {
		return nil, err
	}
	config.ExecutionMode = executionMode
	if config.jitter == nil {
		config.jitter = equalJitter
	}
	var metrics *applicationMetrics
	if len(metricObservers) > 0 {
		metrics = metricObservers[0]
	}
	return &outboxWorker{db: db, publisher: publisher, config: config, metrics: metrics}, nil
}

func newMessageOutboxWorker(
	db *pgxpool.Pool,
	publisher outboxPublisher,
	config outboxWorkerConfig,
	metricObservers ...*applicationMetrics,
) (*outboxWorker, error) {
	projectionMode, err := normalizeSyncProjectionMode(config.ProjectionMode)
	if err != nil {
		return nil, err
	}
	config.ProjectionMode = projectionMode
	projectionStorage, err := normalizeSyncProjectionStorage(config.ProjectionStorage)
	if err != nil {
		return nil, err
	}
	config.ProjectionStorage = projectionStorage
	worker, err := newOutboxWorker(db, publisher, config, metricObservers...)
	if err != nil {
		return nil, err
	}
	worker.preparer = &messageSyncProjector{
		db:      db,
		metrics: worker.metrics,
		mode:    projectionMode,
		storage: projectionStorage,
	}
	return worker, nil
}

func (worker *outboxWorker) Run(ctx context.Context) error {
	switch worker.config.ExecutionMode {
	case outboxExecutionModeSerial:
		return worker.runSerial(ctx)
	case outboxExecutionModePipeline:
		return worker.runPipeline(ctx)
	default:
		return fmt.Errorf("unsupported outbox execution mode %q", worker.config.ExecutionMode)
	}
}

func (worker *outboxWorker) runSerial(ctx context.Context) error {
	for {
		processed, err := worker.RunOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("outbox worker batch: %v", err)
		}
		if !worker.continueAfterBatch(ctx, processed) {
			return nil
		}
	}
}

// runPipeline overlaps database preparation for batch N+1 with realtime
// delivery and durable completion for batch N. The unbuffered handoff keeps at
// most one batch in each stage, so a slow publisher cannot accumulate leases.
func (worker *outboxWorker) runPipeline(ctx context.Context) error {
	preparedBatches := make(chan []outboxEvent)
	go func() {
		defer close(preparedBatches)
		worker.prepareBatches(ctx, preparedBatches)
	}()

	for events := range preparedBatches {
		if err := worker.deliverBatch(ctx, events); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("outbox worker delivery batch: %v", err)
		}
	}
	return nil
}

func (worker *outboxWorker) prepareBatches(ctx context.Context, preparedBatches chan<- []outboxEvent) {
	for {
		processed, events, err := worker.claimAndPrepare(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("outbox worker preparation batch: %v", err)
		}
		if len(events) > 0 {
			// Always hand an already-claimed batch to the delivery stage. On
			// shutdown, delivery observes the canceled context and releases each
			// lease through the normal retry path instead of abandoning it.
			preparedBatches <- events
		}
		if !worker.continueAfterBatch(ctx, processed) {
			return
		}
	}
}

func (worker *outboxWorker) continueAfterBatch(ctx context.Context, processed int) bool {
	if ctx.Err() != nil {
		return false
	}
	if processed == worker.config.BatchSize {
		return true
	}
	timer := time.NewTimer(worker.config.PollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RunOnce deliberately remains a complete serial batch operation. Production
// Run can pipeline consecutive batches, while tests and maintenance callers can
// still execute one deterministic claim -> prepare -> publish -> mark cycle.
func (worker *outboxWorker) RunOnce(ctx context.Context) (int, error) {
	processed, events, err := worker.claimAndPrepare(ctx)
	if err != nil || len(events) == 0 {
		return processed, err
	}
	return processed, worker.deliverBatch(ctx, events)
}

func (worker *outboxWorker) claimAndPrepare(ctx context.Context) (int, []outboxEvent, error) {
	claimStarted := time.Now()
	events, err := worker.claim(ctx)
	if len(events) > 0 {
		worker.metrics.ObserveOutboxStage(outboxStageClaim, time.Since(claimStarted))
	}
	if err != nil || len(events) == 0 {
		return len(events), nil, err
	}
	if worker.preparer != nil {
		prepareStarted := time.Now()
		prepareContext, cancelPrepare := context.WithTimeout(ctx, worker.config.AttemptTimeout)
		events, err = worker.preparer.PrepareBatch(prepareContext, events)
		cancelPrepare()
		worker.metrics.ObserveOutboxStage(outboxStagePrepare, time.Since(prepareStarted))
		if err != nil {
			failure := retryablePublishError(fmt.Errorf("prepare outbox batch: %w", err), 0)
			var stateErrors []error
			for _, event := range events {
				stateContext, cancelState := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				stateErr := worker.markFailed(stateContext, event, failure)
				cancelState()
				if stateErr != nil {
					stateErrors = append(stateErrors, fmt.Errorf("event %s: %w", event.EventID, stateErr))
				}
			}
			return len(events), nil, errors.Join(append([]error{err}, stateErrors...)...)
		}
	}
	return len(events), events, nil
}

func (worker *outboxWorker) deliverBatch(ctx context.Context, events []outboxEvent) error {
	publishStageStarted := time.Now()
	semaphore := make(chan struct{}, worker.config.Concurrency)
	outcomes := make(chan outboxPublishOutcome, len(events))
	var wait sync.WaitGroup
	for _, event := range events {
		semaphore <- struct{}{}
		wait.Add(1)
		go func(current outboxEvent) {
			defer wait.Done()
			defer func() { <-semaphore }()
			started := time.Now()
			attemptContext, cancelAttempt := context.WithTimeout(ctx, worker.config.AttemptTimeout)
			publishErr := worker.publisher.Publish(attemptContext, current)
			cancelAttempt()
			if worker.metrics != nil {
				worker.metrics.outboxPublishDuration.WithLabelValues(current.EventType).Observe(time.Since(started).Seconds())
			}
			outcomes <- outboxPublishOutcome{event: current, err: publishErr}
		}(event)
	}
	wait.Wait()
	worker.metrics.ObserveOutboxStage(outboxStagePublish, time.Since(publishStageStarted))
	close(outcomes)

	allOutcomes := make([]outboxPublishOutcome, 0, len(events))
	successful := make([]outboxEvent, 0, len(events))
	for outcome := range outcomes {
		allOutcomes = append(allOutcomes, outcome)
		if outcome.err == nil {
			successful = append(successful, outcome.event)
		}
	}

	var processingErrors []error
	var publishedStateErr error
	if len(successful) > 0 {
		markPublishedStarted := time.Now()
		stateContext, cancelState := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		publishedStateErr = worker.markPublishedBatch(stateContext, successful)
		cancelState()
		worker.metrics.ObserveOutboxStage(outboxStageMarkPublished, time.Since(markPublishedStarted))
		if publishedStateErr != nil {
			processingErrors = append(processingErrors, fmt.Errorf("mark %d published events: %w", len(successful), publishedStateErr))
		}
	}

	for _, outcome := range allOutcomes {
		result := "published"
		stateErr := publishedStateErr
		if outcome.err != nil {
			var failure *publishFailure
			if errors.As(outcome.err, &failure) && failure.permanent || outcome.event.AttemptCount >= worker.config.MaxAttempts {
				result = "dead"
			} else {
				result = "retry_scheduled"
			}
			stateContext, cancelState := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			stateErr = worker.markFailed(stateContext, outcome.event, outcome.err)
			cancelState()
			if stateErr != nil {
				processingErrors = append(processingErrors, fmt.Errorf("event %s: %w", outcome.event.EventID, stateErr))
			}
			log.Printf(
				"outbox_publish_failed event_id=%s message_id=%d event_type=%s attempt=%d result=%s err=%q",
				outcome.event.EventID,
				outcome.event.MessageID,
				outcome.event.EventType,
				outcome.event.AttemptCount,
				result,
				outcome.err,
			)
		}
		if stateErr != nil {
			if errors.Is(stateErr, errOutboxLeaseLost) {
				result = "lease_lost"
			} else {
				result = "state_error"
			}
		}
		if worker.metrics != nil {
			worker.metrics.outboxPublish.WithLabelValues(outcome.event.EventType, result).Inc()
		}
	}
	return errors.Join(processingErrors...)
}

func (worker *outboxWorker) claim(ctx context.Context) ([]outboxEvent, error) {
	rows, err := worker.db.Query(
		ctx,
		`WITH candidates AS (
		   SELECT event_id
		   FROM outbox_events
		   WHERE published_at IS NULL
		     AND dead_at IS NULL
		     AND event_type = ANY($3)
		     AND next_attempt_at <= CURRENT_TIMESTAMP
		     AND (locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)
		   ORDER BY next_attempt_at, created_at, event_id
		   FOR UPDATE SKIP LOCKED
		   LIMIT $1
		 )
		 UPDATE outbox_events AS event
		 SET attempt_count = event.attempt_count + 1,
		     locked_until = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 millisecond'),
		     lock_token = gen_random_uuid()
		 FROM candidates
		 WHERE event.event_id = candidates.event_id
		 RETURNING event.event_id::text,
		           event.event_type,
		           event.payload_version,
		           event.message_id,
		           event.payload,
		           event.attempt_count,
		           event.lock_token::text,
		           event.ready_at`,
		worker.config.BatchSize,
		worker.config.LeaseDuration.Milliseconds(),
		worker.config.EventTypes,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]outboxEvent, 0, worker.config.BatchSize)
	for rows.Next() {
		var event outboxEvent
		if err := rows.Scan(
			&event.EventID,
			&event.EventType,
			&event.PayloadVersion,
			&event.MessageID,
			&event.Payload,
			&event.AttemptCount,
			&event.LockToken,
			&event.ReadyAt,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (worker *outboxWorker) markPublished(ctx context.Context, event outboxEvent) error {
	command, err := worker.db.Exec(
		ctx,
		`UPDATE outbox_events
		 SET published_at = CURRENT_TIMESTAMP,
		     locked_until = NULL,
		     lock_token = NULL,
		     last_error = NULL
		 WHERE event_id = $1
		   AND lock_token = $2
		   AND published_at IS NULL
		   AND dead_at IS NULL`,
		event.EventID,
		event.LockToken,
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return errOutboxLeaseLost
	}
	return nil
}

func (worker *outboxWorker) markPublishedBatch(ctx context.Context, events []outboxEvent) error {
	if len(events) == 0 {
		return nil
	}
	eventIDs := make([]string, len(events))
	lockTokens := make([]string, len(events))
	for index, event := range events {
		eventIDs[index] = event.EventID
		lockTokens[index] = event.LockToken
	}
	command, err := worker.db.Exec(
		ctx,
		`UPDATE outbox_events AS event
		 SET published_at = CURRENT_TIMESTAMP,
		     locked_until = NULL,
		     lock_token = NULL,
		     last_error = NULL
		 FROM unnest($1::text[], $2::text[]) AS claimed(event_id, lock_token)
		 WHERE event.event_id = claimed.event_id::uuid
		   AND event.lock_token = claimed.lock_token::uuid
		   AND event.published_at IS NULL
		   AND event.dead_at IS NULL`,
		eventIDs,
		lockTokens,
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != int64(len(events)) {
		return errOutboxLeaseLost
	}
	return nil
}

func (worker *outboxWorker) markFailed(ctx context.Context, event outboxEvent, publishErr error) error {
	lastError := truncateRunes(publishErr.Error(), 2000)
	var failure *publishFailure
	permanent := errors.As(publishErr, &failure) && failure.permanent
	if permanent || event.AttemptCount >= worker.config.MaxAttempts {
		command, err := worker.db.Exec(
			ctx,
			`UPDATE outbox_events
			 SET dead_at = CURRENT_TIMESTAMP,
			     locked_until = NULL,
			     lock_token = NULL,
			     last_error = $3
			 WHERE event_id = $1
			   AND lock_token = $2
			   AND published_at IS NULL
			   AND dead_at IS NULL`,
			event.EventID,
			event.LockToken,
			lastError,
		)
		if err != nil {
			return err
		}
		if command.RowsAffected() != 1 {
			return errOutboxLeaseLost
		}
		return nil
	}

	delay := worker.retryDelay(event.AttemptCount)
	if failure != nil && failure.retryAfter > delay {
		delay = failure.retryAfter
	}
	command, err := worker.db.Exec(
		ctx,
		`UPDATE outbox_events
		 SET next_attempt_at = CURRENT_TIMESTAMP + ($3 * INTERVAL '1 millisecond'),
		     locked_until = NULL,
		     lock_token = NULL,
		     last_error = $4
		 WHERE event_id = $1
		   AND lock_token = $2
		   AND published_at IS NULL
		   AND dead_at IS NULL`,
		event.EventID,
		event.LockToken,
		delay.Milliseconds(),
		lastError,
	)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return errOutboxLeaseLost
	}
	return nil
}

func (worker *outboxWorker) retryDelay(attempt int) time.Duration {
	delay := worker.config.BaseBackoff
	for current := 1; current < attempt && delay < worker.config.MaxBackoff; current++ {
		if delay > worker.config.MaxBackoff/2 {
			delay = worker.config.MaxBackoff
			break
		}
		delay *= 2
	}
	if delay > worker.config.MaxBackoff {
		delay = worker.config.MaxBackoff
	}
	return worker.config.jitter(delay)
}

func (worker *outboxWorker) cleanupPublished(ctx context.Context, olderThan time.Time, batchSize int) (int64, error) {
	if batchSize <= 0 {
		return 0, errors.New("cleanup batch size must be positive")
	}
	command, err := worker.db.Exec(
		ctx,
		`WITH doomed AS (
		   SELECT event_id
		   FROM outbox_events
		   WHERE published_at IS NOT NULL
		     AND published_at < $1
		   ORDER BY published_at, event_id
		   LIMIT $2
		 )
		 DELETE FROM outbox_events AS event
		 USING doomed
		 WHERE event.event_id = doomed.event_id`,
		olderThan,
		batchSize,
	)
	if err != nil {
		return 0, err
	}
	return command.RowsAffected(), nil
}

func equalJitter(delay time.Duration) time.Duration {
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
