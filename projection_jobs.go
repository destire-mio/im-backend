package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	messageProjectionVirtualShards = 256
	messageProjectionAdvisoryLock  = 0x494d5052

	outboxStageProjectionDispatch = "projection_dispatch"
	outboxStageProjectionBegin    = "projection_begin"
	outboxStageProjectionClaim    = "projection_claim"
	outboxStageProjectionUsers    = "projection_project_users"
	outboxStageProjectionStore    = "projection_store"
	outboxStageProjectionCommit   = "projection_commit"
	outboxStageProjectionBatch    = "projection_batch"
)

type messageProjectionPool struct {
	db             *pgxpool.Pool
	workers        int
	batchSize      int
	pollInterval   time.Duration
	attemptTimeout time.Duration
	metrics        *applicationMetrics
	projector      *messageSyncProjector
}

type messageProjectionJob struct {
	eventID     string
	userID      int64
	messageID   int64
	createdAt   time.Time
	existingSeq *int64
}

func newMessageProjectionPool(
	db *pgxpool.Pool,
	config outboxWorkerConfig,
	metrics *applicationMetrics,
) (*messageProjectionPool, error) {
	if db == nil {
		return nil, errors.New("message projection pool requires a database")
	}
	if config.PrepareMode != outboxPrepareModeUserSharded {
		return nil, fmt.Errorf("message projection pool requires %q prepare mode", outboxPrepareModeUserSharded)
	}
	if config.PrepareWorkers <= 0 || config.PrepareWorkers > messageProjectionVirtualShards {
		return nil, fmt.Errorf(
			"message projection workers must be between 1 and %d",
			messageProjectionVirtualShards,
		)
	}
	if config.BatchSize <= 0 || config.PollInterval <= 0 || config.AttemptTimeout <= 0 {
		return nil, errors.New("message projection batch size, poll interval and attempt timeout must be positive")
	}
	return &messageProjectionPool{
		db:             db,
		workers:        config.PrepareWorkers,
		batchSize:      config.BatchSize,
		pollInterval:   config.PollInterval,
		attemptTimeout: config.AttemptTimeout,
		metrics:        metrics,
		projector: &messageSyncProjector{
			db:      db,
			metrics: metrics,
			mode:    config.ProjectionMode,
			storage: config.ProjectionStorage,
		},
	}, nil
}

func (pool *messageProjectionPool) Run(ctx context.Context) error {
	ctx = withDatabaseWorkload(ctx, databaseWorkloadOutbox)
	var waitGroup sync.WaitGroup
	waitGroup.Add(pool.workers * 2)
	for dispatcherIndex := 0; dispatcherIndex < pool.workers; dispatcherIndex++ {
		go func(index int) {
			defer waitGroup.Done()
			pool.runDispatcher(ctx, index)
		}(dispatcherIndex)
	}
	for workerIndex := 0; workerIndex < pool.workers; workerIndex++ {
		go func(index int) {
			defer waitGroup.Done()
			pool.runWorker(ctx, index)
		}(workerIndex)
	}
	waitGroup.Wait()
	return nil
}

func (pool *messageProjectionPool) runDispatcher(ctx context.Context, dispatcherIndex int) {
	limit := pool.batchSize
	for {
		started := time.Now()
		attemptContext, cancelAttempt := context.WithTimeout(ctx, pool.attemptTimeout)
		inserted, err := pool.dispatchJobs(attemptContext, limit)
		cancelAttempt()
		pool.metrics.ObserveOutboxStage(outboxStageProjectionDispatch, time.Since(started))
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("message projection dispatcher %d: %v", dispatcherIndex, err)
		}
		if ctx.Err() != nil {
			return
		}
		if err == nil && inserted > 0 {
			continue
		}
		if !waitForProjectionPoll(ctx, pool.pollInterval) {
			return
		}
	}
}

func (pool *messageProjectionPool) runWorker(ctx context.Context, workerIndex int) {
	for {
		attemptContext, cancelAttempt := context.WithTimeout(ctx, pool.attemptTimeout)
		shards, err := pool.pendingShards(attemptContext, workerIndex)
		cancelAttempt()
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("message projection worker %d list shards: %v", workerIndex, err)
		}
		processed := 0
		if err == nil {
			for _, shard := range shards {
				attemptContext, cancelAttempt := context.WithTimeout(ctx, pool.attemptTimeout)
				count, processErr := pool.processShard(attemptContext, shard)
				cancelAttempt()
				processed += count
				if processErr != nil && !errors.Is(processErr, context.Canceled) {
					log.Printf("message projection worker %d shard %d: %v", workerIndex, shard, processErr)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
		if ctx.Err() != nil {
			return
		}
		if processed > 0 {
			continue
		}
		if !waitForProjectionPoll(ctx, pool.pollInterval) {
			return
		}
	}
}

func waitForProjectionPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (pool *messageProjectionPool) dispatchJobs(ctx context.Context, limit int) (int, error) {
	var inserted int
	err := pool.db.QueryRow(
		ctx,
		`WITH candidates AS (
		   SELECT event.event_id,
		          event.message_id,
		          event.created_at,
		          message.sender_id,
		          message.receiver_id
		   FROM outbox_events AS event
		   JOIN messages AS message ON message.id = event.message_id
		   WHERE event.event_type = 'message.created'
		     AND event.ready_at IS NULL
		     AND event.published_at IS NULL
		     AND event.dead_at IS NULL
			     AND (
			       NOT EXISTS (
			         SELECT 1
			         FROM message_projection_jobs AS sender_job
			         WHERE sender_job.event_id = event.event_id
			           AND sender_job.user_id = message.sender_id
			       )
			       OR (
			         message.receiver_id <> message.sender_id
			         AND NOT EXISTS (
			           SELECT 1
			           FROM message_projection_jobs AS receiver_job
			           WHERE receiver_job.event_id = event.event_id
			             AND receiver_job.user_id = message.receiver_id
			         )
			       )
			     )
		   ORDER BY event.created_at, event.event_id
		   FOR UPDATE OF event SKIP LOCKED
		   LIMIT $1
		 ), participants AS (
		   SELECT candidate.event_id,
		          participant.user_id,
		          candidate.message_id,
			          mod(participant.user_id, $2::bigint)::smallint AS shard,
		          candidate.created_at
		   FROM candidates AS candidate
		   CROSS JOIN LATERAL (
		     SELECT DISTINCT user_id
		     FROM unnest(ARRAY[candidate.sender_id, candidate.receiver_id]) AS users(user_id)
		   ) AS participant
		 ), inserted AS (
		   INSERT INTO message_projection_jobs (event_id, user_id, message_id, shard, created_at)
		   SELECT event_id, user_id, message_id, shard, created_at
		   FROM participants
		   ON CONFLICT (event_id, user_id) DO NOTHING
		   RETURNING 1
		 )
		 SELECT count(*) FROM inserted`,
		limit,
		messageProjectionVirtualShards,
	).Scan(&inserted)
	if err != nil {
		return 0, fmt.Errorf("dispatch message projection jobs: %w", err)
	}
	return inserted, nil
}

func (pool *messageProjectionPool) pendingShards(ctx context.Context, workerIndex int) ([]int, error) {
	rows, err := pool.db.Query(
		ctx,
		`SELECT owned.shard
		 FROM generate_series($2::integer, $3::integer, $1::integer) AS owned(shard)
		 WHERE EXISTS (
		   SELECT 1
		   FROM message_projection_jobs AS job
		   WHERE job.shard = owned.shard
		     AND job.projected_at IS NULL
		 )
		 ORDER BY owned.shard`,
		pool.workers,
		workerIndex,
		messageProjectionVirtualShards-1,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending message projection shards: %w", err)
	}
	defer rows.Close()
	shards := make([]int, 0)
	for rows.Next() {
		var shard int
		if err := rows.Scan(&shard); err != nil {
			return nil, fmt.Errorf("scan pending message projection shard: %w", err)
		}
		shards = append(shards, shard)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending message projection shards: %w", err)
	}
	return shards, nil
}

func (pool *messageProjectionPool) processShard(ctx context.Context, shard int) (int, error) {
	batchStarted := time.Now()
	beginStarted := time.Now()
	tx, err := pool.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	pool.metrics.ObserveOutboxStage(outboxStageProjectionBegin, time.Since(beginStarted))

	var ownsShard bool
	if err := tx.QueryRow(
		ctx,
		`SELECT pg_try_advisory_xact_lock($1::integer, $2::integer)`,
		messageProjectionAdvisoryLock,
		shard,
	).Scan(&ownsShard); err != nil {
		return 0, fmt.Errorf("lock message projection shard %d: %w", shard, err)
	}
	if !ownsShard {
		return 0, nil
	}

	claimStarted := time.Now()
	jobs, err := pool.claimJobs(ctx, tx, shard)
	pool.metrics.ObserveOutboxStage(outboxStageProjectionClaim, time.Since(claimStarted))
	if err != nil || len(jobs) == 0 {
		return 0, err
	}

	userStarted := time.Now()
	if err := pool.projectJobs(ctx, tx, jobs); err != nil {
		return 0, err
	}
	pool.metrics.ObserveOutboxStage(outboxStageProjectionUsers, time.Since(userStarted))

	storeStarted := time.Now()
	if err := pool.completeJobsAndReadyEvents(ctx, tx, jobs); err != nil {
		return 0, err
	}
	pool.metrics.ObserveOutboxStage(outboxStageProjectionStore, time.Since(storeStarted))

	commitStarted := time.Now()
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	pool.metrics.ObserveOutboxStage(outboxStageProjectionCommit, time.Since(commitStarted))
	pool.metrics.ObserveOutboxStage(outboxStageProjectionBatch, time.Since(batchStarted))
	return len(jobs), nil
}

func (pool *messageProjectionPool) claimJobs(
	ctx context.Context,
	tx pgx.Tx,
	shard int,
) ([]messageProjectionJob, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT job.event_id::text,
		        job.user_id,
		        job.message_id,
		        job.created_at,
		        sync_event.seq
		 FROM message_projection_jobs AS job
		 LEFT JOIN user_message_events AS sync_event
		   ON sync_event.user_id = job.user_id
		  AND sync_event.message_id = job.message_id
		 WHERE job.shard = $1
		   AND job.projected_at IS NULL
		 ORDER BY job.created_at, job.message_id, job.user_id
		 FOR UPDATE OF job SKIP LOCKED
		 LIMIT $2`,
		shard,
		pool.batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("claim message projection jobs for shard %d: %w", shard, err)
	}
	defer rows.Close()
	jobs := make([]messageProjectionJob, 0, pool.batchSize)
	for rows.Next() {
		var job messageProjectionJob
		if err := rows.Scan(
			&job.eventID,
			&job.userID,
			&job.messageID,
			&job.createdAt,
			&job.existingSeq,
		); err != nil {
			return nil, fmt.Errorf("scan message projection job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate message projection jobs: %w", err)
	}
	return jobs, nil
}

func (pool *messageProjectionPool) projectJobs(
	ctx context.Context,
	tx pgx.Tx,
	jobs []messageProjectionJob,
) error {
	jobsByUser := make(map[int64][]messageProjectionJob)
	for _, job := range jobs {
		if job.existingSeq == nil {
			jobsByUser[job.userID] = append(jobsByUser[job.userID], job)
		}
	}
	if len(jobsByUser) == 0 {
		return nil
	}

	userIDs := make([]int64, 0, len(jobsByUser))
	for userID := range jobsByUser {
		userIDs = append(userIDs, userID)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	pool.metrics.ObserveOutboxProjectionBatch(len(userIDs))
	if err := pool.projector.ensureSyncCounters(ctx, tx, userIDs); err != nil {
		return err
	}
	counters, err := pool.projector.lockSyncCounters(ctx, tx, userIDs)
	if err != nil {
		return err
	}
	if len(counters) != len(userIDs) {
		return fmt.Errorf("lock projection job counters: got %d rows, want %d", len(counters), len(userIDs))
	}

	lastSeqs := make([]int64, 0, len(counters))
	eventUserIDs := make([]int64, 0, len(jobs))
	eventSeqs := make([]int64, 0, len(jobs))
	eventMessageIDs := make([]int64, 0, len(jobs))
	for index, counter := range counters {
		if counter.userID != userIDs[index] {
			return fmt.Errorf("projection job counter %d is user %d, want %d", index, counter.userID, userIDs[index])
		}
		userJobs := jobsByUser[counter.userID]
		sort.Slice(userJobs, func(i, j int) bool {
			if userJobs[i].createdAt.Equal(userJobs[j].createdAt) {
				return userJobs[i].messageID < userJobs[j].messageID
			}
			return userJobs[i].createdAt.Before(userJobs[j].createdAt)
		})
		if counter.lastSeq > int64(^uint64(0)>>1)-int64(len(userJobs)) {
			return fmt.Errorf("project %d messages for user %d: cursor overflow", len(userJobs), counter.userID)
		}
		lastSeq := counter.lastSeq
		for _, job := range userJobs {
			lastSeq++
			eventUserIDs = append(eventUserIDs, counter.userID)
			eventSeqs = append(eventSeqs, lastSeq)
			eventMessageIDs = append(eventMessageIDs, job.messageID)
		}
		lastSeqs = append(lastSeqs, lastSeq)
	}

	inserted, err := pool.projector.storeBulkProjection(
		ctx,
		tx,
		userIDs,
		lastSeqs,
		eventUserIDs,
		eventSeqs,
		eventMessageIDs,
	)
	if err != nil {
		return err
	}
	if inserted != int64(len(eventUserIDs)) {
		return fmt.Errorf("store projection jobs: inserted %d rows, want %d", inserted, len(eventUserIDs))
	}
	return nil
}

func (pool *messageProjectionPool) completeJobsAndReadyEvents(
	ctx context.Context,
	tx pgx.Tx,
	jobs []messageProjectionJob,
) error {
	jobEventIDs := make([]string, 0, len(jobs))
	jobUserIDs := make([]int64, 0, len(jobs))
	eventSet := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		jobEventIDs = append(jobEventIDs, job.eventID)
		jobUserIDs = append(jobUserIDs, job.userID)
		eventSet[job.eventID] = struct{}{}
	}
	command, err := tx.Exec(
		ctx,
		`UPDATE message_projection_jobs AS job
		 SET projected_at = CURRENT_TIMESTAMP
		 FROM unnest($1::text[], $2::bigint[]) AS completed(event_id, user_id)
		 WHERE job.event_id = completed.event_id::uuid
		   AND job.user_id = completed.user_id
		   AND job.projected_at IS NULL`,
		jobEventIDs,
		jobUserIDs,
	)
	if err != nil {
		return fmt.Errorf("complete message projection jobs: %w", err)
	}
	if command.RowsAffected() != int64(len(jobs)) {
		return fmt.Errorf("complete message projection jobs: updated %d rows, want %d", command.RowsAffected(), len(jobs))
	}

	eventIDs := make([]string, 0, len(eventSet))
	for eventID := range eventSet {
		eventIDs = append(eventIDs, eventID)
	}
	sort.Strings(eventIDs)
	// Cast each input value to UUID instead of casting the indexed event_id
	// column to text. The latter scans the entire primary-key index per batch.
	rows, err := tx.Query(
		ctx,
		`SELECT event.event_id::text
		 FROM unnest($1::text[]) AS requested(event_id)
		 JOIN outbox_events AS event
		   ON event.event_id = requested.event_id::uuid
		 ORDER BY event.event_id
		 FOR UPDATE OF event`,
		eventIDs,
	)
	if err != nil {
		return fmt.Errorf("lock projected outbox events: %w", err)
	}
	locked := 0
	for rows.Next() {
		locked++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate projected outbox events: %w", err)
	}
	rows.Close()
	if locked != len(eventIDs) {
		return fmt.Errorf("lock projected outbox events: got %d rows, want %d", locked, len(eventIDs))
	}

	_, err = tx.Exec(
		ctx,
		`UPDATE outbox_events AS event
		 SET ready_at = CURRENT_TIMESTAMP
		 FROM unnest($1::text[]) AS requested(event_id)
		 WHERE event.event_id = requested.event_id::uuid
		   AND event.event_type = 'message.created'
		   AND event.ready_at IS NULL
		   AND event.published_at IS NULL
		   AND event.dead_at IS NULL
		   AND NOT EXISTS (
		     SELECT 1
		     FROM messages AS message
		     CROSS JOIN LATERAL (
		       SELECT DISTINCT user_id
		       FROM unnest(ARRAY[message.sender_id, message.receiver_id]) AS users(user_id)
		     ) AS participant
		     WHERE message.id = event.message_id
		       AND NOT EXISTS (
		         SELECT 1
		         FROM user_message_events AS sync_event
		         WHERE sync_event.user_id = participant.user_id
		           AND sync_event.message_id = event.message_id
		       )
		   )`,
		eventIDs,
	)
	if err != nil {
		return fmt.Errorf("mark fully projected outbox events ready: %w", err)
	}
	_, err = tx.Exec(
		ctx,
		`DELETE FROM message_projection_jobs AS job
		 USING unnest($1::text[]) AS requested(event_id)
		 WHERE job.event_id = requested.event_id::uuid
		   AND EXISTS (
		     SELECT 1
		     FROM outbox_events AS event
		     WHERE event.event_id = job.event_id
		       AND event.ready_at IS NOT NULL
		   )`,
		eventIDs,
	)
	if err != nil {
		return fmt.Errorf("delete completed message projection jobs: %w", err)
	}
	return nil
}
