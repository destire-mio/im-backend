package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUserShardedCompatibilityModeProjectsCurrentMessagesForRollback(t *testing.T) {
	db := openTestDatabase(t)
	server := httptest.NewServer(newTestApplication(t, db).routes())
	t.Cleanup(server.Close)
	sender := registerTestAccount(t, db, server.URL, uniqueUsername("cmp_s"), "Compat Sender")
	receiver := registerTestAccount(t, db, server.URL, uniqueUsername("cmp_r"), "Compat Receiver")
	created := createMessageThroughAPI(t, server.URL, sender.Auth.AccessToken, receiver.User.ID, "keep rollback projection")

	config := defaultOutboxWorkerConfig()
	config.BatchSize = 1
	config.PrepareMode = outboxPrepareModeUserSharded
	config.PrepareWorkers = 4
	pool, err := newMessageProjectionPool(db, config, nil)
	if err != nil {
		t.Fatalf("create projection pool: %v", err)
	}
	if dispatched, err := pool.dispatchJobs(context.Background(), 10); err != nil || dispatched != 2 {
		t.Fatalf("compatibility projection dispatch = %d, err %v, want 2", dispatched, err)
	}
	senderShard := int(sender.User.ID % messageProjectionVirtualShards)
	receiverShard := int(receiver.User.ID % messageProjectionVirtualShards)
	if senderShard == receiverShard {
		t.Fatalf("test users unexpectedly share logical shard %d", senderShard)
	}
	if projected, err := pool.processShard(context.Background(), senderShard); err != nil || projected != 1 {
		t.Fatalf("project sender compatibility job = %d, err %v", projected, err)
	}
	if projected, err := pool.processShard(context.Background(), receiverShard); err != nil || projected != 1 {
		t.Fatalf("project receiver compatibility job = %d, err %v", projected, err)
	}

	publisher := &testPublisher{}
	worker := mustMessageTestWorker(t, db, publisher, config)
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("compatibility user-sharded publish = %d, err %v", processed, err)
	}
	if received := publisher.received(); len(received) != 1 || received[0].PayloadVersion != 2 {
		t.Fatalf("published compatibility events = %+v", received)
	}

	var jobs, syncEvents int
	if err := db.QueryRow(
		context.Background(),
		`SELECT (SELECT count(*) FROM message_projection_jobs WHERE message_id = $1),
		        (SELECT count(*) FROM user_message_events WHERE message_id = $1)`,
		created.ID,
	).Scan(&jobs, &syncEvents); err != nil {
		t.Fatalf("read compatibility projection state: %v", err)
	}
	if jobs != 0 || syncEvents != 2 {
		t.Fatalf("compatibility projection state jobs=%d sync=%d, want 0/2", jobs, syncEvents)
	}
}

func TestUserShardedProjectionRequiresAllUsersBeforeDelivery(t *testing.T) {
	db := openTestDatabase(t)
	users := createProjectionTestUsers(t, db, 2)
	created, eventID := createProjectionTestMessage(t, db, users[0], users[1])
	pool := mustProjectionTestPool(t, db, 4, 8)

	publisher := &testPublisher{}
	config := defaultOutboxWorkerConfig()
	config.PrepareMode = outboxPrepareModeUserSharded
	config.PrepareWorkers = 4
	config.BatchSize = 8
	worker := mustMessageTestWorker(t, db, publisher, config)

	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 0 {
		t.Fatalf("deliver unready event = processed %d, err %v", processed, err)
	}
	var attempts int
	if err := db.QueryRow(context.Background(), `SELECT attempt_count FROM outbox_events WHERE event_id = $1`, eventID).Scan(&attempts); err != nil {
		t.Fatalf("read unready event attempts: %v", err)
	}
	if attempts != 0 {
		t.Fatalf("unready event attempts = %d, want 0", attempts)
	}

	inserted, err := pool.dispatchJobs(context.Background(), 8)
	if err != nil || inserted != 2 {
		t.Fatalf("dispatch projection jobs = inserted %d, err %v", inserted, err)
	}
	assertProjectionJobOwner(t, pool, users[0])
	assertProjectionJobOwner(t, pool, users[1])

	firstShard := int(users[0] % messageProjectionVirtualShards)
	secondShard := int(users[1] % messageProjectionVirtualShards)
	if firstShard == secondShard {
		t.Fatalf("test users unexpectedly share logical shard %d", firstShard)
	}
	projected, err := pool.processShard(context.Background(), firstShard)
	if err != nil || projected != 1 {
		t.Fatalf("project first user = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, false)
	assertProjectionCounts(t, db, created.ID, 1, 2)

	projected, err = pool.processShard(context.Background(), secondShard)
	if err != nil || projected != 1 {
		t.Fatalf("project second user = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, true)
	assertProjectionCounts(t, db, created.ID, 2, 0)

	processed, err = worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("deliver ready event = processed %d, err %v", processed, err)
	}
	received := publisher.received()
	if len(received) != 1 || received[0].EventID != eventID || received[0].PayloadVersion != 2 {
		t.Fatalf("published events = %+v", received)
	}
	payload, err := decodeMessageCreatedEvent(received[0])
	if err != nil {
		t.Fatalf("decode delivered projection: %v", err)
	}
	if len(payload.Recipients) != 2 {
		t.Fatalf("delivered recipients = %d, want 2", len(payload.Recipients))
	}
}

func TestUserShardedProjectionReconcilesAnExistingUserSyncRow(t *testing.T) {
	db := openTestDatabase(t)
	users := createProjectionTestUsers(t, db, 2)
	created, eventID := createProjectionTestMessage(t, db, users[0], users[1])
	pool := mustProjectionTestPool(t, db, 4, 8)
	if inserted, err := pool.dispatchJobs(context.Background(), 8); err != nil || inserted != 2 {
		t.Fatalf("dispatch projection jobs = inserted %d, err %v", inserted, err)
	}

	if _, err := db.Exec(
		context.Background(),
		`WITH counter AS (
		   INSERT INTO user_sync_counters (user_id, last_seq)
		   VALUES ($1, 1)
		 )
		 INSERT INTO user_message_events (user_id, seq, message_id)
		 VALUES ($1, 1, $2)`,
		users[0],
		created.ID,
	); err != nil {
		t.Fatalf("seed committed half projection: %v", err)
	}

	firstShard := int(users[0] % messageProjectionVirtualShards)
	if projected, err := pool.processShard(context.Background(), firstShard); err != nil || projected != 1 {
		t.Fatalf("reconcile existing projection = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, false)

	secondShard := int(users[1] % messageProjectionVirtualShards)
	if projected, err := pool.processShard(context.Background(), secondShard); err != nil || projected != 1 {
		t.Fatalf("project missing participant = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, true)
	assertProjectionCounts(t, db, created.ID, 2, 0)

	for _, userID := range users {
		var lastSeq int64
		if err := db.QueryRow(context.Background(), `SELECT last_seq FROM user_sync_counters WHERE user_id = $1`, userID).Scan(&lastSeq); err != nil {
			t.Fatalf("read user %d counter: %v", userID, err)
		}
		if lastSeq != 1 {
			t.Fatalf("user %d last seq = %d, want 1", userID, lastSeq)
		}
	}
}

func TestUserShardedProjectionRepairsAMissingParticipantJob(t *testing.T) {
	db := openTestDatabase(t)
	users := createProjectionTestUsers(t, db, 2)
	created, eventID := createProjectionTestMessage(t, db, users[0], users[1])
	pool := mustProjectionTestPool(t, db, 4, 8)
	if inserted, err := pool.dispatchJobs(context.Background(), 8); err != nil || inserted != 2 {
		t.Fatalf("dispatch projection jobs = inserted %d, err %v", inserted, err)
	}
	if _, err := db.Exec(
		context.Background(),
		`DELETE FROM message_projection_jobs WHERE event_id = $1 AND user_id = $2`,
		eventID,
		users[1],
	); err != nil {
		t.Fatalf("remove one participant job: %v", err)
	}

	firstShard := int(users[0] % messageProjectionVirtualShards)
	if projected, err := pool.processShard(context.Background(), firstShard); err != nil || projected != 1 {
		t.Fatalf("project only existing participant = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, false)
	assertProjectionCounts(t, db, created.ID, 1, 1)

	if inserted, err := pool.dispatchJobs(context.Background(), 8); err != nil || inserted != 1 {
		t.Fatalf("repair missing participant job = inserted %d, err %v", inserted, err)
	}
	secondShard := int(users[1] % messageProjectionVirtualShards)
	if projected, err := pool.processShard(context.Background(), secondShard); err != nil || projected != 1 {
		t.Fatalf("project repaired participant = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, true)
	assertProjectionCounts(t, db, created.ID, 2, 0)
}

func TestProjectionDispatcherSkipsParticipantJobBeingCompleted(t *testing.T) {
	db := openTestDatabase(t)
	users := createProjectionTestUsers(t, db, 2)
	_, eventID := createProjectionTestMessage(t, db, users[0], users[1])
	pool := mustProjectionTestPool(t, db, 4, 8)
	if inserted, err := pool.dispatchJobs(context.Background(), 8); err != nil || inserted != 2 {
		t.Fatalf("dispatch initial projection jobs = inserted %d, err %v", inserted, err)
	}
	if _, err := db.Exec(
		context.Background(),
		`DELETE FROM message_projection_jobs WHERE event_id = $1 AND user_id = $2`,
		eventID,
		users[1],
	); err != nil {
		t.Fatalf("remove receiver projection job: %v", err)
	}

	completing, err := db.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin completing projection job: %v", err)
	}
	defer completing.Rollback(context.Background())
	if _, err := completing.Exec(
		context.Background(),
		`UPDATE message_projection_jobs
		 SET projected_at = CURRENT_TIMESTAMP
		 WHERE event_id = $1 AND user_id = $2`,
		eventID,
		users[0],
	); err != nil {
		t.Fatalf("hold completing projection job: %v", err)
	}

	dispatchContext, cancelDispatch := context.WithTimeout(context.Background(), time.Second)
	defer cancelDispatch()
	inserted, err := pool.dispatchJobs(dispatchContext, 8)
	if err != nil || inserted != 1 {
		t.Fatalf("dispatch around completing participant = inserted %d, err %v", inserted, err)
	}
	if err := completing.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback completing projection job: %v", err)
	}

	var jobs int
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*) FROM message_projection_jobs WHERE event_id = $1`,
		eventID,
	).Scan(&jobs); err != nil {
		t.Fatalf("count repaired projection jobs: %v", err)
	}
	if jobs != 2 {
		t.Fatalf("projection jobs = %d, want 2", jobs)
	}
}

func TestUserShardedProjectionCreatesOneJobForSelfMessage(t *testing.T) {
	db := openTestDatabase(t)
	users := createProjectionTestUsers(t, db, 1)
	created, eventID := createProjectionTestMessage(t, db, users[0], users[0])
	pool := mustProjectionTestPool(t, db, 4, 8)

	inserted, err := pool.dispatchJobs(context.Background(), 8)
	if err != nil || inserted != 1 {
		t.Fatalf("dispatch self-message projection = inserted %d, err %v", inserted, err)
	}
	shard := int(users[0] % messageProjectionVirtualShards)
	projected, err := pool.processShard(context.Background(), shard)
	if err != nil || projected != 1 {
		t.Fatalf("project self-message = projected %d, err %v", projected, err)
	}
	assertProjectionReady(t, db, eventID, true)
	assertProjectionCounts(t, db, created.ID, 1, 0)
}

func TestFourProjectionWorkersKeepEveryUserCursorContiguous(t *testing.T) {
	db := openTestDatabase(t)
	users := createProjectionTestUsers(t, db, 10)
	eventIDs := make([]string, 0, 100)
	messageIDs := make([]int64, 0, 100)
	for index := 0; index < 100; index++ {
		created, eventID := createProjectionTestMessage(
			t,
			db,
			users[index%len(users)],
			users[(index+1)%len(users)],
		)
		messageIDs = append(messageIDs, created.ID)
		eventIDs = append(eventIDs, eventID)
	}

	pool := mustProjectionTestPool(t, db, 4, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var ready int
		if err := db.QueryRow(
			context.Background(),
			`SELECT count(*) FROM outbox_events
			 WHERE event_id::text = ANY($1::text[])
			   AND ready_at IS NOT NULL`,
			eventIDs,
		).Scan(&ready); err != nil {
			cancel()
			t.Fatalf("count ready projection events: %v", err)
		}
		if ready == len(eventIDs) {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("ready events = %d, want %d before timeout", ready, len(eventIDs))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("projection pool stopped with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("projection pool did not stop after cancellation")
	}

	for _, userID := range users {
		var count, minimum, maximum, distinct int64
		if err := db.QueryRow(
			context.Background(),
			`SELECT count(*), min(seq), max(seq), count(DISTINCT seq)
			 FROM user_message_events
			 WHERE user_id = $1
			   AND message_id = ANY($2::bigint[])`,
			userID,
			messageIDs,
		).Scan(&count, &minimum, &maximum, &distinct); err != nil {
			t.Fatalf("read user %d cursor range: %v", userID, err)
		}
		if count != 20 || minimum != 1 || maximum != 20 || distinct != 20 {
			t.Fatalf(
				"user %d cursors = count %d min %d max %d distinct %d, want 20/1/20/20",
				userID,
				count,
				minimum,
				maximum,
				distinct,
			)
		}
	}

	var syncRows, jobRows int
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*) FROM user_message_events WHERE message_id = ANY($1::bigint[])`,
		messageIDs,
	).Scan(&syncRows); err != nil {
		t.Fatalf("count projected sync rows: %v", err)
	}
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*) FROM message_projection_jobs WHERE event_id::text = ANY($1::text[])`,
		eventIDs,
	).Scan(&jobRows); err != nil {
		t.Fatalf("count retained projection jobs: %v", err)
	}
	if syncRows != 200 || jobRows != 0 {
		t.Fatalf("sync rows = %d, retained jobs = %d, want 200/0", syncRows, jobRows)
	}
}

func mustProjectionTestPool(
	t *testing.T,
	db *pgxpool.Pool,
	workers int,
	batchSize int,
) *messageProjectionPool {
	t.Helper()
	config := defaultOutboxWorkerConfig()
	config.PrepareMode = outboxPrepareModeUserSharded
	config.PrepareWorkers = workers
	config.BatchSize = batchSize
	config.PollInterval = 10 * time.Millisecond
	config.AttemptTimeout = time.Second
	pool, err := newMessageProjectionPool(db, config, nil)
	if err != nil {
		t.Fatalf("create message projection pool: %v", err)
	}
	return pool
}

func createProjectionTestUsers(t *testing.T, db *pgxpool.Pool, count int) []int64 {
	t.Helper()
	userIDs := make([]int64, 0, count)
	for index := 0; index < count; index++ {
		var userID int64
		if err := db.QueryRow(
			context.Background(),
			`INSERT INTO users (username, display_name, password_hash)
			 VALUES ($1, $2, 'projection-test')
			 RETURNING id`,
			uniqueUsername(fmt.Sprintf("pj%d", index)),
			fmt.Sprintf("Projection User %d", index),
		).Scan(&userID); err != nil {
			t.Fatalf("create projection test user %d: %v", index, err)
		}
		userIDs = append(userIDs, userID)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(
			context.Background(),
			`DELETE FROM messages WHERE sender_id = ANY($1::bigint[]) OR receiver_id = ANY($1::bigint[])`,
			userIDs,
		); err != nil {
			t.Errorf("clean projection test messages: %v", err)
		}
		if _, err := db.Exec(context.Background(), `DELETE FROM users WHERE id = ANY($1::bigint[])`, userIDs); err != nil {
			t.Errorf("clean projection test users: %v", err)
		}
	})
	return userIDs
}

func createProjectionTestMessage(
	t *testing.T,
	db *pgxpool.Pool,
	senderID int64,
	receiverID int64,
) (message, string) {
	t.Helper()
	tx, err := db.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin projection test message: %v", err)
	}
	defer tx.Rollback(context.Background())
	lowUserID, highUserID := senderID, receiverID
	if lowUserID > highUserID {
		lowUserID, highUserID = highUserID, lowUserID
	}
	var conversationID int64
	if err := tx.QueryRow(
		context.Background(),
		`INSERT INTO conversations (kind, direct_user_low_id, direct_user_high_id)
		 VALUES ('direct', $1, $2)
		 ON CONFLICT (direct_user_low_id, direct_user_high_id) DO UPDATE
		 SET kind = EXCLUDED.kind
		 RETURNING id`,
		lowUserID,
		highUserID,
	).Scan(&conversationID); err != nil {
		t.Fatalf("create projection test conversation: %v", err)
	}
	if _, err := tx.Exec(
		context.Background(),
		`INSERT INTO conversation_members (conversation_id, user_id)
		 SELECT $1, user_id
		 FROM (
		     SELECT DISTINCT user_id
		     FROM unnest($2::bigint[]) AS users(user_id)
		 ) AS participant
		 ON CONFLICT DO NOTHING`,
		conversationID,
		[]int64{senderID, receiverID},
	); err != nil {
		t.Fatalf("create projection test members: %v", err)
	}
	var created message
	if err := tx.QueryRow(
		context.Background(),
		`WITH allocated AS (
		   UPDATE conversations
		   SET last_seq = last_seq + 1,
		       updated_at = GREATEST(updated_at, clock_timestamp())
		   WHERE id = $1
		   RETURNING last_seq, updated_at
		 )
		 INSERT INTO messages (
		     conversation_id,
		     conversation_seq,
		     sender_id,
		     receiver_id,
		     client_message_id,
		     content,
		     created_at
		 )
		 SELECT $1, allocated.last_seq, $2, $3, $4, 'projection test', allocated.updated_at
		 FROM allocated
		 RETURNING id,
		           conversation_id,
		           conversation_seq,
		           client_message_id,
		           sender_id,
		           receiver_id,
		           content,
		           created_at`,
		conversationID,
		senderID,
		receiverID,
		uniqueOpaqueID("projection-message"),
	).Scan(
		&created.ID,
		&created.ConversationID,
		&created.ConversationSeq,
		&created.ClientMessageID,
		&created.SenderID,
		&created.ReceiverID,
		&created.Content,
		&created.CreatedAt,
	); err != nil {
		t.Fatalf("create projection test message: %v", err)
	}
	payload, err := json.Marshal(messageCreatedPendingPayload{Message: created})
	if err != nil {
		t.Fatalf("encode projection test payload: %v", err)
	}
	var eventID string
	if err := tx.QueryRow(
		context.Background(),
		`INSERT INTO outbox_events (event_type, payload_version, message_id, payload)
		 VALUES ('message.created', 3, $1, $2::jsonb)
		 RETURNING event_id::text`,
		created.ID,
		payload,
	).Scan(&eventID); err != nil {
		t.Fatalf("create projection test Outbox event: %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit projection test message: %v", err)
	}
	return created, eventID
}

func assertProjectionJobOwner(t *testing.T, pool *messageProjectionPool, userID int64) {
	t.Helper()
	shard := int(userID % messageProjectionVirtualShards)
	wantOwner := shard % pool.workers
	for workerIndex := 0; workerIndex < pool.workers; workerIndex++ {
		shards, err := pool.pendingShards(context.Background(), workerIndex)
		if err != nil {
			t.Fatalf("list worker %d shards: %v", workerIndex, err)
		}
		found := false
		for _, current := range shards {
			if current == shard {
				found = true
				break
			}
		}
		if found != (workerIndex == wantOwner) {
			t.Fatalf("user %d shard %d found for worker %d, want owner %d", userID, shard, workerIndex, wantOwner)
		}
	}
}

func assertProjectionReady(t *testing.T, db *pgxpool.Pool, eventID string, want bool) {
	t.Helper()
	var ready bool
	if err := db.QueryRow(
		context.Background(),
		`SELECT ready_at IS NOT NULL FROM outbox_events WHERE event_id = $1`,
		eventID,
	).Scan(&ready); err != nil {
		t.Fatalf("read projection ready state: %v", err)
	}
	if ready != want {
		t.Fatalf("event ready = %t, want %t", ready, want)
	}
}

func assertProjectionCounts(t *testing.T, db *pgxpool.Pool, messageID int64, syncWant, jobsWant int) {
	t.Helper()
	var syncRows, jobRows int
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*) FROM user_message_events WHERE message_id = $1`,
		messageID,
	).Scan(&syncRows); err != nil {
		t.Fatalf("count message sync rows: %v", err)
	}
	if err := db.QueryRow(
		context.Background(),
		`SELECT count(*) FROM message_projection_jobs WHERE message_id = $1`,
		messageID,
	).Scan(&jobRows); err != nil {
		t.Fatalf("count message projection jobs: %v", err)
	}
	if syncRows != syncWant || jobRows != jobsWant {
		t.Fatalf("sync rows = %d, projection jobs = %d, want %d/%d", syncRows, jobRows, syncWant, jobsWant)
	}
}
