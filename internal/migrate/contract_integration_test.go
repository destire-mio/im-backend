package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	migrationfiles "github.com/destire-mio/im-backend/migrations"
	"github.com/jackc/pgx/v5"
)

func openContractTestDatabase(t *testing.T) *pgx.Conn {
	t.Helper()
	databaseURL := os.Getenv("TEST_MIGRATION_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_MIGRATION_DATABASE_URL is required for destructive migration tests")
	}
	conn, err := pgx.Connect(t.Context(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	assertDedicatedMigrationDatabase(t, t.Context(), conn)
	return conn
}

func TestContractMigrationPreservesMessagesAndPendingDeliveries(t *testing.T) {
	conn := openContractTestDatabase(t)
	ctx := t.Context()
	resetPublicSchema(t, ctx, conn)
	if err := (&Runner{Conn: conn, Files: migrationSubset(t, 15)}).Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
INSERT INTO users (id, username, display_name, password_hash) VALUES
    (1, 'contract_one', 'One', 'hash'), (2, 'contract_two', 'Two', 'hash');
INSERT INTO conversations (direct_user_low_id, direct_user_high_id, last_seq)
    VALUES (1, 2, 7);
INSERT INTO conversation_members (conversation_id, user_id) VALUES (1, 1), (1, 2);
INSERT INTO messages (conversation_id, conversation_seq, sender_id, receiver_id, client_message_id, content)
    VALUES (1, 7, 1, 2, 'existing-cursor-0007', 'preserve this cursor');`); err != nil {
		t.Fatal(err)
	}
	type fixture struct {
		eventID, originalRow string
		messageID            int64
		version              int
		receiver             int64
		terminal             string
		nextAttempt          time.Time
	}
	fixtures := []fixture{
		{version: 1, receiver: 2}, {version: 2, receiver: 2},
		{version: 3, receiver: 2}, {version: 3, receiver: 1},
		{version: 2, receiver: 2, terminal: "published"},
		{version: 3, receiver: 2, terminal: "dead"},
	}
	const traceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	for index := range fixtures {
		current := &fixtures[index]
		// NULL cursors model messages inserted during the old rollback window.
		if err := conn.QueryRow(ctx, `
WITH inserted AS (
    INSERT INTO messages (sender_id, receiver_id, client_message_id, content)
    VALUES (1, $1, $2, 'authoritative content') RETURNING id
)
INSERT INTO outbox_events (
    event_type, payload_version, message_id, payload, attempt_count,
    next_attempt_at, ready_at, published_at, dead_at, locked_until, lock_token, last_error
)
SELECT 'message.created', $3::smallint, inserted.id,
       jsonb_build_object('legacy', true, 'traceParent', $4::text, 'traceState', 'vendor=test'), 2,
       CURRENT_TIMESTAMP + INTERVAL '5 minutes',
       CASE WHEN $5 = 'published' OR $3::smallint = 2 THEN CURRENT_TIMESTAMP END,
       CASE WHEN $5 = 'published' THEN CURRENT_TIMESTAMP END,
       CASE WHEN $5 = 'dead' THEN CURRENT_TIMESTAMP END,
       CURRENT_TIMESTAMP + INTERVAL '30 seconds', gen_random_uuid(), 'preserve retry history'
FROM inserted
RETURNING event_id::text, message_id, next_attempt_at`,
			current.receiver, fmt.Sprintf("contract-legacy-%04d", index), current.version, traceParent, current.terminal,
		).Scan(&current.eventID, &current.messageID, &current.nextAttempt); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SELECT to_jsonb(event)::text FROM outbox_events AS event WHERE event_id = $1`, current.eventID).Scan(&current.originalRow); err != nil {
			t.Fatal(err)
		}
	}
	runner := &Runner{Conn: conn, Files: migrationfiles.Files}
	if err := runner.Up(ctx); !errors.Is(err, ErrMaintenanceRequired) {
		t.Fatalf("populated contract without maintenance: %v", err)
	}
	assertHistoryCount(t, ctx, conn, 15)
	runner.AllowMaintenance = true
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	for _, current := range fixtures {
		var row string
		if err := conn.QueryRow(ctx, `SELECT to_jsonb(event)::text FROM outbox_events AS event WHERE event_id = $1`, current.eventID).Scan(&row); err != nil {
			t.Fatal(err)
		}
		if current.terminal != "" {
			if row != current.originalRow {
				t.Fatalf("terminal event %s was rewritten", current.eventID)
			}
			continue
		}
		var version, attempts int
		var ready, unlocked bool
		var nextAttempt time.Time
		var raw []byte
		if err := conn.QueryRow(ctx, `
SELECT payload_version, payload, ready_at IS NOT NULL,
       locked_until IS NULL AND lock_token IS NULL, attempt_count, next_attempt_at
FROM outbox_events WHERE event_id = $1`, current.eventID,
		).Scan(&version, &raw, &ready, &unlocked, &attempts, &nextAttempt); err != nil {
			t.Fatal(err)
		}
		if version != 4 || !ready || !unlocked || attempts != 2 || !nextAttempt.Equal(current.nextAttempt) {
			t.Fatalf("converted event: version=%d ready=%t unlocked=%t attempts=%d next=%v", version, ready, unlocked, attempts, nextAttempt)
		}
		var payload struct {
			Message struct {
				ID              int64  `json:"id"`
				ConversationID  int64  `json:"conversationId"`
				ConversationSeq int64  `json:"conversationSeq"`
				Content         string `json:"content"`
			} `json:"message"`
			Recipients  []map[string]int64 `json:"recipients"`
			TraceParent string             `json:"traceParent"`
			TraceState  string             `json:"traceState"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		wantRecipients := 2
		if current.receiver == 1 {
			wantRecipients = 1
		}
		if payload.Message.ID != current.messageID || payload.Message.ConversationID <= 0 || payload.Message.ConversationSeq <= 0 ||
			payload.Message.Content != "authoritative content" || len(payload.Recipients) != wantRecipients ||
			payload.TraceParent != traceParent || payload.TraceState != "vendor=test" {
			t.Fatalf("converted payload = %s", raw)
		}
		for index, recipient := range payload.Recipients {
			if len(recipient) != 1 || recipient["userId"] != int64(index+1) {
				t.Fatalf("unexpected recipients or retained user cursor: %v", payload.Recipients)
			}
		}
	}
	var count, minimum, maximum, lastSeq int64
	if err := conn.QueryRow(ctx, `
SELECT count(*), min(conversation_seq), max(conversation_seq), max(conversation.last_seq)
FROM messages JOIN conversations AS conversation ON conversation.id = messages.conversation_id
WHERE conversation_id = 1`).Scan(&count, &minimum, &maximum, &lastSeq); err != nil {
		t.Fatal(err)
	}
	if count != 6 || minimum != 7 || maximum != 12 || lastSeq != 12 {
		t.Fatalf("existing conversation renumbered or lost messages: count=%d min=%d max=%d last=%d", count, minimum, maximum, lastSeq)
	}
	for _, table := range []string{"user_sync_counters", "user_message_events", "device_sync_states", "outbox_recipients", "message_projection_jobs"} {
		assertRelationMissing(t, ctx, conn, table)
	}
	for _, statement := range []string{
		`INSERT INTO messages (sender_id, receiver_id, client_message_id, content) VALUES (1, 2, 'old-writer-rejected', 'old binary')`,
		`UPDATE outbox_events SET payload_version = 3 WHERE published_at IS NULL AND dead_at IS NULL`,
		`UPDATE outbox_events SET ready_at = NULL WHERE published_at IS NULL AND dead_at IS NULL`,
	} {
		if _, err := conn.Exec(ctx, statement); err == nil {
			t.Fatalf("contract allowed old writer state: %s", statement)
		}
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
	assertHistoryCount(t, ctx, conn, 16)
	if err := CheckReady(ctx, conn, migrationfiles.Files); err != nil {
		t.Fatal(err)
	}
}

func TestContractMigrationRefusesAmbiguousDataAtomically(t *testing.T) {
	conn := openContractTestDatabase(t)
	ctx := t.Context()
	for _, invalid := range []string{"partial cursor", "unknown payload"} {
		t.Run(invalid, func(t *testing.T) {
			resetPublicSchema(t, ctx, conn)
			if err := (&Runner{Conn: conn, Files: migrationSubset(t, 15)}).Up(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `
INSERT INTO users (id, username, display_name, password_hash) VALUES (1, 'invalid_probe', 'Probe', 'hash');
INSERT INTO conversations (direct_user_low_id, direct_user_high_id) VALUES (1, 1);
INSERT INTO messages (sender_id, receiver_id, client_message_id, content) VALUES (1, 1, 'invalid-probe-message', 'probe');`); err != nil {
				t.Fatal(err)
			}
			statement := `UPDATE messages SET conversation_id = 1`
			want := "partial conversation cursor"
			if invalid == "unknown payload" {
				statement = `INSERT INTO outbox_events (event_type, payload_version, message_id, payload) VALUES ('message.created', 99, 1, '{}')`
				want = "unsupported pending message payload version"
			}
			if _, err := conn.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			err := (&Runner{Conn: conn, Files: migrationfiles.Files, AllowMaintenance: true}).Up(ctx)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("invalid contract data error = %v, want %s", err, want)
			}
			assertHistoryCount(t, ctx, conn, 15)
			assertRelationExists(t, ctx, conn, "user_message_events")
			var nullable bool
			if err := conn.QueryRow(ctx, `SELECT NOT attnotnull FROM pg_attribute WHERE attrelid = 'messages'::regclass AND attname = 'conversation_seq'`).Scan(&nullable); err != nil || !nullable {
				t.Fatalf("failed migration partially contracted schema: nullable=%t err=%v", nullable, err)
			}
		})
	}
}

func TestSchemaSnapshotAndReviewedBaselinesAgainstPostgres(t *testing.T) {
	conn := openContractTestDatabase(t)
	ctx := t.Context()
	resetPublicSchema(t, ctx, conn)
	runner := &Runner{Conn: conn, Files: migrationfiles.Files}
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	chainFingerprint, _, err := SchemaFingerprint(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	resetPublicSchema(t, ctx, conn)
	snapshot, err := os.ReadFile("../../schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, string(snapshot)); err != nil {
		t.Fatal(err)
	}
	snapshotFingerprint, _, err := SchemaFingerprint(ctx, conn)
	if err != nil || snapshotFingerprint != chainFingerprint {
		t.Fatalf("schema.sql=%s migration chain=%s err=%v", snapshotFingerprint, chainFingerprint, err)
	}
	if chainFingerprint != ExpectedSchemaFingerprint016 {
		t.Fatalf("verified 016 fingerprint = %s, expected %s", chainFingerprint, ExpectedSchemaFingerprint016)
	}
	if err := runner.Baseline(ctx, 16); err != nil {
		t.Fatalf("baseline schema.sql at 016: %v", err)
	}
	if err := CheckReady(ctx, conn, migrationfiles.Files); err != nil {
		t.Fatal(err)
	}
	resetPublicSchema(t, ctx, conn)
	if err := (&Runner{Conn: conn, Files: migrationSubset(t, 15)}).Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := runner.Baseline(ctx, 15); err != nil {
		t.Fatalf("retain safe adoption of existing 015 databases: %v", err)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	assertHistoryCount(t, ctx, conn, 16)
}
