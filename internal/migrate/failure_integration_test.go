package migrate

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	migrationfiles "github.com/destire-mio/im-backend/migrations"
	"github.com/jackc/pgx/v5"
)

func TestMigrationFailureAndRecoveryAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_MIGRATION_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_MIGRATION_DATABASE_URL is required for destructive migration tests")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	assertDedicatedMigrationDatabase(t, ctx, conn)

	t.Run("transactional failure rolls back and retries", func(t *testing.T) {
		resetPublicSchema(t, ctx, conn)
		brokenFiles := fstest.MapFS{
			"001_probe.sql": &fstest.MapFile{Data: []byte(`BEGIN;
CREATE TABLE rollback_probe (id BIGINT PRIMARY KEY);
SELECT * FROM deliberately_missing_table;
COMMIT;`)},
		}
		err := (&Runner{Conn: conn, Files: brokenFiles}).Up(ctx)
		if err == nil || !strings.Contains(err.Error(), "deliberately_missing_table") {
			t.Fatalf("broken migration error = %v", err)
		}
		assertRelationMissing(t, ctx, conn, "rollback_probe")
		assertHistoryCount(t, ctx, conn, 0)

		fixedFiles := fstest.MapFS{
			"001_probe.sql": &fstest.MapFile{Data: []byte(`BEGIN;
CREATE TABLE rollback_probe (id BIGINT PRIMARY KEY);
COMMIT;`)},
		}
		if err := (&Runner{Conn: conn, Files: fixedFiles}).Up(ctx); err != nil {
			t.Fatalf("retry fixed migration: %v", err)
		}
		assertRelationExists(t, ctx, conn, "rollback_probe")
		assertHistoryCount(t, ctx, conn, 1)
	})

	t.Run("maintenance migration stops then completes", func(t *testing.T) {
		resetPublicSchema(t, ctx, conn)
		if err := (&Runner{Conn: conn, Files: migrationSubset(t, 14)}).Up(ctx); err != nil {
			t.Fatalf("prepare schema through 014: %v", err)
		}
		if _, err := conn.Exec(ctx, `
INSERT INTO users (username, display_name, password_hash)
VALUES ('fault_user_one', 'Fault One', 'hash'),
       ('fault_user_two', 'Fault Two', 'hash');
INSERT INTO messages (sender_id, receiver_id, client_message_id, content)
SELECT first_user.id, second_user.id, 'fault-message-001', 'before migration 015'
FROM users AS first_user
CROSS JOIN users AS second_user
WHERE first_user.username = 'fault_user_one'
  AND second_user.username = 'fault_user_two'`); err != nil {
			t.Fatalf("seed historical message: %v", err)
		}

		err := (&Runner{Conn: conn, Files: migrationfiles.Files}).Up(ctx)
		if !errors.Is(err, ErrMaintenanceRequired) {
			t.Fatalf("migration 015 without maintenance error = %v", err)
		}
		assertRelationMissing(t, ctx, conn, "conversations")
		assertHistoryCount(t, ctx, conn, 14)

		if err := (&Runner{
			Conn:             conn,
			Files:            migrationfiles.Files,
			AllowMaintenance: true,
		}).Up(ctx); err != nil {
			t.Fatalf("migration 015 in maintenance mode: %v", err)
		}
		assertHistoryCount(t, ctx, conn, 16)
		var conversationID, conversationSeq int64
		if err := conn.QueryRow(
			ctx,
			`SELECT conversation_id, conversation_seq FROM messages LIMIT 1`,
		).Scan(&conversationID, &conversationSeq); err != nil {
			t.Fatal(err)
		}
		if conversationID <= 0 || conversationSeq != 1 {
			t.Fatalf("backfilled cursor = conversation %d seq %d", conversationID, conversationSeq)
		}
		if err := CheckReady(ctx, conn, migrationfiles.Files); err != nil {
			t.Fatalf("completed maintenance schema is not ready: %v", err)
		}
	})

	t.Run("partial nontransactional migration requires repair", func(t *testing.T) {
		resetPublicSchema(t, ctx, conn)
		if err := (&Runner{Conn: conn, Files: migrationSubset(t, 13)}).Up(ctx); err != nil {
			t.Fatalf("prepare schema through 013: %v", err)
		}
		if _, err := conn.Exec(ctx, `CREATE INDEX CONCURRENTLY messages_direction_history_idx
ON messages (sender_id, receiver_id, created_at DESC, id DESC)`); err != nil {
			t.Fatalf("create partial migration index: %v", err)
		}

		err := (&Runner{Conn: conn, Files: migrationfiles.Files}).Up(ctx)
		if err == nil || !strings.Contains(err.Error(), "partial database objects") {
			t.Fatalf("partial migration error = %v", err)
		}
		assertHistoryCount(t, ctx, conn, 13)

		for _, statement := range []string{
			`CREATE STATISTICS messages_sender_receiver_stats (dependencies, mcv)
             ON sender_id, receiver_id FROM messages`,
			`ALTER STATISTICS messages_sender_receiver_stats SET STATISTICS 1000`,
			`ANALYZE messages`,
		} {
			if _, err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("repair partial migration: %v", err)
			}
		}
		if err := (&Runner{Conn: conn, Files: migrationfiles.Files}).Up(ctx); err != nil {
			t.Fatalf("resume repaired migration: %v", err)
		}
		assertHistoryCount(t, ctx, conn, 16)
		if err := CheckReady(ctx, conn, migrationfiles.Files); err != nil {
			t.Fatalf("repaired schema is not ready: %v", err)
		}
	})

	t.Run("contract migration repairs rollback messages once", func(t *testing.T) {
		resetPublicSchema(t, ctx, conn)
		if err := (&Runner{Conn: conn, Files: migrationSubset(t, 15)}).Up(ctx); err != nil {
			t.Fatalf("prepare schema through 015: %v", err)
		}
		if _, err := conn.Exec(ctx, `
INSERT INTO users (username, display_name, password_hash)
VALUES ('rollback_user_one', 'Rollback One', 'hash'),
       ('rollback_user_two', 'Rollback Two', 'hash');
WITH inserted AS (
    INSERT INTO messages (sender_id, receiver_id, client_message_id, content)
    SELECT first_user.id, second_user.id, 'rollback-msg-001', 'written by old binary'
    FROM users AS first_user
    CROSS JOIN users AS second_user
    WHERE first_user.username = 'rollback_user_one'
      AND second_user.username = 'rollback_user_two'
    RETURNING *
)
INSERT INTO outbox_events (event_type, payload_version, message_id, payload)
SELECT 'message.created', 3, inserted.id,
       jsonb_build_object(
           'message', jsonb_build_object(
               'id', inserted.id,
               'clientMessageId', inserted.client_message_id,
               'senderId', inserted.sender_id,
               'receiverId', inserted.receiver_id,
               'content', inserted.content,
               'createdAt', inserted.created_at
           )
       )
FROM inserted`); err != nil {
			t.Fatalf("simulate pre-015 writer: %v", err)
		}
		if err := CheckReady(ctx, conn, migrationfiles.Files); !errors.Is(err, ErrSchemaNotReady) || !strings.Contains(err.Error(), "requires 016") {
			t.Fatalf("unreconciled readiness error = %v", err)
		}
		if err := (&Runner{Conn: conn, Files: migrationfiles.Files}).Up(ctx); !errors.Is(err, ErrMaintenanceRequired) {
			t.Fatalf("reconcile without maintenance error = %v", err)
		}

		runner := &Runner{Conn: conn, Files: migrationfiles.Files, AllowMaintenance: true}
		if err := runner.Up(ctx); err != nil {
			t.Fatalf("contract migration: %v", err)
		}
		assertHistoryCount(t, ctx, conn, 16)
		var conversationID, conversationSeq int64
		var payloadConversationID, payloadConversationSeq int64
		if err := conn.QueryRow(ctx, `
SELECT message.conversation_id,
       message.conversation_seq,
       (event.payload #>> '{message,conversationId}')::bigint,
       (event.payload #>> '{message,conversationSeq}')::bigint
FROM messages AS message
JOIN outbox_events AS event ON event.message_id = message.id
WHERE message.client_message_id = 'rollback-msg-001'`).Scan(
			&conversationID,
			&conversationSeq,
			&payloadConversationID,
			&payloadConversationSeq,
		); err != nil {
			t.Fatal(err)
		}
		if conversationID <= 0 || conversationSeq != 1 || payloadConversationID != conversationID || payloadConversationSeq != conversationSeq {
			t.Fatalf(
				"reconciled message=%d/%d payload=%d/%d",
				conversationID,
				conversationSeq,
				payloadConversationID,
				payloadConversationSeq,
			)
		}
		if err := runner.Up(ctx); err != nil {
			t.Fatalf("repeat contract migration: %v", err)
		}
		if err := CheckReady(ctx, conn, migrationfiles.Files); err != nil {
			t.Fatalf("reconciled roll-forward schema is not ready: %v", err)
		}
	})
}

func migrationSubset(t *testing.T, latest int) fstest.MapFS {
	t.Helper()
	migrations, err := Load(migrationfiles.Files)
	if err != nil {
		t.Fatal(err)
	}
	files := make(fstest.MapFS, latest)
	for _, migration := range migrations {
		if migration.Version > latest {
			break
		}
		files[migration.Filename] = &fstest.MapFile{Data: []byte(migration.SQL)}
	}
	return files
}

func assertDedicatedMigrationDatabase(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	var databaseName string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&databaseName); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "im_migration_test_") {
		t.Fatalf("refusing destructive migration test against database %q", databaseName)
	}
}

func resetPublicSchema(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset dedicated migration schema: %v", err)
	}
}

func assertRelationMissing(t *testing.T, ctx context.Context, conn *pgx.Conn, relation string) {
	t.Helper()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, relation).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("relation %s unexpectedly exists", relation)
	}
}

func assertRelationExists(t *testing.T, ctx context.Context, conn *pgx.Conn, relation string) {
	t.Helper()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.' || $1) IS NOT NULL`, relation).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatalf("relation %s is missing", relation)
	}
}

func assertHistoryCount(t *testing.T, ctx context.Context, conn *pgx.Conn, want int) {
	t.Helper()
	var count int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("migration history count = %d, want %d", count, want)
	}
}
