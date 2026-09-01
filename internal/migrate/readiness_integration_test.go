package migrate

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	migrationfiles "github.com/destire-mio/im-backend/migrations"
	"github.com/jackc/pgx/v5"
)

func TestCheckReadyAgainstPostgres(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required for integration tests")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	if err := CheckReady(ctx, conn, migrationfiles.Files); err != nil {
		t.Fatalf("current schema rejected: %v", err)
	}

	tests := []struct {
		name      string
		mutateSQL string
		want      string
	}{
		{
			name:      "missing history",
			mutateSQL: `DROP TABLE schema_migrations`,
			want:      "schema_migrations is missing",
		},
		{
			name:      "behind application",
			mutateSQL: `DELETE FROM schema_migrations WHERE version = 15`,
			want:      "at migration 014",
		},
		{
			name:      "checksum changed",
			mutateSQL: `UPDATE schema_migrations SET checksum = decode(repeat('11', 32), 'hex') WHERE version = 15`,
			want:      "checksum does not match",
		},
		{
			name: "newer than application",
			mutateSQL: `INSERT INTO schema_migrations (
			                version, name, checksum, execution_milliseconds
			            ) VALUES (16, 'future', decode(repeat('00', 32), 'hex'), 0)`,
			want: "unknown migration version 016",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, test.mutateSQL); err != nil {
				t.Fatal(err)
			}
			err = CheckReady(ctx, tx, migrationfiles.Files)
			if !errors.Is(err, ErrSchemaNotReady) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("CheckReady error = %v, want schema-not-ready containing %q", err, test.want)
			}
		})
	}
}
