package migrate

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	migrationfiles "github.com/destire-mio/im-backend/migrations"
)

func TestLoadEmbeddedMigrationChain(t *testing.T) {
	migrations, err := Load(migrationfiles.Files)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 16 {
		t.Fatalf("migration count = %d, want 16", len(migrations))
	}
	if migrations[0].Version != 1 || migrations[0].Name != "create_messages" || !migrations[0].Transactional {
		t.Fatalf("first migration = %+v", migrations[0])
	}
	if migrations[13].Version != 14 || migrations[13].Transactional {
		t.Fatalf("migration 014 transaction classification = %+v", migrations[13])
	}
	if migrations[14].Version != 15 || !migrations[14].Transactional {
		t.Fatalf("migration 015 transaction classification = %+v", migrations[14])
	}
	if strings.Contains(migrations[14].Body, "\nBEGIN;") || strings.Contains(migrations[14].Body, "\nCOMMIT;") {
		t.Fatal("outer transaction wrapper remained in migration 015 body")
	}
}

func TestLoadRejectsGapAndBrokenTransactionWrapper(t *testing.T) {
	tests := []struct {
		name  string
		files fs.FS
		want  string
	}{
		{
			name: "gap",
			files: fstest.MapFS{
				"001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
				"003_third.sql": &fstest.MapFile{Data: []byte("SELECT 3;")},
			},
			want: "version 003 at position 002",
		},
		{
			name: "broken wrapper",
			files: fstest.MapFS{
				"001_first.sql": &fstest.MapFile{Data: []byte("BEGIN;\nSELECT 1;")},
			},
			want: "both outer BEGIN and COMMIT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load(test.files)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateAppliedRejectsChecksumChangesAndHistoryGaps(t *testing.T) {
	migrations, err := Load(fstest.MapFS{
		"001_first.sql":  &fstest.MapFile{Data: []byte("SELECT 1;")},
		"002_second.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := validateApplied(migrations, map[int]AppliedMigration{
		1: {Version: 1, Name: migrations[0].Name, Checksum: []byte("wrong")},
	}); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("checksum validation error = %v", err)
	}

	if err := validateApplied(migrations, map[int]AppliedMigration{
		2: {Version: 2, Name: migrations[1].Name, Checksum: migrations[1].Checksum[:]},
	}); err == nil || !strings.Contains(err.Error(), "gap at version 001") {
		t.Fatalf("gap validation error = %v", err)
	}
}

func TestValidateReadyRequiresExactApplicationMigrationSet(t *testing.T) {
	migrations, err := Load(fstest.MapFS{
		"001_first.sql":  &fstest.MapFile{Data: []byte("SELECT 1;")},
		"002_second.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
	})
	if err != nil {
		t.Fatal(err)
	}

	applied := map[int]AppliedMigration{
		1: {Version: 1, Name: migrations[0].Name, Checksum: migrations[0].Checksum[:]},
	}
	if err := validateReady(migrations, applied); err == nil || !strings.Contains(err.Error(), "at migration 001") {
		t.Fatalf("behind readiness error = %v", err)
	}

	applied[2] = AppliedMigration{Version: 2, Name: migrations[1].Name, Checksum: migrations[1].Checksum[:]}
	if err := validateReady(migrations, applied); err != nil {
		t.Fatalf("current schema rejected: %v", err)
	}

	applied[3] = AppliedMigration{Version: 3, Name: "future", Checksum: make([]byte, 32)}
	if err := validateReady(migrations, applied); err == nil || !strings.Contains(err.Error(), "unknown migration version 003") {
		t.Fatalf("future readiness error = %v", err)
	}
}

func TestSplitSQLStatementsPreservesQuotedSemicolons(t *testing.T) {
	sql := `-- comment; is not a boundary
CREATE INDEX CONCURRENTLY test_idx ON test_table (id);
SELECT ';' AS value, "semi;colon" AS identifier;
DO $body$ BEGIN PERFORM 'inside;body'; END $body$;
/* another; comment */ ANALYZE test_table;`

	statements, err := splitSQLStatements(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 4 {
		t.Fatalf("statement count = %d, want 4: %#v", len(statements), statements)
	}
}
