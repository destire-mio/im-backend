package migrate

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrBaselineRequired       = errors.New("database has business tables but no migration history; baseline is required")
	ErrBaselineMismatch       = errors.New("database schema does not match the requested baseline")
	ErrMigrationHistoryExists = errors.New("database already has migration history")
	ErrMaintenanceRequired    = errors.New("migration requires maintenance mode because historical messages exist")
)

var migrationFilenamePattern = regexp.MustCompile(`^(\d{3})_([a-z0-9_]+)\.sql$`)

type Migration struct {
	Version       int
	Name          string
	Filename      string
	SQL           string
	Checksum      [sha256.Size]byte
	Transactional bool
	Body          string
}

type AppliedMigration struct {
	Version  int
	Name     string
	Checksum []byte
}

type Runner struct {
	Conn             *pgx.Conn
	Files            fs.FS
	AllowMaintenance bool
	Progress         func(Migration)
}

func Load(files fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		match := migrationFilenamePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("invalid migration filename %q", entry.Name())
		}
		version, err := strconv.Atoi(match[1])
		if err != nil {
			return nil, fmt.Errorf("parse migration version %q: %w", entry.Name(), err)
		}
		contents, err := fs.ReadFile(files, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		body, transactional, err := unwrapTransaction(string(contents))
		if err != nil {
			return nil, fmt.Errorf("inspect migration %q: %w", entry.Name(), err)
		}
		migrations = append(migrations, Migration{
			Version:       version,
			Name:          match[2],
			Filename:      entry.Name(),
			SQL:           string(contents),
			Checksum:      sha256.Sum256(contents),
			Transactional: transactional,
			Body:          body,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
	for index, migration := range migrations {
		wantVersion := index + 1
		if migration.Version != wantVersion {
			return nil, fmt.Errorf("migration chain has version %03d at position %03d", migration.Version, wantVersion)
		}
		if index > 0 && migrations[index-1].Version == migration.Version {
			return nil, fmt.Errorf("duplicate migration version %03d", migration.Version)
		}
	}
	if len(migrations) == 0 {
		return nil, errors.New("no migrations found")
	}
	return migrations, nil
}

func (runner *Runner) Up(ctx context.Context) error {
	if runner.Conn == nil {
		return errors.New("migration database connection is required")
	}
	migrations, err := Load(runner.Files)
	if err != nil {
		return err
	}

	if err := runner.acquireLock(ctx); err != nil {
		return err
	}
	defer runner.releaseLock(ctx)

	if err := runner.ensureHistory(ctx); err != nil {
		return err
	}
	applied, err := runner.loadApplied(ctx)
	if err != nil {
		return err
	}
	if err := validateApplied(migrations, applied); err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, exists := applied[migration.Version]; exists {
			continue
		}
		if migration.Version == 15 && !runner.AllowMaintenance {
			hasMessages, err := runner.hasRows(ctx, "messages")
			if err != nil {
				return fmt.Errorf("check migration 015 maintenance requirement: %w", err)
			}
			if hasMessages {
				return fmt.Errorf("migration %03d_%s: %w", migration.Version, migration.Name, ErrMaintenanceRequired)
			}
		}
		if err := runner.apply(ctx, migration); err != nil {
			return fmt.Errorf("apply migration %03d_%s: %w", migration.Version, migration.Name, err)
		}
		if runner.Progress != nil {
			runner.Progress(migration)
		}
	}
	return nil
}

func (runner *Runner) Baseline(ctx context.Context, target int) error {
	if runner.Conn == nil {
		return errors.New("migration database connection is required")
	}
	migrations, err := Load(runner.Files)
	if err != nil {
		return err
	}
	if target != 15 || target > len(migrations) {
		return fmt.Errorf("baseline target %03d is unsupported; only 015 has a reviewed schema fingerprint", target)
	}

	if err := runner.acquireLock(ctx); err != nil {
		return err
	}
	defer runner.releaseLock(ctx)

	tx, err := runner.Conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin baseline: %w", err)
	}
	defer tx.Rollback(ctx)

	var historyExists bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('public.schema_migrations') IS NOT NULL`).Scan(&historyExists); err != nil {
		return fmt.Errorf("inspect migration history: %w", err)
	}
	if historyExists {
		return ErrMigrationHistoryExists
	}

	var businessTablesExist bool
	if err := tx.QueryRow(
		ctx,
		`SELECT EXISTS (
		     SELECT 1
		     FROM pg_class AS relation
		     JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		     WHERE namespace.nspname = 'public'
		       AND relation.relkind IN ('r', 'p')
		 )`,
	).Scan(&businessTablesExist); err != nil {
		return fmt.Errorf("inspect existing business tables: %w", err)
	}
	if !businessTablesExist {
		return errors.New("baseline refused: database has no business tables; use up for an empty database")
	}

	fingerprint, objectCount, err := SchemaFingerprint(ctx, tx)
	if err != nil {
		return err
	}
	if fingerprint != ExpectedSchemaFingerprint015 {
		return fmt.Errorf(
			"baseline 015 fingerprint mismatch (objects=%d actual=%s expected=%s): %w",
			objectCount,
			fingerprint,
			ExpectedSchemaFingerprint015,
			ErrBaselineMismatch,
		)
	}

	if err := createHistory(ctx, tx); err != nil {
		return err
	}
	for _, migration := range migrations[:target] {
		if err := recordMigration(ctx, tx, migration, 0); err != nil {
			return fmt.Errorf("record baseline migration %03d_%s: %w", migration.Version, migration.Name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit baseline: %w", err)
	}
	return nil
}

func (runner *Runner) acquireLock(ctx context.Context) error {
	if _, err := runner.Conn.Exec(
		ctx,
		`SELECT pg_advisory_lock(hashtextextended('im-backend-schema-migrations', 0))`,
	); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	return nil
}

func (runner *Runner) releaseLock(ctx context.Context) {
	_, _ = runner.Conn.Exec(
		context.WithoutCancel(ctx),
		`SELECT pg_advisory_unlock(hashtextextended('im-backend-schema-migrations', 0))`,
	)
}

func (runner *Runner) ensureHistory(ctx context.Context) error {
	var historyExists bool
	if err := runner.Conn.QueryRow(
		ctx,
		`SELECT to_regclass('public.schema_migrations') IS NOT NULL`,
	).Scan(&historyExists); err != nil {
		return fmt.Errorf("inspect migration history: %w", err)
	}
	if historyExists {
		return nil
	}

	var businessTablesExist bool
	if err := runner.Conn.QueryRow(
		ctx,
		`SELECT EXISTS (
		     SELECT 1
		     FROM pg_class AS relation
		     JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
		     WHERE namespace.nspname = 'public'
		       AND relation.relkind IN ('r', 'p')
		 )`,
	).Scan(&businessTablesExist); err != nil {
		return fmt.Errorf("inspect existing business tables: %w", err)
	}
	if businessTablesExist {
		return ErrBaselineRequired
	}

	return createHistory(ctx, runner.Conn)
}

func createHistory(
	ctx context.Context,
	executor interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	},
) error {
	if _, err := executor.Exec(
		ctx,
		`CREATE TABLE schema_migrations (
		     version INTEGER PRIMARY KEY,
		     name TEXT NOT NULL,
		     checksum BYTEA NOT NULL,
		     applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		     execution_milliseconds BIGINT NOT NULL,
		     CONSTRAINT schema_migrations_version_valid CHECK (version > 0),
		     CONSTRAINT schema_migrations_name_valid CHECK (name ~ '^[a-z0-9_]+$'),
		     CONSTRAINT schema_migrations_checksum_valid CHECK (octet_length(checksum) = 32),
		     CONSTRAINT schema_migrations_execution_time_valid CHECK (execution_milliseconds >= 0)
		 )`,
	); err != nil {
		return fmt.Errorf("create migration history: %w", err)
	}
	return nil
}

func (runner *Runner) loadApplied(ctx context.Context) (map[int]AppliedMigration, error) {
	return loadApplied(ctx, runner.Conn)
}

func loadApplied(ctx context.Context, queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}) (map[int]AppliedMigration, error) {
	rows, err := queryer.Query(
		ctx,
		`SELECT version, name, checksum
		 FROM schema_migrations
		 ORDER BY version`,
	)
	if err != nil {
		return nil, fmt.Errorf("load migration history: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]AppliedMigration)
	for rows.Next() {
		var migration AppliedMigration
		if err := rows.Scan(&migration.Version, &migration.Name, &migration.Checksum); err != nil {
			return nil, fmt.Errorf("scan migration history: %w", err)
		}
		applied[migration.Version] = migration
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate migration history: %w", err)
	}
	return applied, nil
}

func validateApplied(migrations []Migration, applied map[int]AppliedMigration) error {
	byVersion := make(map[int]Migration, len(migrations))
	for _, migration := range migrations {
		byVersion[migration.Version] = migration
	}
	for version, recorded := range applied {
		migration, exists := byVersion[version]
		if !exists {
			return fmt.Errorf("database contains unknown migration version %03d", version)
		}
		if recorded.Name != migration.Name {
			return fmt.Errorf("migration %03d name changed from %q to %q", version, recorded.Name, migration.Name)
		}
		if !equalBytes(recorded.Checksum, migration.Checksum[:]) {
			return fmt.Errorf("migration %03d_%s checksum does not match migration history", version, migration.Name)
		}
	}
	for version := 1; version <= len(applied); version++ {
		if _, exists := applied[version]; !exists {
			return fmt.Errorf("migration history has a gap at version %03d", version)
		}
	}
	return nil
}

func (runner *Runner) apply(ctx context.Context, migration Migration) error {
	startedAt := time.Now()
	if migration.Transactional {
		tx, err := runner.Conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, migration.Body); err != nil {
			return err
		}
		if err := recordMigration(ctx, tx, migration, time.Since(startedAt)); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	complete, partial, err := runner.nonTransactionalState(ctx, migration.Version)
	if err != nil {
		return err
	}
	if partial {
		return errors.New("non-transactional migration left partial database objects; manual recovery is required")
	}
	if !complete {
		statements, err := splitSQLStatements(migration.SQL)
		if err != nil {
			return err
		}
		for _, statement := range statements {
			if _, err := runner.Conn.Exec(ctx, statement); err != nil {
				return err
			}
		}
		complete, partial, err = runner.nonTransactionalState(ctx, migration.Version)
		if err != nil {
			return err
		}
		if !complete || partial {
			return errors.New("non-transactional migration did not produce valid database objects")
		}
	}
	return recordMigration(ctx, runner.Conn, migration, time.Since(startedAt))
}

func recordMigration(
	ctx context.Context,
	executor interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	},
	migration Migration,
	duration time.Duration,
) error {
	_, err := executor.Exec(
		ctx,
		`INSERT INTO schema_migrations (
		     version,
		     name,
		     checksum,
		     execution_milliseconds
		 ) VALUES ($1, $2, $3, $4)`,
		migration.Version,
		migration.Name,
		migration.Checksum[:],
		duration.Milliseconds(),
	)
	return err
}

func (runner *Runner) nonTransactionalState(ctx context.Context, version int) (complete bool, partial bool, err error) {
	switch version {
	case 14:
		var indexExists, indexValid, statisticsExist bool
		err = runner.Conn.QueryRow(
			ctx,
			`SELECT index_relation.oid IS NOT NULL,
			        COALESCE(index_data.indisready AND index_data.indisvalid, false),
			        EXISTS (
			            SELECT 1
			            FROM pg_statistic_ext
			            WHERE stxnamespace = 'public'::regnamespace
			              AND stxname = 'messages_sender_receiver_stats'
			        )
			 FROM (SELECT to_regclass('public.messages_direction_history_idx') AS oid) AS index_relation
			 LEFT JOIN pg_index AS index_data ON index_data.indexrelid = index_relation.oid`,
		).Scan(&indexExists, &indexValid, &statisticsExist)
		if err != nil {
			return false, false, err
		}
		complete = indexExists && indexValid && statisticsExist
		partial = !complete && (indexExists || statisticsExist)
		return complete, partial, nil
	default:
		return false, false, fmt.Errorf("non-transactional migration %03d has no completion verifier", version)
	}
}

func (runner *Runner) hasRows(ctx context.Context, table string) (bool, error) {
	if table != "messages" {
		return false, fmt.Errorf("unsupported maintenance table %q", table)
	}
	var exists bool
	err := runner.Conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM messages LIMIT 1)`).Scan(&exists)
	return exists, err
}

func unwrapTransaction(sql string) (body string, transactional bool, err error) {
	lines := strings.Split(sql, "\n")
	significant := make([]int, 0, len(lines))
	for index, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		significant = append(significant, index)
	}
	if len(significant) == 0 {
		return "", false, errors.New("migration contains no SQL")
	}
	first := strings.ToUpper(strings.TrimSpace(lines[significant[0]]))
	last := strings.ToUpper(strings.TrimSpace(lines[significant[len(significant)-1]]))
	hasBegin := first == "BEGIN;" || first == "BEGIN"
	hasCommit := last == "COMMIT;" || last == "COMMIT"
	if hasBegin != hasCommit {
		return "", false, errors.New("migration must contain both outer BEGIN and COMMIT or neither")
	}
	if !hasBegin {
		return sql, false, nil
	}
	lines[significant[0]] = ""
	lines[significant[len(significant)-1]] = ""
	return strings.Join(lines, "\n"), true, nil
}

// splitSQLStatements separates commands so PostgreSQL does not wrap a
// multi-command query in an implicit transaction. That distinction matters for
// commands such as CREATE INDEX CONCURRENTLY. It deliberately handles quoted
// strings, identifiers, dollar-quoted bodies, and SQL comments rather than
// blindly splitting every semicolon.
func splitSQLStatements(sql string) ([]string, error) {
	var statements []string
	start := 0
	inSingleQuote := false
	inDoubleQuote := false
	inLineComment := false
	inBlockComment := false
	dollarTag := ""

	for index := 0; index < len(sql); {
		if inLineComment {
			if sql[index] == '\n' {
				inLineComment = false
			}
			index++
			continue
		}
		if inBlockComment {
			if index+1 < len(sql) && sql[index:index+2] == "*/" {
				inBlockComment = false
				index += 2
				continue
			}
			index++
			continue
		}
		if dollarTag != "" {
			if strings.HasPrefix(sql[index:], dollarTag) {
				index += len(dollarTag)
				dollarTag = ""
				continue
			}
			index++
			continue
		}
		if inSingleQuote {
			if sql[index] == '\'' {
				if index+1 < len(sql) && sql[index+1] == '\'' {
					index += 2
					continue
				}
				inSingleQuote = false
			}
			index++
			continue
		}
		if inDoubleQuote {
			if sql[index] == '"' {
				if index+1 < len(sql) && sql[index+1] == '"' {
					index += 2
					continue
				}
				inDoubleQuote = false
			}
			index++
			continue
		}

		if index+1 < len(sql) {
			switch sql[index : index+2] {
			case "--":
				inLineComment = true
				index += 2
				continue
			case "/*":
				inBlockComment = true
				index += 2
				continue
			}
		}
		switch sql[index] {
		case '\'':
			inSingleQuote = true
			index++
		case '"':
			inDoubleQuote = true
			index++
		case '$':
			end := index + 1
			for end < len(sql) && (sql[end] == '_' || sql[end] >= 'a' && sql[end] <= 'z' || sql[end] >= 'A' && sql[end] <= 'Z' || sql[end] >= '0' && sql[end] <= '9') {
				end++
			}
			if end < len(sql) && sql[end] == '$' {
				dollarTag = sql[index : end+1]
				index = end + 1
			} else {
				index++
			}
		case ';':
			if statement := strings.TrimSpace(sql[start : index+1]); statement != "" {
				statements = append(statements, statement)
			}
			index++
			start = index
		default:
			index++
		}
	}

	if inSingleQuote || inDoubleQuote || inBlockComment || dollarTag != "" {
		return nil, errors.New("non-transactional migration contains unterminated SQL syntax")
	}
	if statement := strings.TrimSpace(sql[start:]); statement != "" {
		statements = append(statements, statement)
	}
	if len(statements) == 0 {
		return nil, errors.New("non-transactional migration contains no SQL statements")
	}
	return statements, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}
