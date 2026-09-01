package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
)

var ErrSchemaNotReady = errors.New("database schema is not ready for this application version")

type migrationQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// CheckReady is a read-only startup gate. Schema changes remain the dedicated
// migrator's responsibility; application instances only verify that every
// embedded migration has exactly one matching history record.
func CheckReady(ctx context.Context, queryer migrationQueryer, files fs.FS) error {
	if queryer == nil {
		return errors.New("migration database queryer is required")
	}
	migrations, err := Load(files)
	if err != nil {
		return fmt.Errorf("%w: load application migrations: %v", ErrSchemaNotReady, err)
	}

	var historyExists bool
	if err := queryer.QueryRow(
		ctx,
		`SELECT to_regclass('public.schema_migrations') IS NOT NULL`,
	).Scan(&historyExists); err != nil {
		return fmt.Errorf("%w: inspect migration history: %v", ErrSchemaNotReady, err)
	}
	if !historyExists {
		return fmt.Errorf("%w: schema_migrations is missing", ErrSchemaNotReady)
	}

	applied, err := loadApplied(ctx, queryer)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSchemaNotReady, err)
	}
	if err := validateReady(migrations, applied); err != nil {
		return fmt.Errorf("%w: %v", ErrSchemaNotReady, err)
	}
	if migrations[len(migrations)-1].Version >= 15 {
		var unreconciledMessagesExist bool
		if err := queryer.QueryRow(
			ctx,
			`SELECT EXISTS (
			     SELECT 1
			     FROM messages
			     WHERE conversation_id IS NULL OR conversation_seq IS NULL
			 )`,
		).Scan(&unreconciledMessagesExist); err != nil {
			return fmt.Errorf("%w: inspect conversation cursor reconciliation: %v", ErrSchemaNotReady, err)
		}
		if unreconciledMessagesExist {
			return fmt.Errorf(
				"%w: messages written during rollback require im-migrate reconcile-conversations",
				ErrSchemaNotReady,
			)
		}
	}
	return nil
}

func validateReady(migrations []Migration, applied map[int]AppliedMigration) error {
	if err := validateApplied(migrations, applied); err != nil {
		return err
	}
	if len(applied) != len(migrations) {
		currentVersion := 0
		for version := range applied {
			if version > currentVersion {
				currentVersion = version
			}
		}
		return fmt.Errorf(
			"database is at migration %03d but application requires %03d",
			currentVersion,
			migrations[len(migrations)-1].Version,
		)
	}
	return nil
}
