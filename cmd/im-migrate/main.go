package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/destire-mio/im-backend/internal/migrate"
	migrationfiles "github.com/destire-mio/im-backend/migrations"
	"github.com/jackc/pgx/v5"
)

const defaultMigrationDatabaseURL = "postgres://im:im@localhost:5433/im?sslmode=disable"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout io.Writer, stderr io.Writer) int {
	if len(arguments) == 0 || (arguments[0] != "up" && arguments[0] != "baseline") {
		printUsage(stderr)
		return 2
	}

	command := arguments[0]
	flags := flag.NewFlagSet("im-migrate "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	databaseURL := flags.String("database-url", migrationDatabaseURL(), "PostgreSQL connection URL")
	var allowMaintenance *bool
	var baselineTarget *int
	if command == "up" {
		allowMaintenance = flags.Bool("allow-maintenance", false, "allow migrations that rewrite historical data")
	} else {
		baselineTarget = flags.Int("to", 0, "verified existing schema version to record")
	}
	if err := flags.Parse(arguments[1:]); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected positional arguments")
		return 2
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	conn, err := pgx.Connect(ctx, *databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "connect to migration database: %v\n", err)
		return 1
	}
	defer conn.Close(context.Background())

	runner := &migrate.Runner{
		Conn:  conn,
		Files: migrationfiles.Files,
		Progress: func(migration migrate.Migration) {
			fmt.Fprintf(stdout, "applied %03d_%s\n", migration.Version, migration.Name)
		},
	}
	if command == "baseline" {
		if *baselineTarget == 0 {
			fmt.Fprintln(stderr, "baseline requires an explicit -to version")
			return 2
		}
		if err := runner.Baseline(ctx, *baselineTarget); err != nil {
			switch {
			case errors.Is(err, migrate.ErrBaselineMismatch):
				fmt.Fprintf(stderr, "baseline refused: %v\n", err)
			case errors.Is(err, migrate.ErrMigrationHistoryExists):
				fmt.Fprintln(stderr, "baseline refused: database already has migration history; use up")
			default:
				fmt.Fprintf(stderr, "baseline failed: %v\n", err)
			}
			return 1
		}
		fmt.Fprintf(stdout, "recorded verified baseline through %03d\n", *baselineTarget)
		return 0
	}

	runner.AllowMaintenance = *allowMaintenance
	if err := runner.Up(ctx); err != nil {
		switch {
		case errors.Is(err, migrate.ErrBaselineRequired):
			fmt.Fprintln(stderr, "migration refused: existing database has no migration history; run a verified baseline before up")
		case errors.Is(err, migrate.ErrMaintenanceRequired):
			fmt.Fprintln(stderr, "migration refused: stop message writers and rerun with -allow-maintenance")
		default:
			fmt.Fprintf(stderr, "migration failed: %v\n", err)
		}
		return 1
	}
	fmt.Fprintln(stdout, "database schema is current")
	return 0
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "usage:")
	fmt.Fprintln(output, "  im-migrate up [-database-url URL] [-allow-maintenance]")
	fmt.Fprintln(output, "  im-migrate baseline -to VERSION [-database-url URL] (reviewed: 15, 16)")
}

func migrationDatabaseURL() string {
	if value := os.Getenv("DATABASE_URL"); value != "" {
		return value
	}
	return defaultMigrationDatabaseURL
}
