package reliabilitylab

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStaleOwner = errors.New("expired or superseded owner")

// AcquireFence stores the current generation and lease in SQL. Redis restart
// cannot reset its sequence. Lease rows must not be deleted/recreated.
func AcquireFence(ctx context.Context, db *pgxpool.Pool, name string, lease time.Duration) (int64, error) {
	if lease <= 0 {
		return 0, errors.New("lease must be positive")
	}
	var token int64
	err := db.QueryRow(ctx, `INSERT INTO resource_leases VALUES ($1,1,clock_timestamp()+$2*interval '1 millisecond')
		ON CONFLICT(name) DO UPDATE SET token=resource_leases.token+1,until=clock_timestamp()+$2*interval '1 millisecond'
        WHERE resource_leases.until<=clock_timestamp() RETURNING token`, name, lease.Milliseconds()).Scan(&token)
	return token, err
}

// WriteFenced locks the same lease row as AcquireFence and validates the token
// at the destination, atomically with the resource write. Checking Redis first
// would leave a check-to-write race. This protects only resources in this DB.
func WriteFenced(ctx context.Context, db *pgxpool.Pool, name string, token int64, value string) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var current int64
	err = tx.QueryRow(ctx, `SELECT token FROM resource_leases WHERE name=$1 FOR UPDATE`, name).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleOwner
	}
	if err != nil {
		return err
	}
	if current != token {
		return ErrStaleOwner
	}
	// Recheck time after any wait for the lease-row lock, in the write statement.
	tag, err := tx.Exec(ctx, `UPDATE resources SET value=$3,last_token=$2 WHERE name=$1 AND last_token<=$2
		AND EXISTS(SELECT 1 FROM resource_leases WHERE name=$1 AND token=$2 AND until>clock_timestamp())`, name, token, value)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleOwner
	}
	return tx.Commit(ctx)
}
