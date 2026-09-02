// Package reliabilitylab contains isolated correctness experiments, not IM API routes.
package reliabilitylab

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var Schema string

var ErrExhausted = errors.New("packet exhausted")

type Receipt struct {
	Packet string `json:"packet"`
	User   string `json:"user"`
	Slot   int    `json:"slot"`
	Amount int64  `json:"amount"`
}

type PacketStore struct {
	DB *pgxpool.Pool
	// Test-only barriers can stop a real process at an exact transaction boundary.
	checkpoint func(string)
}

// Create preallocates integer minor units; no float arithmetic or Redis reservation.
// It models an already funded envelope, not sender-wallet debit or external payment.
func (s *PacketStore) Create(ctx context.Context, id string, total int64, shares int) error {
	if id == "" || shares < 1 || shares > 10000 || total < int64(shares) {
		return errors.New("invalid packet allocation")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `INSERT INTO packets VALUES ($1,$2,$3)`, id, total, shares); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO packet_slots(packet_id,slot,amount)
        SELECT $1, n, $2::bigint / $3::bigint + CASE WHEN n < $2::bigint % $3::bigint THEN 1 ELSE 0 END
        FROM generate_series(0,$3::integer - 1) AS n`, id, total, shares)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Claim serializes only this packet's short transaction. Existing receipts are
// checked after taking its row lock, so concurrent retries see the committed result.
func (s *PacketStore) Claim(ctx context.Context, packet, user string) (Receipt, error) {
	result := Receipt{Packet: packet, User: user}
	if user == "" || len(user) > 128 {
		return result, errors.New("invalid user")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(context.Background())
	var id string
	if err = tx.QueryRow(ctx, `SELECT id FROM packets WHERE id=$1 FOR UPDATE`, packet).Scan(&id); err != nil {
		return result, err
	}
	err = tx.QueryRow(ctx, `SELECT slot,amount FROM packet_credits WHERE packet_id=$1 AND user_id=$2`, packet, user).Scan(&result.Slot, &result.Amount)
	if err == nil {
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	err = tx.QueryRow(ctx, `UPDATE packet_slots SET claimed_by=$2
        WHERE packet_id=$1 AND slot=(SELECT slot FROM packet_slots WHERE packet_id=$1 AND claimed_by IS NULL ORDER BY slot LIMIT 1)
        RETURNING slot,amount`, packet, user).Scan(&result.Slot, &result.Amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, ErrExhausted
	}
	if err != nil {
		return result, err
	}
	s.at("after_slot")
	if _, err = tx.Exec(ctx, `INSERT INTO packet_credits VALUES ($1,$2,$3,$4)`, packet, result.Slot, result.Amount, user); err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO credit_outbox VALUES ($1,$2)`, packet, user); err != nil {
		return result, err
	}
	s.at("before_commit")
	if err = tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("claim commit (retry same packet/user to resolve outcome): %w", err)
	}
	s.at("after_commit")
	return result, nil
}

func (s *PacketStore) at(point string) {
	if s.checkpoint != nil {
		s.checkpoint(point)
	}
}

// Deliver commits the consumer effect before acknowledging the durable intent.
// A crash between these steps replays safely using the receiver's unique key.
func (s *PacketStore) Deliver(ctx context.Context, packet, user string) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO received_credits SELECT packet_id,user_id,amount
        FROM packet_credits JOIN credit_outbox USING(packet_id,user_id)
        WHERE packet_id=$1 AND user_id=$2 ON CONFLICT DO NOTHING`, packet, user)
	if err != nil {
		return err
	}
	s.at("after_delivery")
	_, err = s.DB.Exec(ctx, `DELETE FROM credit_outbox WHERE packet_id=$1 AND user_id=$2`, packet, user)
	return err
}
