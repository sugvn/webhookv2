package store

import (
	"context"
	"encoding/json"
	"fmt"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/sugvn/webhookv2/internal/model"
)

// ProducerByName returns found=false if the producer doesn't exist.
func (s *Store) ProducerByName(ctx context.Context, name string) (id int64, passwordHash string, found bool, err error) {
	err = s.pool.QueryRow(ctx,
		`SELECT id, password_hash FROM producers WHERE name = $1`, name).Scan(&id, &passwordHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("producer lookup: %w", err)
	}
	return id, passwordHash, true, nil
}

// CreateProducer atomically claims a new name. created=false means someone else got it first.
func (s *Store) CreateProducer(ctx context.Context, name, passwordHash string) (id int64, created bool, err error) {
	err = s.pool.QueryRow(ctx, `
		INSERT INTO producers (name, password_hash) VALUES ($1, $2)
		ON CONFLICT (name) DO NOTHING
		RETURNING id`, name, passwordHash).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("create producer: %w", err)
	}
	return id, true, nil
}

// RegisterProducer adds/updates subscriptions. It performs NO authentication:
// callers (internal/registry) must have verified the producer first.
func (s *Store) RegisterProducer(ctx context.Context, name, passwordHash string, subs []model.SubscriptionSpec) (int64, error) {
	b, err := json.Marshal(subs)
	if err != nil {
		return 0, fmt.Errorf("marshal subscriptions: %w", err)
	}
	var id int64
	err = s.pool.QueryRow(ctx,
		`SELECT register_producer($1, $2, $3::jsonb)`, name, passwordHash, string(b)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("register producer: %w", err)
	}
	return id, nil
}
