package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool builds the shared pool used by workers and store calls.
// Size it as workers + a few spare. The listener does NOT draw from this pool.
func NewPool(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 10 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// The DB may still be starting (docker compose). Retry instead of dying.
	delay := 250 * time.Millisecond
	deadline := time.Now().Add(30 * time.Second)
	for {
		err = pool.Ping(ctx)
		if err == nil {
			return pool, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			pool.Close()
			return nil, fmt.Errorf("ping database: %w", err)
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

// NewListenConn opens a standalone connection for LISTEN. The caller owns it.
func NewListenConn(ctx context.Context, url string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect listener: %w", err)
	}
	return conn, nil
}
