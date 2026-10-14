package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Arbitrary app-wide constant. All instances must use the same key.
const migrateLockKey = 726001

// Migrate applies unapplied migrations in filename order, each in its own transaction.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Advisory locks are per-session, so lock, migrate and unlock on ONE connection.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer func() { // runs before Release; ctx may be cancelled, so use Background
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("load applied: %w", err)
	}
	for rows.Next() {
		var v, c string
		if err := rows.Scan(&v, &c); err != nil {
			rows.Close()
			return err
		}
		applied[v] = c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	entries, err := fs.ReadDir(migrationFiles, "migrations") // sorted by filename
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := fs.ReadFile(migrationFiles, "migrations/"+e.Name())
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])

		if prev, ok := applied[e.Name()]; ok {
			if prev != checksum {
				return fmt.Errorf("migration %s was edited after being applied; add a new migration instead", e.Name())
			}
			continue
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		// No args => simple protocol, so a multi-statement file is fine.
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`,
			e.Name(), checksum); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record %s: %w", e.Name(), err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit %s: %w", e.Name(), err)
		}
		slog.Info("applied migration", "version", e.Name())
	}
	return nil
}
