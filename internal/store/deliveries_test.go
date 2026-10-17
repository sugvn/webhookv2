package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sugvn/webhookv2/internal/db"
)

func testStore(t *testing.T) (*Store, *pgxpool.Pool) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := db.NewPool(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE producers RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	var pid int64
	err = pool.QueryRow(ctx, `SELECT register_producer('t', 'h',
		'[{"event_type":"x","subscriber":"http://localhost:1/h"}]'::jsonb)`).Scan(&pid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO events (uuid, producer_id, event_type, data_body)
		VALUES (gen_random_uuid(), $1, 'x', '{"a":1}')`, pid); err != nil {
		t.Fatal(err)
	}
	return New(pool), pool
}

func TestFencing(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()

	first, err := s.Claim(ctx, 10, 5, 5*time.Second)
	if err != nil || len(first) != 1 || first[0].Attempts != 1 {
		t.Fatalf("first claim: %v %+v", err, first)
	}

	// Lease expires; a second worker reclaims the same row.
	pool.Exec(ctx, `UPDATE deliveries SET lease_expires_at = now() - interval '1 second'`)
	second, err := s.Claim(ctx, 10, 5, 5*time.Second)
	if err != nil || len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("reclaim: %v %+v", err, second)
	}

	// The stale worker must not be able to complete it.
	if held, err := s.Complete(ctx, first[0].ID, first[0].Attempts, 200); err != nil || held {
		t.Fatalf("stale complete: held=%v err=%v", held, err)
	}
	if held, err := s.Complete(ctx, second[0].ID, second[0].Attempts, 200); err != nil || !held {
		t.Fatalf("current complete: held=%v err=%v", held, err)
	}
	if again, _ := s.Claim(ctx, 10, 5, 5*time.Second); len(again) != 0 {
		t.Fatalf("succeeded row was claimable again: %+v", again)
	}
}

func TestRescheduleThenDead(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()

	d, _ := s.Claim(ctx, 1, 5, 5*time.Second)
	if len(d) != 1 {
		t.Fatal("no claim")
	}
	if held, err := s.Reschedule(ctx, d[0].ID, d[0].Attempts, time.Hour, 500, "bad\x00\xffbody"); err != nil || !held {
		t.Fatalf("reschedule: held=%v err=%v", held, err)
	}
	if got, _ := s.Claim(ctx, 1, 5, 5*time.Second); len(got) != 0 {
		t.Fatalf("backoff row claimed early: %+v", got)
	}
	// Row is pending now, so a late dead-letter from the old claim must be rejected.
	if held, _ := s.DeadLetter(ctx, d[0].ID, d[0].Attempts, 500, "x"); held {
		t.Fatal("dead-letter succeeded on a non-in-flight row")
	}
}
