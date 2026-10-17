package listener

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestWakeAndReconnect(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := New(url, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()

	expectWake := func(what string) {
		t.Helper()
		select {
		case <-l.Wake():
		case <-time.After(5 * time.Second):
			t.Fatalf("no wake: %s", what)
		}
	}
	expectWake("initial connect") // sent only after LISTEN is active

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)

	if _, err := admin.Exec(ctx, `SELECT pg_notify('deliveries', '')`); err != nil {
		t.Fatal(err)
	}
	expectWake("notify")

	// Kill the listener's backend; it must reconnect and kick (notifications were missed).
	var n int
	err = admin.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE query = 'LISTEN deliveries' AND pid <> pg_backend_pid()) s`).Scan(&n)
	if err != nil || n != 1 {
		t.Fatalf("terminated %d listener backends (err=%v), want 1", n, err)
	}
	expectWake("reconnect")

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not exit after cancel")
	}
}
