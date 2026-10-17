package store

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/sugvn/webhookv2/internal/model"
)

func TestInsertEventIdempotent(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	pid, _, found, err := s.ProducerByName(ctx, "t")
	if err != nil || !found {
		t.Fatalf("producer: %v found=%v", err, found)
	}

	u := uuid.New()
	for i, want := range []bool{true, false} {
		got, err := s.InsertEvent(ctx, pid, u, "x", json.RawMessage(`{"n":1}`))
		if err != nil || got != want {
			t.Fatalf("call %d: inserted=%v err=%v want %v", i, got, err, want)
		}
	}
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM deliveries`).Scan(&n)
	if n != 2 { // 1 from the seed event + 1 from u; the duplicate added nothing
		t.Fatalf("deliveries=%d want 2", n)
	}
	if _, err := s.InsertEvent(ctx, pid, uuid.New(), "x", json.RawMessage(`{bad`)); err != ErrInvalidJSON {
		t.Fatalf("want ErrInvalidJSON, got %v", err)
	}
}

func TestCreateProducerOnce(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	if _, created, err := s.CreateProducer(ctx, "fresh", "h1"); err != nil || !created {
		t.Fatalf("first: created=%v err=%v", created, err)
	}
	if _, created, err := s.CreateProducer(ctx, "fresh", "h2"); err != nil || created {
		t.Fatalf("second: created=%v err=%v", created, err)
	}
}

func TestRegisterAdditiveIdempotent(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	specs := []model.SubscriptionSpec{{EventType: "y", Subscriber: "http://localhost:2/h"}}
	for i := 0; i < 2; i++ {
		if _, err := s.RegisterProducer(ctx, "t", "ignored", specs); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM subscriptions`).Scan(&n)
	if n != 2 { // seed subscription + the new one, registered twice
		t.Fatalf("subscriptions=%d want 2", n)
	}
}
