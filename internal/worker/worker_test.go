package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sugvn/webhookv2/internal/model"
	"github.com/sugvn/webhookv2/internal/retry"
)

type fakeStore struct {
	mu        sync.Mutex
	queue     []model.Delivery
	calls     []string
	lastDelay time.Duration
	lost      bool
}

func (f *fakeStore) rec(op string, id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fmt.Sprintf("%s:%d", op, id))
}
func (f *fakeStore) Claim(context.Context, int, int, time.Duration) ([]model.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		return nil, nil
	}
	d := f.queue[0]
	f.queue = f.queue[1:]
	return []model.Delivery{d}, nil
}
func (f *fakeStore) Complete(_ context.Context, id int64, _, _ int) (bool, error) {
	f.rec("complete", id)
	return !f.lost, nil
}
func (f *fakeStore) Reschedule(_ context.Context, id int64, _ int, delay time.Duration, _ int, _ string) (bool, error) {
	f.mu.Lock()
	f.lastDelay = delay
	f.mu.Unlock()
	f.rec("reschedule", id)
	return !f.lost, nil
}
func (f *fakeStore) DeadLetter(_ context.Context, id int64, _, _ int, _ string) (bool, error) {
	f.rec("dead", id)
	return !f.lost, nil
}
func (f *fakeStore) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type fakeSender func(model.Delivery) model.Outcome

func (f fakeSender) Send(_ context.Context, d model.Delivery) model.Outcome { return f(d) }

func newPool(s Store, snd Sender, wake <-chan struct{}) *Pool {
	return New(s, snd, Config{Workers: 3, LeaseBuffer: time.Second, PollInterval: time.Hour, Policy: retry.Default()},
		wake, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestOutcomes(t *testing.T) {
	ok := model.Outcome{StatusCode: 200}
	bad := model.Outcome{StatusCode: 500, Err: "status 500"}
	cases := []struct {
		name     string
		out      model.Outcome
		attempts int
		lost     bool
		want     string
	}{
		{"success", ok, 1, false, "complete:1"},
		{"retry", bad, 1, false, "reschedule:1"},
		{"last attempt dead-letters", bad, 5, false, "dead:1"},
		{"lost lease is not fatal", ok, 1, true, "complete:1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs := &fakeStore{lost: c.lost}
			p := newPool(fs, fakeSender(func(model.Delivery) model.Outcome { return c.out }), nil)
			p.process(context.Background(), model.Delivery{ID: 1, Attempts: c.attempts})
			if got := fs.snapshot(); len(got) != 1 || got[0] != c.want {
				t.Fatalf("calls=%v want [%s]", got, c.want)
			}
			if c.want == "reschedule:1" && (fs.lastDelay < 15*time.Second || fs.lastDelay > 30*time.Second) {
				t.Fatalf("delay %v outside [15s,30s]", fs.lastDelay)
			}
		})
	}
}

func TestRunDrainsOnWake(t *testing.T) {
	fs := &fakeStore{}
	wake := make(chan struct{}, 1)
	p := newPool(fs, fakeSender(func(model.Delivery) model.Outcome { return model.Outcome{StatusCode: 200} }), wake)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	fs.mu.Lock()
	for i := int64(1); i <= 5; i++ {
		fs.queue = append(fs.queue, model.Delivery{ID: i, Attempts: 1})
	}
	fs.mu.Unlock()
	wake <- struct{}{}

	deadline := time.Now().Add(2 * time.Second)
	for len(fs.snapshot()) < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := len(fs.snapshot()); n != 5 {
		t.Fatalf("processed %d of 5 after one wake (poll is 1h)", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
