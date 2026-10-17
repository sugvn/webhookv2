package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sugvn/webhookv2/internal/model"
	"github.com/sugvn/webhookv2/internal/retry"
)

// Store and Sender are satisfied by *store.Store and *sender.Sender.
type Store interface {
	Claim(ctx context.Context, limit, maxAttempts int, leaseBuffer time.Duration) ([]model.Delivery, error)
	Complete(ctx context.Context, id int64, attempts, statusCode int) (bool, error)
	Reschedule(ctx context.Context, id int64, attempts int, delay time.Duration, statusCode int, errMsg string) (bool, error)
	DeadLetter(ctx context.Context, id int64, attempts, statusCode int, errMsg string) (bool, error)
}

type Sender interface {
	Send(ctx context.Context, d model.Delivery) model.Outcome
}

type Config struct {
	Workers      int
	LeaseBuffer  time.Duration
	PollInterval time.Duration
	Policy       retry.Policy // Policy.MaxAttempts is also what claim_deliveries() receives
}

type Pool struct {
	store  Store
	sender Sender
	cfg    Config
	wake   <-chan struct{} // from listener; may be nil (poll-only)
	log    *slog.Logger
}

func New(s Store, snd Sender, cfg Config, wake <-chan struct{}, log *slog.Logger) *Pool {
	return &Pool{store: s, sender: snd, cfg: cfg, wake: wake, log: log}
}

// Run blocks until ctx is cancelled and every worker has finished its current delivery.
func (p *Pool) Run(ctx context.Context) {
	kicks := make([]chan struct{}, p.cfg.Workers)
	var wg sync.WaitGroup
	for i := range kicks {
		kicks[i] = make(chan struct{}, 1)
		kicks[i] <- struct{}{} // initial drain at startup
		wg.Add(1)
		go func(k <-chan struct{}) {
			defer wg.Done()
			p.workerLoop(ctx, k)
		}(kicks[i])
	}

	tick := time.NewTicker(p.cfg.PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-p.wake:
		case <-tick.C:
		}
		for _, k := range kicks { // wake every worker; SKIP LOCKED makes the herd cheap
			select {
			case k <- struct{}{}:
			default:
			}
		}
	}
}

func (p *Pool) workerLoop(ctx context.Context, kick <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-kick:
		}
		for ctx.Err() == nil { // drain until the queue is empty
			ds, err := p.store.Claim(ctx, 1, p.cfg.Policy.MaxAttempts, p.cfg.LeaseBuffer)
			if err != nil {
				if ctx.Err() == nil {
					p.log.Error("claim failed", "err", err)
				}
				break // wait for the next kick / poll
			}
			if len(ds) == 0 {
				break
			}
			p.process(ctx, ds[0])
		}
	}
}

func (p *Pool) process(ctx context.Context, d model.Delivery) {
	defer func() {
		if r := recover(); r != nil { // lease expires and the row is retried
			p.log.Error("panic processing delivery", "id", d.ID, "panic", r)
		}
	}()

	// Detached from shutdown: a send that started always finishes (bounded by its own
	// timeout) and its result is always recorded.
	bg := context.WithoutCancel(ctx)
	out := p.sender.Send(bg, d)

	wctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()

	var (
		held   bool
		err    error
		action string
	)
	if out.Success() {
		action = "succeeded"
		held, err = p.store.Complete(wctx, d.ID, d.Attempts, out.StatusCode)
	} else if delay, dead := p.cfg.Policy.Next(d.Attempts); dead {
		action = "dead"
		held, err = p.store.DeadLetter(wctx, d.ID, d.Attempts, out.StatusCode, out.Err)
	} else {
		action = "retry"
		held, err = p.store.Reschedule(wctx, d.ID, d.Attempts, delay, out.StatusCode, out.Err)
	}

	log := p.log.With("id", d.ID, "delivery_id", d.DeliveryID, "attempt", d.Attempts, "status", out.StatusCode)
	switch {
	case err != nil: // row stays in_flight; it will be re-sent after the lease expires
		log.Error("recording outcome failed", "action", action, "err", err)
	case !held:
		log.Warn("lost lease before recording outcome", "action", action)
	case action == "succeeded":
		log.Debug("delivered")
	case action == "retry":
		log.Warn("delivery failed, will retry", "err", out.Err)
	default:
		log.Error("delivery dead-lettered", "err", out.Err)
	}
}
