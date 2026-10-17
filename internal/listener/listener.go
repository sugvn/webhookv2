package listener

import (
	"context"
	"log/slog"
	"time"

	"github.com/sugvn/webhookv2/internal/db"
)

const (
	idlePing    = 30 * time.Second // max silence before proving the connection is alive
	minBackoff  = 500 * time.Millisecond
	maxBackoff  = 10 * time.Second
	channelName = "deliveries"
)

// Listener turns Postgres NOTIFYs into coalesced wake signals. Wakes are only hints:
// the worker's poll is the correctness backstop.
type Listener struct {
	url  string
	wake chan struct{} // buffered(1): bursts coalesce into one signal
	log  *slog.Logger
}

func New(url string, log *slog.Logger) *Listener {
	return &Listener{url: url, wake: make(chan struct{}, 1), log: log}
}

func (l *Listener) Wake() <-chan struct{} { return l.wake }

func (l *Listener) kick() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is cancelled, reconnecting with backoff.
func (l *Listener) Run(ctx context.Context) {
	backoff := minBackoff
	for ctx.Err() == nil {
		up, err := l.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if up {
			backoff = minBackoff // it worked for a while; start retries fast again
		}
		l.log.Warn("listener disconnected, reconnecting", "err", err, "in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// session runs one connection's lifetime. up reports whether LISTEN was established.
func (l *Listener) session(ctx context.Context) (up bool, err error) {
	conn, err := db.NewListenConn(ctx, l.url)
	if err != nil {
		return false, err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+channelName); err != nil {
		return false, err
	}
	// Anything committed while we were disconnected sent a NOTIFY nobody heard.
	// Kick AFTER LISTEN is active, so there's no gap between the drain and the subscription.
	l.kick()

	for {
		wctx, cancel := context.WithTimeout(ctx, idlePing)
		_, err := conn.WaitForNotification(wctx)
		idle := wctx.Err() != nil
		cancel()

		switch {
		case err == nil:
			l.kick()
		case ctx.Err() != nil:
			return true, ctx.Err()
		case idle:
			// Quiet period. A half-open TCP connection looks identical to silence,
			// so prove the link works instead of trusting the wait.
			pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
			perr := conn.Ping(pctx)
			pcancel()
			if perr != nil {
				return true, perr
			}
		default:
			return true, err
		}
	}
}
