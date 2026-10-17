package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sugvn/webhookv2/internal/config"
	"github.com/sugvn/webhookv2/internal/db"
	"github.com/sugvn/webhookv2/internal/listener"
	"github.com/sugvn/webhookv2/internal/sender"
	"github.com/sugvn/webhookv2/internal/store"
	"github.com/sugvn/webhookv2/internal/worker"
)

// Max wait for in-flight sends after a signal. Beyond this we exit and let leases expire.
const shutdownGrace = 30 * time.Second

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, cfg.PoolSize())
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}

	lis := listener.New(cfg.DatabaseURL, log)
	wp := worker.New(store.New(pool), sender.New(), worker.Config{
		Workers:      cfg.Workers,
		LeaseBuffer:  cfg.LeaseBuffer,
		PollInterval: cfg.PollInterval,
		Policy:       cfg.Retry(), // same MaxAttempts goes to claim_deliveries()
	}, lis.Wake(), log)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); lis.Run(ctx) }()
	go func() { defer wg.Done(); wp.Run(ctx) }()

	log.Info("webhookd started", "workers", cfg.Workers, "max_attempts", cfg.MaxAttempts,
		"poll", cfg.PollInterval, "lease_buffer", cfg.LeaseBuffer)

	<-ctx.Done()
	stop() // restore default handling: a second Ctrl-C kills the process immediately
	log.Info("shutting down", "grace", shutdownGrace)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Info("stopped cleanly")
		return nil
	case <-time.After(shutdownGrace):
		// os.Exit, not return: the deferred pool.Close() blocks until every connection
		// is released, which is exactly what is stuck here.
		log.Error("grace period exceeded; in-flight leases will expire and be reclaimed")
		os.Exit(1)
		return nil
	}
}
