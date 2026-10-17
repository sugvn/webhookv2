package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sugvn/webhookv2/internal/api"
	"github.com/sugvn/webhookv2/internal/config"
	"github.com/sugvn/webhookv2/internal/db"
	"github.com/sugvn/webhookv2/internal/listener"
	"github.com/sugvn/webhookv2/internal/registry"
	"github.com/sugvn/webhookv2/internal/sender"
	"github.com/sugvn/webhookv2/internal/store"
	"github.com/sugvn/webhookv2/internal/worker"
)

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

	st := store.New(pool)
	lis := listener.New(cfg.DatabaseURL, log)
	wp := worker.New(st, sender.New(), worker.Config{
		Workers:      cfg.Workers,
		LeaseBuffer:  cfg.LeaseBuffer,
		PollInterval: cfg.PollInterval,
		Policy:       cfg.Retry(),
	}, lis.Wake(), log)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(registry.New(st, 0), st, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); lis.Run(ctx) }()
	go func() { defer wg.Done(); wp.Run(ctx) }()
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			stop() // take the whole process down
		}
	}()

	log.Info("webhookd started", "http", cfg.HTTPAddr, "workers", cfg.Workers,
		"max_attempts", cfg.MaxAttempts, "poll", cfg.PollInterval, "lease_buffer", cfg.LeaseBuffer)

	<-ctx.Done()
	stop() // a second Ctrl-C kills immediately
	log.Info("shutting down", "grace", shutdownGrace)

	sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = srv.Shutdown(sctx) // stop accepting events, finish requests in flight
		wg.Wait()              // workers finish their current send
		close(done)
	}()
	select {
	case <-done:
		log.Info("stopped cleanly")
		return nil
	case <-sctx.Done():
		log.Error("grace period exceeded; in-flight leases will expire and be reclaimed")
		os.Exit(1) // not return: the deferred pool.Close() would block on stuck connections
		return nil
	}
}
