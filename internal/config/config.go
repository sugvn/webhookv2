package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/sugvn/webhookv2/internal/retry"
)

type Config struct {
	DatabaseURL  string
	Workers      int
	PollInterval time.Duration // reconcile poll; also the max latency before a backed-off retry is picked up
	LeaseBuffer  time.Duration // added to the subscription timeout to form the lease
	MaxAttempts  int
	RetryBase    time.Duration
	RetryCap     time.Duration
	HTTPAddr string
	DBPoolMax int 
}

func Load() (Config, error) {
	var errs []error
	get := func(v int, err error) int { errs = append(errs, err); return v }
	dur := func(v time.Duration, err error) time.Duration { errs = append(errs, err); return v }

	c := Config{
		DatabaseURL:  envStr("DATABASE_URL", "postgres://webhookd:webhookd@localhost:5432/webhookd"),
		Workers:      get(envInt("WORKERS", 8)),
		PollInterval: dur(envDur("POLL_INTERVAL", 5*time.Second)),
		LeaseBuffer:  dur(envDur("LEASE_BUFFER", 10*time.Second)),
		MaxAttempts:  get(envInt("MAX_ATTEMPTS", 5)),
		RetryBase:    dur(envDur("RETRY_BASE", 30*time.Second)),
		RetryCap:     dur(envDur("RETRY_CAP", 10*time.Minute)),
		HTTPAddr:     envStr("HTTP_ADDR", ":8080"),
		DBPoolMax:    get(envInt("DB_POOL_MAX", 0)),
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}

	switch {
	case c.Workers < 1:
		return Config{}, fmt.Errorf("WORKERS must be >= 1")
	case c.MaxAttempts < 1:
		return Config{}, fmt.Errorf("MAX_ATTEMPTS must be >= 1")
	case c.PollInterval <= 0 || c.LeaseBuffer <= 0 || c.RetryBase <= 0:
		return Config{}, fmt.Errorf("durations must be > 0")
	case c.RetryCap < c.RetryBase:
		return Config{}, fmt.Errorf("RETRY_CAP must be >= RETRY_BASE")
	}
	return c, nil
}

// PoolSize: one connection per worker plus headroom. The listener has its own connection.
func (c Config) PoolSize() int32 { 
	if c.DBPoolMax > 0 {
		return int32(c.DBPoolMax)
	}
	return int32(c.Workers + 2) 
}

// Retry builds the policy from the same MaxAttempts passed to claim_deliveries().
func (c Config) Retry() retry.Policy {
	return retry.Policy{MaxAttempts: c.MaxAttempts, Base: c.RetryBase, Cap: c.RetryCap}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func envDur(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
