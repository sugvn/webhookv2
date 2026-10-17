package registry

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/sugvn/webhookv2/internal/model"
)

var ErrUnauthorized = errors.New("invalid producer credentials")

// ValidationError is a caller mistake: map it to HTTP 400.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, a ...any) error { return &ValidationError{fmt.Sprintf(format, a...)} }

// Satisfied by *store.Store.
type Store interface {
	ProducerByName(ctx context.Context, name string) (id int64, passwordHash string, found bool, err error)
	CreateProducer(ctx context.Context, name, passwordHash string) (id int64, created bool, err error)
	RegisterProducer(ctx context.Context, name, passwordHash string, subs []model.SubscriptionSpec) (int64, error)
}

const (
	minPassword = 8
	maxPassword = 72 // bcrypt silently/loudly can't use more
	maxSpecs    = 100
	maxTimeout  = 60000 // keep in sync with 004_timeout_cap.sql
	cacheTTL    = 5 * time.Minute
)

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type cacheEntry struct {
	id      int64
	expires time.Time
}

type Registry struct {
	store     Store
	cost      int
	dummyHash []byte // compared against when the producer doesn't exist, to equalize timing

	mu    sync.Mutex
	cache map[[32]byte]cacheEntry // successful credentials only
}

func New(s Store, bcryptCost int) *Registry {
	if bcryptCost == 0 {
		bcryptCost = bcrypt.DefaultCost
	}
	dummy, _ := bcrypt.GenerateFromPassword([]byte("dummy-password"), bcryptCost)
	return &Registry{store: s, cost: bcryptCost, dummyHash: dummy, cache: map[[32]byte]cacheEntry{}}
}

// Register creates the producer on first use, otherwise requires the correct password,
// then adds the given subscriptions. It never changes an existing password.
func (r *Registry) Register(ctx context.Context, name, password string, specs []model.SubscriptionSpec) (int64, error) {
	if err := validateCreds(name, password); err != nil {
		return 0, err
	}
	if err := validateSpecs(specs); err != nil {
		return 0, err
	}
	id, hash, err := r.authenticateOrCreate(ctx, name, password)
	if err != nil {
		return 0, err
	}
	if _, err := r.store.RegisterProducer(ctx, name, hash, specs); err != nil {
		return 0, err
	}
	return id, nil
}

func (r *Registry) authenticateOrCreate(ctx context.Context, name, password string) (int64, string, error) {
	id, hash, found, err := r.store.ProducerByName(ctx, name)
	if err != nil {
		return 0, "", err
	}
	if !found {
		newHash, err := bcrypt.GenerateFromPassword([]byte(password), r.cost)
		if err != nil {
			return 0, "", fmt.Errorf("hash password: %w", err)
		}
		newID, created, err := r.store.CreateProducer(ctx, name, string(newHash))
		if err != nil {
			return 0, "", err
		}
		if created {
			return newID, string(newHash), nil
		}
		// Lost the race: someone else created this name between our lookup and insert.
		// Treat them as the owner and verify against THEIR hash.
		id, hash, found, err = r.store.ProducerByName(ctx, name)
		if err != nil {
			return 0, "", err
		}
		if !found {
			return 0, "", errors.New("producer disappeared during registration")
		}
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return 0, "", ErrUnauthorized
	}
	return id, hash, nil
}

// Authenticate verifies credentials for the events endpoint. Successes are cached
// briefly: bcrypt costs tens of milliseconds, which would cap event throughput.
func (r *Registry) Authenticate(ctx context.Context, name, password string) (int64, error) {
	if len(password) == 0 || len(password) > maxPassword {
		return 0, ErrUnauthorized
	}
	key := sha256.Sum256([]byte(name + "\x00" + password))
	if id, ok := r.cached(key); ok {
		return id, nil
	}
	id, hash, found, err := r.store.ProducerByName(ctx, name)
	if err != nil {
		return 0, err
	}
	if !found {
		_ = bcrypt.CompareHashAndPassword(r.dummyHash, []byte(password)) // same cost as a real miss
		return 0, ErrUnauthorized
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return 0, ErrUnauthorized
	}
	r.remember(key, id)
	return id, nil
}

func (r *Registry) cached(key [32]byte) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cache[key]
	if ok && time.Now().After(e.expires) {
		delete(r.cache, key)
		return 0, false
	}
	return e.id, ok
}

func (r *Registry) remember(key [32]byte, id int64) {
	r.mu.Lock()
	r.cache[key] = cacheEntry{id: id, expires: time.Now().Add(cacheTTL)}
	r.mu.Unlock()
}

func validateCreds(name, password string) error {
	if !nameRE.MatchString(name) {
		return invalid("name must be 1-64 chars of letters, digits, '.', '_' or '-'")
	}
	if len(password) < minPassword || len(password) > maxPassword {
		return invalid("password must be %d-%d bytes", minPassword, maxPassword)
	}
	return nil
}

func validateSpecs(specs []model.SubscriptionSpec) error {
	if len(specs) == 0 || len(specs) > maxSpecs {
		return invalid("subscriptions must contain 1-%d entries", maxSpecs)
	}
	for i, s := range specs {
		if s.EventType == "" || len(s.EventType) > 128 {
			return invalid("subscriptions[%d]: event_type must be 1-128 chars", i)
		}
		u, err := url.Parse(s.Subscriber)
		if err != nil || len(s.Subscriber) > 2048 || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return invalid("subscriptions[%d]: subscriber must be an http(s) URL with a host", i)
		}
		if u.User != nil {
			return invalid("subscriptions[%d]: credentials in the URL are not allowed", i)
		}
		if s.TimeoutMs != nil && (*s.TimeoutMs < 1 || *s.TimeoutMs > maxTimeout) {
			return invalid("subscriptions[%d]: timeout_ms must be 1-%d", i, maxTimeout)
		}
	}
	return nil
}
