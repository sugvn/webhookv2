package registry

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/sugvn/webhookv2/internal/model"
)

type rec struct {
	id   int64
	hash string
}

type fakeStore struct {
	mu           sync.Mutex
	producers    map[string]rec
	nextID       int64
	lookups      int
	registered   int
	beforeCreate func() // simulates a concurrent registration winning the race
}

func newFake() *fakeStore { return &fakeStore{producers: map[string]rec{}, nextID: 1} }

func (f *fakeStore) put(name, password string) {
	h, _ := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.producers[name] = rec{f.nextID, string(h)}
	f.nextID++
}

func (f *fakeStore) ProducerByName(_ context.Context, name string) (int64, string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	r, ok := f.producers[name]
	return r.id, r.hash, ok, nil
}

func (f *fakeStore) CreateProducer(_ context.Context, name, hash string) (int64, bool, error) {
	if f.beforeCreate != nil {
		f.beforeCreate()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.producers[name]; ok {
		return 0, false, nil
	}
	id := f.nextID
	f.nextID++
	f.producers[name] = rec{id, hash}
	return id, true, nil
}

func (f *fakeStore) RegisterProducer(_ context.Context, name, _ string, _ []model.SubscriptionSpec) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered++
	return f.producers[name].id, nil
}

func spec(u string) []model.SubscriptionSpec {
	return []model.SubscriptionSpec{{EventType: "x", Subscriber: u}}
}

const good = "http://localhost:1/h"

func TestRegisterNewThenReturning(t *testing.T) {
	f := newFake()
	r := New(f, bcrypt.MinCost)
	ctx := context.Background()

	id1, err := r.Register(ctx, "billing", "correct horse", spec(good))
	if err != nil {
		t.Fatal(err)
	}
	if h := f.producers["billing"].hash; h == "" || h == "correct horse" {
		t.Fatalf("password not hashed: %q", h)
	}
	id2, err := r.Register(ctx, "billing", "correct horse", spec(good))
	if err != nil || id2 != id1 {
		t.Fatalf("re-register: id=%d err=%v want id=%d", id2, err, id1)
	}
	before := f.registered
	if _, err := r.Register(ctx, "billing", "wrong password", spec(good)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong password: %v", err)
	}
	if f.registered != before {
		t.Fatal("subscriptions were modified by an unauthenticated caller")
	}
}

func TestRegisterRaceDoesNotHijack(t *testing.T) {
	f := newFake()
	f.beforeCreate = func() { f.put("billing", "owner-password") }
	r := New(f, bcrypt.MinCost)

	_, err := r.Register(context.Background(), "billing", "attacker-password", spec(good))
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
	if f.registered != 0 {
		t.Fatal("attacker added subscriptions to someone else's producer")
	}
}

func TestValidation(t *testing.T) {
	zero, big := 0, 60001
	cases := []struct {
		name, user, pass string
		specs            []model.SubscriptionSpec
	}{
		{"empty name", "", "password123", spec(good)},
		{"bad name chars", "bill ing", "password123", spec(good)},
		{"short password", "a", "short", spec(good)},
		{"long password", "a", strings.Repeat("x", 73), spec(good)},
		{"no specs", "a", "password123", nil},
		{"ftp scheme", "a", "password123", spec("ftp://x/h")},
		{"userinfo", "a", "password123", spec("http://u:p@x/h")},
		{"no host", "a", "password123", spec("http:///h")},
		{"empty event type", "a", "password123", []model.SubscriptionSpec{{Subscriber: good}}},
		{"timeout zero", "a", "password123", []model.SubscriptionSpec{{EventType: "x", Subscriber: good, TimeoutMs: &zero}}},
		{"timeout too big", "a", "password123", []model.SubscriptionSpec{{EventType: "x", Subscriber: good, TimeoutMs: &big}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake()
			_, err := New(f, bcrypt.MinCost).Register(context.Background(), c.user, c.pass, c.specs)
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("want ValidationError, got %v", err)
			}
			if len(f.producers) != 0 {
				t.Fatal("producer was created despite invalid input")
			}
		})
	}
}

func TestAuthenticate(t *testing.T) {
	f := newFake()
	f.put("billing", "correct horse")
	r := New(f, bcrypt.MinCost)
	ctx := context.Background()

	if _, err := r.Authenticate(ctx, "nobody", "whatever1"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown producer: %v", err)
	}
	if _, err := r.Authenticate(ctx, "billing", "wrong"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong password: %v", err)
	}
	f.lookups = 0
	for i := 0; i < 3; i++ {
		if id, err := r.Authenticate(ctx, "billing", "correct horse"); err != nil || id != 1 {
			t.Fatalf("good credentials: id=%d err=%v", id, err)
		}
	}
	if f.lookups != 1 {
		t.Fatalf("lookups=%d want 1 (successes should be cached)", f.lookups)
	}
	f.lookups = 0
	r.Authenticate(ctx, "billing", "wrong")
	r.Authenticate(ctx, "billing", "wrong")
	if f.lookups != 2 {
		t.Fatalf("lookups=%d want 2 (failures must never be cached)", f.lookups)
	}
}
