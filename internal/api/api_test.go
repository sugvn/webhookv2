package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/sugvn/webhookv2/internal/model"
	"github.com/sugvn/webhookv2/internal/registry"
	"github.com/sugvn/webhookv2/internal/store"
)

type fakeReg struct {
	regErr, authErr error
	regCalls        int
	authCalls       int
	gotName         string
	gotSpecs        []model.SubscriptionSpec
}

func (f *fakeReg) Register(_ context.Context, name, _ string, specs []model.SubscriptionSpec) (int64, error) {
	f.regCalls++
	f.gotName, f.gotSpecs = name, specs
	return 7, f.regErr
}

func (f *fakeReg) Authenticate(context.Context, string, string) (int64, error) {
	f.authCalls++
	return 7, f.authErr
}

type fakeEv struct {
	inserted bool
	err      error
	calls    int
	gotPID   int64
	gotData  json.RawMessage
}

func (f *fakeEv) InsertEvent(_ context.Context, pid int64, _ uuid.UUID, _ string, data json.RawMessage) (bool, error) {
	f.calls++
	f.gotPID, f.gotData = pid, data
	return f.inserted, f.err
}

func newAPI(reg *fakeReg, ev *fakeEv) http.Handler {
	return New(reg, ev, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func do(h http.Handler, method, path, body, user string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if user != "" {
		req.SetBasicAuth(user, "pw-long-enough")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func jsonField(t *testing.T, rec *httptest.ResponseRecorder, key string) any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("response is not JSON: %q", rec.Body.String())
	}
	return m[key]
}

const goodSub = `{"subscriptions":[{"event_type":"order.created","subscriber":"http://localhost:9000/hook","timeout_ms":3000}]}`

func TestRegister(t *testing.T) {
	bigSub := `{"subscriptions":[{"event_type":"x","subscriber":"http://a/` + strings.Repeat("a", 70<<10) + `"}]}`
	cases := []struct {
		name    string
		user    string
		body    string
		regErr  error
		want    int
		errPart string
		noCall  bool // the registry must not be reached
	}{
		{"ok", "shop", goodSub, nil, 200, "", false},
		{"no credentials", "", goodSub, nil, 401, "credentials", true},
		{"registry says unauthorized", "shop", goodSub, registry.ErrUnauthorized, 401, "credentials", false},
		{"validation error", "shop", goodSub, &registry.ValidationError{Msg: "bad subscriber"}, 400, "bad subscriber", false},
		{"unknown field", "shop", `{"subscriptions":[],"extra":1}`, nil, 400, "invalid request body", true},
		{"trailing garbage", "shop", goodSub + `{}`, nil, 400, "invalid request body", true},
		{"not json", "shop", `nope`, nil, 400, "invalid request body", true},
		{"too large", "shop", bigSub, nil, 413, "too large", true},
		{"unexpected failure", "shop", goodSub, errors.New("pq: connection refused"), 500, "internal error", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reg := &fakeReg{regErr: c.regErr}
			rec := do(newAPI(reg, &fakeEv{}), "POST", "/register", c.body, c.user)

			if rec.Code != c.want {
				t.Fatalf("status=%d want %d body=%s", rec.Code, c.want, rec.Body)
			}
			if c.errPart != "" {
				if msg, _ := jsonField(t, rec, "error").(string); !strings.Contains(msg, c.errPart) {
					t.Fatalf("error=%q want substring %q", msg, c.errPart)
				}
			}
			if c.want == 401 && rec.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("401 without a WWW-Authenticate header")
			}
			if c.want == 500 && strings.Contains(rec.Body.String(), "refused") {
				t.Fatal("internal error text leaked to the client")
			}
			if c.noCall && reg.regCalls != 0 {
				t.Fatal("registry was called for a request that should be rejected earlier")
			}
			if c.want == 200 {
				if jsonField(t, rec, "producer_id") != float64(7) {
					t.Fatalf("body=%s", rec.Body)
				}
				if reg.gotName != "shop" || len(reg.gotSpecs) != 1 ||
					reg.gotSpecs[0].TimeoutMs == nil || *reg.gotSpecs[0].TimeoutMs != 3000 {
					t.Fatalf("registry got name=%q specs=%+v", reg.gotName, reg.gotSpecs)
				}
			}
		})
	}
}

func TestEvents(t *testing.T) {
	id := uuid.NewString()
	good := fmt.Sprintf(`{"event_id":%q,"event_type":"order.created","data":{"total":99}}`, id)
	huge := fmt.Sprintf(`{"event_id":%q,"event_type":"x","data":"%s"}`, id, strings.Repeat("a", 1<<20+10))

	cases := []struct {
		name       string
		user       string
		body       string
		authErr    error
		inserted   bool
		insErr     error
		want       int
		errPart    string
		wantInsert bool
	}{
		{"accepted", "shop", good, nil, true, nil, 201, "", true},
		{"duplicate", "shop", good, nil, false, nil, 200, "", true},
		{"no credentials", "", good, nil, true, nil, 401, "", false},
		{"bad credentials", "shop", good, registry.ErrUnauthorized, true, nil, 401, "", false},
		{"bad credentials beat a huge body", "shop", huge, registry.ErrUnauthorized, true, nil, 401, "", false},
		{"missing event_id", "shop", `{"event_type":"x","data":{}}`, nil, true, nil, 400, "event_id", false},
		{"malformed event_id", "shop", `{"event_id":"not-a-uuid","event_type":"x","data":{}}`, nil, true, nil, 400, "invalid request body", false},
		{"empty event_type", "shop", fmt.Sprintf(`{"event_id":%q,"event_type":"","data":{}}`, id), nil, true, nil, 400, "event_type", false},
		{"missing data", "shop", fmt.Sprintf(`{"event_id":%q,"event_type":"x"}`, id), nil, true, nil, 400, "data", false},
		{"store rejects JSON", "shop", good, nil, false, store.ErrInvalidJSON, 400, "valid JSON", true},
		{"too large", "shop", huge, nil, true, nil, 413, "too large", false},
		{"store failure", "shop", good, nil, false, errors.New("pq: boom"), 500, "internal error", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reg := &fakeReg{authErr: c.authErr}
			ev := &fakeEv{inserted: c.inserted, err: c.insErr}
			rec := do(newAPI(reg, ev), "POST", "/events", c.body, c.user)

			if rec.Code != c.want {
				t.Fatalf("status=%d want %d body=%s", rec.Code, c.want, rec.Body)
			}
			if c.errPart != "" {
				if msg, _ := jsonField(t, rec, "error").(string); !strings.Contains(msg, c.errPart) {
					t.Fatalf("error=%q want substring %q", msg, c.errPart)
				}
			}
			if (ev.calls == 1) != c.wantInsert || ev.calls > 1 {
				t.Fatalf("InsertEvent calls=%d, wantInsert=%v", ev.calls, c.wantInsert)
			}
			if c.user == "" && reg.authCalls != 0 {
				t.Fatal("Authenticate called without credentials")
			}
			if c.want == 500 && strings.Contains(rec.Body.String(), "boom") {
				t.Fatal("internal error text leaked to the client")
			}
			switch c.name {
			case "accepted":
				if jsonField(t, rec, "status") != "accepted" || ev.gotPID != 7 || string(ev.gotData) != `{"total":99}` {
					t.Fatalf("pid=%d data=%s body=%s", ev.gotPID, ev.gotData, rec.Body)
				}
			case "duplicate":
				if jsonField(t, rec, "status") != "duplicate" {
					t.Fatalf("body=%s", rec.Body)
				}
			}
		})
	}
}

func TestRoutes(t *testing.T) {
	h := newAPI(&fakeReg{}, &fakeEv{})
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/healthz", 200},
		{"GET", "/events", 405},
		{"GET", "/register", 405},
		{"POST", "/nope", 404},
	} {
		if rec := do(h, c.method, c.path, "", ""); rec.Code != c.want {
			t.Errorf("%s %s: status=%d want %d", c.method, c.path, rec.Code, c.want)
		}
	}
}
