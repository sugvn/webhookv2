package sender

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sugvn/webhookv2/internal/model"
)

func delivery(url string, timeout time.Duration) model.Delivery {
	return model.Delivery{
		DeliveryID: uuid.New(), EventUUID: uuid.New(), EventType: "invoice.paid",
		DataBody: json.RawMessage(`{"amount":42}`), Subscriber: url,
		Timeout: timeout, Attempts: 1,
	}
}

func TestSuccessAndHeaders(t *testing.T) {
	d := delivery("", time.Second)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Delivery-Id") != d.DeliveryID.String() {
			t.Errorf("missing delivery id header")
		}
		var p model.Payload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil || string(p.Data) != `{"amount":42}` {
			t.Errorf("bad payload: %v %s", err, p.Data)
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	d.Subscriber = srv.URL
	if out := New().Send(t.Context(), d); !out.Success() {
		t.Fatalf("want success, got %+v", out)
	}
}

func TestNon2xxIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", 500)
	}))
	defer srv.Close()
	out := New().Send(t.Context(), delivery(srv.URL, time.Second))
	if out.Success() || out.StatusCode != 500 || out.Err == "" {
		t.Fatalf("got %+v", out)
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer srv.Close()
	out := New().Send(t.Context(), delivery(srv.URL, time.Second))
	if out.Success() || out.StatusCode != 302 || hits.Load() != 0 {
		t.Fatalf("redirect followed or counted as success: %+v hits=%d", out, hits.Load())
	}
}

func TestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()
	out := New().Send(t.Context(), delivery(srv.URL, 50*time.Millisecond))
	if out.Success() || out.StatusCode != 0 || out.Err != "timeout" {
		t.Fatalf("got %+v", out)
	}
}
