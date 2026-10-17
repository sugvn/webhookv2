package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/sugvn/webhookv2/internal/model"
	"github.com/sugvn/webhookv2/internal/registry"
	"github.com/sugvn/webhookv2/internal/store"
)

type Registry interface {
	Register(ctx context.Context, name, password string, specs []model.SubscriptionSpec) (int64, error)
	Authenticate(ctx context.Context, name, password string) (int64, error)
}

type Events interface {
	InsertEvent(ctx context.Context, producerID int64, eventUUID uuid.UUID, eventType string, data json.RawMessage) (bool, error)
}

type api struct {
	reg Registry
	ev  Events
	log *slog.Logger
}

func New(reg Registry, ev Events, log *slog.Logger) http.Handler {
	a := &api{reg: reg, ev: ev, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", a.register)
	mux.HandleFunc("POST /events", a.postEvent)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// POST /register  (Basic auth = producer name:password)
// {"subscriptions":[{"event_type":"invoice.paid","subscriber":"https://...","timeout_ms":3000}]}
func (a *api) register(w http.ResponseWriter, r *http.Request) {
	name, pass, ok := r.BasicAuth()
	if !ok {
		a.unauthorized(w)
		return
	}
	var body struct {
		Subscriptions []model.SubscriptionSpec `json:"subscriptions"`
	}
	if !a.decode(w, r, 64<<10, &body) {
		return
	}
	id, err := a.reg.Register(r.Context(), name, pass, body.Subscriptions)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"producer_id": id})
}

// POST /events  (Basic auth)
// {"event_id":"<uuid>","event_type":"invoice.paid","data":{...}}
// 201 = stored, 200 = duplicate event_id (safe retry).
func (a *api) postEvent(w http.ResponseWriter, r *http.Request) {
	name, pass, ok := r.BasicAuth()
	if !ok {
		a.unauthorized(w)
		return
	}
	pid, err := a.reg.Authenticate(r.Context(), name, pass) // before reading the body
	if err != nil {
		a.fail(w, err)
		return
	}
	var body struct {
		EventID   uuid.UUID       `json:"event_id"`
		EventType string          `json:"event_type"`
		Data      json.RawMessage `json:"data"`
	}
	if !a.decode(w, r, 1<<20, &body) {
		return
	}
	switch {
	case body.EventID == uuid.Nil:
		writeErr(w, http.StatusBadRequest, "event_id (uuid) is required")
		return
	case body.EventType == "" || len(body.EventType) > 128:
		writeErr(w, http.StatusBadRequest, "event_type must be 1-128 chars")
		return
	case len(body.Data) == 0:
		writeErr(w, http.StatusBadRequest, "data is required")
		return
	}
	inserted, err := a.ev.InsertEvent(r.Context(), pid, body.EventID, body.EventType, body.Data)
	if err != nil {
		a.fail(w, err)
		return
	}
	if inserted {
		writeJSON(w, http.StatusCreated, map[string]string{"status": "accepted"})
	} else {
		writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
	}
}

func (a *api) decode(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil {
		if err2 := dec.Decode(&struct{}{}); !errors.Is(err2, io.EOF) {
			err = errors.New("unexpected data after JSON body")
		}
	}
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeErr(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		}
		return false
	}
	return true
}

func (a *api) fail(w http.ResponseWriter, err error) {
	var ve *registry.ValidationError
	switch {
	case errors.As(err, &ve):
		writeErr(w, http.StatusBadRequest, ve.Msg)
	case errors.Is(err, registry.ErrUnauthorized):
		a.unauthorized(w)
	case errors.Is(err, store.ErrInvalidJSON):
		writeErr(w, http.StatusBadRequest, "data must be valid JSON")
	default:
		a.log.Error("request failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

func (a *api) unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="webhookd"`)
	writeErr(w, http.StatusUnauthorized, "invalid producer credentials")
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
