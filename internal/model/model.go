package model

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusInFlight  Status = "in_flight"
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead"
)

// Delivery is one claimed job, exactly as returned by claim_deliveries().
// Attempts doubles as the fencing token: every completion update must pass it back.
type Delivery struct {
	ID         int64
	DeliveryID uuid.UUID       // stable across retries, sent to the subscriber for dedup
	EventUUID  uuid.UUID
	EventType  string
	DataBody   json.RawMessage // sent verbatim; never re-marshal into a map
	Subscriber string
	Timeout    time.Duration   // from subscriptions.timeout_ms
	Attempts   int
}

// Payload is the JSON body POSTed to a subscriber.
type Payload struct {
	EventID   uuid.UUID       `json:"event_id"`
	EventType string          `json:"event_type"`
	Data      json.RawMessage `json:"data"`
}

// SubscriptionSpec is one element of the JSON array passed to register_producer().
// Tags must match the column names the SQL function reads.
type SubscriptionSpec struct {
	EventType  string `json:"event_type"`
	Subscriber string `json:"subscriber"`
	TimeoutMs  *int   `json:"timeout_ms,omitempty"` // nil → DB default (5000)
}

// Outcome of one send attempt, produced by sender, consumed by worker.
type Outcome struct {
	StatusCode int    // 0 if no HTTP response (timeout, connection error)
	Err        string // empty on success
}

func (o Outcome) Success() bool { return o.Err == "" && o.StatusCode >= 200 && o.StatusCode < 300 }
