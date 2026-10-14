package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sugvn/webhookv2/internal/model"
)

// Claim leases up to `limit` deliveries. Each returned row's Attempts is its fencing token.
func (s *Store) Claim(ctx context.Context, limit, maxAttempts int, leaseBuffer time.Duration) ([]model.Delivery, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, delivery_id, event_uuid, event_type, data_body, subscriber, timeout_ms, attempts
		FROM claim_deliveries($1, $2, make_interval(secs => $3))`,
		limit, maxAttempts, leaseBuffer.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	var out []model.Delivery
	for rows.Next() {
		var d model.Delivery
		var timeoutMs int
		if err := rows.Scan(&d.ID, &d.DeliveryID, &d.EventUUID, &d.EventType,
			&d.DataBody, &d.Subscriber, &timeoutMs, &d.Attempts); err != nil {
			return nil, fmt.Errorf("scan claim: %w", err)
		}
		d.Timeout = time.Duration(timeoutMs) * time.Millisecond
		out = append(out, d)
	}
	return out, rows.Err()
}

// All three writers below return held=false when the lease was lost (row reclaimed,
// already finished, or deleted). That is normal, not an error.

func (s *Store) Complete(ctx context.Context, id int64, attempts, statusCode int) (held bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'succeeded', lease_expires_at = NULL, completed_at = now(),
		    last_status_code = $3, last_error = NULL
		WHERE id = $1 AND status = 'in_flight' AND attempts = $2`,
		id, attempts, statusCode)
	if err != nil {
		return false, fmt.Errorf("complete delivery %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Reschedule returns the row to pending, due after `delay` (computed with the DB clock).
func (s *Store) Reschedule(ctx context.Context, id int64, attempts int, delay time.Duration, statusCode int, errMsg string) (held bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'pending', lease_expires_at = NULL,
		    next_attempt_at = now() + make_interval(secs => $3),
		    last_status_code = NULLIF($4::int, 0), last_error = $5
		WHERE id = $1 AND status = 'in_flight' AND attempts = $2`,
		id, attempts, delay.Seconds(), statusCode, cleanText(errMsg))
	if err != nil {
		return false, fmt.Errorf("reschedule delivery %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) DeadLetter(ctx context.Context, id int64, attempts, statusCode int, errMsg string) (held bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'dead', lease_expires_at = NULL, completed_at = now(),
		    last_status_code = NULLIF($3::int, 0), last_error = $4
		WHERE id = $1 AND status = 'in_flight' AND attempts = $2`,
		id, attempts, statusCode, cleanText(errMsg))
	if err != nil {
		return false, fmt.Errorf("dead-letter delivery %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Postgres TEXT rejects NUL bytes and invalid UTF-8; a bad error string must never
// make the failure update itself fail.
func cleanText(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	if len(s) > 512 {
		s = s[:512]
	}
	return strings.ToValidUTF8(s, "")
}
