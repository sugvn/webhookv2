package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var ErrInvalidJSON = errors.New("data is not valid JSON")

// InsertEvent stores an event; the AFTER INSERT trigger fans it out in the same tx.
// inserted=false means (producer_id, uuid) already existed: a safe producer retry.
func (s *Store) InsertEvent(ctx context.Context, producerID int64, eventUUID uuid.UUID, eventType string, data json.RawMessage) (inserted bool, err error) {
	if !json.Valid(data) {
		return false, ErrInvalidJSON
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO events (uuid, producer_id, event_type, data_body)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (producer_id, uuid) DO NOTHING`,
		eventUUID, producerID, eventType, data)
	if err != nil {
		return false, fmt.Errorf("insert event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
