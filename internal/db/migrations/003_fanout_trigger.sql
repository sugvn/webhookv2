-- 003_fanout_trigger.sql

-- Fan-out: atomic with the event insert (same transaction).
CREATE OR REPLACE FUNCTION fanout_event() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO deliveries (event_no, subscription_id)
    SELECT NEW.event_no, s.id
    FROM subscriptions s
    WHERE s.producer_id = NEW.producer_id
      AND s.event_type  = NEW.event_type
    ON CONFLICT (event_no, subscription_id) DO NOTHING;

    IF FOUND THEN
        PERFORM pg_notify('deliveries', '');   -- delivered only on commit
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER events_fanout
AFTER INSERT ON events
FOR EACH ROW EXECUTE FUNCTION fanout_event();

-- Claim: dead-letter exhausted expired leases, then lease a batch.
CREATE OR REPLACE FUNCTION claim_deliveries(
    p_limit        INT,
    p_max_attempts INT,
    p_lease_buffer INTERVAL
) RETURNS TABLE (
    id          BIGINT,
    delivery_id UUID,
    event_uuid  UUID,
    event_type  TEXT,
    data_body   JSONB,
    subscriber  TEXT,
    timeout_ms  INT,
    attempts    INT
)
LANGUAGE sql AS $$
    -- 1. Crashed on the final attempt: don't send a 6th time.
    UPDATE deliveries
    SET status = 'dead', lease_expires_at = NULL, completed_at = now(),
        last_error = 'lease expired on final attempt'
    WHERE deliveries.id IN (
        SELECT d.id FROM deliveries d
        WHERE d.status = 'in_flight'
          AND d.lease_expires_at < now()
          AND d.attempts >= p_max_attempts
        FOR UPDATE SKIP LOCKED
    );

    -- 2. Claim.
    WITH picked AS (
        SELECT d.id
        FROM deliveries d
        WHERE (d.status = 'pending'   AND d.next_attempt_at  <= now())
           OR (d.status = 'in_flight' AND d.lease_expires_at <  now()
               AND d.attempts < p_max_attempts)
        ORDER BY d.created_at, d.id
        LIMIT p_limit
        FOR UPDATE OF d SKIP LOCKED
    ), claimed AS (
        UPDATE deliveries d
        SET status = 'in_flight',
            attempts = d.attempts + 1,
            lease_expires_at = now()
                + make_interval(secs => s.timeout_ms / 1000.0)
                + p_lease_buffer
        FROM picked p, subscriptions s
        WHERE d.id = p.id AND s.id = d.subscription_id
        RETURNING d.id, d.delivery_id, d.event_no, d.attempts,
                  s.subscriber, s.timeout_ms
    )
    SELECT c.id, c.delivery_id, e.uuid, e.event_type, e.data_body,
           c.subscriber, c.timeout_ms, c.attempts
    FROM claimed c
    JOIN events e ON e.event_no = c.event_no
    ORDER BY c.id;
$$;
