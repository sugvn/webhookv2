-- 001_schema.sql

CREATE TABLE producers (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          TEXT        NOT NULL UNIQUE,   -- upsert target for registration
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE subscriptions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    producer_id BIGINT      NOT NULL REFERENCES producers(id),
    event_type  TEXT        NOT NULL,
    subscriber  TEXT        NOT NULL,            -- target URL
    timeout_ms  INT         NOT NULL DEFAULT 5000 CHECK (timeout_ms > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (producer_id, event_type, subscriber)
);

CREATE TABLE events (
    event_no    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid        UUID        NOT NULL,
    producer_id BIGINT      NOT NULL REFERENCES producers(id),
    event_type  TEXT        NOT NULL,
    data_body   JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (producer_id, uuid)                   -- producer-retry idempotency
);

CREATE TABLE deliveries (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id      UUID        NOT NULL DEFAULT gen_random_uuid() UNIQUE,  -- sent to subscriber for dedup
    event_no         BIGINT      NOT NULL REFERENCES events(event_no) ON DELETE CASCADE,
    subscription_id  BIGINT      NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    status           TEXT        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'in_flight', 'succeeded', 'dead')),
    attempts         INT         NOT NULL DEFAULT 0,   -- incremented at CLAIM time, not on failure
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at TIMESTAMPTZ,
    last_status_code INT,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    UNIQUE (event_no, subscription_id),                -- fan-out idempotency
    CHECK (status <> 'in_flight' OR lease_expires_at IS NOT NULL)
);

-- Claim path: FIFO scan over live rows only. Terminal rows never enter the index.
CREATE INDEX deliveries_claimable_idx
    ON deliveries (created_at, id)
    WHERE status IN ('pending', 'in_flight');

CREATE INDEX deliveries_subscription_idx ON deliveries (subscription_id);
