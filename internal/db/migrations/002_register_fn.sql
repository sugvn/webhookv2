-- 002_register_fn.sql

CREATE OR REPLACE FUNCTION register_producer(
    p_name          TEXT,
    p_password_hash TEXT,
    p_subscriptions JSONB          -- [{"event_type":"...","subscriber":"https://...","timeout_ms":3000}, ...]
) RETURNS BIGINT
LANGUAGE plpgsql AS $$
DECLARE
    v_producer_id BIGINT;
BEGIN
    IF jsonb_typeof(p_subscriptions) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'p_subscriptions must be a JSON array';
    END IF;

    -- No-op update so RETURNING yields the id on conflict.
    -- Deliberately does NOT touch password_hash.
    INSERT INTO producers (name, password_hash)
    VALUES (p_name, p_password_hash)
    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
    RETURNING id INTO v_producer_id;

    INSERT INTO subscriptions (producer_id, event_type, subscriber, timeout_ms)
    SELECT v_producer_id, x.event_type, x.subscriber, COALESCE(x.timeout_ms, 5000)
    FROM jsonb_to_recordset(p_subscriptions)
         AS x(event_type TEXT, subscriber TEXT, timeout_ms INT)
    WHERE x.event_type IS NOT NULL
      AND x.subscriber ~ '^https?://'
    ON CONFLICT (producer_id, event_type, subscriber)
    DO UPDATE SET timeout_ms = EXCLUDED.timeout_ms;

    RETURN v_producer_id;
END;
$$;
