-- 004_timeout_cap.sql
ALTER TABLE subscriptions ADD CONSTRAINT subscriptions_timeout_max CHECK (timeout_ms <= 60000);
