# webhookd

At-least-once webhook delivery service in **Go** and **PostgreSQL**.

Producers register subscribers and emit events over an authenticated HTTP API. Each event fans out into independent per-subscriber delivery records, and a pool of concurrent workers delivers them with timeouts, exponential backoff with jitter, and dead-lettering. Postgres is the only piece of infrastructure: it is the queue, the lease store and the source of truth.

- **At-least-once delivery** with a stable `X-Delivery-Id` so subscribers can deduplicate
- **Crash-safe**: leases recover jobs from killed workers with no reaper process
- **Idempotent ingestion**: producers can retry `POST /events` safely
- **Independent deliveries**: one subscriber failing never changes another's delivery state
- **Low latency**: `LISTEN/NOTIFY` wakeups backed by a reconciliation poll

---

## Contents

1. [Quick start](#quick-start)
2. [Configuration](#configuration)
3. [HTTP API](#http-api)
4. [Subscriber contract](#subscriber-contract)
5. [Architecture](#architecture)
6. [How it works](#how-it-works)
7. [Data model](#data-model)
8. [Design decisions and tradeoffs](#design-decisions-and-tradeoffs)
9. [Testing](#testing)
10. [Evidence](#evidence)
11. [Benchmarks](#benchmarks)
12. [Known limitations](#known-limitations)
13. [Roadmap](#roadmap)
14. [Project layout](#project-layout)

---

## Quick start

**Prerequisites**

- Go (version in `go.mod`)
- Docker with the Compose plugin (runs PostgreSQL 16)
- `psql` (optional, for inspecting the database)
- `curl`

**1. Start PostgreSQL**

```bash
git clone https://github.com/sugvn/webhookv2.git
cd webhookv2
docker compose up -d
docker compose ps        # wait for (healthy)
```

The compose file creates database `webhookd` with user and password `webhookd` and a named volume `pgdata`. These credentials are for local development only.

**2. Run the service**

```bash
go run ./cmd/webhookd
```

Migrations are embedded in the binary and applied automatically at startup, under a Postgres advisory lock so several instances can start safely.

**3. Run a demo subscriber**

```bash
go run ./cmd/subscriber-demo -addr :9000
```

Flags: `-fail N` answers the first N requests with 500, `-slow N -delay D` makes the first N requests hang for D. They exist to exercise the retry, timeout and crash-recovery paths.

**4. Register and emit**

```bash
# register a producer (created on first use) and a subscription
curl -u shop:change-me-123 -X POST localhost:8080/register -d '{
  "subscriptions": [
    {"event_type": "order.created", "subscriber": "http://localhost:9000/hook", "timeout_ms": 3000}
  ]}'

# emit an event; the event_id is the idempotency key
curl -u shop:change-me-123 -X POST localhost:8080/events -d '{
  "event_id": "7d4f0c1e-6b0a-4f5e-9d53-0c2a5e0c9a11",
  "event_type": "order.created",
  "data": {"total": 99}
}'
# -> 201 {"status":"accepted"}; repeating the same call -> 200 {"status":"duplicate"}
```

**Inspecting the database**

```bash
export DB=postgres://webhookd:webhookd@localhost:5432/webhookd
psql $DB -c "SELECT id, status, attempts, last_status_code, last_error FROM deliveries ORDER BY id DESC LIMIT 10"
```

---

## Configuration

All settings are environment variables with working defaults.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://webhookd:webhookd@localhost:5432/webhookd` | PostgreSQL connection string |
| `HTTP_ADDR` | `:8080` | Listen address for the producer API |
| `WORKERS` | `8` | Concurrent delivery workers (one send at a time each) |
| `DB_POOL_MAX` | `0` | Connection pool size; `0` means `WORKERS + 2`. The listener uses its own extra connection |
| `POLL_INTERVAL` | `5s` | Reconciliation poll. Also bounds how quickly a backed-off retry is picked up |
| `LEASE_BUFFER` | `10s` | Added to a subscription's timeout to form the lease |
| `MAX_ATTEMPTS` | `5` | Attempts before dead-lettering |
| `RETRY_BASE` | `30s` | Backoff base |
| `RETRY_CAP` | `10m` | Backoff ceiling |

`MAX_ATTEMPTS` is the single source for both the retry policy and the claim function, so the two cannot drift.

---

## HTTP API

Authentication is HTTP Basic: the username is the producer name and the password is the producer's password. Request bodies are strict JSON (unknown fields are rejected). Bodies are limited to 64 KiB for `/register` and 1 MiB for `/events`.

### `POST /register`

Creates the producer on first use, otherwise requires the correct password. Adds the given subscriptions (additive; existing ones are updated). It never changes an existing password.

```json
{"subscriptions": [
  {"event_type": "order.created", "subscriber": "https://example.com/hook", "timeout_ms": 3000}
]}
```

`timeout_ms` is optional (default 5000, maximum 60000). Subscriber URLs must be `http(s)` with a host and no embedded credentials. Returns `{"producer_id": N}`.

### `POST /events`

```json
{"event_id": "<uuid>", "event_type": "order.created", "data": {...}}
```

`data` is required; an explicit `null` is allowed and is delivered as `"data": null`.

| Status | Meaning |
|---|---|
| `201` | Stored. The event and all its delivery records are committed |
| `200` | Duplicate `event_id` for this producer; nothing was created (safe retry) |
| `400` | Validation error or invalid JSON |
| `401` | Missing or wrong credentials |
| `413` | Body too large |
| `500` | Internal error (details are logged, not returned) |

Authentication happens before the body is read.

### `GET /healthz`

Returns `200` when the process is up.

---

## Subscriber contract

webhookd sends `POST <subscriber URL>` with:

```
Content-Type: application/json
X-Delivery-Id: <uuid>        stable across all retries of this delivery: use it to deduplicate
X-Event-Id: <uuid>           the producer's event_id
X-Event-Type: order.created
X-Delivery-Attempt: 1        1-based attempt counter

{"event_id": "...", "event_type": "order.created", "data": {...}}
```

- Any **2xx** response is success. Everything else is a failure, including 3xx: **redirects are never followed**.
- A subscriber must answer within its `timeout_ms`, otherwise the attempt counts as a failure. A timeout means the event may still have been processed, which is why duplicates are possible.
- **Deduplicate on `X-Delivery-Id`.** It identifies one subscriber's copy of one event and never changes between retries.
- Delivery order is not guaranteed.

### Retry schedule

Equal jitter: half of the exponential delay is fixed, half is random.

| Failed attempt | Next attempt after |
|---|---|
| 1 | 15 to 30 s |
| 2 | 30 to 60 s |
| 3 | 60 to 120 s |
| 4 | 120 to 240 s |
| 5 | dead-lettered |

The whole window is roughly 4 to 8 minutes plus up to `POLL_INTERVAL` of pickup delay per retry. A subscriber that is down longer than that loses the delivery to the dead state (see [limitations](#known-limitations)).

---

## Architecture

```
 producer ──HTTP──▶  api ──▶ registry (auth: bcrypt + short-lived cache)
                      │
                      ▼
                   store.InsertEvent
                      │   INSERT ... ON CONFLICT (producer_id, uuid) DO NOTHING
                      ▼
   ┌────────────── PostgreSQL ──────────────────────────────────────────┐
   │ events ──AFTER INSERT trigger (same transaction)──▶ deliveries     │
   │                                   one row per matching subscription│
   │                                   └─ pg_notify('deliveries')       │
   └───────────────┬───────────────────────────────▲────────────────────┘
                   │ NOTIFY (a hint)               │ claim_deliveries():
                   ▼                               │ FOR UPDATE SKIP LOCKED + lease
            listener ──wake──▶ worker pool ────────┘
         (dedicated conn,      N goroutines:  claim → sender (HTTP POST) → complete │ reschedule │ dead-letter
          reconnect+drain)              ▲
                   poll ticker ─────────┘  (correctness backstop)
```

Packages:

| Package | Responsibility |
|---|---|
| `internal/api` | HTTP handlers, error mapping, body limits |
| `internal/registry` | Producer creation and authentication, input validation |
| `internal/store` | All SQL. Claim, fenced outcome writes, event and subscription writes |
| `internal/worker` | Worker pool, claim loop, outcome handling, shutdown behavior |
| `internal/sender` | One HTTP delivery attempt; no retries, no database |
| `internal/retry` | Backoff and dead-letter policy (pure functions) |
| `internal/listener` | `LISTEN/NOTIFY` with reconnect, turns notifications into wake signals |
| `internal/db` | Pool, dedicated listener connection, embedded migration runner |
| `internal/config` | Environment configuration |
| `internal/model` | Shared types |

---

## How it works

### Ingestion and fan-out

`POST /events` inserts one row into `events`. An `AFTER INSERT` trigger creates one `deliveries` row for every subscription matching the producer and event type, **in the same transaction**. A `201` therefore means the event and all its deliveries are durable: there is no crash window in which an accepted event has no deliveries. The unique key `(producer_id, uuid)` makes producer retries a no-op, and `UNIQUE (event_no, subscription_id)` keeps fan-out idempotent.

Events with no matching subscription at emit time create no deliveries and are not replayed to subscribers registered later.

### Claiming and leases

Workers call `claim_deliveries()`, which in one transaction:

1. dead-letters rows whose lease expired while they were already on the final attempt, so a crash-looping payload is never sent a sixth time;
2. selects due rows (`pending` and `next_attempt_at <= now()`, or `in_flight` with an expired lease) in FIFO order with `FOR UPDATE SKIP LOCKED`;
3. marks them `in_flight`, **increments `attempts`**, and sets `lease_expires_at = now() + timeout + LEASE_BUFFER`.

Each worker claims one delivery at a time, because the lease starts at claim and sends within a worker are sequential. All time arithmetic uses the database clock, never the application clock.

### Outcome handling and fencing

After the HTTP attempt the worker writes one of three outcomes, each guarded by:

```sql
WHERE id = $1 AND status = 'in_flight' AND attempts = $2
```

`attempts` is the **fencing token**: every claim increments it. A worker that was slow past its lease and was superseded matches zero rows when it finally reports, so it cannot overwrite the new owner's state. Zero rows affected is logged as a lost lease, not treated as an error.

| Outcome | Resulting row state |
|---|---|
| 2xx | `succeeded` |
| failure, attempts left | `pending` with `next_attempt_at = now() + backoff` |
| failure, attempt limit reached | `dead` |

### Wakeups

`NOTIFY` is only a hint. The listener holds a dedicated connection, coalesces bursts into one signal, and the pool broadcasts that signal to every worker; each worker drains until the claim returns nothing. On every (re)connect the listener issues `LISTEN` first and then kicks the workers, so notifications missed while disconnected cannot strand work. An idle ping detects half-open connections. A periodic poll catches everything else and is also what picks up backed-off retries, since time passing sends no notification.

### Crash recovery

There is no reaper. A killed worker's rows stay `in_flight` until their lease expires and are picked up by the next claim, with `attempts` already incremented. Recovery latency is `timeout_ms + LEASE_BUFFER` plus one claim.

### Graceful shutdown

On SIGINT/SIGTERM the HTTP server stops accepting requests and the workers stop claiming. Sends already in progress run to completion on a context detached from shutdown, and their outcomes are always recorded, so shutdown does not create duplicates. A hard 30 s grace period applies, after which the process exits and lease expiry recovers whatever was cut off. A second signal exits immediately.

---

## Data model

```
producers(id, name UNIQUE, password_hash)
subscriptions(id, producer_id, event_type, subscriber, timeout_ms,
              UNIQUE(producer_id, event_type, subscriber), CHECK timeout_ms <= 60000)
events(event_no, uuid, producer_id, event_type, data_body JSONB,
       UNIQUE(producer_id, uuid))
deliveries(id, delivery_id UUID UNIQUE, event_no, subscription_id,
           status, attempts, next_attempt_at, lease_expires_at,
           last_status_code, last_error, completed_at,
           UNIQUE(event_no, subscription_id))
schema_migrations(version, checksum, applied_at)
```

`deliveries.status` is one of `pending`, `in_flight`, `succeeded`, `dead`. A retry is `pending` with a future `next_attempt_at`. A partial index on `(created_at, id)` covers only live rows (`pending`, `in_flight`).

Migrations (`internal/db/migrations/`, embedded, forward-only):

| File | Contents |
|---|---|
| `001_schema.sql` | Tables, constraints, indexes |
| `002_register_fn.sql` | `register_producer()`: upserts the producer and subscriptions atomically |
| `003_fanout_trigger.sql` | Fan-out trigger, `NOTIFY`, `claim_deliveries()` |
| `004_timeout_cap.sql` | `CHECK (timeout_ms <= 60000)` |

The runner records a SHA-256 checksum per file and refuses to start if an applied migration was edited. Changes go in a new file.

---

## Design decisions and tradeoffs

**Postgres as the queue.** One system to run, transactional enqueue (events and deliveries commit atomically), and `SKIP LOCKED` gives duplicate-free parallel claiming. The cost is throughput well below a dedicated broker and queue-table maintenance (bloat, vacuum).

**Fan-out in a trigger.** Atomic with the event insert, so accepted events can never lack deliveries. The cost is that it runs inside the producer's transaction, and subscriptions added later do not see old events.

**`attempts` increments at claim, not on failure.** A payload that crashes a worker still consumes attempts and eventually dead-letters instead of crash-looping. The cost is that a claim lost to a crash burns an attempt without a send.

**Leases instead of heartbeats or a reaper.** Simple and stateless; recovery latency equals the lease. Shorter timeouts recover faster but raise the chance of duplicates when sends run long.

**Duplicates are an accepted cost.** A timeout, a lost lease, or a crash after a send but before the outcome write all produce a second send. The stable `X-Delivery-Id` is the contract that makes this safe for subscribers.

**Database clock only.** Lease expiry and backoff use `now()` in SQL, so skew between application hosts cannot break recovery.

**Redirects are never followed.** A subscriber cannot bounce deliveries to another address, and a redirect counts as a failure.

**Listener on its own connection.** `LISTEN` state is per connection, so it must not come from the pool. The pool is sized independently.

**Auth cache.** bcrypt costs tens of milliseconds, which would cap the events endpoint. Successful credentials are cached for 5 minutes (keyed by a hash, never stored in plaintext). Failures are never cached. This is safe only because passwords cannot change; adding password changes or producer deletion requires invalidation.

---

## Testing

```bash
go vet ./... && go test -race ./...
```

Tests that need PostgreSQL read `TEST_DATABASE_URL` and are **skipped** when it is unset. They truncate tables, so point them at a separate database:

```bash
psql $DB -c "CREATE DATABASE webhookd_test"
export TEST_DATABASE_URL=postgres://webhookd:webhookd@localhost:5432/webhookd_test
go test -race -count=1 ./...
```

| Area | What is covered |
|---|---|
| `retry` | Delay ranges per attempt, cap, dead-letter threshold (table-driven) |
| `sender` | Success, non-2xx, redirect not followed, timeout, headers and payload |
| `registry` | New vs returning producer, wrong password, **concurrent first registration cannot hijack an account**, input validation, auth cache behavior |
| `store` | Claim fencing across a lease reclaim, backoff rows not claimable, idempotent insert, error text sanitization |
| `worker` | Outcome routing, lost lease handling, wake-driven draining, shutdown |
| `listener` | Wake on notify, reconnect after the backend is killed |
| `api` | Status mapping, auth before body, no internal error leakage, null `data` |

---

## Evidence

Captured from real runs against local subscribers (`cmd/subscriber-demo`).

### Crash recovery

The service was killed with `kill -9` while a send was in flight (subscriber hanging). The row stayed `in_flight`, nothing recovered it while no worker was running, and the first claim after restart delivered it again:

```
-- row right after the kill (lease still 20s):
 id |  status   | attempts |   lease_left
  5 | in_flight |        1 | 00:00:19.92

-- 73 s later, no worker running (lease long expired, row still stuck):
  5 | in_flight |        1 | -00:01:13.86

-- after restarting webhookd:
  5 | succeeded |        2 |

-- subscriber log: same delivery id on both attempts
09:38:20 req=1 delivery=2f0fcb71-4b32-4ca0-9b5b-530e887023ec attempt=1
09:40:22 req=2 delivery=2f0fcb71-4b32-4ca0-9b5b-530e887023ec attempt=2
```

### Dead-lettering after five attempts

Subscriber answering 500 forever. Exactly five requests, one stable delivery id, and a healthy subscriber for the same event was unaffected:

```
09:29:18 req=1 delivery=36db68c3-520d-4a49-920e-dbfeb8f8744d attempt=1
09:29:19 req=2 delivery=36db68c3-520d-4a49-920e-dbfeb8f8744d attempt=2
09:29:21 req=3 delivery=36db68c3-520d-4a49-920e-dbfeb8f8744d attempt=3
09:29:24 req=4 delivery=36db68c3-520d-4a49-920e-dbfeb8f8744d attempt=4
09:29:32 req=5 delivery=36db68c3-520d-4a49-920e-dbfeb8f8744d attempt=5   (RETRY_BASE=1s for the demo)

 id |  status   | attempts | last_status_code |      last_error
  4 | dead      |        5 |              500 | status 500: "nope\n"
  3 | succeeded |        1 |              204 |
```

### Timeout then retry

A subscriber that held the first request for 30 s against a 20 s timeout. The attempt was recorded as a timeout, rescheduled with backoff, and the retry carried the same delivery id (a real at-least-once duplicate, which the delivery id lets a subscriber detect):

```
13:27:09 req=1 delivery=5c33cdf5-3597-4fdc-a208-227e04f5ecd2 attempt=1
13:27:53 req=3 delivery=5c33cdf5-3597-4fdc-a208-227e04f5ecd2 attempt=2
```

### Idempotent ingestion over HTTP

```
POST /events (event_id X)  -> {"status":"accepted"}
POST /events (event_id X)  -> {"status":"duplicate"}
-- deliveries table: exactly one new row, subscriber received exactly one request
```

---

## Benchmarks

**Setup.** Intel Core i3-N305 (8 efficiency cores), 8 GB RAM. PostgreSQL 16 in Docker with default settings (synchronous commit, no tuning). webhookd, the Postgres container, and a local sink returning `204` immediately all ran on the same machine, so the results are a lower bound and are useful for comparing configurations on this hardware, not as absolute figures. `WORKERS=64`, `DB_POOL_MAX=24`.

**Method.** Seed N events with a single statement (the trigger fans them out and one `NOTIFY` wakes the workers), then sample completed and pending counts every 10 s:

```bash
psql $BENCH -c "INSERT INTO events (uuid, producer_id, event_type, data_body)
  SELECT gen_random_uuid(), (SELECT id FROM producers WHERE name='bench'), 'bench', '{\"n\":1}'
  FROM generate_series(1,50000)"

for i in $(seq 1 20); do
  psql $BENCH -Atc "select now()::time(0), count(*) filter (where status='succeeded'),
                           count(*) filter (where status='pending') from deliveries"
  sleep 10
done
```

**Results**

| Run | Rows already in `deliveries` | Throughput |
|---|---|---|
| 1 | 0 (empty table) | 50,000 deliveries in 167 s, **about 300/s** average; 16 to 25 deliveries in flight on average |
| 2 | about 50k | **about 830/s** at the start, falling to **about 90/s** after roughly 30k deliveries; about 250/s averaged over the sampled window |
| 3 | about 100k | about 780/s at the start, falling to **about 60/s** |

**Observations**

1. **Throughput degrades as the table grows.** The plateau rate times the table size came out nearly identical across runs 2 and 3 (about 9.2M and 9.3M), which indicates a per-claim cost proportional to the total number of rows rather than the backlog. The leading suspect is the dead-letter step at the top of `claim_deliveries()`, which filters on `status = 'in_flight' AND lease_expires_at < now()` and has no supporting index. **This is a hypothesis: the query plan has not been captured and the fix has not been applied or measured.** See the [roadmap](#roadmap).
2. **Workers are not the limit.** With 64 workers and an instant sink, only 16 to 25 deliveries were in flight at once. In-flight time here is dominated by the claim and outcome-write database round trips (two transactions per delivery), not by the HTTP send. During the plateau, 22 to 23 of 24 pool connections were active running `claim_deliveries()` on CPU with no lock waits.
3. **Real subscribers change the picture.** An instant local sink measures the engine only. With subscribers answering in 100 ms, throughput is bounded by `WORKERS / latency` instead.

These figures are from a single laptop with default Postgres settings and should not be read as a capacity claim.

---

## Known limitations

- **Throughput degrades with table size** (above). Completed rows are never deleted, so there is also no retention job; `events` and `deliveries` grow without bound.
- **No per-subscriber concurrency cap.** Claiming is global FIFO, so a slow subscriber with a large backlog can occupy many workers and delay healthy subscribers. Delivery records and retry state are independent per subscriber; worker capacity is shared.
- **Short retry window** (about 4 to 8 minutes). A subscriber down longer loses deliveries to the `dead` state.
- **Dead-lettered deliveries are not replayed automatically.** They stay in the table with the last error; replay is a manual SQL update (reset `status`, `attempts`, `next_attempt_at`).
- **Permanent failures are retried.** A 4xx such as 404 or 410 still consumes all five attempts.
- **Duplicates are possible** by design (timeouts, crashes between send and outcome write). Subscribers must deduplicate on `X-Delivery-Id`.
- **No ordering guarantee.**
- **No TLS in the service.** Basic auth credentials travel in cleartext without a TLS-terminating proxy in front.
- **No rate limiting.** Each wrong-password attempt costs a bcrypt, so the API is open to CPU exhaustion.
- **No SSRF protection.** webhookd will call any `http(s)` URL a producer registers, including private addresses. Blocking should happen at dial time in the sender (after DNS resolution).
- **No payload signing** (HMAC).
- **No way to remove a subscription** through the API; registration is additive.
- **Payloads are normalized by JSONB** (key order and whitespace may differ from what the producer sent).
- **Events with no subscribers at emit time are dropped silently**, and are not replayed to later subscribers.
- **Credentials in the compose file and defaults are development-only.**

---

## Roadmap

1. Capture the `claim_deliveries()` query plan and add a partial index on `deliveries (lease_expires_at) WHERE status = 'in_flight'`, then re-run the benchmark above on a clean table.
2. Sweep `DB_POOL_MAX` downward (the machine has 8 cores), and evaluate asynchronous commit for the worker connections (a lost claim or outcome commit can only cause a re-send).
3. Per-subscriber concurrency cap in the claim path.
4. Batched claims and batched outcome writes to cut database round trips per delivery.
5. Retention job for old rows; dead-letter replay endpoint.
6. TLS guidance, rate limiting, private-address blocking in the sender, HMAC signing.
7. Metrics (queue depth, delivery latency, attempts histogram) and structured per-delivery tracing.

---

## Project layout

```
cmd/
  webhookd/            service entrypoint (config, wiring, graceful shutdown)
  subscriber-demo/     failure-injecting subscriber for manual testing
internal/
  api/                 HTTP handlers
  config/              environment configuration
  db/                  pool, listener connection, migration runner
    migrations/        001..004 SQL, embedded in the binary
  listener/            LISTEN/NOTIFY wakeups with reconnect
  model/               shared types
  registry/            producer auth and validation
  retry/               backoff and dead-letter policy
  sender/              one HTTP delivery attempt
  store/               all SQL
  worker/              worker pool
docker-compose.yml     PostgreSQL 16 for local development
```
