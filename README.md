# Transaction Webhook Service

This service receives transaction webhooks from payment providers such as Razorpay. It replies `202 Accepted` immediately, waits 30 seconds to simulate a call to an external API, and then marks the transaction `PROCESSED`. A transaction ID is processed only once, no matter how many times its webhook arrives.

## Architecture

```
 payment provider ──POST──▶  API (FastAPI)  ──┐   one DB transaction:
                             returns 202      │   INSERT transaction + INSERT job
                                              ▼
                                      ┌───────────────┐
                                      │  PostgreSQL   │  transactions, jobs
                                      └───────────────┘
                                              ▲
       Worker process(es)  ───────────────────┘
       1. claim job   (SELECT ... FOR UPDATE SKIP LOCKED, short transaction)
       2. sleep 30 s  (no transaction or lock held)
       3. finish      (transaction → PROCESSED, job → COMPLETED, in one transaction)
```

- **API** (`app/main.py`, `app/routes/`): validates the request, then stores the transaction and its job in a single database transaction and returns. It never waits for processing.
- **Worker** (`app/worker.py`, run with `python -m app.worker`): a separate process. It runs `WORKER_CONCURRENCY` threads, and each thread polls the `jobs` table for work.
- **PostgreSQL** is the only place state is kept, and the `jobs` table is the queue. Pending work therefore survives restarts and crashes of both the API and the worker.

### Technology choices

| Choice | Why |
|---|---|
| FastAPI + Pydantic | Request validation and OpenAPI docs (`/docs`) with very little code |
| PostgreSQL as the queue | The transaction and its job are written atomically, and `SKIP LOCKED` lets several workers share the queue safely. No second piece of infrastructure (Redis, RabbitMQ, SQS) is needed. |
| SQLAlchemy 2 (sync) + psycopg 3 | Simple, readable code. The work is I/O-bound and low-volume, so threads are enough. |
| Alembic | The schema is created by versioned migrations instead of by hand |
| Docker Compose | Starts the API, worker, Postgres and the migration step with one command |

## Data model

**`transactions`**: `transaction_id` (primary key), `source_account`, `destination_account`, `amount` `NUMERIC(18,2)`, `currency`, `status` (`PROCESSING` / `PROCESSED` / `FAILED`), `created_at`, `processed_at` (null until processed).

**`jobs`**: `id`, `transaction_id` (unique, foreign key), `status` (`PENDING` / `RUNNING` / `COMPLETED` / `FAILED`), `attempts`, `run_after` (time of the next retry), `locked_at` / `locked_by` (lease), `last_error`, `created_at`, `completed_at`.

All timestamps are `timestamptz` and stored in UTC. The API returns them as ISO-8601 with a trailing `Z`.

## How correctness is guaranteed

**Duplicate webhooks.** The webhook handler runs `INSERT INTO transactions ... ON CONFLICT (transaction_id) DO NOTHING RETURNING transaction_id`. The job is inserted only if that insert actually created a row. Uniqueness is enforced by the primary key on `transactions.transaction_id` and the unique constraint on `jobs.transaction_id`. When several identical requests arrive at once, Postgres lets exactly one insert succeed and turns the rest into no-ops. Every valid request, including duplicates, gets a `202` response, and the body includes `"duplicate": true|false`.

**One worker per job.** A worker claims a job with `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`. It then sets `status=RUNNING`, `locked_by` and `locked_at`, increments `attempts`, and commits. Other workers skip a locked row rather than wait for it. The row lock is released before the 30-second delay, so no database transaction is held open while the worker waits.

**Processed only once.** When it finishes, the worker locks the job again and checks that it still holds the lease (same `locked_by` and `attempts`). Only then does it update the transaction, and only if the transaction's status is still `PROCESSING`. That update and `job → COMPLETED` happen in one database transaction. A worker that lost its lease (for example, it stalled and another worker reclaimed the job) has its result thrown away.

**Retries.** If processing raises an error, the job goes back to `PENDING` with exponential backoff (`RETRY_BACKOFF_SECONDS × 2^(attempt−1)`, at most 5 minutes) and the error is saved in `last_error`. A failed attempt never marks the transaction as processed. After `JOB_MAX_ATTEMPTS` attempts, the job and the transaction are both marked `FAILED`.

**Recovery.**
- *Graceful stop* (SIGTERM/SIGINT): the worker interrupts its sleep and puts the job back to `PENDING` immediately. The interrupted attempt does not count toward the limit.
- *Crash or kill*: the job stays `RUNNING`. Once `locked_at` is older than `JOB_LEASE_SECONDS` (default 120 s, which must be longer than the processing delay), any worker reclaims it.

**Database unavailable when a webhook arrives.** The API returns `503`, and the provider redelivers the webhook later. A `202` is only sent after the data has been committed.

**Exactly-once limitation.** These guarantees make processing *logically* exactly-once inside this service: there is one `PROCESSED` transition per transaction, and duplicates are never enqueued. The simulated external call itself is *at-least-once*: if a worker crashes after the external call succeeds but before it commits, the job runs again. For real money movement, the downstream payment provider has to support idempotency, for example by receiving `transaction_id` as an idempotency key. No consumer-side design can guarantee exactly-once side effects on an external system.

## API

### `POST /v1/webhooks/transactions` → `202`

```bash
curl -i -X POST http://localhost:8000/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
```
```json
{"status":"ACCEPTED","transaction_id":"txn_abc123def456","duplicate":false}
```
Validation: all fields are required. `amount` must be greater than 0 with at most 2 decimal places. `currency` is a 3-letter code. An invalid body returns `422`.

### `GET /` → `200`

```bash
curl http://localhost:8000/
```
```json
{"status":"HEALTHY","current_time":"2026-10-19T13:14:02.937243Z"}
```

### `GET /v1/transactions/{transaction_id}` → `200` or `404`

```bash
curl http://localhost:8000/v1/transactions/txn_abc123def456
```
```json
[{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456",
  "amount":1500,"currency":"INR","status":"PROCESSING",
  "created_at":"2026-10-19T13:14:02.982633Z","processed_at":null}]
```

**Response shape:** the assignment's example shows this response wrapped in an array, so the endpoint returns a one-element array. An unknown ID returns `404 {"detail": "Transaction not found"}`. `amount` is stored as an exact `NUMERIC` and returned as a JSON number to match the example.

## Running locally

**Prerequisites:** Docker with Docker Compose v2. Python 3.12+ is only needed to run the tests on your machine.

```bash
cp .env.example .env            # optional; built-in defaults work for local use
docker compose up -d --build    # db → migrate (alembic upgrade head) → api + worker
docker compose ps               # api should be "healthy", migrate "Exited (0)"
docker compose logs -f worker   # follow processing
```

The API runs on http://localhost:8000 (docs at `/docs`). Postgres is published on host port **5433**, so it doesn't clash with a locally installed Postgres.

Stop everything with `docker compose down`. Add `-v` to also delete the database volume.

### Database migrations

The schema is managed by Alembic (`migrations/versions/`). With Compose, the one-off `migrate` service runs `alembic upgrade head` before the API and worker start. To run it by hand:

```bash
docker compose run --rm migrate                  # inside Docker
DATABASE_URL=postgresql+psycopg://... alembic upgrade head   # from the host
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://postgres:postgres@localhost:5433/webhooks` | Connection string. `postgres://` and `postgresql://` also work. |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external processing time |
| `WORKER_CONCURRENCY` | `10` | Jobs processed in parallel per worker process |
| `WORKER_POLL_INTERVAL_SECONDS` | `1` | How long an idle worker thread waits between polls |
| `JOB_LEASE_SECONDS` | `120` | Time before a `RUNNING` job counts as abandoned. Must be longer than the delay. |
| `JOB_MAX_ATTEMPTS` | `5` | Maximum attempts before a job is `FAILED` |
| `RETRY_BACKOFF_SECONDS` | `5` | Base retry delay |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` / `POSTGRES_HOST_PORT` / `API_PORT` | see `.env.example` | Local Compose only |
| `TEST_DATABASE_URL` | `postgresql+psycopg://postgres:postgres@localhost:5433/webhooks_test` | Database used by the tests |

Secrets are read only from the environment. `.env` is git-ignored.

## Running tests

The tests run against a real PostgreSQL database because they exercise `SKIP LOCKED` and concurrent unique-constraint conflicts. The test database (`webhooks_test`) is created and migrated automatically. Each test starts with empty tables. The worker tests inject a 0.2–1.5 s delay or a fake processing function, so no test waits 30 s.

```bash
docker compose up -d db
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -v
```

The tests cover: the health check; webhook `202`; the webhook response being fast while the production 30 s delay is configured; `PROCESSING` → `PROCESSED` with `processed_at` null and then set; `404`; sequential, concurrent-HTTP and concurrent-database-level duplicates each producing exactly one transaction and one job; `SKIP LOCKED` exclusivity; parallel workers processing each job once; retry with backoff; bounded retries ending in `FAILED`; recovery of abandoned jobs; a worker that lost its lease being unable to overwrite the result; and graceful shutdown releasing the in-progress job.

The end-to-end script runs against a live stack (Python standard library only):

```bash
python scripts/smoke_test.py http://localhost:8000
```

## Manual verification

**30-second delay**

```bash
curl -s -X POST localhost:8000/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_delay_1","source_account":"a","destination_account":"b","amount":100,"currency":"INR"}'
curl -s localhost:8000/v1/transactions/txn_delay_1    # "PROCESSING", processed_at null
sleep 32
curl -s localhost:8000/v1/transactions/txn_delay_1    # "PROCESSED", processed_at ≈ created_at + 30s
```

**Duplicate prevention**

```bash
for i in $(seq 1 10); do
  curl -s -X POST localhost:8000/v1/webhooks/transactions -H "Content-Type: application/json" \
    -d '{"transaction_id":"txn_dup_1","source_account":"a","destination_account":"b","amount":100,"currency":"INR"}' &
done; wait                       # all 202, exactly one with "duplicate":false
docker compose exec db psql -U postgres -d webhooks -c \
  "select (select count(*) from transactions where transaction_id='txn_dup_1') txns,
          (select count(*) from jobs where transaction_id='txn_dup_1') jobs;"   # 1 | 1
```

**Worker recovery**

```bash
JOB_SQL="select status, attempts, locked_by from jobs where transaction_id='txn_recover_1';"

curl -s -X POST localhost:8000/v1/webhooks/transactions -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_recover_1","source_account":"a","destination_account":"b","amount":100,"currency":"INR"}'
sleep 5
docker compose exec db psql -U postgres -d webhooks -c "$JOB_SQL"   # RUNNING | 1

# Graceful stop: the job is released right away
docker compose stop worker
docker compose exec db psql -U postgres -d webhooks -c "$JOB_SQL"   # PENDING | 0
docker compose start worker      # processed ~30 s later

# Crash: the job stays RUNNING and is reclaimed after JOB_LEASE_SECONDS (120 s) + 30 s
docker compose kill worker && docker compose start worker
docker compose logs worker | grep "recovering abandoned job"
```

## Deployment

The same Docker image runs as two services that share one **managed** PostgreSQL database:

| Service | Command |
|---|---|
| API (web service, public) | default `CMD` (uvicorn on `$PORT`) |
| Worker (background service, no public port) | `python -m app.worker` |
| Migrations (release/pre-deploy step) | `alembic upgrade head` |

Set `DATABASE_URL` on both services to the managed database's connection string. Provider URLs such as `postgres://...?...sslmode=require` work as-is. Do not run Postgres as a container on the platform.

**Render (blueprint included: `render.yaml`).** Choose New → Blueprint and select the repository. This creates a Postgres database, the `webhooks-api` web service (runs migrations as a pre-deploy command) and the `webhooks-worker` background worker, with `DATABASE_URL` connected to both. Background workers and pre-deploy commands need paid instance types on Render.

**Other platforms** (Railway, Fly.io, Cloud Run + Cloud SQL, ECS + RDS) follow the same pattern: two services from one image, one managed Postgres, and migrations as a release step. Keep the API always on, without scale-to-zero, so cold starts don't break the 500 ms response target. The worker must run continuously and must not be scaled to zero.

## Limitations and production considerations

- **No webhook authentication.** Authentication was out of scope. In production, verify the provider's signature (for example `X-Razorpay-Signature`) before accepting a webhook.
- **First write wins for duplicates.** A duplicate with a *different* payload is still acknowledged and ignored. It could be flagged instead.
- **`FAILED` status.** Besides `PROCESSING` and `PROCESSED`, a transaction can become `FAILED`, but only after every retry is exhausted. Failed jobs keep `last_error` for investigation. There is no automatic replay.
- **Polling.** Idle workers poll every second. That is enough at this scale; `LISTEN/NOTIFY` would cut the extra idle latency.
- **Throughput.** Each worker process runs `WORKER_CONCURRENCY` jobs in parallel. Add more workers (`docker compose up -d --scale worker=3`) to scale. `SKIP LOCKED` keeps them from interfering with each other.
- **Table growth.** Completed jobs stay in the table. A retention or cleanup job would be needed at high volume.
- **500 ms target.** This assumes the API and the database are in the same region. Locally, a webhook takes about 20–120 ms from end to end.
- **Other gaps.** There is no rate limiting, metrics or tracing. Logs go to stdout.

## Project structure

```
app/
  main.py                      FastAPI app + health check
  config.py                    Settings from environment variables
  database.py                  Engine, session factory, request-scoped session dependency
  models.py                    transactions + jobs tables
  schemas.py                   Pydantic request/response models
  routes/webhooks.py           POST /v1/webhooks/transactions
  routes/transactions.py       GET /v1/transactions/{id}
  services/transaction_service.py  Atomic insert of transaction + job (idempotent)
  worker.py                    Worker process: claim / process / complete / retry / recover
migrations/                    Alembic environment and versioned migrations
tests/                         pytest suite (real PostgreSQL)
scripts/smoke_test.py          End-to-end check against a running deployment
Dockerfile, docker-compose.yml, render.yaml
```
