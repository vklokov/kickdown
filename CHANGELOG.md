# Changelog

## 0.4.0 - 2026-09-17

### Added
- Tasks are no longer lost when a worker process dies mid-task: claiming a task atomically moves it from its queue into an in-flight list private to that consumer (`kickdown:inflight:{consumer_id}`), and it is only removed once the task finishes
- Every server keeps a heartbeat key (`kickdown:beat:{consumer_id}`) alive and registers itself in `kickdown:consumers`; a `Reaper` loop returns the in-flight tasks of any consumer whose heartbeat expired, guarded by a lock so servers do not race
- A clean shutdown returns the server's own unfinished tasks to their queues instead of leaving them for the reaper
- The admin dashboard's `In-flight` column now shows real per-queue counts

### Changed
- Delivery is now at-least-once, so workers must be idempotent: a task interrupted by a crash runs again
- Queues are polled by a Lua claim script instead of `BLPOP`; polling is now interval-based (100ms) because no Redis primitive can block on a move across several keys at once
- Workers can be registered as classes with `perform` as a `classmethod`, not just as instances — `add_workers` accepts both, typed as the new `Worker` alias (`type[Performable] | Performable`); passing a class did not type-check before
- `Task` field defaults are declared so that type checkers see them — `Task(queue=..., operation=..., params=...)` now type-checks for users, which it did not before despite the package shipping `py.typed`

## 0.4.0a1 - 2026-09-17

### Changed
- Rewritten around an arbitrary number of queues instead of a single configured queue
  - Workers (`Performable`) now declare their own `queue` and `operation`; the server derives the set of polled queues from the registered workers automatically
  - Queues are polled in round-robin order with equal frequency — no priority between queues at this stage
  - Workers are keyed by `(queue, operation)`, so the same `operation` name can be reused safely across different queues
- `Client.enqueue` now takes a single `Task` (carrying `queue`, `operation`, `params`, `jid`, `retry_count`) instead of a worker class and payload dict
- `Client.pending(queue)` / `Client.stats(queue)` moved onto the queue handle: `client.queue(name).pending()` / `.stats()`
- `Client` is now fully async (`enqueue`, `pending`, `close`), and supports `async with` instead of a sync context manager
- `Server` no longer takes a `Config` object; it's constructed directly with `redis_url` and `concurrency`
- Retries use a hardcoded exponential backoff (`1s * 1.5 ** attempt`) instead of a per-task `backoff_coefficient` — a failed task is retried with `retry_count` decremented and `attempt` incremented until `retry_count` reaches `0`
- Retries are no longer awaited in memory: the failed task is stored in a scheduled sorted set in Redis and picked up when due, so it survives a process restart and does not hold a concurrency slot for the duration of the delay
- On shutdown, the server now waits for in-flight tasks (including ones mid-retry) to finish before closing the Redis connection

### Added
- A default logger (plain `logging.Logger`, text output to stdout) on `Client` and `Server` — no custom logger interface/protocol; swap it by assigning `server.logger = ...` / `client.logger = ...`
  - `Client` logs when a task is accepted (enqueued)
  - `Server` logs process start/shutdown (with the polled queues and concurrency), each task's start/completion, retries, permanent failures, and unrecognized operations — all messages include the task's `jid`
- `Server` always starts a small HTTP server (`web_port`, default `3030`) with `/live` and `/ready` for liveness/readiness probes; `/ready` checks Redis connectivity
- Per-queue `processed`/`failed` counters (`Stats` model) via `client.queue(name).stats()`; the consumer increments them on task completion, permanent failure (retries exhausted), and unrecognized operations
- `GET /admin` renders an HTML dashboard: total processed/failed/scheduled counters, and a per-queue table of pending/scheduled/in-flight counts (in-flight is a placeholder until tasks are tracked while they run); optionally gated behind HTTP Basic Auth via `Server(admin_username=..., admin_password=...)` (no auth if either is left unset)
- `Queue.purge()` drops every pending task in a queue and returns how many were dropped (scheduled tasks and counters are untouched); not exposed over HTTP
- `Server.enqueue(task)` pushes a task using the server's own Redis connection, so workers/hooks can schedule tasks without a separate `Client`
- A `Scheduler` loop runs inside every `Server`, moving due tasks from each queue's `kickdown:scheduled:{name}` set back into the queue (atomically, via a Lua script); a server only sweeps the queues it has workers for
- `Queue`, a handle bound to one queue name, carrying every per-queue operation (`push`, `pending`, `length`, `scheduled`, `stats`, `purge`, counters); obtained via `Client.queue(name)` / `Server.queue(name)`, and `Server.queues` now returns `Queue` objects instead of names

### Removed
- The old `Loggable` protocol and the custom `Logger` wrapper class (replaced by the stdlib logger above)
- `Server.uptime()` / `Status` and the `humanize` dependency

---

## 0.3.3 - 2026-06-19

### Changed
- Pinned `redis` to the 7.x line (`~=7.4`) to avoid an unintended upgrade to `redis` 8.x; previously `>=7.4.0` allowed any newer major version

---

## 0.3.2 - 2026-06-03

### Added
- Job execution duration is now tracked and logged (in seconds) on job completion

---

## 0.3.1 - 2026-05-26

### Added
- Lifecycle hooks: `Server.on_startup` and `Server.on_shutdown` register async callbacks for server startup and shutdown
  - Usable as decorators or called directly with a callable argument
  - Startup hooks run after the Redis connection is verified, before the consumer starts
  - Shutdown hooks run after the consumer is stopped, before the Redis connection is closed
  - Exceptions in hooks are logged and do not crash the server

---

## 0.3.0 - 2026-05-21

### Added
- Retry mechanism for failed jobs
  - `Client.enqueue` accepts `retry_count` (default: `1`) and `backoff_coefficient` (default: `1.5`)
  - Both values are stored on the `Job` model and travel with the job through the queue
  - On failure, the consumer re-enqueues the job with an exponential backoff delay (`backoff_coefficient ** attempt` seconds) until retries are exhausted
  - Once all retries are exhausted the job is marked failed as before
  - Jobs log a warning on each retry attempt and an error only when permanently failed

---

## 0.2.8 - 2026-05-20

### Changed
- `Server.add_worker` now accepts a worker **instance** (`Performable`) — the same instance is reused across all jobs of that type
- `Client.enqueue` accepts a worker **class** (`type[Performable]`) — no instantiation cost at enqueue time, only the class name is needed to build the job
- Healthcheck rewritten with FastAPI + uvicorn, replacing the manual raw TCP/HTTP server
  - `GET /live` — always returns 200, used as a liveness probe
  - `GET /ready` — checks consumer heartbeat and Redis connectivity (`store.ping`); returns 503 with reason on failure
- Job IDs switched from `uuid4` (stdlib) to `uuid7` (`uuid_extensions`), providing time-sortable identifiers

### Fixed
- `config.queue` was logged as a bound method object; now called correctly as `config.queue()`

### Dependencies
- Added `fastapi>=0.136.1`
- Added `uvicorn>=0.34.0`

---

## 0.2.6 - 2026-05-20

### Changed
- All Redis operations extracted into a new internal `Store` class (`kickdown/store.py`)
- Redis key names (`kickdown:queue:*`, `kickdown:stats:*`) are now centralized in `Store`
- `Consumer` and `Client` no longer interact with Redis directly — all calls go through `Store`
- `asyncio.to_thread` wrapping for blocking Redis calls moved from `Consumer` into `Store`

---

## 0.2.5 - 2026-05-20

### Changed
- Workers are now registered on `Server` via `add_worker()` instead of being passed to `Config`
- `Server` lazily initializes the consumer on `start()`, decoupling construction from Redis connection
- `Consumer` receives workers directly rather than reading them from `Config`

### Added
- `Server.uptime()` returns a human-readable duration (e.g. `"1 day, 4 hours"`) via the `humanize` library
- `Status` response includes an `uptime` field
- `Server.start()` raises `RuntimeError` if no workers have been registered

### Fixed
- `Consumer.is_ok` converted from a method to a property, fixing incorrect truthy evaluation in `status()`

---

## 0.2.4 — 2026-05-19

### Changed
- Queue names are now raw identifiers (e.g. `"default"`, `"emails"`); the full Redis key is constructed internally as `kickdown:queue:{name}`
- Stats counters moved from a Redis hash to separate keys (`kickdown:stats:processed`, `kickdown:stats:failed`), incremented with `INCR`

---

## 0.2.3 — 2026-05-19

### Added
- `Client.stats()` returns a `Stats` model with cumulative `processed` and `failed` job counts
- Consumer increments `kickdown.stats.processed` / `kickdown.stats.failed` in Redis after each job completes or raises

---

## 0.2.2 — 2026-05-19

### Added
- `Client.pending()` returns a list of jobs currently waiting in the queue (non-destructive)