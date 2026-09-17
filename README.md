# kickdown

A Redis-backed background job queue for Python.

Requires Python 3.13+ and **Redis 6.2 or newer** (the queue relies on `LMOVE`
and on server-side Lua scripts introduced in that release).

## Installation

```sh
uv add kickdown
```

Pre-releases are not picked up by default, so ask for one explicitly:

```sh
uv add kickdown --prerelease=allow
```

## Usage

### Defining a worker

A worker declares which queue it consumes from, which `operation` name
identifies it, and an async `perform`:

```python
class SendEmailWorker:
    queue = "emails"
    operation = "send_email"

    @classmethod
    async def perform(cls, payload: dict) -> None:
        recipient = payload["to"]
        # ... send email
```

Workers are registered as classes, so nothing has to be instantiated to run a
task. If a worker needs per-instance state, register an instance instead — with
`perform` as a regular `async def perform(self, payload)`; both forms are
accepted, and `Worker` is the type covering them.

Any number of queues is supported — a worker's `queue` attribute is what
determines which Redis list it consumes from. The server automatically polls
every queue that has at least one registered worker.

### Running the server

```python
import asyncio
from kickdown import Server
from your_workers import SendEmailWorker, ExportReportWorker

server = Server(
    redis_url="redis://localhost:6379",
    concurrency=5,  # optional, default: 1 - max tasks processed concurrently
)
server.add_workers(SendEmailWorker, ExportReportWorker)

asyncio.run(server.run())
```

`add_workers` accepts any number of workers, as classes or as instances.
Workers are resolved by their `(queue, operation)` pair, so the same
`operation` name can be reused safely across different queues.

Queues are polled with equal frequency in round-robin order — there is
currently no notion of priority between queues.

### Enqueueing jobs

```python
from kickdown import Client, Task

client = Client(redis_url="redis://localhost:6379")

task = Task(
    queue="emails",
    operation="send_email",
    params={"to": "user@example.com"},
)
jid = await client.enqueue(task)
```

`enqueue` returns the job ID (`jid`) that can be used for tracing. It is
generated automatically (a time-sortable `uuid7`) if not set explicitly on
the `Task`.

`Client` can also be used as an async context manager, which closes the
underlying Redis connection on exit:

```python
async with Client(redis_url="redis://localhost:6379") as client:
    await client.enqueue(task)
```

`Server` can enqueue tasks too, using the same Redis connection — handy for
a worker that needs to schedule a follow-up task, or for enqueueing from a
startup hook, without opening a separate `Client`:

```python
jid = await server.enqueue(task)
```

#### Retries

`retry_count` (default `1`) on `Task` sets how many times a failed task is
retried before being dropped. On failure, the consumer re-enqueues the task
with `retry_count` decremented by one and `attempt` incremented by one, after
an exponentially growing delay. Once `retry_count` reaches `0` the task is
dropped.

The delay is `1s * 1.5 ** attempt` — both the base delay and the backoff
coefficient are fixed in the library and cannot be configured per task:

| Retry | Delay |
| ----- | ----- |
| 1st   | 1.0s  |
| 2nd   | 1.5s  |
| 3rd   | 2.3s  |

A retried task is not held in memory while it waits: it goes into the queue's
scheduled sorted set in Redis (`kickdown:scheduled:{name}`, scored by its due
timestamp), and a scheduler loop running inside every server moves due tasks
back into the queue. So a retry survives a process restart, and the
concurrency slot is freed immediately instead of being blocked for the whole
delay.

A server only sweeps the queues it has workers for, which is also the only
place its own retries can land.

#### Crash recovery

A task is never held only in the worker process's memory. Claiming one moves
it, in a single Redis operation, from its queue into an in-flight list private
to that consumer (`kickdown:inflight:{consumer_id}`), where it stays until the
worker finishes. Each server keeps a heartbeat key alive while it runs, and a
reaper loop inside every server watches for consumers whose heartbeat has
expired: whatever is left in a dead consumer's list is pushed back into the
queue it came from. On a clean shutdown a server returns its own unfinished
tasks immediately instead of waiting to be reaped.

That makes delivery **at-least-once**: a task interrupted by a crash runs
again, and a task that crashed the process *after* its side effects completed
runs those side effects twice. Workers must be idempotent.

Two consequences worth knowing:

- A task that reliably kills its process (an OOM, say) will be requeued and
  kill it again. There is no poison-pill limit yet.
- A reaped task keeps its `retry_count`: being interrupted is not counted as a
  failed attempt.

Queue names are raw identifiers (e.g. `"default"`, `"emails"`). The client
constructs the full Redis key internally as `kickdown:queue:{name}`.

### Inspecting a queue

`client.queue(name)` returns a `Queue` handle — every per-queue operation
lives on it, so the queue name is given once instead of on every call:

```python
emails = client.queue("emails")

# Tasks waiting to be processed (non-destructive)
tasks = await emails.pending()
count = await emails.length()

# Tasks waiting for their retry delay to elapse
retries = await emails.scheduled()

# Cumulative processed/failed counters
stats = await emails.stats()
print(stats.processed, stats.failed)
```

Enqueueing stays on the client (`client.enqueue(task)`): a `Task` already
carries its own `queue`, and that field remains the single source of truth
for routing.

`purge()` drops every task waiting in the queue and returns how many were
dropped. It does not touch scheduled tasks or the counters, and there is no
undo:

```python
dropped = await emails.purge()
```

`processed` counts tasks whose worker completed successfully; `failed`
counts tasks that were permanently dropped (retries exhausted, or no
worker registered for the task's `operation`).

### Lifecycle hooks

Register async callbacks to run on server startup and shutdown — useful for
initialising shared resources like database pools.

```python
server = Server(redis_url=...)


@server.on_startup
async def init_db():
    app.db = await asyncpg.create_pool(DATABASE_URL)


@server.on_shutdown
async def close_db():
    await app.db.close()
```

Both methods can also be called directly instead of used as decorators:

```python
server.on_startup(init_db)
server.on_shutdown(close_db)
```

Startup hooks run after the Redis connection is verified, before the
consumer starts. Shutdown hooks run after the consumer stops (whether by
`SIGTERM`/`SIGINT` or an unexpected error), before the Redis connection is
closed.

### Logging

`Client` and `Server` each expose a plain `logging.Logger` as `.logger`,
pre-configured to write text to stdout — there's no custom logger
interface to implement. To use your own logger (different format, handler,
sink, etc.), just assign it:

```python
import logging

server.logger = logging.getLogger("myapp.kickdown")
client.logger = logging.getLogger("myapp.kickdown")
```

`Client` logs when a task is accepted. `Server` logs process start/shutdown
(with the polled queues and concurrency), and each task's start,
completion, retries and permanent failures — every task-related message
includes the task's `jid`.

### Healthcheck

`Server` always starts a small HTTP server for liveness/readiness probes,
on the port given by `web_port` (default `3030`):

```python
server = Server(redis_url=..., web_port=3030)
```

```
GET /live   -> 200 {"status": "ok"}                         # process is up
GET /ready  -> 200 {"status": "ok"}                          # Redis reachable
            -> 503 {"status": "redis unavailable"}           # Redis unreachable
```

### Admin page

`GET /admin` renders an HTML dashboard: a summary block with total
processed/failed counters (aggregated across all queues, from the same
counters as `client.stats()`), and a table of every polled queue with its
current pending count (how many tasks are physically waiting in it).

By default it's open to anyone who can reach the port. To require HTTP
Basic Auth, set both `admin_username` and `admin_password`:

```python
server = Server(
    redis_url=...,
    admin_username="alice",
    admin_password="secret",
)
```

If either is left unset, `/admin` requires no credentials.

## Scaling

`Server.run()` uses a single asyncio event loop with an `asyncio.Semaphore`
to cap concurrent task execution within the process — this is a good fit
since `Performable.perform` is a coroutine. To scale across CPU cores or
machines, run multiple `Server` processes against the same Redis instance;
each process independently pops from the shared queues, so Redis balances
the work between them without any extra coordination.
