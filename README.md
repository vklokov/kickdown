# rqueue-py

A Redis-backed background job queue for Python.

## Usage

### Defining a worker

Implement the `Performable` protocol: declare which queue a worker consumes
from and which `operation` name identifies it, plus the async `perform` method.

```python
class SendEmailWorker:
    queue = "emails"
    operation = "send_email"

    async def perform(self, payload: dict) -> None:
        recipient = payload["to"]
        # ... send email
```

Any number of queues is supported — a worker's `queue` attribute is what
determines which Redis list it consumes from. The server automatically polls
every queue that has at least one registered worker.

### Running the server

```python
import asyncio
from rqueue import Server
from your_workers import SendEmailWorker, ExportReportWorker

server = Server(
    redis_url="redis://localhost:6379",
    concurrency=5,  # optional, default: 1 - max tasks processed concurrently
)
server.add_workers(SendEmailWorker(), ExportReportWorker())

asyncio.run(server.run())
```

`add_workers` accepts any number of `Performable` instances. Workers are
resolved by their `(queue, operation)` pair, so the same `operation` name can
be reused safely across different queues.

Queues are polled with equal frequency in round-robin order — there is
currently no notion of priority between queues.

### Enqueueing jobs

```python
from rqueue import Client, Task

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
with `retry_count` decremented by one, after a short fixed delay. Once
`retry_count` reaches `0` the task is dropped.

Queue names are raw identifiers (e.g. `"default"`, `"emails"`). The client
constructs the full Redis key internally as `rqueue:queue:{name}`.

### Inspecting a queue

```python
# Tasks waiting to be processed in a given queue (non-destructive)
tasks = await client.pending("emails")

# Cumulative processed/failed counters for that queue
stats = await client.stats("emails")
print(stats.processed, stats.failed)
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

server.logger = logging.getLogger("myapp.rqueue")
client.logger = logging.getLogger("myapp.rqueue")
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
