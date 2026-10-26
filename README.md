# kickdown

A Redis-backed background job queue for Python.

Requires Python 3.13+ and **Redis 6.2 or newer** (the queue relies on `LMOVE`
and on server-side Lua scripts introduced in that release). It runs against a
single Redis instance or a master managed by [Redis Sentinel](#redis-sentinel),
following the master through a failover.

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
    redis_url="redis://localhost:6379",  # or redis+sentinel://..., see below
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

### Redis Sentinel

To run against a master managed by [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/),
give `Client` and `Server` a `redis+sentinel://` URL instead of a `redis://`
one. Nothing else changes:

```python
server = Server(
    redis_url="redis+sentinel://sentinel-0:26379,sentinel-1:26379,sentinel-2:26379/mymaster",
)
```

```
redis+sentinel://[[username]:password@]host[:port][,host[:port]...]/master_name[?options]
```

- The hosts are the sentinels, not the Redis nodes; a host without a port
  uses `26379`. List every sentinel you have — a sentinel that is down is
  skipped, so one is enough to find the master as long as it answers.
- The path is the master name the sentinels monitor (`sentinel monitor
  <name> ...`).
- `username`/`password` in the URL authenticate against the Redis master.
  Sentinels have their own ACLs, so they take `sentinel_username` and
  `sentinel_password` from the query string instead.

Query options:

| Option                   | Default | Meaning                                              |
| ------------------------ | ------- | ---------------------------------------------------- |
| `db`                     | `0`     | Database number on the master                        |
| `sentinel_username`      | –       | ACL user for the sentinels                           |
| `sentinel_password`      | –       | Password for the sentinels                           |
| `socket_timeout`         | `5`     | Seconds a command may take before it counts as failed |
| `socket_connect_timeout` | `2`     | Seconds to establish a TCP connection                |

The timeouts apply to sentinel and master connections alike. Unlike plain
`redis://` URLs, which keep redis-py's default of no timeout, the Sentinel
mode sets them so that a node that disappears without closing its sockets
cannot hang a call indefinitely — a hung heartbeat would get the consumer
reaped by its peers while it is still working.

#### What happens during a failover

Every connection asks the sentinels where the master currently is before it
is established, and a failed command drops its connection, so the next call
resolves the master again and lands on the new one as soon as the sentinels
announce it. A connection still pointed at the old master after it came back
as a replica is dropped on the first `READONLY` reply the same way.

No command is ever re-sent by the client, in Sentinel mode or otherwise: a
reply lost to a timeout does not mean the command did not run, and
re-sending a claim or a push would strand a task in an in-flight list nobody
is working on, or enqueue it twice. A call that fails raises `StoreError`
instead, and each caller decides what to do with it.

Inside `Server`, nothing dies while the master is unreachable: the consumer,
scheduler, reaper and heartbeat loops log the error and retry on their own
schedule, and `/ready` answers `503` until Redis is back while `/live`
stays `200`. The consumer backs off from its 100ms poll interval up to 2s
between attempts while claims keep failing, logging the first failure, a
line each time the wait grows, and then one roughly every 30s; it logs once
more when Redis answers again and goes back to polling at full speed. Tasks that were in flight when the master went down are kept in
the consumer's in-flight list, which the replica has (asynchronously)
replicated, and run to completion on the worker. `Client.enqueue` and
`Server.enqueue` raise `StoreError` for the duration of the switch, as they
do for any other Redis error — what to do with a task that could not be
accepted is the caller's call.

Two consequences of the at-least-once model are worth knowing here:

- Replication is asynchronous, so the last few writes before a crash can be
  lost with the old master: a task enqueued or acknowledged a moment before
  the failure may reappear (and run again) or vanish.
- An outage longer than the heartbeat TTL (30s) expires every consumer's
  heartbeat on the new master. The first reaper to run after the failover
  requeues the tasks those consumers were still working on, so they run
  twice. Consumers re-register themselves with the next heartbeat.

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

How a failed task is retried is decided by its worker, not by the task. A
worker can override two optional attributes:

```python
class ChargeCardWorker:
    queue = "payments"
    operation = "charge"
    retry = False  # default: True


class SendEmailWorker:
    queue = "emails"
    operation = "send_email"
    retry_count = 5  # default: 1
```

- `retry_count` is how many times a failed task runs again, so it runs at
  most `1 + retry_count` times.
- `retry = False` turns retries off altogether, on exceptions and on crashes
  alike (see below); `retry_count` is then ignored.

`add_workers` rejects a worker whose `retry` is not a `bool` or whose
`retry_count` is not an `int` of at least `0`.

The task itself only carries `attempt`, the number of runs that already
failed. On failure, the consumer re-enqueues the task with `attempt`
incremented by one, after an exponentially growing delay, as long as
`attempt` is below the worker's `retry_count`. After that the task is
dropped and counted as failed.

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
tasks immediately instead of waiting to be reaped. A consumer re-registers
itself with every heartbeat, so one that was reaped while Redis was
unreachable (and is in fact still running) is watched again as soon as it
is back in touch; the reaper only forgets a consumer whose heartbeat is
still missing at that moment.

A crash counts as a failed attempt, under the retry policy of the task's
worker. The reaper pushes a task back into its queue straight away (no
backoff delay) with `attempt` incremented by one, and only while the worker
allows another retry. Otherwise, or if the worker sets `retry = False`, the
task is dropped and counted as failed. So a task that reliably kills its
process (an OOM, say) runs at most `1 + retry_count` times.

The reaper looks the policy up among the workers registered on *its own*
server, which need not be the server that died. A task for a worker the
reaping server does not have gets the default policy (`retry = True`,
`retry_count = 1`). Keep the workers' retry settings identical across the
servers sharing a Redis.

That makes delivery **at-least-once** for workers that retry: a task
interrupted by a crash runs again, and a task that crashed the process
*after* its side effects completed runs those side effects twice. Such
workers must be idempotent. With `retry = False` it is at-most-once instead:
an interrupted task never runs again.

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
machines, run multiple `Server` processes against the same Redis (a single
instance or a Sentinel-managed master); each process independently claims
from the shared queues, so Redis balances the work between them without any
extra coordination.
