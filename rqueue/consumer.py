import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from rqueue.config import Config
from rqueue.schemas import Job, Performable
from rqueue.store import Store, StoreError


class Consumer:
    def __init__(
        self,
        store: Store,
        config: Config,
        workers: dict[str, Performable],
    ):
        self._store = store
        self.config = config
        self.logger = config.logger
        self._semaphore = asyncio.Semaphore(config.concurrency)
        self._last_heartbeat = datetime.now(UTC)
        self._workers = workers
        self._retry_tasks: set[asyncio.Task] = set()

    async def consume(self):
        while True:
            await self._semaphore.acquire()

            try:
                raw = await self._store.pop(timeout=self.config.redis_ping_timeout)
            except StoreError as err:
                self._semaphore.release()
                self.logger.error(
                    "[RQueueServer] redis error", extra={"error": str(err)}
                )
                await asyncio.sleep(self.config.redis_reconnect_delay)
                continue

            await self._ping()

            if raw is None:
                self._semaphore.release()
                continue

            try:
                payload = json.loads(raw)
                asyncio.create_task(self._run_job(payload))
            except json.JSONDecodeError as err:
                self._semaphore.release()
                self.logger.error(
                    "[RQueueServer] failed to parse payload",
                    extra={"payload": raw, "error": str(err)},
                )

    async def _run_job(self, envelop: dict):
        job = None
        try:
            job = Job.model_validate(envelop)
            worker = self._workers.get(job.worker)

            if not worker:
                raise RuntimeError(f"worker not found: {job.worker}")

            async with self._track_job(job):
                await worker.perform(job.payload)

        except Exception as err:
            if job is not None and job.attempt < job.retry_count:
                job = job.model_copy(update={"attempt": job.attempt + 1})
                delay = job.backoff_coefficient**job.attempt
                self.logger.warning(
                    f"[RQueueServer] jid={job.jid} failed, retrying ({job.attempt}/{job.retry_count}) in {delay:.1f}s",
                    extra={"error": str(err)},
                )
                task = asyncio.create_task(self._retry_job(job, delay))
                self._retry_tasks.add(task)
                task.add_done_callback(self._retry_tasks.discard)
            else:
                self.logger.error("[RQueueServer] error", extra={"error": str(err)})
                await self._store.increment_failed()
        finally:
            self._semaphore.release()

    @asynccontextmanager
    async def _track_job(self, job: Job):
        """
        Executor wrapper aimed to track job processing like duration and metrics
        """
        self.logger.info(f"[RqueueServer] jid={job.jid} started")
        started_at = datetime.now(UTC)
        yield
        duration = (datetime.now(UTC) - started_at).total_seconds()
        self.logger.info(
            f"[RqueueServer] jid={job.jid} done", extra={"duration": duration}
        )
        await self._store.increment_processed()

    async def _retry_job(self, job: Job, delay: float) -> None:
        await asyncio.sleep(delay)
        await self._store.push_async(job)

    @property
    def is_ok(self) -> bool:
        elapsed = (datetime.now(UTC) - self._last_heartbeat).total_seconds()
        return elapsed < self.config.redis_ping_timeout * 2

    @property
    def last_ping(self) -> str:
        return self._last_heartbeat.strftime("%Y-%m-%d %H:%M:%S UTC")

    async def _ping(self):
        try:
            await self._store.ping_async()
            self._last_heartbeat = datetime.now(UTC)
        except StoreError as e:
            self.logger.error(
                "[RQueueServer] redis ping failed", extra={"error": str(e)}
            )
