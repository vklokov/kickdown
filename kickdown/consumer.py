import asyncio
import logging
import os
import socket
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping

from pydantic import ValidationError
from uuid_extensions import uuid7str

from .log import default_logger
from .models import Task, Worker
from .queue import Queue
from .store import Store, StoreError

_default_poll_interval = 0.1
_retry_delay = 1
_backoff_coefficient = 1.5
_heartbeat_interval = 10
_heartbeat_ttl = 30


def _default_consumer_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid7str()[-8:]}"


class Consumer:
    def __init__(
        self,
        store: Store,
        workers: Mapping[tuple[str, str], Worker],
        concurrency: int = 1,
        logger: logging.Logger | None = None,
        consumer_id: str | None = None,
    ):
        self.id = consumer_id or _default_consumer_id()
        self._store = store
        self._workers = workers
        self._queues: dict[str, Queue] = {
            name: Queue(name, store)
            for name in sorted({worker.queue for worker in workers.values()})
        }
        self._order: deque[str] = deque(self._queues)
        self._semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger or default_logger()
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        await asyncio.to_thread(self._store.register_consumer, self.id, _heartbeat_ttl)
        self.logger.info(f"consumer {self.id} registered")

    async def stop(self) -> None:
        """Returns whatever is still in flight, then leaves the registry.

        Without this a clean restart would leave its tasks sitting until the
        heartbeat expired and another server reaped them.
        """
        try:
            returned = await asyncio.to_thread(self._store.reap, self.id)
            if returned:
                self.logger.warning(
                    f"consumer {self.id} returned {returned} unfinished task(s)"
                )
            await asyncio.to_thread(self._store.deregister_consumer, self.id)
        except StoreError as err:
            self.logger.error(
                "redis error while deregistering consumer", extra={"error": str(err)}
            )

    async def heartbeat(self) -> None:
        while True:
            try:
                await asyncio.to_thread(self._store.heartbeat, self.id, _heartbeat_ttl)
            except StoreError as err:
                self.logger.error(
                    "redis error while refreshing heartbeat",
                    extra={"error": str(err)},
                )
            await asyncio.sleep(_heartbeat_interval)

    async def consume(self) -> None:
        while True:
            await self._semaphore.acquire()

            try:
                claimed = await asyncio.to_thread(
                    self._store.claim, self._poll_order(), self.id
                )
            except StoreError as err:
                self._semaphore.release()
                self.logger.error(
                    "redis error while claiming task", extra={"error": str(err)}
                )
                await asyncio.sleep(_default_poll_interval)
                continue
            except ValidationError as err:
                self._semaphore.release()
                self.logger.error(
                    "failed to parse task payload", extra={"error": str(err)}
                )
                continue

            if claimed is None:
                self._semaphore.release()
                await asyncio.sleep(_default_poll_interval)
                continue

            raw, task = claimed
            handle = asyncio.create_task(self._run_task(task, raw))
            self._tasks.add(handle)
            handle.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _poll_order(self) -> list[str]:
        order = list(self._order)
        self._order.rotate(-1)
        return order

    def _queue(self, name: str) -> Queue:
        # a payload naming a queue we do not serve is malformed, but its stats
        # should still land on the queue it claims
        return self._queues.get(name) or Queue(name, self._store)

    async def _run_task(self, task: Task, raw: bytes) -> None:
        queue = self._queue(task.queue)
        failure: Exception | None = None
        try:
            worker = self._workers.get((task.queue, task.operation))
            if worker is None:
                self.logger.error(
                    f"no worker registered for task jid={task.jid}",
                    extra={
                        "jid": task.jid,
                        "queue": task.queue,
                        "operation": task.operation,
                    },
                )
                await self._increment(queue.increment_failed)
                await self._ack(raw)
                return

            self.logger.info(
                f"jid={task.jid} started",
                extra={"queue": task.queue, "operation": task.operation},
            )
            await worker.perform(task.params)
            self.logger.info(f"jid={task.jid} done")
            await self._increment(queue.increment_processed)
        except Exception as err:  # noqa: BLE001 - worker code is arbitrary; retry boundary must catch anything
            failure = err
        finally:
            self._semaphore.release()

        if failure is None:
            await self._ack(raw)
            return

        if task.retry_count > 0:
            delay = _retry_delay * _backoff_coefficient**task.attempt
            self.logger.warning(
                f"jid={task.jid} failed, retrying in {delay:.1f}s "
                f"({task.retry_count} attempt(s) left)",
                extra={"error": str(failure)},
            )
            retry_task = task.model_copy(
                update={
                    "retry_count": task.retry_count - 1,
                    "attempt": task.attempt + 1,
                }
            )
            try:
                await queue.schedule(retry_task, time.time() + delay)
            except StoreError as schedule_err:
                # leave it in flight: the reaper will put it back rather than
                # drop it on the floor
                self.logger.error(
                    f"jid={task.jid} failed to schedule retry",
                    extra={"error": str(schedule_err)},
                )
                return
            await self._ack(raw)
        else:
            self.logger.error(
                f"jid={task.jid} failed permanently", extra={"error": str(failure)}
            )
            await self._increment(queue.increment_failed)
            await self._ack(raw)

    async def _ack(self, raw: bytes) -> None:
        try:
            await asyncio.to_thread(self._store.ack, self.id, raw)
        except StoreError as err:
            self.logger.error(
                "failed to remove task from the in-flight list",
                extra={"error": str(err)},
            )

    async def _increment(self, increment: Callable[[], Awaitable[None]]) -> None:
        try:
            await increment()
        except StoreError as err:
            self.logger.error("failed to update stats", extra={"error": str(err)})
