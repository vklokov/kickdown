import asyncio
import logging
from collections import deque
from collections.abc import Mapping

from pydantic import ValidationError

from .log import default_logger
from .models import Performable, Task
from .store import Store, StoreError

_default_pop_timeout = 5
_retry_delay = 1


class Consumer:
    def __init__(
        self,
        store: Store,
        workers: Mapping[tuple[str, str], Performable],
        concurrency: int = 1,
        logger: logging.Logger | None = None,
    ):
        self._store = store
        self._workers = workers
        self._queues: deque[str] = deque(sorted({worker.queue for worker in workers.values()}))
        self._semaphore = asyncio.Semaphore(concurrency)
        self.logger = logger or default_logger()
        self._tasks: set[asyncio.Task] = set()

    async def consume(self) -> None:
        while True:
            await self._semaphore.acquire()

            try:
                task = await asyncio.to_thread(self._store.pop, self._poll_order(), _default_pop_timeout)
            except StoreError as err:
                self._semaphore.release()
                self.logger.error("redis error while popping task", extra={"error": str(err)})
                await asyncio.sleep(_default_pop_timeout)
                continue
            except ValidationError as err:
                self._semaphore.release()
                self.logger.error("failed to parse task payload", extra={"error": str(err)})
                continue

            if task is None:
                self._semaphore.release()
                continue

            handle = asyncio.create_task(self._run_task(task))
            self._tasks.add(handle)
            handle.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _poll_order(self) -> list[str]:
        order = list(self._queues)
        self._queues.rotate(-1)
        return order

    async def _run_task(self, task: Task) -> None:
        failure: Exception | None = None
        try:
            worker = self._workers.get((task.queue, task.operation))
            if worker is None:
                self.logger.error(
                    "no worker registered for task",
                    extra={"jid": task.jid, "queue": task.queue, "operation": task.operation},
                )
                return

            self.logger.info(f"jid={task.jid} started", extra={"queue": task.queue, "operation": task.operation})
            await worker.perform(task.params)
            self.logger.info(f"jid={task.jid} done")
        except Exception as err:  # noqa: BLE001 - worker code is arbitrary; retry boundary must catch anything
            failure = err
        finally:
            self._semaphore.release()

        if failure is None:
            return

        if task.retry_count > 0:
            self.logger.warning(
                f"jid={task.jid} failed, retrying ({task.retry_count} attempt(s) left)",
                extra={"error": str(failure)},
            )
            retry_task = task.model_copy(update={"retry_count": task.retry_count - 1})
            await asyncio.sleep(_retry_delay)
            try:
                await asyncio.to_thread(self._store.push, retry_task)
            except StoreError as push_err:
                self.logger.error(f"jid={task.jid} failed to requeue for retry", extra={"error": str(push_err)})
        else:
            self.logger.error(f"jid={task.jid} failed permanently", extra={"error": str(failure)})
