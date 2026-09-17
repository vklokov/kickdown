import asyncio
import logging
import time

from .log import default_logger
from .queue import Queue
from .store import StoreError

_default_poll_interval = 1
_default_batch_size = 100


class Scheduler:
    """Moves tasks whose scheduled time has come back into their queues."""

    def __init__(
        self,
        queues: list[Queue],
        poll_interval: float = _default_poll_interval,
        logger: logging.Logger | None = None,
    ):
        self._queues = queues
        self._poll_interval = poll_interval
        self.logger = logger or default_logger()

    async def run(self) -> None:
        while True:
            await self._tick()
            await asyncio.sleep(self._poll_interval)

    async def _tick(self) -> None:
        now = time.time()
        for queue in self._queues:
            try:
                moved = await queue.enqueue_due(now, _default_batch_size)
            except StoreError as err:
                self.logger.error(
                    "redis error while enqueueing due tasks",
                    extra={"queue": queue.name, "error": str(err)},
                )
                continue

            if moved:
                self.logger.info(
                    f"scheduler enqueued {moved} due task(s)",
                    extra={"queue": queue.name},
                )
