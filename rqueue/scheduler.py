import asyncio
import logging
import time

from .log import default_logger
from .store import Store, StoreError

_default_poll_interval = 1
_default_batch_size = 100


class Scheduler:
    """Moves tasks whose scheduled time has come back into their queues."""

    def __init__(
        self,
        store: Store,
        poll_interval: float = _default_poll_interval,
        logger: logging.Logger | None = None,
    ):
        self._store = store
        self._poll_interval = poll_interval
        self.logger = logger or default_logger()

    async def run(self) -> None:
        while True:
            await self._tick()
            await asyncio.sleep(self._poll_interval)

    async def _tick(self) -> None:
        try:
            moved = await asyncio.to_thread(
                self._store.enqueue_due, time.time(), _default_batch_size
            )
        except StoreError as err:
            self.logger.error(
                "redis error while enqueueing due tasks", extra={"error": str(err)}
            )
            return

        if moved:
            self.logger.info(f"scheduler enqueued {moved} due task(s)")
