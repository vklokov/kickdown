import asyncio
import logging

from .log import default_logger
from .store import Store, StoreError

_default_poll_interval = 15
_reap_lock_ttl = 60


class Reaper:
    """Returns tasks stranded by consumers that died mid-task.

    A consumer holds its claimed tasks in its own in-flight list and keeps a
    heartbeat key alive. When the heartbeat expires the consumer is gone, and
    whatever is left in its list is pushed back into the queues it came from.
    """

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
            consumers = await asyncio.to_thread(self._store.consumers)
        except StoreError as err:
            self.logger.error(
                "redis error while listing consumers", extra={"error": str(err)}
            )
            return

        for consumer_id in consumers:
            try:
                if await asyncio.to_thread(self._store.is_alive, consumer_id):
                    continue
                # one server reaps a given corpse; the rest skip it
                if not await asyncio.to_thread(
                    self._store.claim_reap, consumer_id, _reap_lock_ttl
                ):
                    continue
                reaped = await asyncio.to_thread(self._store.reap, consumer_id)
                await asyncio.to_thread(self._store.deregister_consumer, consumer_id)
            except StoreError as err:
                self.logger.error(
                    "redis error while reaping consumer",
                    extra={"consumer": consumer_id, "error": str(err)},
                )
                continue

            self.logger.warning(
                f"consumer {consumer_id} died, requeued {reaped} task(s)",
                extra={"consumer": consumer_id},
            )
