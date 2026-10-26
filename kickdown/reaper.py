import asyncio
import logging
from collections.abc import Mapping

from pydantic import ValidationError

from .log import default_logger
from .models import RetryPolicy, Task, Worker, retry_policy
from .store import Store, StoreError

_default_poll_interval = 15
_reap_lock_ttl = 60


class Reaper:
    """Settles tasks stranded by consumers that died mid-task.

    A consumer holds its claimed tasks in its own in-flight list and keeps a
    heartbeat key alive. When the heartbeat expires the consumer is gone, and
    whatever is left in its list is decided on one task at a time: the death
    counts as a failed attempt, so a task is pushed back into its queue only
    while its worker's retry policy allows another attempt, and is dropped as
    failed otherwise.

    The policy is looked up among the workers registered on this server; a
    task for a worker it does not know gets the default policy.
    """

    def __init__(
        self,
        store: Store,
        workers: Mapping[tuple[str, str], Worker],
        poll_interval: float = _default_poll_interval,
        logger: logging.Logger | None = None,
    ):
        self._store = store
        self._workers = workers
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
                requeued, dropped = await self._reap(consumer_id)
                await asyncio.to_thread(self._store.forget_if_dead, consumer_id)
            except StoreError as err:
                self.logger.error(
                    "redis error while reaping consumer",
                    extra={"consumer": consumer_id, "error": str(err)},
                )
                continue

            self.logger.warning(
                f"consumer {consumer_id} died, requeued {requeued} "
                f"and dropped {dropped} task(s)",
                extra={"consumer": consumer_id},
            )

    async def _reap(self, consumer_id: str) -> tuple[int, int]:
        requeued = dropped = 0
        while raw := await asyncio.to_thread(self._store.inflight_head, consumer_id):
            try:
                task = Task.model_validate_json(raw)
            except ValidationError as err:
                self.logger.error(
                    "dropped an unparsable task of a dead consumer",
                    extra={"consumer": consumer_id, "error": str(err)},
                )
                settled = await asyncio.to_thread(
                    self._store.drop_head, consumer_id, raw, None
                )
                dropped += settled
            else:
                worker = self._workers.get((task.queue, task.operation))
                policy = RetryPolicy() if worker is None else retry_policy(worker)
                if policy.allows_retry(task):
                    retry_task = task.model_copy(update={"attempt": task.attempt + 1})
                    settled = await asyncio.to_thread(
                        self._store.requeue_head, consumer_id, raw, retry_task
                    )
                    requeued += settled
                else:
                    reason = "retries exhausted" if policy.retry else "retries disabled"
                    self.logger.error(
                        f"jid={task.jid} dropped, its consumer died mid-task "
                        f"({reason})",
                        extra={
                            "consumer": consumer_id,
                            "queue": task.queue,
                            "operation": task.operation,
                        },
                    )
                    settled = await asyncio.to_thread(
                        self._store.drop_head, consumer_id, raw, task.queue
                    )
                    dropped += settled
            # the head moved under us, which only another reaper could do;
            # leave the rest to whoever holds the list now
            if not settled:
                break
        return requeued, dropped
