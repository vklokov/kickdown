import asyncio

from .models import Stats, Task
from .store import Store


class Queue:
    """
    A named queue: every per-queue operation, bound to one name.

    Wraps the synchronous `Store` calls in threads, so callers stay async.
    """

    def __init__(self, name: str, store: Store):
        self._name = name
        self._store = store

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"Queue({self._name!r})"

    async def push(self, task: Task) -> None:
        await asyncio.to_thread(self._store.push, task)

    async def schedule(self, task: Task, run_at: float) -> None:
        await asyncio.to_thread(self._store.schedule, task, run_at)

    async def pending(self) -> list[Task]:
        return await asyncio.to_thread(self._store.pending, self._name)

    async def length(self) -> int:
        return await asyncio.to_thread(self._store.queue_length, self._name)

    async def scheduled(self) -> list[Task]:
        return await asyncio.to_thread(self._store.scheduled, self._name)

    async def scheduled_length(self) -> int:
        return await asyncio.to_thread(self._store.scheduled_length, self._name)

    async def enqueue_due(self, now: float, limit: int) -> int:
        return await asyncio.to_thread(self._store.enqueue_due, self._name, now, limit)

    async def stats(self) -> Stats:
        return await asyncio.to_thread(self._store.stats, self._name)

    async def increment_processed(self) -> None:
        await asyncio.to_thread(self._store.increment_processed, self._name)

    async def increment_failed(self) -> None:
        await asyncio.to_thread(self._store.increment_failed, self._name)
