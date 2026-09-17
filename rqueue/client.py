import asyncio

from .log import default_logger
from .models import Stats, Task
from .store import Store


class Client:
    def __init__(self, redis_url: str):
        self._store = Store(redis_url)
        self.logger = default_logger()

    async def enqueue(self, task: Task) -> str:
        await asyncio.to_thread(self._store.push, task)
        self.logger.info(
            f"jid={task.jid} accepted",
            extra={"queue": task.queue, "operation": task.operation},
        )
        return task.jid

    async def pending(self, queue: str) -> list[Task]:
        return await asyncio.to_thread(self._store.pending, queue)

    async def stats(self, queue: str) -> Stats:
        return await asyncio.to_thread(self._store.stats, queue)

    async def close(self):
        await asyncio.to_thread(self._store.close)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        await self.close()
