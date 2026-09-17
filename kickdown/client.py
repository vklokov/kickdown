import asyncio

from .log import default_logger
from .models import Task
from .queue import Queue
from .store import Store


class Client:
    def __init__(self, redis_url: str):
        self._store = Store(redis_url)
        self.logger = default_logger()

    def queue(self, name: str) -> Queue:
        return Queue(name, self._store)

    async def enqueue(self, task: Task) -> str:
        await self.queue(task.queue).push(task)
        self.logger.info(
            f"jid={task.jid} accepted",
            extra={"queue": task.queue, "operation": task.operation},
        )
        return task.jid

    async def close(self):
        await asyncio.to_thread(self._store.close)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        await self.close()
