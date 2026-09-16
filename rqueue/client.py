
from uuid_extensions import uuid7str

from rqueue.config import _default_queue
from rqueue.schemas import Job, Performable, Stats
from rqueue.store import Store


class Client:
    def __init__(self, redis_url: str, queue: str | None = None):
        self._store = Store(redis_url, queue or _default_queue)

    def enqueue(
        self,
        worker: type[Performable],
        payload: dict,
        *,
        retry_count: int = 1,
        backoff_coefficient: float = 1.5,
    ) -> str:
        job = Job(
            jid=uuid7str(),
            worker=worker.__name__,
            payload=payload,
            retry_count=retry_count,
            backoff_coefficient=backoff_coefficient,
        )
        self._store.push(job)
        return job.jid

    def pending(self) -> list[Job]:
        return self._store.pending()

    def stats(self) -> Stats:
        return self._store.stats()

    def close(self):
        self._store.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
