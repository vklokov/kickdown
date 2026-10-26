from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field
from uuid_extensions import uuid7str


@runtime_checkable
class Performable(Protocol):
    queue: str
    operation: str

    async def perform(self, payload: dict) -> None: ...


# Workers are registered either as classes (with `perform` as a classmethod) or
# as instances; both are called the same way.
type Worker = type[Performable] | Performable


class Task(BaseModel):
    queue: str
    operation: str
    params: dict
    jid: str = Field(default_factory=lambda: uuid7str())
    attempt: int = 0


class RetryPolicy(BaseModel):
    """How a worker's tasks are retried, read from its optional overrides.

    `retry_count` is how many times a failed task runs again, so it runs at
    most `1 + retry_count` times. With `retry` off a failed or interrupted
    task is dropped and `retry_count` is ignored.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    retry: bool = True
    retry_count: int = Field(default=1, ge=0)

    def allows_retry(self, task: Task) -> bool:
        return self.retry and task.attempt < self.retry_count


def retry_policy(worker: Worker) -> RetryPolicy:
    """Reads a worker's `retry`/`retry_count`, defaulting whichever is unset.

    An override of the wrong type or out of range raises `ValidationError`.
    """
    overrides = {
        name: getattr(worker, name)
        for name in ("retry", "retry_count")
        if hasattr(worker, name)
    }
    return RetryPolicy.model_validate(overrides)


class Stats(BaseModel):
    processed: int = 0
    failed: int = 0
