from typing import Protocol, runtime_checkable

from pydantic import BaseModel, Field
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
    retry_count: int = 1
    attempt: int = 0


class Stats(BaseModel):
    processed: int = 0
    failed: int = 0
