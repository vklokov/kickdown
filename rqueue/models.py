from typing import Annotated, Protocol, runtime_checkable

from pydantic import BaseModel, Field
from uuid_extensions import uuid7str


@runtime_checkable
class Performable(Protocol):
    queue: str
    operation: str

    async def perform(self, payload: dict) -> None: ...


class Task(BaseModel):
    queue: str
    operation: str
    params: dict
    jid: Annotated[str, Field(default_factory=lambda: uuid7str())]
    retry_count: Annotated[int, Field(default=1)]


class Stats(BaseModel):
    processed: int = 0
    failed: int = 0
