from typing import Protocol, runtime_checkable

from pydantic import BaseModel


@runtime_checkable
class Performable(Protocol):
    async def perform(self, payload: dict) -> None: ...


@runtime_checkable
class Loggable(Protocol):
    def info(self, msg: str, extra: dict | None = None): ...
    def error(self, msg: str, extra: dict | None = None): ...
    def debug(self, msg: str, extra: dict | None = None): ...
    def warning(self, msg: str, extra: dict | None = None): ...


class Status(BaseModel):
    ok: bool
    last_ping: str
    concurrency: int
    uptime: str


class Observable(Protocol):
    def status(self) -> Status: ...


class Job(BaseModel):
    jid: str
    worker: str
    payload: dict
    retry_count: int = 1
    backoff_coefficient: float = 1.5
    attempt: int = 0


class Stats(BaseModel):
    processed: int = 0
    failed: int = 0
