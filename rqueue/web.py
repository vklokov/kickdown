import asyncio
from typing import Protocol, runtime_checkable

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .store import Store, StoreError


@runtime_checkable
class ServerLike(Protocol):
    @property
    def store(self) -> Store: ...


class Web:
    def __init__(self, port: int, server: ServerLike):
        self._port = port
        self._server = server
        self._api = FastAPI()
        self._api.get("/live")(self._live)
        self._api.get("/ready", response_model=None)(self._ready)

    async def _live(self) -> dict:
        return {"status": "ok"}

    async def _ready(self) -> dict | JSONResponse:
        try:
            await asyncio.to_thread(self._server.store.ping)
        except StoreError:
            return JSONResponse(status_code=503, content={"status": "redis unavailable"})
        return {"status": "ok"}

    async def run(self) -> None:
        config = uvicorn.Config(self._api, host="0.0.0.0", port=self._port, log_level="error")
        server = uvicorn.Server(config)
        await server.serve()
