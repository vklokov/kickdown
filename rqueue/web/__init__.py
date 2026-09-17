import asyncio
import secrets
from html import escape
from pathlib import Path
from typing import Protocol, runtime_checkable

import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from ..store import Store, StoreError

_ASSETS_DIR = Path(__file__).parent / "assets"
_ADMIN_TEMPLATE = (Path(__file__).parent / "admin.html").read_text()

_security = HTTPBasic(auto_error=False)


@runtime_checkable
class ServerLike(Protocol):
    @property
    def store(self) -> Store: ...

    @property
    def queues(self) -> list[str]: ...


class Web:
    def __init__(
        self,
        port: int,
        server: ServerLike,
        admin_username: str | None = None,
        admin_password: str | None = None,
    ):
        self._port = port
        self._server = server
        self._admin_username = admin_username
        self._admin_password = admin_password
        self._api = FastAPI()
        self._api.get("/live")(self._live)
        self._api.get("/ready", response_model=None)(self._ready)
        self._api.get("/admin", response_class=HTMLResponse)(self._admin)
        self._api.mount("/admin/assets", StaticFiles(directory=_ASSETS_DIR), name="admin-assets")

    async def _live(self) -> dict:
        return {"status": "ok"}

    async def _ready(self) -> dict | JSONResponse:
        try:
            await asyncio.to_thread(self._server.store.ping)
        except StoreError:
            return JSONResponse(status_code=503, content={"status": "redis unavailable"})
        return {"status": "ok"}

    def _authorized(self, credentials: HTTPBasicCredentials | None) -> bool:
        if not (self._admin_username and self._admin_password):
            return True
        return (
            credentials is not None
            and secrets.compare_digest(credentials.username, self._admin_username)
            and secrets.compare_digest(credentials.password, self._admin_password)
        )

    async def _admin(self, credentials: HTTPBasicCredentials | None = Depends(_security)) -> HTMLResponse:
        if not self._authorized(credentials):
            raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})
        return HTMLResponse(await self._render_admin())

    async def _render_admin(self) -> str:
        queues = self._server.queues
        stats = await asyncio.gather(*(asyncio.to_thread(self._server.store.stats, queue) for queue in queues))
        lengths = await asyncio.gather(
            *(asyncio.to_thread(self._server.store.queue_length, queue) for queue in queues)
        )

        if queues:
            rows = "\n".join(
                f"<tr><td>{escape(queue)}</td><td>{length}</td></tr>"
                for queue, length in zip(queues, lengths, strict=True)
            )
        else:
            rows = '<tr><td colspan="2">No queues registered</td></tr>'

        return (
            _ADMIN_TEMPLATE.replace("__TOTAL_PROCESSED__", str(sum(s.processed for s in stats)))
            .replace("__TOTAL_FAILED__", str(sum(s.failed for s in stats)))
            .replace("__QUEUE_ROWS__", rows)
        )

    async def run(self) -> None:
        config = uvicorn.Config(self._api, host="0.0.0.0", port=self._port, log_level="error")
        server = uvicorn.Server(config)
        await server.serve()
