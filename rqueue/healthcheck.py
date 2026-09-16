import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from rqueue.schemas import Observable
from rqueue.store import Store, StoreError


class Healthchecker:
    def __init__(self, port: int, app: Observable, store: Store):
        self.port = port
        self.app = app
        self.store = store
        self._api = FastAPI()
        self._setup_routes()

    def _setup_routes(self):
        @self._api.get("/live")
        async def live():
            return {"status": "ok"}

        @self._api.get("/ready")
        async def ready():
            s = self.app.status()
            if not s.ok:
                return JSONResponse(status_code=503, content={"status": "unhealthy"})
            try:
                await self.store.ping_async()
            except StoreError:
                return JSONResponse(status_code=503, content={"status": "redis unavailable"})
            return s.model_dump()

    async def run(self):
        config = uvicorn.Config(self._api, host="0.0.0.0", port=self.port, log_level="error")
        server = uvicorn.Server(config)
        await server.serve()