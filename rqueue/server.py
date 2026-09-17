import asyncio
import signal
from collections.abc import Awaitable, Callable

from .consumer import Consumer
from .log import default_logger
from .models import Performable
from .store import Store, StoreError
from .web import Web

Hook = Callable[[], Awaitable[None]]


class Server:
    def __init__(
        self,
        redis_url: str,
        concurrency: int = 1,
        web_port: int = 3030,
        admin_username: str | None = None,
        admin_password: str | None = None,
    ):
        self._store = Store(redis_url)
        self._concurrency = concurrency
        self._web_port = web_port
        self._admin_username = admin_username
        self._admin_password = admin_password
        self._startup_hooks: list[Hook] = []
        self._shutdown_hooks: list[Hook] = []
        self._worker: dict[tuple[str, str], Performable] = {}
        self.logger = default_logger()

    @property
    def store(self) -> Store:
        return self._store

    @property
    def queues(self) -> list[str]:
        return sorted({worker.queue for worker in self._worker.values()})

    def add_workers(self, *args: Performable):
        for worker in args:
            self._worker[(worker.queue, worker.operation)] = worker

    def on_startup(self, fn: Hook) -> Hook:
        self._startup_hooks.append(fn)
        return fn

    def on_shutdown(self, fn: Hook) -> Hook:
        self._shutdown_hooks.append(fn)
        return fn

    async def run(self):
        if not self._worker:
            raise RuntimeError(
                "No workers registered. Register them with add_workers() before running the server."
            )

        try:
            await asyncio.to_thread(self._store.ping)
        except StoreError as e:
            raise RuntimeError(f"Redis connection failed: {e}") from e

        consumer = Consumer(
            store=self._store,
            workers=self._worker,
            concurrency=self._concurrency,
            logger=self.logger,
        )
        web = Web(
            port=self._web_port,
            server=self,
            admin_username=self._admin_username,
            admin_password=self._admin_password,
        )

        await self._run_hooks(self._startup_hooks, "startup")

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        self.logger.info(
            f"server starting (queues={self.queues}, concurrency={self._concurrency}, web_port={self._web_port})"
        )

        consume_task = asyncio.create_task(consumer.consume(), name="consumer")
        web_task = asyncio.create_task(web.run(), name="web")
        stop_task = asyncio.create_task(stop_event.wait(), name="stop")

        try:
            await asyncio.wait(
                [consume_task, web_task, stop_task], return_when=asyncio.FIRST_COMPLETED
            )

            for task in (consume_task, web_task):
                if task.done() and not task.cancelled():
                    exc = task.exception()
                    if exc is not None:
                        self.logger.error(f"{task.get_name()} task failed unexpectedly", extra={"error": str(exc)})
        finally:
            self.logger.info("server shutting down")
            consume_task.cancel()
            web_task.cancel()
            stop_task.cancel()
            await asyncio.gather(consume_task, web_task, stop_task, return_exceptions=True)
            await consumer.drain()

            await self._run_hooks(self._shutdown_hooks, "shutdown")

            await asyncio.to_thread(self._store.close)

    async def _run_hooks(self, hooks: list[Hook], phase: str) -> None:
        for hook in hooks:
            try:
                await hook()
            except Exception as err:  # noqa: BLE001 - hook code is arbitrary; must not abort the server
                self.logger.error(
                    f"{phase} hook failed",
                    extra={"hook": getattr(hook, "__name__", repr(hook)), "error": str(err)},
                )
