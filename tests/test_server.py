from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rqueue.models import Task
from rqueue.server import Server
from rqueue.store import Store, StoreError


def make_worker(queue: str, operation: str) -> MagicMock:
    worker = MagicMock()
    worker.queue = queue
    worker.operation = operation
    worker.perform = AsyncMock()
    return worker


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    return store


@pytest.fixture
def server(mock_store):
    with patch("rqueue.server.Store", return_value=mock_store):
        return Server("redis://localhost:6379")


def test_add_workers_indexes_by_queue_and_operation(server):
    a = make_worker("q1", "a")
    b = make_worker("q2", "b")
    server.add_workers(a, b)
    assert server._worker == {("q1", "a"): a, ("q2", "b"): b}


def test_add_workers_allows_same_operation_on_different_queues(server):
    a = make_worker("q1", "process")
    b = make_worker("q2", "process")
    server.add_workers(a, b)
    assert server._worker == {("q1", "process"): a, ("q2", "process"): b}


def test_queues_returns_sorted_deduped_worker_queues(server):
    server.add_workers(make_worker("reports", "a"), make_worker("emails", "b"), make_worker("emails", "c"))
    assert server.queues == ["emails", "reports"]


def make_task(**overrides) -> Task:
    defaults = {"queue": "emails", "operation": "send", "params": {}}
    defaults.update(overrides)
    return Task.model_validate(defaults)


async def test_enqueue_pushes_task_to_store(server, mock_store):
    task = make_task()
    await server.enqueue(task)
    pushed = mock_store.push.call_args[0][0]
    assert pushed is task


async def test_enqueue_returns_jid(server):
    task = make_task()
    jid = await server.enqueue(task)
    assert jid == task.jid


def test_on_startup_registers_and_returns_fn(server):
    async def hook():
        pass

    result = server.on_startup(hook)
    assert result is hook
    assert server._startup_hooks == [hook]


def test_on_shutdown_registers_and_returns_fn(server):
    async def hook():
        pass

    result = server.on_shutdown(hook)
    assert result is hook
    assert server._shutdown_hooks == [hook]


async def test_run_raises_when_no_workers_registered(server):
    with pytest.raises(RuntimeError, match="No workers registered"):
        await server.run()


async def test_run_raises_when_redis_ping_fails(server, mock_store):
    mock_store.ping.side_effect = StoreError("connection refused")
    server.add_workers(make_worker("q1", "a"))

    with pytest.raises(RuntimeError, match="Redis connection failed"):
        await server.run()


async def test_run_executes_startup_and_shutdown_hooks_and_closes_store(server, mock_store):
    server.add_workers(make_worker("q1", "a"))

    startup_calls = []
    shutdown_calls = []

    @server.on_startup
    async def on_start():
        startup_calls.append(1)

    @server.on_shutdown
    async def on_stop():
        shutdown_calls.append(1)

    with (
        patch("rqueue.server.Consumer") as MockConsumer,
        patch("rqueue.server.Web") as MockWeb,
    ):
        MockConsumer.return_value.consume = AsyncMock(return_value=None)
        MockConsumer.return_value.drain = AsyncMock(return_value=None)
        MockWeb.return_value.run = AsyncMock(return_value=None)
        await server.run()

    assert startup_calls == [1]
    assert shutdown_calls == [1]
    mock_store.close.assert_called_once()
    MockWeb.assert_called_once_with(port=3030, server=server, admin_username=None, admin_password=None)


async def test_web_port_is_configurable(mock_store):
    with patch("rqueue.server.Store", return_value=mock_store):
        server = Server("redis://localhost:6379", web_port=9000)
    server.add_workers(make_worker("q1", "a"))

    with (
        patch("rqueue.server.Consumer") as MockConsumer,
        patch("rqueue.server.Web") as MockWeb,
    ):
        MockConsumer.return_value.consume = AsyncMock(return_value=None)
        MockConsumer.return_value.drain = AsyncMock(return_value=None)
        MockWeb.return_value.run = AsyncMock(return_value=None)
        await server.run()

    MockWeb.assert_called_once_with(port=9000, server=server, admin_username=None, admin_password=None)


async def test_admin_credentials_are_passed_to_web(mock_store):
    with patch("rqueue.server.Store", return_value=mock_store):
        server = Server("redis://localhost:6379", admin_username="alice", admin_password="secret")
    server.add_workers(make_worker("q1", "a"))

    with (
        patch("rqueue.server.Consumer") as MockConsumer,
        patch("rqueue.server.Web") as MockWeb,
    ):
        MockConsumer.return_value.consume = AsyncMock(return_value=None)
        MockConsumer.return_value.drain = AsyncMock(return_value=None)
        MockWeb.return_value.run = AsyncMock(return_value=None)
        await server.run()

    MockWeb.assert_called_once_with(
        port=3030, server=server, admin_username="alice", admin_password="secret"
    )


async def test_run_logs_and_still_shuts_down_on_unexpected_consumer_crash(server, mock_store):
    server.add_workers(make_worker("q1", "a"))
    server.logger = MagicMock()

    shutdown_calls = []

    @server.on_shutdown
    async def on_stop():
        shutdown_calls.append(1)

    with (
        patch("rqueue.server.Consumer") as MockConsumer,
        patch("rqueue.server.Web") as MockWeb,
    ):
        MockConsumer.return_value.consume = AsyncMock(side_effect=RuntimeError("consumer died"))
        MockConsumer.return_value.drain = AsyncMock(return_value=None)
        MockWeb.return_value.run = AsyncMock(return_value=None)
        await server.run()

    assert shutdown_calls == [1]
    mock_store.close.assert_called_once()
    server.logger.error.assert_called_once()


async def test_run_logs_startup_hook_failure_and_continues(server, mock_store):
    server.add_workers(make_worker("q1", "a"))
    server.logger = MagicMock()

    @server.on_startup
    async def failing_hook():
        raise ValueError("boom")

    with (
        patch("rqueue.server.Consumer") as MockConsumer,
        patch("rqueue.server.Web") as MockWeb,
    ):
        MockConsumer.return_value.consume = AsyncMock(return_value=None)
        MockConsumer.return_value.drain = AsyncMock(return_value=None)
        MockWeb.return_value.run = AsyncMock(return_value=None)
        await server.run()

    server.logger.error.assert_any_call(
        "startup hook failed",
        extra={"hook": "failing_hook", "error": "boom"},
    )
