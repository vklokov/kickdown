from unittest.mock import AsyncMock, MagicMock

import pytest

from kickdown.reaper import Reaper
from kickdown.store import Store, StoreError


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.consumers.return_value = []
    store.is_alive.return_value = True
    store.claim_reap.return_value = True
    store.reap.return_value = 0
    return store


@pytest.fixture
def reaper(mock_store):
    return Reaper(store=mock_store)


async def test_tick_leaves_live_consumers_alone(reaper, mock_store):
    mock_store.consumers.return_value = ["host:1:aaa"]

    await reaper._tick()

    mock_store.reap.assert_not_called()
    mock_store.deregister_consumer.assert_not_called()


async def test_tick_requeues_tasks_of_a_dead_consumer(reaper, mock_store):
    mock_store.consumers.return_value = ["host:1:aaa"]
    mock_store.is_alive.return_value = False
    mock_store.reap.return_value = 2

    await reaper._tick()

    mock_store.reap.assert_called_once_with("host:1:aaa")
    mock_store.deregister_consumer.assert_called_once_with("host:1:aaa")


async def test_tick_skips_a_consumer_another_server_is_already_reaping(
    reaper, mock_store
):
    mock_store.consumers.return_value = ["host:1:aaa"]
    mock_store.is_alive.return_value = False
    mock_store.claim_reap.return_value = False

    await reaper._tick()

    mock_store.reap.assert_not_called()
    mock_store.deregister_consumer.assert_not_called()


async def test_tick_reports_what_was_requeued(reaper, mock_store):
    mock_store.consumers.return_value = ["host:1:aaa"]
    mock_store.is_alive.return_value = False
    mock_store.reap.return_value = 3
    reaper.logger = MagicMock()

    await reaper._tick()

    reaper.logger.warning.assert_called_once_with(
        "consumer host:1:aaa died, requeued 3 task(s)",
        extra={"consumer": "host:1:aaa"},
    )


async def test_tick_keeps_going_after_a_failing_consumer(reaper, mock_store):
    mock_store.consumers.return_value = ["host:1:aaa", "host:2:bbb"]
    mock_store.is_alive.side_effect = [StoreError("connection lost"), False]
    reaper.logger = MagicMock()

    await reaper._tick()

    mock_store.reap.assert_called_once_with("host:2:bbb")


async def test_tick_logs_but_does_not_raise_when_the_registry_is_unreachable(
    reaper, mock_store
):
    mock_store.consumers.side_effect = StoreError("connection lost")
    reaper.logger = MagicMock()

    await reaper._tick()  # must not raise

    reaper.logger.error.assert_called_once_with(
        "redis error while listing consumers", extra={"error": "connection lost"}
    )


async def test_run_keeps_ticking(mock_store):
    reaper = Reaper(store=mock_store, poll_interval=0)
    tick = AsyncMock(side_effect=[None, None, RuntimeError("stop")])
    reaper._tick = tick  # ty: ignore[invalid-assignment]

    with pytest.raises(RuntimeError):
        await reaper.run()

    assert tick.await_count == 3
