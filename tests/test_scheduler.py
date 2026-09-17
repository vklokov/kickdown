import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from rqueue.scheduler import Scheduler
from rqueue.store import Store, StoreError


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.enqueue_due.return_value = 0
    return store


async def test_tick_enqueues_tasks_due_now(mock_store):
    scheduler = Scheduler(store=mock_store)

    before = time.time()
    await scheduler._tick()
    after = time.time()

    now, limit = mock_store.enqueue_due.call_args[0]
    assert before <= now <= after
    assert limit > 0


async def test_tick_logs_how_many_tasks_were_enqueued(mock_store):
    mock_store.enqueue_due.return_value = 3
    scheduler = Scheduler(store=mock_store)
    scheduler.logger = MagicMock()

    await scheduler._tick()

    scheduler.logger.info.assert_called_once_with("scheduler enqueued 3 due task(s)")


async def test_tick_stays_quiet_when_nothing_is_due(mock_store):
    scheduler = Scheduler(store=mock_store)
    scheduler.logger = MagicMock()

    await scheduler._tick()

    scheduler.logger.info.assert_not_called()


async def test_tick_logs_but_does_not_raise_on_store_error(mock_store):
    mock_store.enqueue_due.side_effect = StoreError("connection lost")
    scheduler = Scheduler(store=mock_store)
    scheduler.logger = MagicMock()

    await scheduler._tick()  # must not raise

    scheduler.logger.error.assert_called_once_with(
        "redis error while enqueueing due tasks", extra={"error": "connection lost"}
    )


async def test_run_keeps_ticking(mock_store):
    scheduler = Scheduler(store=mock_store, poll_interval=0)
    tick = AsyncMock(side_effect=[None, None, RuntimeError("stop")])
    scheduler._tick = tick  # ty: ignore[invalid-assignment]

    with pytest.raises(RuntimeError):
        await scheduler.run()

    assert tick.await_count == 3
