import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from rqueue.queue import Queue
from rqueue.scheduler import Scheduler
from rqueue.store import Store, StoreError


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.enqueue_due.return_value = 0
    return store


def make_queues(mock_store, *names: str) -> list[Queue]:
    return [Queue(name, mock_store) for name in names]


async def test_tick_enqueues_tasks_due_now(mock_store):
    scheduler = Scheduler(queues=make_queues(mock_store, "emails"))

    before = time.time()
    await scheduler._tick()
    after = time.time()

    queue, now, limit = mock_store.enqueue_due.call_args[0]
    assert queue == "emails"
    assert before <= now <= after
    assert limit > 0


async def test_tick_sweeps_every_queue_with_the_same_timestamp(mock_store):
    scheduler = Scheduler(queues=make_queues(mock_store, "emails", "reports"))

    await scheduler._tick()

    swept = [call[0][0] for call in mock_store.enqueue_due.call_args_list]
    timestamps = {call[0][1] for call in mock_store.enqueue_due.call_args_list}
    assert swept == ["emails", "reports"]
    assert len(timestamps) == 1


async def test_tick_logs_how_many_tasks_were_enqueued(mock_store):
    mock_store.enqueue_due.return_value = 3
    scheduler = Scheduler(queues=make_queues(mock_store, "emails"))
    scheduler.logger = MagicMock()

    await scheduler._tick()

    scheduler.logger.info.assert_called_once_with(
        "scheduler enqueued 3 due task(s)", extra={"queue": "emails"}
    )


async def test_tick_stays_quiet_when_nothing_is_due(mock_store):
    scheduler = Scheduler(queues=make_queues(mock_store, "emails"))
    scheduler.logger = MagicMock()

    await scheduler._tick()

    scheduler.logger.info.assert_not_called()


async def test_tick_logs_but_does_not_raise_on_store_error(mock_store):
    mock_store.enqueue_due.side_effect = StoreError("connection lost")
    scheduler = Scheduler(queues=make_queues(mock_store, "emails"))
    scheduler.logger = MagicMock()

    await scheduler._tick()  # must not raise

    scheduler.logger.error.assert_called_once_with(
        "redis error while enqueueing due tasks",
        extra={"queue": "emails", "error": "connection lost"},
    )


async def test_tick_keeps_sweeping_after_a_failing_queue(mock_store):
    mock_store.enqueue_due.side_effect = [StoreError("connection lost"), 0]
    scheduler = Scheduler(queues=make_queues(mock_store, "emails", "reports"))
    scheduler.logger = MagicMock()

    await scheduler._tick()

    swept = [call[0][0] for call in mock_store.enqueue_due.call_args_list]
    assert swept == ["emails", "reports"]


async def test_run_keeps_ticking(mock_store):
    scheduler = Scheduler(queues=make_queues(mock_store, "emails"), poll_interval=0)
    tick = AsyncMock(side_effect=[None, None, RuntimeError("stop")])
    scheduler._tick = tick  # ty: ignore[invalid-assignment]

    with pytest.raises(RuntimeError):
        await scheduler.run()

    assert tick.await_count == 3
