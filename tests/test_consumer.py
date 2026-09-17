import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import rqueue.consumer as consumer_module
from rqueue.consumer import Consumer
from rqueue.models import Task
from rqueue.store import Store, StoreError


def make_worker(queue: str, operation: str) -> MagicMock:
    worker = MagicMock()
    worker.queue = queue
    worker.operation = operation
    worker.perform = AsyncMock()
    return worker


def workers_dict(*workers: MagicMock) -> dict[tuple[str, str], MagicMock]:
    return {(w.queue, w.operation): w for w in workers}


def make_task(**overrides) -> Task:
    defaults = {"queue": "emails", "operation": "send", "params": {}}
    defaults.update(overrides)
    return Task.model_validate(defaults)


@pytest.fixture
def mock_store():
    return MagicMock(spec=Store)


@pytest.fixture(autouse=True)
def no_retry_delay(monkeypatch):
    monkeypatch.setattr(consumer_module, "_retry_delay", 0)


# --- queue discovery / round robin ---


def test_queues_are_derived_from_worker_queues_deduped_and_sorted(mock_store):
    workers = workers_dict(
        make_worker("emails", "send"),
        make_worker("emails", "notify"),
        make_worker("reports", "export"),
    )
    consumer = Consumer(store=mock_store, workers=workers)
    assert list(consumer._queues) == ["emails", "reports"]


def test_poll_order_rotates_between_calls(mock_store):
    workers = workers_dict(
        make_worker("q1", "a"),
        make_worker("q2", "b"),
        make_worker("q3", "c"),
    )
    consumer = Consumer(store=mock_store, workers=workers)
    first = consumer._poll_order()
    second = consumer._poll_order()
    third = consumer._poll_order()
    assert first == ["q1", "q2", "q3"]
    assert second == ["q2", "q3", "q1"]
    assert third == ["q3", "q1", "q2"]


# --- worker resolution keyed by (queue, operation) ---


def test_same_operation_name_on_different_queues_does_not_collide(mock_store):
    email_worker = make_worker("emails", "process")
    report_worker = make_worker("reports", "process")
    workers = workers_dict(email_worker, report_worker)
    consumer = Consumer(store=mock_store, workers=workers)

    assert list(consumer._queues) == ["emails", "reports"]
    assert consumer._workers[("emails", "process")] is email_worker
    assert consumer._workers[("reports", "process")] is report_worker


# --- _run_task ---


async def test_run_task_calls_worker_perform_with_params(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", params={"to": "a@b.com"})
    await consumer._run_task(task)

    worker.perform.assert_awaited_once_with({"to": "a@b.com"})


async def test_run_task_does_not_dispatch_across_queues(mock_store):
    worker = make_worker("reports", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(queue="emails", operation="send")
    await consumer._run_task(task)

    worker.perform.assert_not_awaited()
    mock_store.push.assert_not_called()


async def test_run_task_does_nothing_for_unknown_operation(mock_store):
    consumer = Consumer(store=mock_store, workers={})

    task = make_task(operation="missing")
    await consumer._run_task(task)

    mock_store.push.assert_not_called()


async def test_run_task_retries_on_worker_failure(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=2)
    await consumer._run_task(task)

    pushed = mock_store.push.call_args[0][0]
    assert pushed.retry_count == 1
    assert pushed.jid == task.jid


async def test_run_task_drops_task_when_retries_exhausted(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=0)
    await consumer._run_task(task)

    mock_store.push.assert_not_called()


async def test_run_task_swallows_store_error_on_retry_push(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    mock_store.push.side_effect = StoreError("connection lost")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=1)
    await consumer._run_task(task)  # must not raise


async def test_run_task_releases_semaphore_on_success(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker), concurrency=1)

    await consumer._semaphore.acquire()
    assert consumer._semaphore.locked()
    await consumer._run_task(make_task(operation="send"))
    assert not consumer._semaphore.locked()


async def test_run_task_releases_semaphore_on_failure(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker), concurrency=1)

    await consumer._semaphore.acquire()
    await consumer._run_task(make_task(operation="send", retry_count=0))
    assert not consumer._semaphore.locked()


async def test_run_task_releases_semaphore_before_retry_delay(mock_store, monkeypatch):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker), concurrency=1)

    locked_during_sleep = []

    async def fake_sleep(_seconds):
        locked_during_sleep.append(consumer._semaphore.locked())

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    await consumer._semaphore.acquire()
    await consumer._run_task(make_task(operation="send", retry_count=1))

    assert locked_during_sleep == [False]


# --- drain ---


async def test_drain_awaits_pending_run_task_calls(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    started = asyncio.Event()
    finished = asyncio.Event()

    async def slow_perform(_params):
        started.set()
        await asyncio.sleep(0.01)
        finished.set()

    worker.perform.side_effect = slow_perform
    mock_store.pop.side_effect = [make_task(operation="send"), StoreError("stop")]

    consume_task = asyncio.create_task(consumer.consume())
    try:
        await started.wait()
    finally:
        consume_task.cancel()
        try:
            await consume_task
        except asyncio.CancelledError:
            pass

    await consumer.drain()
    assert finished.is_set()


async def test_drain_is_noop_with_no_pending_tasks(mock_store):
    consumer = Consumer(store=mock_store, workers={})
    await consumer.drain()  # must not raise


# --- consume loop ---


async def test_consume_dispatches_popped_task_to_worker(mock_store, monkeypatch):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", params={"x": 1})
    mock_store.pop.side_effect = [task, StoreError("connection lost")]

    async def fake_sleep(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.consume()

    worker.perform.assert_awaited_once_with({"x": 1})
