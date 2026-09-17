import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

import kickdown.consumer as consumer_module
from kickdown.consumer import Consumer
from kickdown.models import Task
from kickdown.store import Store, StoreError


def make_worker(queue: str, operation: str) -> MagicMock:
    worker = MagicMock()
    worker.queue = queue
    worker.operation = operation
    worker.perform = AsyncMock()
    return worker


def workers_dict(*workers: MagicMock) -> dict[tuple[str, str], MagicMock]:
    return {(w.queue, w.operation): w for w in workers}


def raw_of(task: Task) -> bytes:
    return task.model_dump_json().encode()


def claimed(task: Task) -> tuple[bytes, Task]:
    return raw_of(task), task


async def run_task(consumer: Consumer, task: Task) -> None:
    await consumer._run_task(task, raw_of(task))


def make_task(**overrides) -> Task:
    defaults = {"queue": "emails", "operation": "send", "params": {}}
    defaults.update(overrides)
    return Task.model_validate(defaults)


@pytest.fixture
def mock_store():
    return MagicMock(spec=Store)


def scheduled_task(mock_store) -> Task:
    return mock_store.schedule.call_args[0][0]


def scheduled_at(mock_store) -> float:
    return mock_store.schedule.call_args[0][1]


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
    await run_task(consumer, task)

    worker.perform.assert_awaited_once_with({"to": "a@b.com"})


async def test_run_task_increments_processed_on_success(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    await run_task(consumer, make_task(operation="send"))

    mock_store.increment_processed.assert_called_once_with("emails")
    mock_store.increment_failed.assert_not_called()


async def test_run_task_does_not_dispatch_across_queues(mock_store):
    worker = make_worker("reports", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(queue="emails", operation="send")
    await run_task(consumer, task)

    worker.perform.assert_not_awaited()
    mock_store.push.assert_not_called()


async def test_run_task_does_nothing_for_unknown_operation(mock_store):
    consumer = Consumer(store=mock_store, workers={})

    task = make_task(operation="missing")
    await run_task(consumer, task)

    mock_store.push.assert_not_called()


async def test_run_task_increments_failed_for_unknown_operation(mock_store):
    consumer = Consumer(store=mock_store, workers={})

    task = make_task(operation="missing")
    await run_task(consumer, task)

    mock_store.increment_failed.assert_called_once_with("emails")
    mock_store.increment_processed.assert_not_called()


async def test_run_task_retries_on_worker_failure(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=2)
    await run_task(consumer, task)

    retried = scheduled_task(mock_store)
    assert retried.retry_count == 1
    assert retried.jid == task.jid
    mock_store.push.assert_not_called()


async def test_run_task_increments_attempt_on_retry(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    await run_task(consumer, make_task(operation="send", retry_count=2, attempt=1))

    assert scheduled_task(mock_store).attempt == 2


async def test_retry_delay_grows_with_backoff_coefficient(mock_store, monkeypatch):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    monkeypatch.setattr(consumer_module, "_retry_delay", 1)
    monkeypatch.setattr(consumer_module, "_backoff_coefficient", 2.0)

    delays = []
    for attempt in range(3):
        now = time.time()
        await run_task(
            consumer, make_task(operation="send", retry_count=3, attempt=attempt)
        )
        delays.append(round(scheduled_at(mock_store) - now))

    assert delays == [1, 2, 4]


async def test_run_task_does_not_update_stats_while_retries_remain(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    await run_task(consumer, make_task(operation="send", retry_count=2))

    mock_store.increment_processed.assert_not_called()
    mock_store.increment_failed.assert_not_called()


async def test_run_task_drops_task_when_retries_exhausted(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=0)
    await run_task(consumer, task)

    mock_store.schedule.assert_not_called()


async def test_run_task_increments_failed_when_retries_exhausted(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    await run_task(consumer, make_task(operation="send", retry_count=0))

    mock_store.increment_failed.assert_called_once_with("emails")
    mock_store.increment_processed.assert_not_called()


async def test_run_task_logs_but_does_not_raise_when_stats_update_fails(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))
    consumer.logger = MagicMock()
    mock_store.increment_processed.side_effect = StoreError("connection lost")

    await run_task(consumer, make_task(operation="send"))  # must not raise

    consumer.logger.error.assert_called_with(
        "failed to update stats", extra={"error": "connection lost"}
    )


async def test_run_task_swallows_store_error_on_retry_schedule(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    mock_store.schedule.side_effect = StoreError("connection lost")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=1)
    await run_task(consumer, task)  # must not raise


async def test_run_task_releases_semaphore_on_success(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker), concurrency=1)

    await consumer._semaphore.acquire()
    assert consumer._semaphore.locked()
    await run_task(consumer, make_task(operation="send"))
    assert not consumer._semaphore.locked()


async def test_run_task_releases_semaphore_on_failure(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker), concurrency=1)

    await consumer._semaphore.acquire()
    await run_task(consumer, make_task(operation="send", retry_count=0))
    assert not consumer._semaphore.locked()


async def test_run_task_releases_semaphore_on_retry(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker), concurrency=1)

    await consumer._semaphore.acquire()
    await run_task(consumer, make_task(operation="send", retry_count=1))

    assert not consumer._semaphore.locked()


# --- in-flight bookkeeping ---


async def test_run_task_acks_after_success(mock_store):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send")
    await run_task(consumer, task)

    mock_store.ack.assert_called_once_with(consumer.id, raw_of(task))


async def test_run_task_acks_after_scheduling_a_retry(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=1)
    await run_task(consumer, task)

    mock_store.ack.assert_called_once_with(consumer.id, raw_of(task))


async def test_run_task_acks_after_permanent_failure(mock_store):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", retry_count=0)
    await run_task(consumer, task)

    mock_store.ack.assert_called_once_with(consumer.id, raw_of(task))


async def test_run_task_acks_unknown_operations(mock_store):
    consumer = Consumer(store=mock_store, workers={})

    task = make_task(operation="missing")
    await run_task(consumer, task)

    mock_store.ack.assert_called_once_with(consumer.id, raw_of(task))


async def test_run_task_keeps_task_in_flight_when_the_retry_cannot_be_scheduled(
    mock_store,
):
    worker = make_worker("emails", "send")
    worker.perform.side_effect = RuntimeError("boom")
    mock_store.schedule.side_effect = StoreError("connection lost")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    await run_task(consumer, make_task(operation="send", retry_count=1))

    # the reaper has to be able to find it, so it must not be acked away
    mock_store.ack.assert_not_called()


async def test_run_task_survives_a_failing_ack(mock_store):
    worker = make_worker("emails", "send")
    mock_store.ack.side_effect = StoreError("connection lost")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    await run_task(consumer, make_task(operation="send"))  # must not raise


# --- consumer lifecycle ---


def test_consumer_id_is_unique_per_instance(mock_store):
    first = Consumer(store=mock_store, workers={})
    second = Consumer(store=mock_store, workers={})
    assert first.id != second.id


def test_consumer_id_can_be_given(mock_store):
    consumer = Consumer(store=mock_store, workers={}, consumer_id="fixed")
    assert consumer.id == "fixed"


async def test_start_registers_the_consumer(mock_store):
    consumer = Consumer(store=mock_store, workers={}, consumer_id="fixed")

    await consumer.start()

    registered_id, ttl = mock_store.register_consumer.call_args[0]
    assert registered_id == "fixed"
    assert ttl > 0


async def test_stop_returns_unfinished_tasks_and_deregisters(mock_store):
    mock_store.reap.return_value = 1
    consumer = Consumer(store=mock_store, workers={}, consumer_id="fixed")

    await consumer.stop()

    mock_store.reap.assert_called_once_with("fixed")
    mock_store.deregister_consumer.assert_called_once_with("fixed")


async def test_stop_survives_a_redis_outage(mock_store):
    mock_store.reap.side_effect = StoreError("connection lost")
    consumer = Consumer(store=mock_store, workers={}, consumer_id="fixed")

    await consumer.stop()  # must not raise


async def test_heartbeat_refreshes_until_cancelled(mock_store, monkeypatch):
    consumer = Consumer(store=mock_store, workers={}, consumer_id="fixed")

    beats = 0

    async def fake_sleep(_seconds):
        nonlocal beats
        beats += 1
        if beats == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.heartbeat()

    assert mock_store.heartbeat.call_count == 2


async def test_heartbeat_keeps_beating_after_a_redis_error(mock_store, monkeypatch):
    mock_store.heartbeat.side_effect = [StoreError("connection lost"), None]
    consumer = Consumer(store=mock_store, workers={}, consumer_id="fixed")
    consumer.logger = MagicMock()

    beats = 0

    async def fake_sleep(_seconds):
        nonlocal beats
        beats += 1
        if beats == 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.heartbeat()

    assert mock_store.heartbeat.call_count == 2


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
    mock_store.claim.side_effect = [
        claimed(make_task(operation="send")),
        StoreError("stop"),
    ]

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


async def test_consume_dispatches_claimed_task_to_worker(mock_store, monkeypatch):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))

    task = make_task(operation="send", params={"x": 1})
    mock_store.claim.side_effect = [claimed(task), StoreError("connection lost")]

    async def fake_sleep(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.consume()

    worker.perform.assert_awaited_once_with({"x": 1})


async def test_consume_claims_into_this_consumers_in_flight_list(
    mock_store, monkeypatch
):
    worker = make_worker("emails", "send")
    consumer = Consumer(store=mock_store, workers=workers_dict(worker))
    mock_store.claim.side_effect = [None, StoreError("stop")]

    async def fake_sleep(_seconds):
        if mock_store.claim.call_count > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.consume()

    queues, consumer_id = mock_store.claim.call_args[0]
    assert queues == ["emails"]
    assert consumer_id == consumer.id
