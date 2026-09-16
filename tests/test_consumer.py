import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rqueue.config import Config
from rqueue.consumer import Consumer
from rqueue.schemas import Job
from rqueue.store import Store, StoreError


@pytest.fixture
def mock_config():
    config = MagicMock(spec=Config)
    config.concurrency = 1
    config.queue.return_value = "rqueue:default"
    config.redis_ping_timeout = 30
    config.redis_reconnect_delay = 5
    config.logger = MagicMock()
    return config


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.pop = AsyncMock(return_value=None)
    store.ping_async = AsyncMock()
    store.push_async = AsyncMock()
    store.increment_processed = AsyncMock()
    store.increment_failed = AsyncMock()
    return store


@pytest.fixture
def consumer(mock_store, mock_config):
    return Consumer(store=mock_store, config=mock_config, workers={})


# --- is_ok ---


def test_is_ok_returns_true_when_heartbeat_is_fresh(consumer):
    assert consumer.is_ok is True


def test_is_ok_returns_false_when_heartbeat_is_stale(consumer, mock_config):
    stale = datetime.now(UTC) - timedelta(
        seconds=mock_config.redis_ping_timeout * 2 + 1
    )
    consumer._last_heartbeat = stale
    assert consumer.is_ok is False


# --- last_ping ---


def test_last_ping_format(consumer):
    consumer._last_heartbeat = datetime(2026, 5, 18, 12, 34, 56, tzinfo=UTC)
    assert consumer.last_ping == "2026-05-18 12:34:56 UTC"


# --- _ping ---


async def test_ping_updates_heartbeat_on_success(consumer, mock_store):
    before = consumer._last_heartbeat
    await consumer._ping()
    assert consumer._last_heartbeat > before
    mock_store.ping_async.assert_awaited_once()


async def test_ping_logs_error_and_keeps_heartbeat_on_redis_error(consumer, mock_store):
    before = consumer._last_heartbeat
    mock_store.ping_async.side_effect = StoreError("connection refused")
    await consumer._ping()
    assert consumer._last_heartbeat == before
    consumer.logger.error.assert_called_once()


# --- _track_job ---


async def test_track_job_logs_start_and_done_with_duration(consumer):
    job = Job(jid="xyz", worker="MyWorker", payload={})

    async with consumer._track_job(job):
        pass

    assert consumer.logger.info.call_count == 2
    done_call = consumer.logger.info.call_args
    assert "duration" in done_call.kwargs.get("extra", {})
    assert isinstance(done_call.kwargs["extra"]["duration"], float)


async def test_track_job_increments_processed(consumer, mock_store):
    job = Job(jid="xyz", worker="MyWorker", payload={})

    async with consumer._track_job(job):
        pass

    mock_store.increment_processed.assert_awaited_once()


# --- _run_job ---


async def test_run_job_calls_worker_perform(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {"x": 1}})

    worker.perform.assert_awaited_once_with({"x": 1})


async def test_run_job_increments_processed_on_success(consumer, mock_store):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}})

    mock_store.increment_processed.assert_awaited_once()


async def test_run_job_increments_failed_on_error(consumer, mock_store):
    worker = MagicMock()
    worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}, "retry_count": 0})

    mock_store.increment_failed.assert_awaited_once()


async def test_run_job_logs_start_and_done(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}})

    assert consumer.logger.info.call_count == 2


async def test_run_job_logs_error_when_worker_not_found(consumer):
    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "Missing", "payload": {}, "retry_count": 0})

    consumer.logger.error.assert_called_once()


async def test_run_job_logs_error_when_perform_raises(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}, "retry_count": 0})

    consumer.logger.error.assert_called_once()


async def test_run_job_releases_semaphore_on_success(consumer):
    worker = MagicMock()
    worker.perform = AsyncMock()
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    assert consumer._semaphore._value == 0
    await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}})
    assert consumer._semaphore._value == 1


async def test_run_job_releases_semaphore_on_error(consumer):
    await consumer._semaphore.acquire()
    assert consumer._semaphore._value == 0
    await consumer._run_job({"jid": "abc", "worker": "Missing", "payload": {}, "retry_count": 0})
    assert consumer._semaphore._value == 1


# --- retry ---


async def test_run_job_retries_on_failure(consumer, mock_store):
    with patch("asyncio.sleep", new_callable=AsyncMock):
        worker = MagicMock()
        worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
        consumer._workers = {"MyWorker": worker}

        await consumer._semaphore.acquire()
        await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}, "retry_count": 1})

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            await asyncio.gather(*tasks)

    mock_store.increment_failed.assert_not_called()
    mock_store.push_async.assert_awaited_once()


async def test_run_job_increments_attempt_on_retry(consumer, mock_store):
    with patch("asyncio.sleep", new_callable=AsyncMock):
        worker = MagicMock()
        worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
        consumer._workers = {"MyWorker": worker}

        await consumer._semaphore.acquire()
        await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}, "retry_count": 2})

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            await asyncio.gather(*tasks)

    pushed_job = mock_store.push_async.call_args[0][0]
    assert pushed_job.attempt == 1


async def test_run_job_increments_failed_after_exhausting_retries(consumer, mock_store):
    worker = MagicMock()
    worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
    consumer._workers = {"MyWorker": worker}

    await consumer._semaphore.acquire()
    await consumer._run_job({
        "jid": "abc", "worker": "MyWorker", "payload": {},
        "retry_count": 1, "attempt": 1,
    })

    mock_store.increment_failed.assert_awaited_once()
    mock_store.push_async.assert_not_called()


async def test_run_job_logs_warning_when_retrying(consumer):
    with patch("asyncio.sleep", new_callable=AsyncMock):
        worker = MagicMock()
        worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
        consumer._workers = {"MyWorker": worker}

        await consumer._semaphore.acquire()
        await consumer._run_job({"jid": "abc", "worker": "MyWorker", "payload": {}, "retry_count": 1})

    consumer.logger.warning.assert_called_once()
    consumer.logger.error.assert_not_called()


async def test_retry_job_sleeps_for_delay_then_pushes(consumer, mock_store):
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        job = Job(jid="abc", worker="MyWorker", payload={})
        await consumer._retry_job(job, 2.25)

    mock_sleep.assert_awaited_once_with(2.25)
    mock_store.push_async.assert_awaited_once_with(job)


async def test_retry_job_uses_backoff_coefficient(consumer, mock_store):
    with patch("asyncio.sleep", new_callable=AsyncMock):
        worker = MagicMock()
        worker.perform = AsyncMock(side_effect=RuntimeError("boom"))
        consumer._workers = {"MyWorker": worker}

        await consumer._semaphore.acquire()
        await consumer._run_job({
            "jid": "abc", "worker": "MyWorker", "payload": {},
            "retry_count": 3, "backoff_coefficient": 2.0, "attempt": 1,
        })

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            await asyncio.gather(*tasks)

    pushed_job = mock_store.push_async.call_args[0][0]
    assert pushed_job.attempt == 2
    assert pushed_job.backoff_coefficient == 2.0