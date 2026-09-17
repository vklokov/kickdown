from unittest.mock import MagicMock

import pytest

from kickdown.models import Stats, Task
from kickdown.queue import Queue
from kickdown.store import Store, StoreError


@pytest.fixture
def mock_store():
    return MagicMock(spec=Store)


@pytest.fixture
def queue(mock_store):
    return Queue("emails", mock_store)


def make_task(**overrides) -> Task:
    defaults = {"queue": "emails", "operation": "send", "params": {}}
    defaults.update(overrides)
    return Task.model_validate(defaults)


def test_name_is_exposed(queue):
    assert queue.name == "emails"


def test_repr_shows_the_name(queue):
    assert repr(queue) == "Queue('emails')"


async def test_push_hands_the_task_to_the_store(queue, mock_store):
    task = make_task()
    await queue.push(task)
    mock_store.push.assert_called_once_with(task)


async def test_schedule_passes_the_due_timestamp(queue, mock_store):
    task = make_task()
    await queue.schedule(task, 1234.5)
    mock_store.schedule.assert_called_once_with(task, 1234.5)


async def test_pending_reads_the_bound_queue(queue, mock_store):
    task = make_task()
    mock_store.pending.return_value = [task]
    assert await queue.pending() == [task]
    mock_store.pending.assert_called_once_with("emails")


async def test_length_reads_the_bound_queue(queue, mock_store):
    mock_store.queue_length.return_value = 4
    assert await queue.length() == 4
    mock_store.queue_length.assert_called_once_with("emails")


async def test_stats_reads_the_bound_queue(queue, mock_store):
    mock_store.stats.return_value = Stats(processed=2, failed=1)
    assert await queue.stats() == Stats(processed=2, failed=1)
    mock_store.stats.assert_called_once_with("emails")


async def test_counters_are_scoped_to_the_bound_queue(queue, mock_store):
    await queue.increment_processed()
    await queue.increment_failed()
    mock_store.increment_processed.assert_called_once_with("emails")
    mock_store.increment_failed.assert_called_once_with("emails")


async def test_purge_drops_the_bound_queue_and_reports_the_count(queue, mock_store):
    mock_store.purge.return_value = 12
    assert await queue.purge() == 12
    mock_store.purge.assert_called_once_with("emails")


async def test_store_errors_propagate(queue, mock_store):
    mock_store.queue_length.side_effect = StoreError("connection lost")
    with pytest.raises(StoreError):
        await queue.length()
