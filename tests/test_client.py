from unittest.mock import MagicMock, patch

import pytest

from kickdown.client import Client
from kickdown.models import Stats, Task
from kickdown.store import Store


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.pending.return_value = []
    return store


@pytest.fixture
def client(mock_store):
    with patch("kickdown.client.Store", return_value=mock_store):
        return Client("redis://localhost:6379")


def make_task(**overrides) -> Task:
    defaults = {"queue": "emails", "operation": "send", "params": {"to": "a@b.com"}}
    defaults.update(overrides)
    return Task.model_validate(defaults)


async def test_enqueue_pushes_task_to_store(client, mock_store):
    task = make_task()
    await client.enqueue(task)
    pushed = mock_store.push.call_args[0][0]
    assert pushed is task


async def test_enqueue_returns_jid(client, mock_store):
    task = make_task()
    jid = await client.enqueue(task)
    assert jid == task.jid


async def test_enqueue_preserves_task_queue(client, mock_store):
    task = make_task(queue="reports")
    await client.enqueue(task)
    pushed = mock_store.push.call_args[0][0]
    assert pushed.queue == "reports"


async def test_queue_returns_handle_bound_to_the_name(client):
    assert client.queue("emails").name == "emails"


async def test_queue_handle_reads_through_the_shared_store(client, mock_store):
    mock_store.stats.return_value = Stats(processed=3, failed=1)
    assert await client.queue("emails").stats() == Stats(processed=3, failed=1)
    mock_store.stats.assert_called_once_with("emails")


async def test_close_closes_store(client, mock_store):
    await client.close()
    mock_store.close.assert_called_once()


async def test_context_manager_closes_store_on_exit(mock_store):
    with patch("kickdown.client.Store", return_value=mock_store):
        async with Client("redis://localhost:6379"):
            pass
    mock_store.close.assert_called_once()


async def test_context_manager_closes_store_on_exception(mock_store):
    with (
        patch("kickdown.client.Store", return_value=mock_store),
        pytest.raises(ValueError),
    ):
        async with Client("redis://localhost:6379"):
            raise ValueError("boom")
    mock_store.close.assert_called_once()
