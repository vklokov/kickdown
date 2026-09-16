from unittest.mock import MagicMock, patch

import pytest

from rqueue.client import Client
from rqueue.config import _default_queue
from rqueue.store import Store


class MyWorker:
    async def perform(self, payload: dict) -> None:
        pass


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.pending.return_value = []
    return store


@pytest.fixture
def client(mock_store):
    with patch("rqueue.client.Store", return_value=mock_store):
        return Client("redis://localhost:6379")


def test_enqueue_pushes_to_default_queue(client, mock_store):
    client.enqueue(MyWorker, {})
    job = mock_store.push.call_args[0][0]
    assert job.worker == MyWorker.__name__


def test_enqueue_pushes_to_custom_queue():
    with patch("rqueue.client.Store") as MockStore:
        Client("redis://localhost:6379", queue="custom")
        MockStore.assert_called_once_with("redis://localhost:6379", "custom")


def test_enqueue_uses_default_queue():
    with patch("rqueue.client.Store") as MockStore:
        Client("redis://localhost:6379")
        MockStore.assert_called_once_with("redis://localhost:6379", _default_queue)


def test_enqueue_returns_jid(client, mock_store):
    jid = client.enqueue(MyWorker, {"key": "value"})
    assert isinstance(jid, str)
    assert len(jid) > 0


def test_enqueue_jid_matches_pushed_job(client, mock_store):
    jid = client.enqueue(MyWorker, {"key": "value"})
    job = mock_store.push.call_args[0][0]
    assert job.jid == jid


def test_enqueue_serializes_worker_and_payload(client, mock_store):
    client.enqueue(MyWorker, {"key": "value"})
    job = mock_store.push.call_args[0][0]
    assert job.worker == MyWorker.__name__
    assert job.payload == {"key": "value"}


def test_pending_delegates_to_store(client, mock_store):
    client.pending()
    mock_store.pending.assert_called_once()


def test_stats_delegates_to_store(client, mock_store):
    client.stats()
    mock_store.stats.assert_called_once()


def test_enqueue_generates_unique_jids(client, mock_store):
    jid1 = client.enqueue(MyWorker, {})
    jid2 = client.enqueue(MyWorker, {})
    assert jid1 != jid2


def test_close_closes_store(client, mock_store):
    client.close()
    mock_store.close.assert_called_once()


def test_context_manager_closes_store_on_exit(mock_store):
    with patch("rqueue.client.Store", return_value=mock_store):
        with Client("redis://localhost:6379"):
            pass
    mock_store.close.assert_called_once()


def test_context_manager_closes_store_on_exception(mock_store):
    with patch("rqueue.client.Store", return_value=mock_store):
        with pytest.raises(ValueError):
            with Client("redis://localhost:6379"):
                raise ValueError("boom")


def test_enqueue_uses_default_retry_count(client, mock_store):
    client.enqueue(MyWorker, {})
    job = mock_store.push.call_args[0][0]
    assert job.retry_count == 1


def test_enqueue_uses_default_backoff_coefficient(client, mock_store):
    client.enqueue(MyWorker, {})
    job = mock_store.push.call_args[0][0]
    assert job.backoff_coefficient == 1.5


def test_enqueue_stores_custom_retry_count(client, mock_store):
    client.enqueue(MyWorker, {}, retry_count=3)
    job = mock_store.push.call_args[0][0]
    assert job.retry_count == 3


def test_enqueue_stores_custom_backoff_coefficient(client, mock_store):
    client.enqueue(MyWorker, {}, backoff_coefficient=2.0)
    job = mock_store.push.call_args[0][0]
    assert job.backoff_coefficient == 2.0
