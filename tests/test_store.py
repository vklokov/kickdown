from unittest.mock import MagicMock, patch

import pytest

from kickdown.models import Task
from kickdown.store import Store, StoreError


@pytest.fixture
def redis():
    return MagicMock()


@pytest.fixture
def store(redis):
    with patch("kickdown.store.connect", return_value=redis):
        return Store("redis://localhost:6379")


def test_store_connects_through_the_url_resolver(redis):
    with patch("kickdown.store.connect", return_value=redis) as mock_connect:
        store = Store("redis+sentinel://s1/mymaster")
    mock_connect.assert_called_once_with("redis+sentinel://s1/mymaster")
    assert store._redis is redis
    assert redis.register_script.call_count == 5


def test_close_goes_through_the_connection_module(store, redis):
    with patch("kickdown.store.close") as mock_close:
        store.close()
    mock_close.assert_called_once_with(redis)


def test_heartbeat_reasserts_registry_membership(store, redis):
    store.heartbeat("c1", 30)
    pipe = redis.pipeline.return_value
    pipe.sadd.assert_called_once_with("kickdown:consumers", "c1")
    pipe.set.assert_called_once_with("kickdown:beat:c1", "1", ex=30)
    pipe.execute.assert_called_once()


def test_forget_if_dead_runs_the_guarded_script(store, redis):
    script = redis.register_script.return_value
    script.return_value = 1
    assert store.forget_if_dead("c1") is True
    script.assert_called_once_with(
        keys=["kickdown:beat:c1", "kickdown:consumers"], args=["c1"]
    )


def test_forget_if_dead_reports_a_consumer_that_came_back(store, redis):
    redis.register_script.return_value.return_value = 0
    assert store.forget_if_dead("c1") is False


def test_forget_if_dead_wraps_redis_errors(store, redis):
    from redis.exceptions import ConnectionError

    redis.register_script.return_value.side_effect = ConnectionError("gone")
    with pytest.raises(StoreError, match="gone"):
        store.forget_if_dead("c1")


def test_inflight_head_reads_the_oldest_in_flight_task(store, redis):
    redis.lindex.return_value = b"payload"
    assert store.inflight_head("c1") == b"payload"
    redis.lindex.assert_called_once_with("kickdown:inflight:c1", 0)


def test_requeue_head_pushes_the_replacement_onto_its_queue(store, redis):
    script = redis.register_script.return_value
    script.return_value = 1
    task = Task(queue="emails", operation="send", params={"ids": []}, attempt=1)

    assert store.requeue_head("c1", b"old", task) is True
    script.assert_called_once_with(
        keys=["kickdown:inflight:c1", "kickdown:queue:emails"],
        args=[b"old", task.model_dump_json()],
    )


def test_drop_head_counts_the_task_as_failed(store, redis):
    script = redis.register_script.return_value
    script.return_value = 1

    assert store.drop_head("c1", b"old", "emails") is True
    script.assert_called_once_with(
        keys=["kickdown:inflight:c1", "kickdown:stats:emails:failed"], args=[b"old"]
    )


def test_drop_head_without_a_queue_counts_nothing(store, redis):
    script = redis.register_script.return_value
    script.return_value = 1

    store.drop_head("c1", b"garbage", None)
    script.assert_called_once_with(keys=["kickdown:inflight:c1"], args=[b"garbage"])


def test_settling_reports_a_head_that_moved(store, redis):
    redis.register_script.return_value.return_value = 0
    assert store.drop_head("c1", b"old", "emails") is False


def test_settling_wraps_redis_errors(store, redis):
    from redis.exceptions import ConnectionError

    redis.register_script.return_value.side_effect = ConnectionError("gone")
    with pytest.raises(StoreError, match="gone"):
        store.drop_head("c1", b"old", "emails")
