from unittest.mock import MagicMock, patch

import pytest

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
    assert redis.register_script.call_count == 4


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
