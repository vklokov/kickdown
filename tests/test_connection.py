from unittest.mock import MagicMock, patch

import pytest
from redis.backoff import NoBackoff
from redis.retry import Retry
from redis.sentinel import SentinelConnectionPool

from kickdown.connection import (
    SentinelSettings,
    close,
    connect,
    is_sentinel_url,
    parse_sentinel_url,
)

# --- URL parsing ---


def test_parses_hosts_master_and_defaults():
    settings = parse_sentinel_url("redis+sentinel://s1:26379,s2:26380/mymaster")
    assert settings == SentinelSettings(
        sentinels=(("s1", 26379), ("s2", 26380)), master_name="mymaster"
    )
    assert settings.db == 0
    assert settings.password is None
    assert settings.sentinel_password is None
    assert settings.socket_timeout == 5.0
    assert settings.socket_connect_timeout == 2.0


def test_host_without_port_uses_the_sentinel_default():
    settings = parse_sentinel_url("redis+sentinel://s1,s2:26380/mymaster")
    assert settings.sentinels == (("s1", 26379), ("s2", 26380))


def test_ipv6_hosts_are_accepted():
    settings = parse_sentinel_url("redis+sentinel://[::1]:26379,[fe80::2]/mymaster")
    assert settings.sentinels == (("::1", 26379), ("fe80::2", 26379))


def test_bare_ipv6_host_is_rejected():
    with pytest.raises(ValueError, match="enclosed in brackets"):
        parse_sentinel_url("redis+sentinel://::1/mymaster")


def test_credentials_apply_to_the_master():
    settings = parse_sentinel_url("redis+sentinel://alice:s%40cret@s1/mymaster")
    assert settings.username == "alice"
    assert settings.password == "s@cret"
    assert settings.sentinel_password is None


def test_password_only_credentials():
    settings = parse_sentinel_url("redis+sentinel://:secret@s1/mymaster")
    assert settings.username is None
    assert settings.password == "secret"


def test_query_values_are_percent_decoded_exactly_once():
    settings = parse_sentinel_url(
        "redis+sentinel://s1/mymaster?sentinel_password=p%2541ss"
    )
    assert settings.sentinel_password == "p%41ss"


def test_query_options_are_parsed_and_typed():
    settings = parse_sentinel_url(
        "redis+sentinel://s1/mymaster"
        "?db=3&sentinel_username=watch&sentinel_password=pw%21"
        "&socket_timeout=1.5&socket_connect_timeout=0.25"
    )
    assert settings.db == 3
    assert settings.sentinel_username == "watch"
    assert settings.sentinel_password == "pw!"
    assert settings.socket_timeout == 1.5
    assert settings.socket_connect_timeout == 0.25


@pytest.mark.parametrize(
    ("url", "match"),
    [
        ("redis://s1:26379/mymaster", "scheme"),
        ("redis+sentinel:///mymaster", "at least one sentinel"),
        ("redis+sentinel://s1:26379", "name the master"),
        ("redis+sentinel://s1:26379/", "name the master"),
        ("redis+sentinel://s1:26379/mymaster/0", "name the master"),
        ("redis+sentinel://s1:abc/mymaster", "port"),
        ("redis+sentinel://s1/mymaster?db=three", "Invalid value for 'db'"),
        ("redis+sentinel://s1/mymaster?password=x", "Unknown option 'password'"),
    ],
)
def test_rejects_malformed_urls(url, match):
    with pytest.raises(ValueError, match=match):
        parse_sentinel_url(url)


def test_is_sentinel_url():
    assert is_sentinel_url("redis+sentinel://s1/mymaster")
    assert not is_sentinel_url("redis://localhost:6379")
    assert not is_sentinel_url("rediss://localhost:6379")


# --- client construction ---


def test_plain_urls_go_straight_to_redis_py():
    with patch("kickdown.connection.Redis") as MockRedis:
        client = connect("redis://localhost:6379/1")
    MockRedis.from_url.assert_called_once_with("redis://localhost:6379/1")
    assert client is MockRedis.from_url.return_value


def test_sentinel_url_builds_a_master_client():
    with patch("kickdown.connection.Sentinel") as MockSentinel:
        client = connect("redis+sentinel://:pw@s1,s2:26380/mymaster?db=2")

    sentinels, kwargs = MockSentinel.call_args.args[0], MockSentinel.call_args.kwargs
    assert sentinels == [("s1", 26379), ("s2", 26380)]
    assert kwargs["sentinel_kwargs"]["socket_timeout"] == 5.0
    assert kwargs["sentinel_kwargs"]["socket_connect_timeout"] == 2.0
    # a dead sentinel is skipped, not retried
    assert kwargs["sentinel_kwargs"]["retry"] == Retry(NoBackoff(), 0)
    assert "password" not in kwargs["sentinel_kwargs"]

    master_for = MockSentinel.return_value.master_for
    assert master_for.call_args.args == ("mymaster",)
    master_kwargs = master_for.call_args.kwargs
    assert master_kwargs["db"] == 2
    assert master_kwargs["password"] == "pw"
    assert master_kwargs["username"] is None
    assert master_kwargs["socket_timeout"] == 5.0
    assert master_kwargs["socket_connect_timeout"] == 2.0
    assert client is master_for.return_value


def test_master_client_never_resends_a_command():
    with patch("kickdown.connection.Sentinel") as MockSentinel:
        connect("redis+sentinel://s1/mymaster")
    retry = MockSentinel.return_value.master_for.call_args.kwargs["retry"]
    assert retry == Retry(NoBackoff(), 0)


def test_sentinel_credentials_and_timeouts_reach_the_sentinels():
    with patch("kickdown.connection.Sentinel") as MockSentinel:
        connect(
            "redis+sentinel://s1/mymaster"
            "?sentinel_username=watch&sentinel_password=spw&socket_timeout=1"
        )
    sentinel_kwargs = MockSentinel.call_args.kwargs["sentinel_kwargs"]
    assert sentinel_kwargs["username"] == "watch"
    assert sentinel_kwargs["password"] == "spw"
    assert sentinel_kwargs["socket_timeout"] == 1.0
    master_kwargs = MockSentinel.return_value.master_for.call_args.kwargs
    assert master_kwargs["socket_timeout"] == 1.0
    assert master_kwargs["password"] is None


# --- closing ---


def test_close_closes_a_plain_client():
    client = MagicMock()
    client.connection_pool = MagicMock()
    close(client)
    client.close.assert_called_once()


def test_close_also_closes_the_sentinel_clients():
    client = MagicMock()
    client.connection_pool = MagicMock(spec=SentinelConnectionPool)
    sentinels = [MagicMock(), MagicMock()]
    client.connection_pool.sentinel_manager = MagicMock(sentinels=sentinels)
    close(client)
    client.close.assert_called_once()
    for sentinel in sentinels:
        sentinel.close.assert_called_once()
