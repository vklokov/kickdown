"""Builds the Redis client behind a `Store` from a connection URL.

Two kinds of URL are understood:

- ``redis://``, ``rediss://`` and ``unix://`` are handed to redis-py's own
  ``Redis.from_url`` untouched.
- ``redis+sentinel://`` names a master managed by Redis Sentinel. Every new
  connection asks the sentinels where the master currently lives, so after a
  failover the next call lands on the new master; a node going down costs a
  few failed calls, not the process.

    redis+sentinel://[[username]:password@]host[:port][,host[:port]...]/master_name
                     [?db=0&sentinel_password=...&socket_timeout=5]

Credentials in the URL authenticate against the *master*; sentinels have
their own ACLs, so they get ``sentinel_username``/``sentinel_password`` from
the query string instead. Sentinels without a port use 26379.
"""

from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlsplit

from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel, SentinelConnectionPool

SENTINEL_SCHEME = "redis+sentinel"

_DEFAULT_SENTINEL_PORT = 26379

# A node that vanishes without closing its sockets must not hang a call
# forever: every blocking Store call runs in a thread, and a stuck heartbeat
# gets the consumer reaped while it is still working. Plain redis:// URLs
# keep redis-py's defaults (no timeouts), as before.
_DEFAULT_SOCKET_TIMEOUT = 5.0
_DEFAULT_CONNECT_TIMEOUT = 2.0

# No command is ever re-sent: a reply lost to a timeout or a dropped socket
# does not mean the command did not run, and re-sending a claim (LMOVE) or a
# push (RPUSH) would strand a task in an in-flight list nobody is working on,
# or enqueue it twice. A failed call surfaces as a StoreError instead, and the
# server loops retry on their own schedule, each attempt re-resolving the
# master through the sentinels. This matches plain redis:// clients, which
# redis-py also builds without retries.
_NO_RETRY = Retry(NoBackoff(), 0)

_QUERY_PARSERS = {
    "db": int,
    "sentinel_username": str,
    "sentinel_password": str,
    "socket_timeout": float,
    "socket_connect_timeout": float,
}


@dataclass(frozen=True)
class SentinelSettings:
    sentinels: tuple[tuple[str, int], ...]
    master_name: str
    db: int = 0
    username: str | None = None
    password: str | None = None
    sentinel_username: str | None = None
    sentinel_password: str | None = None
    socket_timeout: float = _DEFAULT_SOCKET_TIMEOUT
    socket_connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT


def is_sentinel_url(redis_url: str) -> bool:
    return urlsplit(redis_url).scheme == SENTINEL_SCHEME


def parse_sentinel_url(redis_url: str) -> SentinelSettings:
    url = urlsplit(redis_url)
    if url.scheme != SENTINEL_SCHEME:
        raise ValueError(f"Sentinel URL must use the {SENTINEL_SCHEME}:// scheme")

    # urlsplit cannot make sense of a comma-separated host list, so the
    # authority is taken apart by hand
    userinfo, _, hosts = url.netloc.rpartition("@")
    username, password = _parse_userinfo(userinfo)
    sentinels = tuple(_parse_host(part) for part in hosts.split(",") if part)
    if not sentinels:
        raise ValueError("Sentinel URL must list at least one sentinel host")

    master_name = unquote(url.path.lstrip("/"))
    if not master_name or "/" in master_name:
        raise ValueError(
            "Sentinel URL must name the master as its path, "
            "e.g. redis+sentinel://host:26379/mymaster"
        )

    options = {}
    # parse_qs already percent-decodes the values
    for name, values in parse_qs(url.query).items():
        parser = _QUERY_PARSERS.get(name)
        if parser is None:
            raise ValueError(
                f"Unknown option {name!r} in Sentinel URL; "
                f"supported: {', '.join(sorted(_QUERY_PARSERS))}"
            )
        try:
            options[name] = parser(values[0])
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid value for {name!r} in Sentinel URL") from e

    return SentinelSettings(
        sentinels=sentinels,
        master_name=master_name,
        username=username,
        password=password,
        **options,
    )


def _parse_userinfo(userinfo: str) -> tuple[str | None, str | None]:
    if not userinfo:
        return None, None
    username, sep, password = userinfo.partition(":")
    return (
        unquote(username) or None,
        unquote(password) if sep else None,
    )


def _parse_host(part: str) -> tuple[str, int]:
    host, sep, port = part.rpartition(":")
    if not sep or (host.startswith("[") and not host.endswith("]")):
        # no port at all, or the colon belongs to a bracketed IPv6 address
        host, port = part, ""
    if host.startswith("["):
        host = host[1:-1]
    elif ":" in host:
        raise ValueError(
            f"Invalid sentinel host {part!r} in Sentinel URL: "
            "an IPv6 address must be enclosed in brackets"
        )
    host = unquote(host)
    if not host:
        raise ValueError(f"Invalid sentinel host {part!r} in Sentinel URL")
    try:
        return host, int(port) if port else _DEFAULT_SENTINEL_PORT
    except ValueError as e:
        raise ValueError(f"Invalid sentinel port in {part!r} in Sentinel URL") from e


def connect(redis_url: str) -> Redis:
    """Returns a redis-py client for the URL; nothing is contacted yet."""
    if not is_sentinel_url(redis_url):
        return Redis.from_url(redis_url)
    return _connect_sentinel(parse_sentinel_url(redis_url))


def close(client: Redis) -> None:
    """Closes the client, including the sentinel connections behind it.

    The master client only owns its own pool; the connections opened to the
    sentinels while resolving the master live in the `Sentinel` manager and
    would otherwise stay open for as long as the process does.
    """
    client.close()
    pool = client.connection_pool
    if isinstance(pool, SentinelConnectionPool):
        for sentinel in pool.sentinel_manager.sentinels:
            sentinel.close()


def _connect_sentinel(settings: SentinelSettings) -> Redis:
    # A sentinel that does not answer is simply the wrong one to ask; the
    # discovery loop moves on to the next instead of retrying it.
    sentinel_kwargs: dict = {
        "socket_timeout": settings.socket_timeout,
        "socket_connect_timeout": settings.socket_connect_timeout,
        "retry": _NO_RETRY,
    }
    if settings.sentinel_username is not None:
        sentinel_kwargs["username"] = settings.sentinel_username
    if settings.sentinel_password is not None:
        sentinel_kwargs["password"] = settings.sentinel_password

    sentinel = Sentinel(list(settings.sentinels), sentinel_kwargs=sentinel_kwargs)
    return sentinel.master_for(
        settings.master_name,
        db=settings.db,
        username=settings.username,
        password=settings.password,
        socket_timeout=settings.socket_timeout,
        socket_connect_timeout=settings.socket_connect_timeout,
        retry=_NO_RETRY,
    )
