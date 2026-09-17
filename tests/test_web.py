from unittest.mock import MagicMock

import pytest

from rqueue.store import Store, StoreError
from rqueue.web import Web


@pytest.fixture
def mock_store():
    return MagicMock(spec=Store)


@pytest.fixture
def web(mock_store):
    server = MagicMock()
    server.store = mock_store
    return Web(port=3030, server=server)


async def test_live_returns_ok(web):
    assert await web._live() == {"status": "ok"}


async def test_ready_returns_ok_when_redis_reachable(web, mock_store):
    result = await web._ready()
    assert result == {"status": "ok"}
    mock_store.ping.assert_called_once()


async def test_ready_returns_503_when_redis_unreachable(web, mock_store):
    mock_store.ping.side_effect = StoreError("connection refused")
    result = await web._ready()
    assert result.status_code == 503
