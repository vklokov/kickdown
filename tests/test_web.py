from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPBasicCredentials

from kickdown.models import Stats
from kickdown.queue import Queue
from kickdown.store import Store, StoreError
from kickdown.web import Web


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.stats.return_value = Stats()
    store.queue_length.return_value = 0
    store.scheduled_length.return_value = 0
    return store


def make_server(mock_store, queues: list[str]) -> MagicMock:
    server = MagicMock()
    server.store = mock_store
    server.queues = [Queue(name, mock_store) for name in queues]
    return server


@pytest.fixture
def web(mock_store):
    return Web(port=3030, server=make_server(mock_store, []))


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


# --- auth ---


def test_authorized_when_no_credentials_configured(web):
    assert web._authorized(None) is True


def test_authorized_rejects_missing_credentials_when_configured(mock_store):
    web = Web(
        port=3030,
        server=make_server(mock_store, []),
        admin_username="a",
        admin_password="b",
    )
    assert web._authorized(None) is False


def test_authorized_rejects_wrong_password(mock_store):
    web = Web(
        port=3030,
        server=make_server(mock_store, []),
        admin_username="a",
        admin_password="b",
    )
    creds = HTTPBasicCredentials(username="a", password="wrong")
    assert web._authorized(creds) is False


def test_authorized_accepts_correct_credentials(mock_store):
    web = Web(
        port=3030,
        server=make_server(mock_store, []),
        admin_username="a",
        admin_password="b",
    )
    creds = HTTPBasicCredentials(username="a", password="b")
    assert web._authorized(creds) is True


async def test_admin_raises_401_when_unauthorized(mock_store):
    web = Web(
        port=3030,
        server=make_server(mock_store, []),
        admin_username="a",
        admin_password="b",
    )
    with pytest.raises(HTTPException) as exc_info:
        await web._admin(credentials=None)
    assert exc_info.value.status_code == 401


async def test_admin_returns_html_when_no_auth_configured(web):
    response = await web._admin(credentials=None)
    assert response.status_code == 200
    assert b"kickdown" in response.body


# --- admin page rendering ---


async def test_render_admin_includes_queue_pending_counts(mock_store):
    mock_store.queue_length.side_effect = lambda queue: {"default": 156, "reports": 3}[
        queue
    ]
    web = Web(port=3030, server=make_server(mock_store, ["default", "reports"]))

    html = await web._render_admin()

    assert "<tr><td>default</td><td>156</td>" in html
    assert "<tr><td>reports</td><td>3</td>" in html


async def test_render_admin_includes_totals_from_stats(mock_store):
    mock_store.stats.side_effect = [
        Stats(processed=3, failed=1),
        Stats(processed=5, failed=0),
    ]
    web = Web(port=3030, server=make_server(mock_store, ["emails", "reports"]))

    html = await web._render_admin()

    assert "8" in html  # total processed
    assert "1" in html  # total failed


async def test_render_admin_totals_scheduled_across_queues(mock_store):
    mock_store.scheduled_length.side_effect = lambda queue: {"emails": 5, "reports": 2}[
        queue
    ]
    web = Web(port=3030, server=make_server(mock_store, ["emails", "reports"]))

    html = await web._render_admin()

    assert '<span class="value">7</span>' in html


async def test_render_admin_lists_scheduled_per_queue(mock_store):
    mock_store.queue_length.return_value = 0
    mock_store.scheduled_length.side_effect = lambda queue: {"emails": 5, "reports": 2}[
        queue
    ]
    web = Web(port=3030, server=make_server(mock_store, ["emails", "reports"]))

    html = await web._render_admin()

    assert "<tr><td>emails</td><td>0</td><td>5</td>" in html
    assert "<tr><td>reports</td><td>0</td><td>2</td>" in html


async def test_render_admin_handles_no_queues(web):
    html = await web._render_admin()
    assert "No queues registered" in html
