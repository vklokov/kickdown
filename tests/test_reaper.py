from unittest.mock import AsyncMock, MagicMock

import pytest

from kickdown.models import Task
from kickdown.reaper import Reaper
from kickdown.store import Store, StoreError

DEAD = "host:1:aaa"


def make_worker(queue: str, operation: str, **retry) -> MagicMock:
    # spec'd so that `retry`/`retry_count` are absent unless given
    worker = MagicMock(spec=["queue", "operation", "perform", *retry])
    worker.queue = queue
    worker.operation = operation
    for name, value in retry.items():
        setattr(worker, name, value)
    return worker


def make_task(**overrides) -> Task:
    defaults = {"queue": "emails", "operation": "send", "params": {}}
    defaults.update(overrides)
    return Task.model_validate(defaults)


def raw_of(task: Task) -> bytes:
    return task.model_dump_json().encode()


def make_reaper(mock_store, *workers: MagicMock) -> Reaper:
    reaper = Reaper(
        store=mock_store, workers={(w.queue, w.operation): w for w in workers}
    )
    reaper.logger = MagicMock()
    return reaper


def strand(mock_store, *payloads: bytes) -> None:
    """Leaves `payloads` in the dead consumer's in-flight list, oldest first."""
    mock_store.consumers.return_value = [DEAD]
    mock_store.is_alive.return_value = False
    mock_store.inflight_head.side_effect = [*payloads, None]


@pytest.fixture
def mock_store():
    store = MagicMock(spec=Store)
    store.consumers.return_value = []
    store.is_alive.return_value = True
    store.claim_reap.return_value = True
    store.inflight_head.return_value = None
    store.requeue_head.return_value = True
    store.drop_head.return_value = True
    return store


@pytest.fixture
def reaper(mock_store):
    return make_reaper(mock_store, make_worker("emails", "send"))


async def test_tick_leaves_live_consumers_alone(reaper, mock_store):
    mock_store.consumers.return_value = [DEAD]

    await reaper._tick()

    mock_store.inflight_head.assert_not_called()
    mock_store.forget_if_dead.assert_not_called()


async def test_tick_forgets_a_dead_consumer_once_its_tasks_are_settled(
    reaper, mock_store
):
    strand(mock_store)

    await reaper._tick()

    mock_store.inflight_head.assert_called_once_with(DEAD)
    mock_store.forget_if_dead.assert_called_once_with(DEAD)


async def test_tick_skips_a_consumer_another_server_is_already_reaping(
    reaper, mock_store
):
    strand(mock_store)
    mock_store.claim_reap.return_value = False

    await reaper._tick()

    mock_store.inflight_head.assert_not_called()
    mock_store.forget_if_dead.assert_not_called()


async def test_a_task_with_retries_left_is_requeued_as_its_next_attempt(
    reaper, mock_store
):
    task = make_task(params={"ids": []})
    strand(mock_store, raw_of(task))

    await reaper._tick()

    consumer_id, raw, requeued = mock_store.requeue_head.call_args.args
    assert (consumer_id, raw) == (DEAD, raw_of(task))
    assert requeued == task.model_copy(update={"attempt": 1})
    mock_store.drop_head.assert_not_called()


async def test_a_task_out_of_retries_is_dropped_as_failed(reaper, mock_store):
    task = make_task(attempt=1)
    strand(mock_store, raw_of(task))

    await reaper._tick()

    mock_store.drop_head.assert_called_once_with(DEAD, raw_of(task), "emails")
    mock_store.requeue_head.assert_not_called()
    reaper.logger.error.assert_called_once_with(
        f"jid={task.jid} dropped, its consumer died mid-task (retries exhausted)",
        extra={"consumer": DEAD, "queue": "emails", "operation": "send"},
    )


async def test_a_task_whose_worker_disables_retries_is_dropped(mock_store):
    reaper = make_reaper(
        mock_store, make_worker("emails", "send", retry=False, retry_count=5)
    )
    reaper.logger = MagicMock()
    task = make_task()
    strand(mock_store, raw_of(task))

    await reaper._tick()

    mock_store.drop_head.assert_called_once_with(DEAD, raw_of(task), "emails")
    mock_store.requeue_head.assert_not_called()
    reaper.logger.error.assert_called_once_with(
        f"jid={task.jid} dropped, its consumer died mid-task (retries disabled)",
        extra={"consumer": DEAD, "queue": "emails", "operation": "send"},
    )


async def test_a_task_for_an_unknown_worker_gets_the_default_policy(mock_store):
    reaper = make_reaper(mock_store)
    fresh = make_task(operation="other")
    retried = make_task(operation="other", attempt=1)
    strand(mock_store, raw_of(fresh), raw_of(retried))

    await reaper._tick()

    mock_store.requeue_head.assert_called_once()
    assert mock_store.requeue_head.call_args.args[1] == raw_of(fresh)
    mock_store.drop_head.assert_called_once_with(DEAD, raw_of(retried), "emails")


async def test_an_unparsable_task_is_dropped_without_counting(reaper, mock_store):
    strand(mock_store, b"not json")

    await reaper._tick()

    mock_store.drop_head.assert_called_once_with(DEAD, b"not json", None)


async def test_reaping_stops_when_the_head_moves_underneath(reaper, mock_store):
    strand(mock_store, raw_of(make_task()), raw_of(make_task()))
    mock_store.requeue_head.return_value = False

    await reaper._tick()

    mock_store.requeue_head.assert_called_once()


async def test_tick_reports_what_was_requeued_and_dropped(reaper, mock_store):
    strand(
        mock_store,
        raw_of(make_task()),
        raw_of(make_task()),
        raw_of(make_task(attempt=1)),
    )

    await reaper._tick()

    reaper.logger.warning.assert_called_once_with(
        f"consumer {DEAD} died, requeued 2 and dropped 1 task(s)",
        extra={"consumer": DEAD},
    )


async def test_tick_keeps_going_after_a_failing_consumer(reaper, mock_store):
    mock_store.consumers.return_value = [DEAD, "host:2:bbb"]
    mock_store.is_alive.side_effect = [StoreError("connection lost"), False]

    await reaper._tick()

    mock_store.inflight_head.assert_called_once_with("host:2:bbb")


async def test_tick_does_not_forget_a_consumer_whose_tasks_could_not_be_settled(
    reaper, mock_store
):
    strand(mock_store, raw_of(make_task()))
    mock_store.requeue_head.side_effect = StoreError("connection lost")

    await reaper._tick()  # must not raise

    mock_store.forget_if_dead.assert_not_called()


async def test_tick_logs_but_does_not_raise_when_the_registry_is_unreachable(
    reaper, mock_store
):
    mock_store.consumers.side_effect = StoreError("connection lost")

    await reaper._tick()  # must not raise

    reaper.logger.error.assert_called_once_with(
        "redis error while listing consumers", extra={"error": "connection lost"}
    )


async def test_run_keeps_ticking(mock_store):
    reaper = Reaper(store=mock_store, workers={}, poll_interval=0)
    tick = AsyncMock(side_effect=[None, None, RuntimeError("stop")])
    reaper._tick = tick  # ty: ignore[invalid-assignment]

    with pytest.raises(RuntimeError):
        await reaper.run()

    assert tick.await_count == 3


async def test_a_worker_registered_late_has_its_policy_applied(mock_store):
    workers = {}
    reaper = Reaper(store=mock_store, workers=workers)
    workers[("emails", "send")] = make_worker("emails", "send", retry=False)
    task = make_task()
    strand(mock_store, raw_of(task))

    await reaper._tick()

    mock_store.drop_head.assert_called_once_with(DEAD, raw_of(task), "emails")
