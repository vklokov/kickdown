from typing import cast

from redis import Redis
from redis.exceptions import RedisError

from .models import Stats, Task


class StoreError(Exception):
    pass


# Moves due tasks from a queue's scheduled set (KEYS[1]) into the queue itself
# (KEYS[2]), atomically: a crash between the removal and the push would lose
# them. Only the payloads actually pushed are removed, so tasks that become due
# mid-script are left for the next sweep instead of being dropped.
_ENQUEUE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due == 0 then
    return 0
end
redis.call('RPUSH', KEYS[2], unpack(due))
redis.call('ZREM', KEYS[1], unpack(due))
return #due
"""


class Store:
    _QUEUE_PREFIX = "rqueue:queue:"
    _STATS_PREFIX = "rqueue:stats:"
    _SCHEDULED_PREFIX = "rqueue:scheduled:"

    def __init__(self, redis_url: str):
        self._redis = Redis.from_url(redis_url)
        self._enqueue_due = self._redis.register_script(_ENQUEUE_DUE_LUA)

    @classmethod
    def queue_key(cls, name: str) -> str:
        return f"{cls._QUEUE_PREFIX}{name}"

    @classmethod
    def scheduled_key(cls, name: str) -> str:
        return f"{cls._SCHEDULED_PREFIX}{name}"

    @classmethod
    def _processed_key(cls, queue: str) -> str:
        return f"{cls._STATS_PREFIX}{queue}:processed"

    @classmethod
    def _failed_key(cls, queue: str) -> str:
        return f"{cls._STATS_PREFIX}{queue}:failed"

    def ping(self) -> None:
        try:
            self._redis.ping()
        except RedisError as e:
            raise StoreError(str(e)) from e

    def push(self, task: Task) -> None:
        try:
            self._redis.rpush(self.queue_key(task.queue), task.model_dump_json())
        except RedisError as e:
            raise StoreError(str(e)) from e

    def schedule(self, task: Task, run_at: float) -> None:
        try:
            self._redis.zadd(
                self.scheduled_key(task.queue), {task.model_dump_json(): run_at}
            )
        except RedisError as e:
            raise StoreError(str(e)) from e

    def enqueue_due(self, queue: str, now: float, limit: int) -> int:
        try:
            moved = self._enqueue_due(
                keys=[self.scheduled_key(queue), self.queue_key(queue)],
                args=[now, limit],
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        return cast(int, moved)

    def scheduled(self, queue: str) -> list[Task]:
        try:
            raw_tasks = cast(
                list[bytes], self._redis.zrange(self.scheduled_key(queue), 0, -1)
            )
            return [Task.model_validate_json(raw) for raw in raw_tasks]
        except RedisError as e:
            raise StoreError(str(e)) from e

    def scheduled_length(self, queue: str) -> int:
        try:
            return cast(int, self._redis.zcard(self.scheduled_key(queue)))
        except RedisError as e:
            raise StoreError(str(e)) from e

    def pending(self, queue: str) -> list[Task]:
        try:
            raw_tasks = cast(
                list[bytes], self._redis.lrange(self.queue_key(queue), 0, -1)
            )
            return [Task.model_validate_json(raw) for raw in raw_tasks]
        except RedisError as e:
            raise StoreError(str(e)) from e

    def queue_length(self, queue: str) -> int:
        try:
            return cast(int, self._redis.llen(self.queue_key(queue)))
        except RedisError as e:
            raise StoreError(str(e)) from e

    def pop(self, queues: list[str], timeout: int) -> Task | None:
        try:
            keys = [self.queue_key(queue) for queue in queues]
            result = cast(
                tuple[bytes, bytes] | None,
                self._redis.blpop(keys, timeout=timeout),
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        if result is None:
            return None
        _, raw = result
        return Task.model_validate_json(raw)

    def increment_processed(self, queue: str) -> None:
        try:
            self._redis.incr(self._processed_key(queue))
        except RedisError as e:
            raise StoreError(str(e)) from e

    def increment_failed(self, queue: str) -> None:
        try:
            self._redis.incr(self._failed_key(queue))
        except RedisError as e:
            raise StoreError(str(e)) from e

    def stats(self, queue: str) -> Stats:
        try:
            processed = cast(bytes | None, self._redis.get(self._processed_key(queue)))
            failed = cast(bytes | None, self._redis.get(self._failed_key(queue)))
            return Stats(
                processed=int(processed) if processed else 0,
                failed=int(failed) if failed else 0,
            )
        except RedisError as e:
            raise StoreError(str(e)) from e

    def close(self) -> None:
        self._redis.close()
