from typing import cast

from redis import Redis
from redis.exceptions import RedisError

from .models import Stats, Task


class StoreError(Exception):
    pass


# Moves every task whose score is due into its own queue, atomically: a crash
# between the ZSET removal and the queue push would lose the task.
_ENQUEUE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, payload in ipairs(due) do
    local task = cjson.decode(payload)
    redis.call('RPUSH', ARGV[3] .. task['queue'], payload)
    redis.call('ZREM', KEYS[1], payload)
end
return #due
"""


class Store:
    _QUEUE_PREFIX = "rqueue:queue:"
    _STATS_PREFIX = "rqueue:stats:"
    _SCHEDULED_KEY = "rqueue:scheduled"

    def __init__(self, redis_url: str):
        self._redis = Redis.from_url(redis_url)
        self._enqueue_due = self._redis.register_script(_ENQUEUE_DUE_LUA)

    @classmethod
    def queue_key(cls, name: str) -> str:
        return f"{cls._QUEUE_PREFIX}{name}"

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
            self._redis.zadd(self._SCHEDULED_KEY, {task.model_dump_json(): run_at})
        except RedisError as e:
            raise StoreError(str(e)) from e

    def enqueue_due(self, now: float, limit: int) -> int:
        try:
            moved = self._enqueue_due(
                keys=[self._SCHEDULED_KEY],
                args=[now, limit, self._QUEUE_PREFIX],
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        return cast(int, moved)

    def scheduled(self) -> list[Task]:
        try:
            raw_tasks = cast(
                list[bytes], self._redis.zrange(self._SCHEDULED_KEY, 0, -1)
            )
            return [Task.model_validate_json(raw) for raw in raw_tasks]
        except RedisError as e:
            raise StoreError(str(e)) from e

    def scheduled_length(self) -> int:
        try:
            return cast(int, self._redis.zcard(self._SCHEDULED_KEY))
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
