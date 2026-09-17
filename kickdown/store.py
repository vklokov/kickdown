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

# Claims the first task available across the polled queues (KEYS, in poll
# order) by moving it into the consumer's in-flight list (ARGV[1]) in one step,
# so a task is never held only in the worker process's memory.
_CLAIM_LUA = """
for i = 1, #KEYS do
    local payload = redis.call('LMOVE', KEYS[i], ARGV[1], 'LEFT', 'RIGHT')
    if payload then
        return payload
    end
end
return false
"""

# Returns everything left in a dead consumer's in-flight list (KEYS[1]) to the
# queue each task names, oldest first.
_REAP_LUA = """
local reaped = 0
while true do
    local payload = redis.call('LPOP', KEYS[1])
    if not payload then
        break
    end
    local task = cjson.decode(payload)
    redis.call('RPUSH', ARGV[1] .. task['queue'], payload)
    reaped = reaped + 1
end
return reaped
"""


class Store:
    _QUEUE_PREFIX = "kickdown:queue:"
    _STATS_PREFIX = "kickdown:stats:"
    _SCHEDULED_PREFIX = "kickdown:scheduled:"
    _INFLIGHT_PREFIX = "kickdown:inflight:"
    _BEAT_PREFIX = "kickdown:beat:"
    _REAP_LOCK_PREFIX = "kickdown:reap:"
    _CONSUMERS_KEY = "kickdown:consumers"

    def __init__(self, redis_url: str):
        self._redis = Redis.from_url(redis_url)
        self._enqueue_due = self._redis.register_script(_ENQUEUE_DUE_LUA)
        self._claim = self._redis.register_script(_CLAIM_LUA)
        self._reap = self._redis.register_script(_REAP_LUA)

    @classmethod
    def queue_key(cls, name: str) -> str:
        return f"{cls._QUEUE_PREFIX}{name}"

    @classmethod
    def scheduled_key(cls, name: str) -> str:
        return f"{cls._SCHEDULED_PREFIX}{name}"

    @classmethod
    def inflight_key(cls, consumer_id: str) -> str:
        return f"{cls._INFLIGHT_PREFIX}{consumer_id}"

    @classmethod
    def _beat_key(cls, consumer_id: str) -> str:
        return f"{cls._BEAT_PREFIX}{consumer_id}"

    @classmethod
    def _reap_lock_key(cls, consumer_id: str) -> str:
        return f"{cls._REAP_LOCK_PREFIX}{consumer_id}"

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

    def purge(self, queue: str) -> int:
        try:
            # MULTI/EXEC so the reported count is exactly what was dropped
            pipe = self._redis.pipeline()
            pipe.llen(self.queue_key(queue))
            pipe.delete(self.queue_key(queue))
            length, _ = cast(tuple[int, int], pipe.execute())
        except RedisError as e:
            raise StoreError(str(e)) from e
        return length

    def queue_length(self, queue: str) -> int:
        try:
            return cast(int, self._redis.llen(self.queue_key(queue)))
        except RedisError as e:
            raise StoreError(str(e)) from e

    def claim(self, queues: list[str], consumer_id: str) -> tuple[bytes, Task] | None:
        """Moves the next available task into the consumer's in-flight list."""
        try:
            raw = cast(
                bytes | None,
                self._claim(
                    keys=[self.queue_key(queue) for queue in queues],
                    args=[self.inflight_key(consumer_id)],
                ),
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        if not raw:
            return None
        return raw, Task.model_validate_json(raw)

    def ack(self, consumer_id: str, raw: bytes) -> None:
        """Drops a finished task from the in-flight list.

        Matches on the exact payload that was claimed, so the stored bytes are
        passed back rather than re-serialized from the model.
        """
        try:
            self._redis.lrem(self.inflight_key(consumer_id), 1, raw)  # ty: ignore[invalid-argument-type]
        except RedisError as e:
            raise StoreError(str(e)) from e

    def inflight(self, consumer_id: str) -> list[Task]:
        try:
            raw_tasks = cast(
                list[bytes], self._redis.lrange(self.inflight_key(consumer_id), 0, -1)
            )
            return [Task.model_validate_json(raw) for raw in raw_tasks]
        except RedisError as e:
            raise StoreError(str(e)) from e

    def register_consumer(self, consumer_id: str, ttl: int) -> None:
        try:
            pipe = self._redis.pipeline()
            pipe.sadd(self._CONSUMERS_KEY, consumer_id)
            pipe.set(self._beat_key(consumer_id), "1", ex=ttl)
            pipe.execute()
        except RedisError as e:
            raise StoreError(str(e)) from e

    def heartbeat(self, consumer_id: str, ttl: int) -> None:
        try:
            self._redis.set(self._beat_key(consumer_id), "1", ex=ttl)
        except RedisError as e:
            raise StoreError(str(e)) from e

    def deregister_consumer(self, consumer_id: str) -> None:
        try:
            pipe = self._redis.pipeline()
            pipe.srem(self._CONSUMERS_KEY, consumer_id)
            pipe.delete(self._beat_key(consumer_id))
            pipe.execute()
        except RedisError as e:
            raise StoreError(str(e)) from e

    def consumers(self) -> list[str]:
        try:
            ids = cast(set[bytes], self._redis.smembers(self._CONSUMERS_KEY))
            return sorted(id.decode() for id in ids)
        except RedisError as e:
            raise StoreError(str(e)) from e

    def is_alive(self, consumer_id: str) -> bool:
        try:
            return bool(self._redis.exists(self._beat_key(consumer_id)))
        except RedisError as e:
            raise StoreError(str(e)) from e

    def claim_reap(self, consumer_id: str, ttl: int) -> bool:
        """Takes the right to reap one dead consumer, so servers do not race."""
        try:
            acquired = self._redis.set(
                self._reap_lock_key(consumer_id), "1", nx=True, ex=ttl
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        return bool(acquired)

    def reap(self, consumer_id: str) -> int:
        try:
            reaped = self._reap(
                keys=[self.inflight_key(consumer_id)], args=[self._QUEUE_PREFIX]
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        return cast(int, reaped)

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
