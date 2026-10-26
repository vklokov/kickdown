from typing import cast

from redis.exceptions import RedisError

from .connection import close, connect
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

# Returns everything left in a stopping consumer's in-flight list (KEYS[1]) to
# the queue each task names, oldest first, untouched.
_RETURN_INFLIGHT_LUA = """
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

# Takes the oldest task off a dead consumer's in-flight list (KEYS[1]) once the
# reaper has decided its fate, provided it is still the payload the decision was
# made on (ARGV[1]). A replacement payload (ARGV[2]) is pushed onto the queue
# (KEYS[2]); without one KEYS[2], if given, is the failed counter to bump.
_SETTLE_HEAD_LUA = """
if redis.call('LINDEX', KEYS[1], 0) ~= ARGV[1] then
    return 0
end
redis.call('LPOP', KEYS[1])
if ARGV[2] then
    redis.call('RPUSH', KEYS[2], ARGV[2])
elseif KEYS[2] then
    redis.call('INCR', KEYS[2])
end
return 1
"""

# Drops a consumer (ARGV[1]) from the registry (KEYS[2]) only if its beat key
# (KEYS[1]) is still missing: a consumer that outlived an outage may have
# re-registered itself between the liveness check and this call, and must not
# be forgotten while it keeps claiming tasks.
_FORGET_DEAD_LUA = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
return redis.call('SREM', KEYS[2], ARGV[1])
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
        self._redis = connect(redis_url)
        self._enqueue_due = self._redis.register_script(_ENQUEUE_DUE_LUA)
        self._claim = self._redis.register_script(_CLAIM_LUA)
        self._return_inflight = self._redis.register_script(_RETURN_INFLIGHT_LUA)
        self._settle_head = self._redis.register_script(_SETTLE_HEAD_LUA)
        self._forget_dead = self._redis.register_script(_FORGET_DEAD_LUA)

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
        """Refreshes the beat key and re-asserts registry membership.

        A consumer that outlives a Redis outage longer than the heartbeat TTL
        (a Sentinel failover, say) is reaped by its peers and dropped from the
        registry while it is still running. If it only refreshed the key, the
        tasks it claims afterwards would never be reaped should it die.
        """
        self.register_consumer(consumer_id, ttl)

    def deregister_consumer(self, consumer_id: str) -> None:
        try:
            pipe = self._redis.pipeline()
            pipe.srem(self._CONSUMERS_KEY, consumer_id)
            pipe.delete(self._beat_key(consumer_id))
            pipe.execute()
        except RedisError as e:
            raise StoreError(str(e)) from e

    def forget_if_dead(self, consumer_id: str) -> bool:
        """Removes a reaped consumer from the registry unless it came back."""
        try:
            removed = self._forget_dead(
                keys=[self._beat_key(consumer_id), self._CONSUMERS_KEY],
                args=[consumer_id],
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        return bool(removed)

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

    def return_inflight(self, consumer_id: str) -> int:
        try:
            returned = self._return_inflight(
                keys=[self.inflight_key(consumer_id)], args=[self._QUEUE_PREFIX]
            )
        except RedisError as e:
            raise StoreError(str(e)) from e
        return cast(int, returned)

    def inflight_head(self, consumer_id: str) -> bytes | None:
        """The oldest task left in a consumer's in-flight list, as stored."""
        try:
            return cast(
                bytes | None, self._redis.lindex(self.inflight_key(consumer_id), 0)
            )
        except RedisError as e:
            raise StoreError(str(e)) from e

    def requeue_head(self, consumer_id: str, raw: bytes, task: Task) -> bool:
        """Replaces the in-flight head `raw` with `task`, pushed onto its queue.

        Returns False, changing nothing, if `raw` is no longer the head.
        """
        return self._settle(
            keys=[self.inflight_key(consumer_id), self.queue_key(task.queue)],
            args=[raw, task.model_dump_json()],
        )

    def drop_head(self, consumer_id: str, raw: bytes, queue: str | None) -> bool:
        """Discards the in-flight head `raw`, counting it as failed on `queue`.

        Without a queue (a payload that could not be parsed) nothing is
        counted. Returns False, changing nothing, if `raw` is no longer the head.
        """
        keys = [self.inflight_key(consumer_id)]
        if queue is not None:
            keys.append(self._failed_key(queue))
        return self._settle(keys=keys, args=[raw])

    def _settle(self, keys: list[str], args: list) -> bool:
        try:
            settled = self._settle_head(keys=keys, args=args)
        except RedisError as e:
            raise StoreError(str(e)) from e
        return bool(settled)

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
        close(self._redis)
