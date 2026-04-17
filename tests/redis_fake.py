"""Minimal async Redis fake for unit tests (no real server)."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any


def _decode(v: Any) -> str:
    if isinstance(v, bytes | bytearray):
        return v.decode()
    return str(v)


class _FakePubSub:
    async def subscribe(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def unsubscribe(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def get_message(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    async def close(self) -> None:
        return None


class FakeAsyncRedis:
    """In-memory stand-in for redis.asyncio.Redis used by RedisBackend tests."""

    def __init__(self) -> None:
        self.sets: dict[str, set[str]] = defaultdict(set)
        self.hashes: dict[str, dict[str, str]] = defaultdict(dict)
        self.ttls: dict[str, int | None] = {}

    def pubsub(self) -> _FakePubSub:
        return _FakePubSub()

    async def close(self) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def sadd(self, key: str, *members: Any) -> int:
        k = _decode(key)
        n = 0
        for m in members:
            mv = _decode(m)
            if mv not in self.sets[k]:
                self.sets[k].add(mv)
                n += 1
        return n

    async def srem(self, key: str, *members: Any) -> int:
        k = _decode(key)
        removed = 0
        for m in members:
            mv = _decode(m)
            if mv in self.sets[k]:
                self.sets[k].discard(mv)
                removed += 1
        return removed

    async def smembers(self, key: str) -> set[str]:
        k = _decode(key)
        return set(self.sets[k])

    async def scard(self, key: str) -> int:
        k = _decode(key)
        return len(self.sets[k])

    async def hset(self, key: str, mapping: dict[str, Any] | None = None, **kwargs: Any) -> int:
        k = _decode(key)
        if mapping:
            for mk, mv in mapping.items():
                self.hashes[k][_decode(mk)] = _decode(mv) if not isinstance(mv, str) else mv
        for mk, mv in kwargs.items():
            self.hashes[k][_decode(mk)] = _decode(mv)
        return 1

    async def hget(self, key: str, field: str) -> str | None:
        k = _decode(key)
        return self.hashes[k].get(_decode(field))

    async def hgetall(self, key: str) -> dict[str, str]:
        k = _decode(key)
        return dict(self.hashes.get(k, {}))

    async def delete(self, *keys: Any) -> int:
        n = 0
        for key in keys:
            k = _decode(key)
            if k in self.hashes:
                del self.hashes[k]
                n += 1
            if k in self.sets:
                del self.sets[k]
                n += 1
            self.ttls.pop(k, None)
        return n

    async def expire(self, key: str, seconds: int) -> bool:
        k = _decode(key)
        self.ttls[k] = seconds
        return True

    async def ttl(self, key: str) -> int:
        k = _decode(key)
        v = self.ttls.get(k)
        if v is None:
            return -1
        return int(v)

    async def persist(self, key: str) -> bool:
        k = _decode(key)
        self.ttls[k] = None
        return True

    async def exists(self, key: str) -> int:
        k = _decode(key)
        if k in self.hashes or k in self.sets:
            return 1
        return 0

    async def publish(self, _channel: str, _message: Any) -> int:
        return 1

    async def scan_iter(self, match: str = "*") -> Any:
        m = _decode(match) if not isinstance(match, str) else match
        prefix = m[:-1] if m.endswith("*") else m
        all_keys = set(self.sets.keys()) | set(self.hashes.keys())
        for k in sorted(all_keys):
            if m == "*" or str(k).startswith(prefix):
                yield k

    async def eval(self, script: str, num_keys: int, *keys_and_args: Any) -> int:
        """Enough for registry_add_connection_if_under_limit Lua."""
        keys = [_decode(x) for x in keys_and_args[:num_keys]]
        args = list(keys_and_args[num_keys:])
        if not keys:
            return 0
        connections_key = keys[0]
        connection_key = keys[1]
        max_connections = int(args[0])
        conn_id = args[1]
        user_id = args[2]
        metadata = args[3]
        heartbeat_timeout = args[4]
        groups = args[5]

        if len(self.sets[connections_key]) >= max_connections:
            return 0

        self.sets[connections_key].add(conn_id)
        self.hashes[connection_key] = {
            "user_id": user_id,
            "metadata": metadata,
            "heartbeat_timeout": heartbeat_timeout,
            "groups": groups,
        }
        if len(keys) >= 3 and user_id:
            user_key = keys[2]
            self.sets[user_key].add(conn_id)
        return 1

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self)

    async def scan(
        self,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[str]]:
        del count
        all_keys = list(self.sets.keys()) + list(self.hashes.keys())
        if match and "*" in match:
            prefix = match.split("*", 1)[0]
            all_keys = [k for k in all_keys if k.startswith(prefix)]
        return 0, all_keys

    async def sscan(
        self,
        key: str,
        cursor: int = 0,
        count: int | None = None,
    ) -> tuple[int, list[str]]:
        del count
        k = _decode(key)
        members = list(self.sets.get(k, set()))
        return 0, members


class FakePipeline:
    def __init__(self, redis: FakeAsyncRedis) -> None:
        self._r = redis
        self._ops: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    @property
    def command_stack(self) -> list[tuple[str, tuple[Any, ...], dict[str, Any]]]:
        """Used by cleanup code that checks whether the pipeline has commands."""
        return self._ops

    def sadd(self, key: str, *members: Any) -> None:
        self._ops.append(("sadd", (key, *members), {}))

    def expire(self, key: str, seconds: int) -> None:
        self._ops.append(("expire", (key, seconds), {}))

    def srem(self, key: str, *members: Any) -> None:
        self._ops.append(("srem", (key, *members), {}))

    def delete(self, key: str) -> None:
        self._ops.append(("delete", (key,), {}))

    def hgetall(self, key: str) -> None:
        self._ops.append(("hgetall", (key,), {}))

    def ttl(self, key: str) -> None:
        self._ops.append(("ttl", (key,), {}))

    def exists(self, key: str) -> None:
        self._ops.append(("exists", (key,), {}))

    async def execute(self) -> list[Any]:
        out: list[Any] = []
        for op, args, _kwargs in self._ops:
            method = getattr(self._r, op)
            out.append(await method(*args))
        self._ops.clear()
        return out
