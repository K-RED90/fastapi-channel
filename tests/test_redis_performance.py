"""RedisBackend performance-style tests using the in-memory fake (no real Redis)."""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from fastapi_channels.backends import RedisBackend

from tests.redis_fake import FakeAsyncRedis

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def redis_backend() -> RedisBackend:
    fake = FakeAsyncRedis()
    prefix = f"test:perf:{uuid.uuid4().hex}:"
    pool = MagicMock()
    pool.disconnect = AsyncMock(return_value=None)
    with patch(
        "fastapi_channels.backends.redis.ConnectionPool.from_url",
        return_value=pool,
    ), patch("fastapi_channels.backends.redis.Redis", return_value=fake):
        backend = RedisBackend(
            redis_url="redis://localhost:6379/0",
            channel_prefix=prefix,
            registry_expiry=None,
            group_expiry=None,
        )
        await backend.connect()
        try:
            yield backend
        finally:
            await backend.cleanup()


async def _registry_add(backend: RedisBackend, user_id: str, conn_id: str, *, limit: int) -> bool:
    return await backend.registry_add_connection_if_under_limit(
        connection_id=conn_id,
        user_id=user_id,
        metadata={},
        groups=set(),
        heartbeat_timeout=30.0,
        max_connections=limit,
    )


class TestRedisPerformance:
    @pytest.mark.asyncio
    async def test_redis_group_operations_performance(self, redis_backend: RedisBackend):
        group_count = 50
        channels_per_group = 50
        total_operations = group_count * channels_per_group

        start_time = time.time()
        for group_id in range(group_count):
            group_name = f"perf_group_{group_id}"
            for channel_id in range(channels_per_group):
                await redis_backend.group_add(group_name, f"channel_{group_id}_{channel_id}")

        total_time = time.time() - start_time
        ops_per_second = total_operations / total_time
        assert ops_per_second > 500

        for group_id in range(min(3, group_count)):
            channels = await redis_backend.group_channels(f"perf_group_{group_id}")
            assert len(channels) == channels_per_group

    @pytest.mark.asyncio
    async def test_redis_registry_operations_performance(self, redis_backend: RedisBackend):
        user_count = 200
        connections_per_user = 5
        total_operations = user_count * connections_per_user

        start_time = time.time()
        for user_id in range(user_count):
            for conn_id in range(connections_per_user):
                await _registry_add(
                    redis_backend,
                    f"user_{user_id}",
                    f"conn_{conn_id}",
                    limit=20,
                )

        total_time = time.time() - start_time
        assert total_operations / total_time > 200

        for user_id in range(min(5, user_count)):
            for conn_id in range(connections_per_user):
                conns = await redis_backend.registry_get_user_connections(f"user_{user_id}")
                assert f"conn_{conn_id}" in conns

    @pytest.mark.asyncio
    async def test_redis_concurrent_operations(self, redis_backend: RedisBackend):
        concurrent_tasks = 20
        operations_per_task = 20

        async def perform_operations(task_id: int) -> None:
            for i in range(operations_per_task):
                if i % 3 == 0:
                    await redis_backend.group_add(f"conc_group_{task_id}", f"channel_{i}")
                elif i % 3 == 1:
                    await _registry_add(
                        redis_backend,
                        f"conc_user_{task_id}",
                        f"conc_conn_{i}",
                        limit=20,
                    )
                else:
                    await redis_backend.publish(f"conc_group_{task_id}", {"type": "test", "i": i})

        await asyncio.gather(*(perform_operations(i) for i in range(concurrent_tasks)))

    @pytest.mark.asyncio
    async def test_redis_broadcast_performance(self, redis_backend: RedisBackend):
        group_name = "broadcast_perf_group"
        subscriber_count = 200
        for i in range(subscriber_count):
            await redis_backend.group_add(group_name, f"subscriber_{i}")

        message = {"type": "broadcast", "data": {"text": "announcement"}}
        t0 = time.time()
        await redis_backend.publish(group_name, message)
        assert time.time() - t0 < 1.0

    @pytest.mark.asyncio
    async def test_redis_large_group_management(self, redis_backend: RedisBackend):
        group_name = "large_group"
        channel_count = 500
        for i in range(channel_count):
            await redis_backend.group_add(group_name, f"channel_{i:05d}")

        channels = await redis_backend.group_channels(group_name)
        assert len(channels) == channel_count

        for i in range(50):
            await redis_backend.group_discard(group_name, f"channel_{i:05d}")

        remaining = await redis_backend.group_channels(group_name)
        assert len(remaining) == channel_count - 50

    @pytest.mark.asyncio
    async def test_redis_connection_scaling_simulation(self, redis_backend: RedisBackend):
        total_users = 50
        tasks = []
        for user_id in range(total_users):
            for conn_num in range(3):
                tasks.append(
                    _registry_add(
                        redis_backend,
                        f"user_{user_id}",
                        f"conn_{conn_num}",
                        limit=50,
                    )
                )
        await asyncio.gather(*tasks)

        conns = await redis_backend.registry_get_user_connections("user_0")
        assert len(conns) >= 1

    @pytest.mark.asyncio
    async def test_redis_memory_efficiency_simulation(self, redis_backend: RedisBackend):
        group_count = 50
        for group_id in range(group_count):
            group_name = f"mem_group_{group_id}"
            await redis_backend.group_add(group_name, f"channel_{group_id}")
            for msg_id in range(5):
                await redis_backend.publish(
                    group_name,
                    {"type": "chat", "data": {"text": f"Message {msg_id}"}},
                )

    @pytest.mark.asyncio
    async def test_redis_latency_under_load(self, redis_backend: RedisBackend):
        latencies: list[float] = []
        for i in range(200):
            t0 = time.time()
            if i % 3 == 0:
                await redis_backend.group_add(f"latency_group_{i % 5}", f"channel_{i}")
            elif i % 3 == 1:
                await _registry_add(
                    redis_backend,
                    f"latency_user_{i % 10}",
                    f"conn_{i}",
                    limit=50,
                )
            else:
                await redis_backend.publish(f"latency_group_{i % 5}", {"type": "ping"})
            latencies.append(time.time() - t0)

        assert statistics.mean(latencies) < 0.05


if __name__ == "__main__":
    pytest.main([__file__])
