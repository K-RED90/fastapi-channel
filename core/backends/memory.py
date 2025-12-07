from collections import defaultdict
import asyncio
from typing import Any, Dict, Optional, Set

from core.backends.base import BaseBackend


class MemoryBackend(BaseBackend):
    """In-memory backend for single-server deployments"""

    def __init__(self):
        super().__init__()
        self.channels: Dict[str, asyncio.Queue] = {}
        self.listeners: Dict[str, Set[asyncio.Queue]] = defaultdict(set)
        # Registry storage
        self._registry_connections: Set[str] = set()
        self._registry_connection_data: Dict[str, Dict[str, Any]] = {}
        self._registry_user_connections: Dict[str, Set[str]] = defaultdict(set)

    async def publish(self, channel: str, message: Dict[str, Any]) -> None:
        """Publish message to all subscribers of a channel"""
        if channel in self.listeners:
            for queue in self.listeners[channel].copy():
                await queue.put(message)

    async def subscribe(self, channel: str) -> None:
        """Subscribe to a channel"""
        if channel not in self.channels:
            self.channels[channel] = asyncio.Queue()

        queue = self.channels[channel]
        self.listeners[channel].add(queue)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel"""
        if channel in self.channels:
            queue = self.channels[channel]
            self.listeners[channel].discard(queue)
            del self.channels[channel]

    async def group_send(self, group: str, message: Dict[str, Any]) -> None:
        """Send message to all channels in a group"""
        channels = self._get_group_channels(group)
        if not channels:
            return

        tasks = [self.publish(channel, message) for channel in channels]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log any exceptions that occurred during publishing
        failed_channels = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_channels.append(list(channels)[i])

        if failed_channels:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                "Failed to publish message to %d/%d channels in group %s",
                len(failed_channels),
                len(channels),
                group,
                extra={
                    "group": group,
                    "total_channels": len(channels),
                    "failed_channels": failed_channels,
                    "component": "memory_backend.group_send",
                },
            )

    async def get_message(
        self, channel: str, timeout: float | None = None
    ) -> Dict[str, Any] | None:
        """Get next message from channel"""
        if channel not in self.channels:
            return None

        return await asyncio.wait_for(self.channels[channel].get(), timeout=timeout)

    async def receive(
        self, channel: str, timeout: float | None = None
    ) -> Dict[str, Any] | None:
        """Alias for get_message to match backend interface."""
        return await self.get_message(channel, timeout)

    async def cleanup(self) -> None:
        """Cleanup resources"""
        self.channels.clear()
        self.listeners.clear()
        await self.flush()

    async def registry_add_connection(
        self,
        connection_id: str,
        user_id: Optional[str],
        metadata: Dict[str, Any],
        groups: Set[str],
        heartbeat_timeout: float,
    ) -> None:
        """Add connection to registry with metadata."""
        async with self._lock:
            self._registry_connections.add(connection_id)
            self._registry_connection_data[connection_id] = {
                "user_id": user_id,
                "metadata": metadata,
                "groups": groups.copy(),
                "heartbeat_timeout": heartbeat_timeout,
            }
            if user_id:
                self._registry_user_connections[user_id].add(connection_id)

    async def registry_remove_connection(
        self, connection_id: str, user_id: Optional[str]
    ) -> None:
        """Remove connection from registry."""
        async with self._lock:
            self._registry_connections.discard(connection_id)
            self._registry_connection_data.pop(connection_id, None)
            if user_id and user_id in self._registry_user_connections:
                self._registry_user_connections[user_id].discard(connection_id)
                if not self._registry_user_connections[user_id]:
                    del self._registry_user_connections[user_id]

    async def registry_update_groups(
        self, connection_id: str, groups: Set[str]
    ) -> None:
        """Update groups for a connection."""
        async with self._lock:
            if connection_id in self._registry_connection_data:
                self._registry_connection_data[connection_id]["groups"] = groups.copy()

    async def registry_get_connection_groups(self, connection_id: str) -> Set[str]:
        """Get groups for a connection."""
        async with self._lock:
            if connection_id in self._registry_connection_data:
                return self._registry_connection_data[connection_id]["groups"].copy()
            return set()

    async def registry_count_connections(self) -> int:
        """Count total connections in registry."""
        async with self._lock:
            return len(self._registry_connections)

    async def registry_get_user_connections(self, user_id: str) -> Set[str]:
        """Get all connection IDs for a user."""
        async with self._lock:
            return self._registry_user_connections.get(user_id, set()).copy()

    def registry_get_prefix(self) -> str:
        """Get prefix for registry keys."""
        return "memory:registry:"

    def supports_broadcast_channel(self) -> bool:
        """Check if backend supports broadcast channel."""
        return False
