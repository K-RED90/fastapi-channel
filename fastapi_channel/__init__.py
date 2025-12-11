"""FastAPI Channel - WebSocket connection management for FastAPI applications."""

from fastapi_channel.channel_layer import ChannelLayer, get_channel_layer
from fastapi_channel.config import WSConfig
from fastapi_channel.connections import Connection, ConnectionManager, ConnectionRegistry
from fastapi_channel.consumer import BaseConsumer
from fastapi_channel.exceptions import BaseError

__all__ = [
    "BaseConsumer",
    "BaseError",
    "ChannelLayer",
    "Connection",
    "ConnectionManager",
    "ConnectionRegistry",
    "WSConfig",
    "get_channel_layer",
]
