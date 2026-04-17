"""FastAPI Channels - WebSocket connection management for FastAPI applications."""

from .backends import BaseBackend, MemoryBackend, RedisBackend
from .config import WSConfig
from .connections import Connection, ConnectionManager, ConnectionRegistry
from .connections.manager import get_manager
from .consumer import BaseConsumer, DefaultProtocolAdapter, ProtocolAdapter
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    BackendError,
    BaseError,
    ConnectionError,
    ErrorCategory,
    ErrorContext,
    ErrorResponse,
    ErrorSeverity,
    MessageError,
    RateLimitError,
    SystemError,
    TimeoutError,
    ValidationError,
    WebSocketError,
    create_error_context,
)
from .middleware import (
    LoggingMiddleware,
    Middleware,
    RateLimitMiddleware,
    ValidationMiddleware,
)
from .serializers import BaseSerializer, JSONSerializer, ORJSONSerializer
from .typed import ConnectionState, Message, MessagePriority

__all__ = [
    # Configuration
    "WSConfig",
    # Connections
    "Connection",
    "ConnectionManager",
    "ConnectionRegistry",
    "get_manager",
    # Consumer
    "BaseConsumer",
    "ProtocolAdapter",
    "DefaultProtocolAdapter",
    # Exceptions
    "BaseError",
    "WebSocketError",
    "ConnectionError",
    "MessageError",
    "BackendError",
    "AuthenticationError",
    "ValidationError",
    "RateLimitError",
    "AuthorizationError",
    "SystemError",
    "TimeoutError",
    "ErrorCategory",
    "ErrorSeverity",
    "ErrorContext",
    "ErrorResponse",
    "create_error_context",
    # Backends
    "BaseBackend",
    "MemoryBackend",
    "RedisBackend",
    # Middleware
    "Middleware",
    "LoggingMiddleware",
    "RateLimitMiddleware",
    "ValidationMiddleware",
    # Serializers
    "BaseSerializer",
    "JSONSerializer",
    "ORJSONSerializer",
    # Types
    "Message",
    "ConnectionState",
    "MessagePriority",
]
