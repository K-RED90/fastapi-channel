import logging
import time
from typing import Dict, Tuple

from core.middleware.base import Middleware
from core.exceptions import RateLimitError
from core.config import Settings

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Simple token bucket rate limiter per key."""

    def __init__(self, rate: int, window_seconds: int, burst_size: int):
        self.rate = rate
        self.window = window_seconds
        self.burst = burst_size
        self.buckets: Dict[str, Tuple[float, int]] = {}

    def allow(self, key: str) -> bool:
        now = time.time()

        if key not in self.buckets:
            self.buckets[key] = (now, self.burst - 1)
            return True

        last_check, tokens = self.buckets[key]
        elapsed = now - last_check

        tokens = min(self.burst, tokens + int(elapsed * (self.rate / self.window)))

        if tokens > 0:
            self.buckets[key] = (now, tokens - 1)
            return True

        self.buckets[key] = (now, 0)
        return False


class RateLimitMiddleware(Middleware):
    """Apply token bucket rate limiting to messages."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        settings = Settings()
        self.enabled = bool(getattr(settings, "RATE_LIMIT_ENABLED", False))
        self.messages_per_window = int(getattr(settings, "RATE_LIMIT_MESSAGES", 100))
        self.window_seconds = int(getattr(settings, "RATE_LIMIT_WINDOW", 60))
        self.burst_size = int(getattr(settings, "RATE_LIMIT_BURST", self.messages_per_window))
        self.limiter = TokenBucketRateLimiter(
            rate=self.messages_per_window,
            window_seconds=self.window_seconds,
            burst_size=self.burst_size,
        )

    async def process(self, message, connection, consumer):
        if not self.enabled:
            return message

        if message.type in {"ping", "pong"}:
            return message

        key = getattr(connection, "channel_name", None) or getattr(connection, "id", "unknown")
        if not self.limiter.allow(key):
            from core.exceptions import create_error_context
            context = create_error_context(
                user_id=getattr(connection, 'user_id', None),
                connection_id=getattr(connection, 'channel_name', None),
                message_type=message.type,
                component="rate_limit_middleware",
                rate_limit_key=key,
            )
            logger.warning("Rate limit exceeded for %s", key)
            raise RateLimitError(
                "Rate limit exceeded",
                context=context,
            )

        return message
