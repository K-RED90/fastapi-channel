import logging

from core.middleware.base import Middleware

logger = logging.getLogger(__name__)


class LoggingMiddleware(Middleware):
    """Log message metadata for observability."""

    async def process(self, message, connection, consumer):
        logger.debug(
            "ws message type=%s channel=%s user=%s",
            getattr(message, "type", None),
            getattr(connection, "channel_name", None),
            getattr(connection, "user_id", None),
        )
        return message
