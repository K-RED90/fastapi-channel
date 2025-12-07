import logging

from core.middleware.base import Middleware
from core.typed import Message
from core.exceptions import AuthenticationError, create_error_context

logger = logging.getLogger(__name__)


class AuthenticationMiddleware(Middleware):
    """Ensure messages come from authenticated users."""

    async def process(self, message: Message, connection, consumer) -> Message:
        # Allow heartbeat and initial connect messages without auth check
        if message.type in {"ping", "pong", "connect"}:
            return message

        if not getattr(connection, "user_id", None):
            context = create_error_context(
                connection_id=getattr(connection, 'channel_name', None),
                message_type=message.type,
                component="authentication_middleware",
            )
            logger.warning("Blocked unauthenticated message %s", message.type)
            raise AuthenticationError(
                "Authentication required",
                context=context,
            )

        return message
