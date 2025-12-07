import json
from core.middleware.base import Middleware
from core.typed import Message
from core.exceptions import ValidationError, create_error_context


class ValidationMiddleware(Middleware):
    def __init__(self, max_message_size: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_message_size = max_message_size

    async def process(self, message: Message, connection, consumer) -> Message | None:
        try:
            size = len(json.dumps(message.to_dict()))
            if size > self.max_message_size:
                context = create_error_context(
                    user_id=getattr(connection, "user_id", None),
                    connection_id=getattr(connection, "channel_name", None),
                    message_type=message.type,
                    component="validation_middleware",
                    message_size=size,
                    max_size=self.max_message_size,
                )
                raise ValidationError(
                    f"Message too large: {size} bytes (max: {self.max_message_size})",
                    context=context,
                )
        except ValidationError:
            raise
        except Exception as exc:
            context = create_error_context(
                user_id=getattr(connection, "user_id", None),
                connection_id=getattr(connection, "channel_name", None),
                message_type=message.type,
                component="validation_middleware",
                original_error=str(exc),
            )
            raise ValidationError(
                f"Validation failed: {str(exc)}",
                context=context,
            )

        if message.is_expired():
            return None

        return message
