from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from core.connections.state import Connection
from core.typed import Message, ConsumerProtocol


class Middleware(ABC):
    def __init__(self, next_middleware: Optional["Middleware"] = None):
        self.next_middleware = next_middleware

    async def __call__(
        self,
        message: Message,
        connection: Connection,
        consumer: "ConsumerProtocol",
    ) -> Optional[Message]:
        """Process message then pass to next middleware."""
        processed_message = await self.process(message, connection, consumer)
        if processed_message is not None and self.next_middleware:
            return await self.next_middleware(processed_message, connection, consumer)
        return processed_message

    @abstractmethod
    async def process(
        self,
        message: Message,
        connection: Connection,
        consumer: "ConsumerProtocol",
    ) -> Message | None:
        """Implement middleware logic and return message or None."""
        raise NotImplementedError
