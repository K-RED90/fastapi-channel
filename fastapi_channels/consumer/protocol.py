from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from ..exceptions import BaseError, ValidationError, create_error_context
from ..typed import Message, MessagePriority

if TYPE_CHECKING:
    from ..connections import Connection
    from .base import BaseConsumer


class ProtocolAdapter(ABC):
    """Protocol adapter for parsing inbound messages and formatting errors."""

    @abstractmethod
    def parse_json(
        self,
        json_str: str,
        connection: Connection,
        consumer: BaseConsumer,
    ) -> Message | None:
        """Parse JSON text into a Message. Return None if handled internally."""

    @abstractmethod
    def parse_binary(
        self,
        binary: bytes,
        connection: Connection,
        consumer: BaseConsumer,
    ) -> Message | None:
        """Parse binary data into a Message. Return None if handled internally."""

    @abstractmethod
    def format_error(self, error: BaseError) -> dict[str, Any] | str:
        """Format an error payload for the client."""

    @abstractmethod
    def format_event(self, event_type: str, data: dict[str, Any]) -> dict[str, Any]:
        """Format an outbound event payload for the client."""


class DefaultProtocolAdapter(ProtocolAdapter):
    """Default ws_service protocol adapter using Message schema."""

    def parse_binary(
        self,
        binary: bytes,
        connection: Connection,
        consumer: BaseConsumer,
    ) -> Message:
        return Message(
            type="binary",
            data=None,
            binary_data=binary,
            sender_id=connection.channel_name,
        )

    def parse_json(
        self,
        json_str: str,
        connection: Connection,
        consumer: BaseConsumer,
    ) -> Message | None:
        context = create_error_context(
            user_id=connection.user_id,
            connection_id=connection.channel_name,
            component="consumer",
        )

        try:
            json_data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise ValidationError(
                message="Invalid JSON format",
                error_code="INVALID_JSON",
                context=context,
                details={"parse_error": str(exc)},
            ) from exc

        if not isinstance(json_data, dict):
            raise ValidationError(
                message="JSON message must be an object with a 'type' field",
                error_code="INVALID_MESSAGE_FORMAT",
                context=context,
                details={"received_type": type(json_data).__name__},
            )

        if "type" not in json_data:
            raise ValidationError(
                message="JSON message missing required 'type'",
                error_code="MISSING_MESSAGE_TYPE",
                context=context,
                details={"payload_keys": list(json_data.keys())},
            )

        message_type = json_data["type"]
        if message_type == "pong":
            connection.update_heartbeat()
            return None

        priority_value = json_data.get("priority", MessagePriority.NORMAL.value)
        priority = (
            MessagePriority(priority_value)
            if priority_value in MessagePriority._value2member_map_
            else MessagePriority.NORMAL
        )

        return Message(
            type=message_type,
            data=json_data.get("data"),
            sender_id=connection.channel_name,
            metadata=json_data.get("metadata"),
            ttl_seconds=json_data.get("ttl_seconds"),
            priority=priority,
        )

    def format_error(self, error: BaseError) -> dict[str, Any] | str:
        return error.to_response().to_dict()

    def format_event(self, event_type: str, data: dict[str, Any]) -> dict[str, Any]:
        if not event_type:
            raise ValueError("event_type must be a non-empty string")
        return {"type": event_type, "data": data}
