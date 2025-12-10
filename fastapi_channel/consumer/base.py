import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from fastapi_channel.connections import Connection, ConnectionManager
from fastapi_channel.exceptions import BaseError, ValidationError, create_error_context
from fastapi_channel.middleware import Middleware
from fastapi_channel.typed import Message, MessagePriority

if TYPE_CHECKING:
    from fastapi_channel.channel_layer import ChannelLayer


class BaseConsumer:
    """Abstract base class for WebSocket consumer implementations.

    BaseConsumer provides the core functionality for handling WebSocket connections,
    processing incoming messages, and managing group memberships. It serves as the
    foundation for application-specific WebSocket consumers.

    The consumer lifecycle:
    1. connect() - Called when WebSocket connection is established
    2. receive() - Called for each incoming message
    3. disconnect() - Called when connection is closed

    Key features:
    - Automatic message parsing and validation
    - Middleware support for message processing
    - Group management for broadcast messaging
    - Error handling with structured error responses
    - Connection state tracking

    Parameters
    ----------
    connection : Connection
        WebSocket connection state and metadata
    manager : ConnectionManager | None, optional
        Manager for connection lifecycle and messaging. If None, must provide channel_layer.
        Default: None
    channel_layer : ChannelLayer | None, optional
        ChannelLayer instance. If provided, manager is extracted automatically.
        Default: None
    middleware_stack : Middleware | None, optional
        Message processing middleware chain. Default: None

    Notes
    -----
    Either `manager` or `channel_layer` must be provided. If both are provided,
    `manager` takes precedence. If `channel_layer` is provided, the manager
    is extracted from it automatically.

    Examples
    --------
    Basic consumer implementation:

    >>> class ChatConsumer(BaseConsumer):
    ...     async def connect(self) -> None:
    ...         await self.join_group("general")
    ...         await self.send_json({"type": "welcome"})
    ...
    ...     async def receive(self, message: Message) -> None:
    ...         if message.type == "chat":
    ...             await self.send_to_group("general", message)
    ...
    ...     async def on_disconnect(self, code: int) -> None:
    ...         await self.leave_group("general")

    Notes
    -----
    Subclasses must implement connect(), on_disconnect(), and receive() methods.
    Messages are automatically parsed from JSON and validated.
    Heartbeat messages ("pong") are handled automatically.
    The disconnect() method is provided by BaseConsumer and handles both
    consumer cleanup (via on_disconnect()) and connection manager disconnection.

    """

    def __init__(
        self,
        connection: Connection,
        manager: ConnectionManager | None = None,
        channel_layer: "ChannelLayer | None" = None,
        middleware_stack: Middleware | None = None,
    ):
        if manager is None and channel_layer is None:
            raise ValueError("Either 'manager' or 'channel_layer' must be provided")

        self.connection = connection
        if manager is not None:
            self.manager = manager
        elif channel_layer is not None:
            self.manager = channel_layer.manager
        else:
            raise ValueError("Either 'manager' or 'channel_layer' must be provided")

        self.middleware_stack = middleware_stack

    @abstractmethod
    async def connect(self) -> None:
        """Called when WebSocket connection is established.

        Implement this method to perform connection setup tasks such as:
        - Authentication checks
        - Initial group subscriptions
        - Sending welcome messages
        - Setting up connection-specific state

        Raises
        ------
        AuthenticationError
            If connection authentication fails
        ValidationError
            If connection setup validation fails

        """

    async def disconnect(self, code: int = 1000) -> None:
        """Disconnect the WebSocket connection and perform cleanup.

        This method handles both consumer-specific cleanup and connection manager
        disconnection. Subclasses should override `on_disconnect()` for custom cleanup.

        Parameters
        ----------
        code : int, optional
            WebSocket close code (e.g., 1000 for normal closure). Default: 1000

        Notes
        -----
        This method:
        1. Calls `on_disconnect()` for consumer-specific cleanup
        2. Disconnects from the connection manager (handles WebSocket closure, group cleanup, etc.)

        Subclasses should override `on_disconnect()` instead of `disconnect()` to add
        custom cleanup logic.

        """
        await self.on_disconnect(code)
        await self.manager.disconnect(self.connection.channel_name, code=code)

    @abstractmethod
    async def on_disconnect(self, code: int) -> None:
        """Hook for consumer-specific cleanup when connection is closed.

        Parameters
        ----------
        code : int
            WebSocket close code (e.g., 1000 for normal closure)

        Override this method to perform cleanup tasks such as:
        - Leaving subscribed groups
        - Cleaning up connection-specific resources
        - Logging disconnection events
        - Updating database state

        Notes
        -----
        This method is called before the connection manager disconnects the WebSocket.
        The manager will handle group cleanup automatically, but you can perform
        additional cleanup here if needed.

        """

    @abstractmethod
    async def receive(self, message: Message) -> None:
        """Process incoming WebSocket message.

        Parameters
        ----------
        message : Message
            Parsed and validated message from client

        This method is called for each message received after:
        - JSON parsing and validation
        - Middleware processing
        - Message priority handling

        Implement message type routing and business logic here.

        Raises
        ------
        MessageError
            If message processing fails
        ValidationError
            If message content is invalid

        """

    async def send(self, message: Message) -> None:
        """Send a Message object to the connected WebSocket client.

        Parameters
        ----------
        message : Message
            Message to send to client

        Notes
        -----
        Serializes message to JSON before sending.
        Updates connection statistics (message count, bytes sent).

        """
        await self.connection.websocket.send_json(message.to_dict())

    async def send_json(self, data: dict[str, Any]) -> None:
        """Send raw data as JSON message to the connected client.

        Parameters
        ----------
        data : dict[str, Any]
            Data to send (will be wrapped in Message)

        Notes
        -----
        Creates Message with type "message" if not specified.
        Equivalent to send(Message(type="message", data=data)).

        """
        message = Message(type=data.get("type") or "message", data=data)
        await self.send(message)

    async def join_group(self, group: str) -> None:
        """Add connection to a messaging group.

        Parameters
        ----------
        group : str
            Group name to join

        Notes
        -----
        Updates manager which updates connection.groups (source of truth).
        Enables receiving group messages.

        """
        await self.manager.join_group(self.connection.channel_name, group)

    async def leave_group(self, group: str) -> None:
        """Remove connection from a messaging group.

        Parameters
        ----------
        group : str
            Group name to leave

        Notes
        -----
        Updates manager which updates connection.groups (source of truth).
        Stops receiving group messages.

        """
        await self.manager.leave_group(self.connection.channel_name, group)

    async def send_to_group(self, group: str, message: dict[str, Any] | Message) -> None:
        """Send message to all connections in a group.

        Parameters
        ----------
        group : str
            Target group name
        message : dict[str, Any] | Message
            Message to send to group

        Notes
        -----
        Automatically serializes Message objects.
        Sends to all connections in group except sender.

        """
        payload = message.to_dict() if isinstance(message, Message) else message
        await self.manager.send_group(group, payload)

    async def handle_message(self, raw_message: str) -> None:
        """Process raw WebSocket message string.

        Main entry point for incoming WebSocket messages. Handles:
        - JSON parsing and validation
        - Heartbeat ("pong") message processing
        - Message construction with metadata
        - Middleware processing
        - Error handling with structured responses

        Parameters
        ----------
        raw_message : str
            Raw JSON message string from WebSocket

        Raises
        ------
        ValidationError
            If JSON parsing fails or message format is invalid
        AuthenticationError
            If message requires authentication
        MessageError
            If message processing fails

        Notes
        -----
        Automatically handles heartbeat messages by updating connection state.
        Tracks message statistics (bytes received, activity).
        Sends error responses directly to client for validation failures.

        """
        try:
            data = json.loads(raw_message)
            message_type = data.get("type", "message")

            if message_type == "pong":
                self.connection.update_heartbeat()
                return

            priority_value = data.get("priority", MessagePriority.NORMAL.value)
            priority = (
                MessagePriority(priority_value)
                if priority_value in MessagePriority._value2member_map_
                else MessagePriority.NORMAL
            )
            message = Message(
                type=message_type,
                data=data.get("data"),
                sender_id=self.connection.channel_name,
                metadata=data.get("metadata"),
                ttl_seconds=data.get("ttl_seconds"),
                priority=priority,
            )

            self.connection.bytes_received += len(raw_message.encode())
            self.connection.update_activity()

            if self.middleware_stack:
                message = await self.middleware_stack(message, self.connection, self)
                if not message:
                    return

            await self.receive(message)

        except json.JSONDecodeError as e:
            context = create_error_context(
                user_id=self.connection.user_id,
                connection_id=self.connection.channel_name,
                component="consumer",
            )
            raise ValidationError(
                message="Invalid JSON format",
                error_code="INVALID_JSON",
                context=context,
                details={"parse_error": str(e)},
            ) from e

    async def handle_error(self, error: BaseError) -> None:
        error_response = error.to_response()
        await self.connection.websocket.send_json(error_response.to_dict())
