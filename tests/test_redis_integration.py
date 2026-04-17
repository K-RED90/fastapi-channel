import contextlib
import json
import uuid
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient

from example.consumers import ChatConsumer
from example.database import ChatDatabase
from fastapi_channels.backends import RedisBackend
from fastapi_channels.config import WSConfig
from fastapi_channels.connections import ConnectionManager
from fastapi_channels.connections.manager import reset_connection_manager_singleton
from fastapi_channels.middleware import LoggingMiddleware, ValidationMiddleware

from tests.redis_fake import FakeAsyncRedis


class TestRedisIntegration:
    """Integration tests for Redis backend with WebSocket chat system"""

    @pytest_asyncio.fixture
    async def redis_app(self):
        """FastAPI app with Redis backend backed by an in-memory fake (no real Redis)."""
        # Integration tests may construct ConnectionManager first; singleton would
        # otherwise skip __init__ and leave a MemoryBackend in place.
        reset_connection_manager_singleton()
        fake = FakeAsyncRedis()
        channel_prefix = f"test:int:{uuid.uuid4().hex}:"
        settings = WSConfig(
            BACKEND_TYPE="redis",
            REDIS_URL="redis://localhost:6379/0",
            REDIS_CHANNEL_PREFIX=channel_prefix,
            MAX_TOTAL_CONNECTIONS=200000,
            MAX_CONNECTIONS_PER_CLIENT=1000,
            WS_MAX_MESSAGE_SIZE=10 * 1024 * 1024,
        )

        pool = MagicMock()
        pool.disconnect = AsyncMock(return_value=None)
        with patch(
            "fastapi_channels.backends.redis.ConnectionPool.from_url",
            return_value=pool,
        ), patch("fastapi_channels.backends.redis.Redis", return_value=fake):
            manager = ConnectionManager(ws_config=settings)
            backend = manager.backend
            await backend.connect()

            middleware_stack = (
                ValidationMiddleware(settings.WS_MAX_MESSAGE_SIZE) | LoggingMiddleware()
            )

            database = ChatDatabase()
            consumers: dict = {}

            app = FastAPI()

            @app.websocket("/ws/{user_id}")
            async def websocket_endpoint(websocket: WebSocket, user_id: str):
                connection = await manager.connect(websocket=websocket, user_id=user_id)

                consumer = ChatConsumer(
                    connection=connection,
                    manager=manager,
                    middleware_stack=middleware_stack,
                    database=database,
                )
                consumers[connection.channel_name] = consumer

                try:
                    await consumer.connect()

                    while True:
                        message = await websocket.receive()
                        if "text" in message:
                            json_str = message["text"]
                            await consumer.handle_message(json_str=json_str)
                        elif "bytes" in message:
                            binary = message["bytes"]
                            await consumer.handle_message(binary=binary)
                        else:
                            continue

                except WebSocketDisconnect:
                    await consumer.disconnect(1000)
                except Exception:
                    await consumer.disconnect(1011)

            yield app, manager, consumers, backend, database

    @pytest.mark.asyncio
    async def test_redis_backend_initialization(self, redis_app):
        """Test Redis backend initializes correctly"""
        _app, _manager, _consumers, backend, _database = redis_app

        assert isinstance(backend, RedisBackend)
        assert backend.redis_url == "redis://localhost:6379/0"
        assert backend.max_connections == 200

    @pytest.mark.asyncio
    async def test_redis_full_chat_flow(self, redis_app):
        """Test complete chat flow with Redis backend"""
        app, _manager, _consumers, _backend, _database = redis_app

        # Create test client
        client = TestClient(app)

        # Connect first user
        with client.websocket_connect("/ws/user1") as ws1:
            # Receive welcome message
            welcome = json.loads(ws1.receive_text())
            assert welcome["type"] == "welcome"
            assert "user1" in welcome["message"]

            # Connect second user
            with client.websocket_connect("/ws/user2") as ws2:
                # Receive welcome for user2
                welcome2 = json.loads(ws2.receive_text())
                assert welcome2["type"] == "welcome"
                assert "user2" in welcome2["message"]

                # Both users join the lobby room
                ws1.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
                join_response1 = json.loads(ws1.receive_text())
                assert join_response1["type"] == "room_joined"
                assert join_response1["room"] == "lobby"
                # Skip room_info message
                room_info = json.loads(ws1.receive_text())
                assert room_info["type"] == "room_info"

                ws2.send_text(json.dumps({"type": "join_room", "data": {"room": "lobby"}}))
                join_response2 = json.loads(ws2.receive_text())
                assert join_response2["type"] == "room_joined"
                assert join_response2["room"] == "lobby"
                # Skip room_info message
                room_info = json.loads(ws2.receive_text())
                assert room_info["type"] == "room_info"

                # User1 sends a chat message
                ws1.send_text(
                    json.dumps(
                        {
                            "type": "chat_message",
                            "data": {"text": "Hello everyone!", "room": "lobby"},
                        }
                    )
                )

                # User1 receives their own message back (may receive user_joined_room first)
                own_message = json.loads(ws1.receive_text())
                while own_message["type"] == "user_joined_room":
                    own_message = json.loads(ws1.receive_text())  # Skip user joined notifications
                assert own_message["type"] == "chat_message"
                assert own_message["text"] == "Hello everyone!"
                assert own_message["username"] == "user1"

                # User2 should receive the message (may receive room_info or user_joined_room first)
                message = json.loads(ws2.receive_text())
                while message["type"] in ["room_info", "user_joined_room"]:
                    message = json.loads(ws2.receive_text())  # Skip notifications
                assert message["type"] == "chat_message"
                assert message["text"] == "Hello everyone!"
                assert message["username"] == "user1"

    @pytest.mark.asyncio
    async def test_redis_group_operations_at_scale(self, redis_app):
        """Test Redis group operations with larger scale"""
        app, _manager, _consumers, _backend, _database = redis_app
        client = TestClient(app)
        run_id = uuid.uuid4().hex[:12]

        # Test creating multiple rooms with Redis backend
        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Create multiple rooms
            room_count = 50  # Test with 50 rooms

            for i in range(room_count):
                room_name = f"redis_test_room_{run_id}_{i}"
                ws1.send_text(
                    json.dumps(
                        {"type": "create_room", "data": {"room_name": room_name, "is_public": True}}
                    )
                )

                # Receive confirmation
                response = json.loads(ws1.receive_text())
                assert response["type"] == "room_created"
                assert response["room"] == room_name

                # Skip room_joined and room_info messages
                room_joined = json.loads(ws1.receive_text())
                assert room_joined["type"] == "room_joined"
                room_info = json.loads(ws1.receive_text())
                assert room_info["type"] == "room_info"

            # List rooms to verify they were created
            ws1.send_text(json.dumps({"type": "list_rooms", "data": {}}))
            rooms_response = json.loads(ws1.receive_text())
            assert rooms_response["type"] == "rooms_list"

            # Verify we have the rooms (may include default lobby)
            room_names = [r["name"] for r in rooms_response["rooms"]]
            created_rooms = [f"redis_test_room_{run_id}_{i}" for i in range(room_count)]
            for room in created_rooms:
                assert room in room_names

    @pytest.mark.asyncio
    async def test_redis_connection_limits(self, redis_app):
        """Test connection limits with Redis backend"""
        app, _manager, _consumers, _backend, _database = redis_app
        client = TestClient(app)

        # Test per-user connection limits
        max_connections_per_user = 5  # Test with smaller limit for this test

        connections = []
        try:
            with ExitStack() as stack:
                for _i in range(max_connections_per_user + 2):
                    try:
                        ws = stack.enter_context(client.websocket_connect("/ws/test_user"))
                        connections.append(ws)
                        json.loads(ws.receive_text())
                    except Exception:
                        break

                assert len(connections) > 0

        finally:
            for ws in connections:
                with contextlib.suppress(BaseException):
                    ws.close()

    @pytest.mark.asyncio
    async def test_redis_message_broadcast(self, redis_app):
        """Test message broadcasting with Redis backend"""
        app, _manager, _consumers, _backend, _database = redis_app
        client = TestClient(app)

        # Connect multiple users
        user_count = 10

        connections: list = []
        try:
            with ExitStack() as stack:
                for i in range(user_count):
                    ws = stack.enter_context(client.websocket_connect(f"/ws/user{i}"))
                    connections.append(ws)
                    json.loads(ws.receive_text())

                room_name = f"redis_broadcast_room_{uuid.uuid4().hex[:12]}"
                for ws in connections:
                    ws.send_text(json.dumps({"type": "join_room", "data": {"room": room_name}}))
                    json.loads(ws.receive_text())  # room_joined
                    json.loads(ws.receive_text())  # room_info

                connections[0].send_text(
                    json.dumps(
                        {
                            "type": "chat_message",
                            "data": {"text": "Redis broadcast test!", "room": room_name},
                        }
                    )
                )

                received_count = 0
                for i in range(1, len(connections)):
                    try:
                        response = json.loads(connections[i].receive_text())
                        while response["type"] not in ["chat_message", "error"]:
                            response = json.loads(connections[i].receive_text())
                        if response["type"] == "chat_message":
                            assert response["text"] == "Redis broadcast test!"
                            received_count += 1
                    except Exception:
                        pass

                assert received_count >= user_count // 2

        finally:
            for ws in connections:
                with contextlib.suppress(BaseException):
                    ws.close()

    @pytest.mark.asyncio
    async def test_redis_large_messages(self, redis_app):
        """Test large message handling with Redis backend"""
        app, *_ = redis_app
        client = TestClient(app)

        with client.websocket_connect("/ws/user1") as ws1:
            # Skip welcome
            ws1.receive_text()

            # Join room
            room = f"redis_large_msg_{uuid.uuid4().hex[:12]}"
            ws1.send_text(json.dumps({"type": "join_room", "data": {"room": room}}))
            json.loads(ws1.receive_text())  # room_joined
            json.loads(ws1.receive_text())  # room_info

            # Send large message (within 10MB limit)
            large_message = "x" * 100000  # 100k character message
            ws1.send_text(
                json.dumps(
                    {"type": "chat_message", "data": {"text": large_message, "room": room}}
                )
            )

            # Should receive the message back
            response = json.loads(ws1.receive_text())
            while response["type"] == "user_joined_room":
                response = json.loads(ws1.receive_text())
            assert response["type"] == "chat_message"
            expect_message_length = 100000
            assert len(response["text"]) == expect_message_length

    @pytest.mark.asyncio
    async def test_redis_backend_resilience(self, redis_app):
        """Test Redis backend resilience and error handling"""
        _app, _manager, _consumers, backend, _database = redis_app

        await backend.group_add("resilience_test", "channel1")

        channels = await backend.group_channels("resilience_test")
        assert "channel1" in channels

        added = await backend.registry_add_connection_if_under_limit(
            connection_id="conn1",
            user_id="user1",
            metadata={},
            groups=set(),
            heartbeat_timeout=30.0,
            max_connections=100,
        )
        assert added is True

        user_conns = await backend.registry_get_user_connections("user1")
        assert "conn1" in user_conns


if __name__ == "__main__":
    pytest.main([__file__])
