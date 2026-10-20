"""Tests for WebSocket real-time task notifications and connection manager."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.testclient import TestClient

from task_queue.api.websocket import (
    ConnectionManager,
    publish_task_event_sync,
)
from task_queue.main import app


def test_websocket_all_tasks_ping_pong() -> None:
    """Verify WebSocket /api/v1/ws/tasks accepts connection and responds to ping."""
    with TestClient(app) as client, client.websocket_connect("/api/v1/ws/tasks") as websocket:
        websocket.send_text("ping")
        response = websocket.receive_text()
        assert json.loads(response) == {"event": "pong"}


def test_websocket_task_detail_ping_pong() -> None:
    """Verify WebSocket /api/v1/ws/tasks/{task_id} sends targeted pong."""
    task_id = "test-task-123"
    with (
        TestClient(app) as client,
        client.websocket_connect(f"/api/v1/ws/tasks/{task_id}") as websocket,
    ):
        websocket.send_text("ping")
        response = websocket.receive_text()
        data = json.loads(response)
        assert data["event"] == "pong"
        assert data["task_id"] == task_id


@pytest.mark.asyncio
async def test_connection_manager_broadcast() -> None:
    """Verify ConnectionManager broadcast dispatches to mock sockets and purges stale."""
    manager = ConnectionManager()
    mock_socket_good = MagicMock()
    mock_socket_good.accept = AsyncMock()
    mock_socket_good.send_text = AsyncMock()

    mock_socket_bad = MagicMock()
    mock_socket_bad.accept = AsyncMock()
    mock_socket_bad.send_text = AsyncMock(side_effect=RuntimeError("Disconnected"))

    await manager.connect(mock_socket_good, task_id="t1")
    await manager.connect(mock_socket_bad, task_id="t1")

    # Broadcast event
    await manager.broadcast_event(
        event_name="task_completed",
        task_id="t1",
        status="success",
        data={"result": "ok"},
    )

    # Bad socket should be disconnected and removed
    assert mock_socket_good in manager._task_connections.get("t1", set())
    assert mock_socket_bad not in manager._task_connections.get("t1", set())

    # Disconnect good socket
    await manager.disconnect(mock_socket_good, task_id="t1")
    assert "t1" not in manager._task_connections


def test_publish_task_event_sync() -> None:
    """Verify publish_task_event_sync handles redis calls and exceptions safely."""
    with patch("redis.from_url") as mock_from_url:
        mock_redis = MagicMock()
        mock_from_url.return_value = mock_redis

        publish_task_event_sync(
            event_name="task_success",
            task_id="task-xyz",
            status="success",
            data={"foo": "bar"},
        )

        mock_from_url.assert_called_once()
        mock_redis.publish.assert_called_once()


def test_publish_task_event_sync_exception() -> None:
    """Verify publish_task_event_sync catches redis exceptions without raising."""
    with patch("redis.from_url", side_effect=Exception("Redis connection error")):
        # Should not raise exception
        publish_task_event_sync(
            event_name="task_failed",
            task_id="task-xyz",
            status="failed",
        )
