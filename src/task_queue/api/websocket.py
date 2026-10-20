"""WebSocket connection manager and real-time task update endpoints."""

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from task_queue.config import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ws", tags=["WebSocket"])


class ConnectionManager:
    """Manages active WebSocket connections and task status event dispatching."""

    def __init__(self) -> None:
        """Initialize connection stores."""
        self._global_connections: set[WebSocket] = set()
        self._task_connections: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, task_id: str | None = None) -> None:
        """Accept connection and register under global or task-specific pool.

        Args:
            websocket: Active FastAPI WebSocket connection.
            task_id: Optional task identifier for targeted subscriptions.
        """
        await websocket.accept()
        async with self._lock:
            if task_id:
                if task_id not in self._task_connections:
                    self._task_connections[task_id] = set()
                self._task_connections[task_id].add(websocket)
            else:
                self._global_connections.add(websocket)
        logger.info(
            "WebSocket connected (task_id=%s, total_global=%d)",
            task_id,
            len(self._global_connections),
        )

    async def disconnect(self, websocket: WebSocket, task_id: str | None = None) -> None:
        """Remove connection from active pools upon disconnection.

        Args:
            websocket: Disconnected WebSocket instance.
            task_id: Optional task identifier previously subscribed to.
        """
        async with self._lock:
            if task_id and task_id in self._task_connections:
                self._task_connections[task_id].discard(websocket)
                if not self._task_connections[task_id]:
                    del self._task_connections[task_id]
            else:
                self._global_connections.discard(websocket)
        logger.info("WebSocket disconnected (task_id=%s)", task_id)

    async def broadcast_event(
        self,
        event_name: str,
        task_id: str,
        status: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """Dispatch task lifecycle event to all subscribed WebSocket clients.

        Args:
            event_name: Descriptive event name (e.g. task_started, task_completed).
            task_id: Unique task identifier.
            status: New task status.
            data: Additional payload details.
        """
        message = {
            "event": event_name,
            "task_id": task_id,
            "status": status,
            "timestamp": datetime.now(UTC).isoformat(),
            "data": data or {},
        }
        raw_text = json.dumps(message)

        # Collect target sockets
        async with self._lock:
            targeted = list(self._task_connections.get(task_id, set()))
            globals_ = list(self._global_connections)
            all_sockets = list(set(targeted + globals_))

        stale_sockets: list[WebSocket] = []
        for socket in all_sockets:
            try:
                await socket.send_text(raw_text)
            except Exception:
                stale_sockets.append(socket)

        if stale_sockets:
            async with self._lock:
                for stale in stale_sockets:
                    self._global_connections.discard(stale)
                    if task_id in self._task_connections:
                        self._task_connections[task_id].discard(stale)


ws_manager = ConnectionManager()


def publish_task_event_sync(
    event_name: str,
    task_id: str,
    status: str,
    data: dict[str, Any] | None = None,
) -> None:
    """Synchronously publish an event to Redis Pub/Sub for worker processes.

    Workers running in separate threads/processes call this to notify the API server.

    Args:
        event_name: Name of the event.
        task_id: Target task identifier.
        status: Task status.
        data: Associated payload.
    """
    settings = get_settings()
    message = {
        "event": event_name,
        "task_id": task_id,
        "status": status,
        "timestamp": datetime.now(UTC).isoformat(),
        "data": data or {},
    }
    try:
        import redis

        r = redis.from_url(settings.redis_url)  # type: ignore[no-untyped-call]
        r.publish("task_updates_channel", json.dumps(message))
    except Exception as exc:
        logger.debug("Redis pubsub broadcast skipped/failed: %s", exc)


@router.websocket("/tasks")
async def websocket_all_tasks(websocket: WebSocket) -> None:
    """WebSocket endpoint streaming status updates for all tasks in real-time."""
    await ws_manager.connect(websocket)
    try:
        while True:
            # Keep connection alive; client can send pings
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"event": "pong"}))
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.warning("WebSocket error in all_tasks: %s", exc)
        await ws_manager.disconnect(websocket)


@router.websocket("/tasks/{task_id}")
async def websocket_task_detail(websocket: WebSocket, task_id: str) -> None:
    """WebSocket endpoint streaming updates for a specific task ID."""
    await ws_manager.connect(websocket, task_id=task_id)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"event": "pong", "task_id": task_id}))
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket, task_id=task_id)
    except Exception as exc:
        logger.warning("WebSocket error for task %s: %s", task_id, exc)
        await ws_manager.disconnect(websocket, task_id=task_id)
