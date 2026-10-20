"""FastAPI application initialization and lifespan management.

Contains strictly application configuration and wiring without business logic.
"""

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from task_queue.api.router import api_router
from task_queue.api.websocket import ws_manager
from task_queue.config import get_settings
from task_queue.db.session import Base, get_async_engine
from task_queue.exceptions import TaskNotFoundError, TaskQueueError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("task_queue")


async def redis_event_listener() -> None:
    """Listen to Redis pub/sub channel for worker events and relay to WebSockets."""
    settings = get_settings()
    try:
        import redis.asyncio as aioredis

        r = aioredis.from_url(settings.redis_url)  # type: ignore[no-untyped-call]
        pubsub = r.pubsub()
        await pubsub.subscribe("task_updates_channel")
        logger.info("Subscribed to Redis pubsub task_updates_channel")

        async for message in pubsub.listen():
            if message and message.get("type") == "message":
                try:
                    payload = json.loads(message["data"])
                    await ws_manager.broadcast_event(
                        event_name=payload.get("event", "task_update"),
                        task_id=payload.get("task_id", ""),
                        status=payload.get("status", ""),
                        data=payload.get("data", {}),
                    )
                except Exception as err:
                    logger.debug("Failed parsing redis pubsub message: %s", err)
    except asyncio.CancelledError:
        logger.info("Redis event listener cancelled during shutdown")
    except Exception as exc:
        logger.warning("Redis event listener encountered error (running in local mode): %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown procedures."""
    settings = get_settings()
    logger.info(
        "Starting %s v%s in %s mode", settings.app_name, settings.app_version, settings.environment
    )

    # In testing or development with sqlite, initialize tables automatically
    engine = get_async_engine()
    if "sqlite" in settings.database_url:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Initialized database tables for SQLite")

    # Start background Redis subscriber for WebSocket broadcasting
    listener_task = asyncio.create_task(redis_event_listener())

    yield

    listener_task.cancel()
    await asyncio.gather(listener_task, return_exceptions=True)
    await engine.dispose()
    logger.info("Disposed database engine and shut down services")


def create_app() -> FastAPI:
    """Construct and configure the FastAPI application.

    Returns:
        FastAPI: Fully configured ASGI app.
    """
    settings = get_settings()

    application = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="Distributed task queue system with priority scheduling, retries, and WebSockets.",
        lifespan=lifespan,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Health check endpoint for container orchestrators
    @application.get("/health", tags=["Health"], summary="Service health check")
    async def health_check() -> dict[str, Any]:
        """Verify service operational readiness."""
        return {
            "status": "ok",
            "app": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
        }

    # Exception Handlers
    @application.exception_handler(TaskNotFoundError)
    async def task_not_found_handler(request: Request, exc: TaskNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc)},
        )

    @application.exception_handler(TaskQueueError)
    async def task_queue_error_handler(request: Request, exc: TaskQueueError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": str(exc)},
        )

    # Mount API routes
    application.include_router(api_router, prefix=settings.api_v1_prefix)

    return application


app = create_app()
