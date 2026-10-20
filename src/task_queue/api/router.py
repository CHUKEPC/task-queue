"""Aggregated API router combining all endpoint namespaces."""

from fastapi import APIRouter

from task_queue.api.status import router as status_router
from task_queue.api.tasks import router as tasks_router
from task_queue.api.websocket import router as websocket_router

api_router = APIRouter()

api_router.include_router(tasks_router)
api_router.include_router(status_router)
api_router.include_router(websocket_router)
