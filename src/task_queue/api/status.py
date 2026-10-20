"""API router for querying task statuses, results, system metrics, and worker health."""

import math
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.db.session import get_db
from task_queue.exceptions import TaskNotFoundError
from task_queue.models.task_record import TaskPriority, TaskStatus, TaskType
from task_queue.schemas.task import (
    TaskListResponse,
    TaskMetricsResponse,
    TaskResponse,
    TaskResultResponse,
    WorkerHealthResponse,
)
from task_queue.services.metrics import MetricsService
from task_queue.services.task_service import TaskService

router = APIRouter(tags=["Status & Monitoring"])


@router.get(
    "/tasks",
    response_model=TaskListResponse,
    summary="List tasks with filtering and pagination",
)
async def list_tasks(
    db: Annotated[AsyncSession, Depends(get_db)],
    status: Annotated[TaskStatus | None, Query(description="Filter by status")] = None,
    task_type: Annotated[TaskType | None, Query(description="Filter by task category")] = None,
    priority: Annotated[TaskPriority | None, Query(description="Filter by priority")] = None,
    page: Annotated[int, Query(ge=1, description="Page number (1-indexed)")] = 1,
    limit: Annotated[int, Query(ge=1, le=100, description="Items per page")] = 20,
) -> TaskListResponse:
    """Retrieve paginated list of task records matching specified criteria."""
    records, total = await TaskService.list_tasks(
        db=db,
        status=status.value if status else None,
        task_type=task_type.value if task_type else None,
        priority=priority.value if priority else None,
        page=page,
        limit=limit,
    )

    total_pages = math.ceil(total / limit) if total > 0 else 0

    return TaskListResponse(
        items=[TaskResponse.model_validate(r) for r in records],
        total=total,
        page=page,
        limit=limit,
        total_pages=total_pages,
    )


@router.get(
    "/tasks/{task_id}",
    response_model=TaskResponse,
    summary="Get task details by ID",
)
async def get_task_details(
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResponse:
    """Retrieve detailed state of a specific task by internal UUID or Celery task ID."""
    try:
        record = await TaskService.get_task(db, task_id)
        return TaskResponse.model_validate(record)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get(
    "/tasks/{task_id}/result",
    response_model=TaskResultResponse,
    summary="Get task execution result",
)
async def get_task_result(
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResultResponse:
    """Fetch task output payload, duration, and error details if terminated."""
    try:
        record = await TaskService.get_task(db, task_id)
        return TaskResultResponse(
            id=record.id,
            celery_task_id=record.celery_task_id,
            status=record.status,
            result=record.result,
            error_message=record.error_message,
            duration_seconds=record.duration_seconds,
            completed_at=record.completed_at,
        )
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc


@router.get(
    "/metrics",
    response_model=TaskMetricsResponse,
    summary="Retrieve task queue throughput and error metrics",
)
async def get_metrics(
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskMetricsResponse:
    """Return dashboard analytics including throughput, failure rate, and status breakdown."""
    return await MetricsService.get_task_metrics(db)


@router.get(
    "/workers/health",
    response_model=WorkerHealthResponse,
    summary="Monitor Celery worker cluster health and availability",
)
async def get_workers_health() -> WorkerHealthResponse:
    """Inspect connected Celery workers, active job load, and registered tasks."""
    return MetricsService.get_worker_health()
