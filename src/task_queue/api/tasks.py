"""API router for submitting, retrying, and cancelling tasks."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.db.session import get_db
from task_queue.exceptions import (
    TaskCancellationError,
    TaskNotFoundError,
    TaskRetryError,
)
from task_queue.schemas.task import (
    DataProcessingTaskRequest,
    EmailTaskRequest,
    ReportTaskRequest,
    TaskCancelResponse,
    TaskChainRequest,
    TaskGroupRequest,
    TaskResponse,
    TaskRetryResponse,
)
from task_queue.services.task_service import TaskService

router = APIRouter(prefix="/tasks", tags=["Task Submission & Control"])


@router.post(
    "/email",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit simulated email delivery task",
)
async def submit_email_task(
    request: EmailTaskRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResponse:
    """Submit an asynchronous email delivery task with priority and scheduling."""
    record = await TaskService.submit_email_task(db, request)
    return TaskResponse.model_validate(record)


@router.post(
    "/report",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit report generation task",
)
async def submit_report_task(
    request: ReportTaskRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResponse:
    """Schedule asynchronous generation of analytical and business reports."""
    record = await TaskService.submit_report_task(db, request)
    return TaskResponse.model_validate(record)


@router.post(
    "/data-processing",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit batch data processing task",
)
async def submit_data_processing_task(
    request: DataProcessingTaskRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResponse:
    """Submit a batch data processing task with cleaning and aggregation."""
    record = await TaskService.submit_data_processing_task(db, request)
    return TaskResponse.model_validate(record)


@router.post(
    "/chain",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit sequential task chain workflow",
)
async def submit_task_chain(
    request: TaskChainRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResponse:
    """Execute a sequential Celery chain where task outputs feed subsequent tasks."""
    record = await TaskService.submit_chain(db, request)
    return TaskResponse.model_validate(record)


@router.post(
    "/group",
    response_model=TaskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit parallel task group workflow",
)
async def submit_task_group(
    request: TaskGroupRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskResponse:
    """Execute a parallel Celery group of tasks concurrently."""
    record = await TaskService.submit_group(db, request)
    return TaskResponse.model_validate(record)


@router.post(
    "/{task_id}/retry",
    response_model=TaskRetryResponse,
    summary="Manually retry failed or cancelled task",
)
async def retry_task(
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskRetryResponse:
    """Manually re-enqueue a previously failed or cancelled task."""
    try:
        record = await TaskService.retry_task(db, task_id)
        return TaskRetryResponse(
            task_id=record.id,
            celery_task_id=record.celery_task_id,
            status=record.status,
            message="Task has been successfully re-queued for execution.",
        )
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TaskRetryError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.post(
    "/{task_id}/cancel",
    response_model=TaskCancelResponse,
    summary="Cancel in-flight or pending task",
)
async def cancel_task(
    task_id: str,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> TaskCancelResponse:
    """Revoke an active Celery execution and mark record as cancelled."""
    try:
        record = await TaskService.cancel_task(db, task_id)
        return TaskCancelResponse(
            task_id=record.id,
            celery_task_id=record.celery_task_id,
            status=record.status,
            message="Task was successfully cancelled and revoked from queue.",
        )
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TaskCancellationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.delete(
    "/expired",
    summary="Purge expired task records according to TTL",
)
async def purge_expired_tasks(
    db: Annotated[AsyncSession, Depends(get_db)],
) -> dict[str, int | str]:
    """Remove task execution records whose expiration timestamp has elapsed."""
    deleted_count = await TaskService.cleanup_expired_tasks(db)
    return {
        "status": "success",
        "deleted_count": deleted_count,
    }
