"""Base Celery task providing retry policies, error handling, and lifecycle hooks."""

import logging
from datetime import UTC, datetime
from typing import Any

from celery import Task
from sqlalchemy import select

from task_queue.api.websocket import publish_task_event_sync
from task_queue.db.session import get_sync_db
from task_queue.models.task_record import TaskRecord, TaskStatus

logger = logging.getLogger(__name__)


def update_task_record_sync(
    celery_task_id: str,
    status: TaskStatus,
    result: Any | None = None,
    error_message: str | None = None,
    retry_count: int | None = None,
    started: bool = False,
    completed: bool = False,
) -> None:
    """Update task record lifecycle fields in the persistent database synchronously.

    Args:
        celery_task_id: Celery execution ID.
        status: Target task status.
        result: Optional result dictionary to record.
        error_message: Optional error message string.
        retry_count: Optional count of retries attempted.
        started: Flag indicating execution start timestamp should be set.
        completed: Flag indicating completion timestamp and duration should be set.
    """
    now = datetime.now(UTC)
    try:
        with get_sync_db() as db:
            query = select(TaskRecord).where(TaskRecord.celery_task_id == celery_task_id)
            record = db.execute(query).scalar_one_or_none()
            if not record:
                logger.debug(
                    "TaskRecord for Celery ID %s not found in DB during sync update",
                    celery_task_id,
                )
                return

            record.status = status.value
            if started and not record.started_at:
                record.started_at = now

            if completed:
                record.completed_at = now
                if record.started_at:
                    record.duration_seconds = max(0.0, (now - record.started_at).total_seconds())

            if result is not None:
                record.result = result

            if error_message is not None:
                record.error_message = error_message

            if retry_count is not None:
                record.retry_count = retry_count

            db.commit()

            # Broadcast update via Redis/WebSocket
            publish_task_event_sync(
                event_name=f"task_{status.value}",
                task_id=record.id,
                status=record.status,
                data={
                    "celery_task_id": celery_task_id,
                    "task_name": record.task_name,
                    "result": record.result,
                    "error_message": record.error_message,
                    "retry_count": record.retry_count,
                    "duration_seconds": record.duration_seconds,
                },
            )
    except Exception as exc:
        logger.warning(
            "Failed to update task record for %s in DB: %s",
            celery_task_id,
            exc,
        )


class BaseTask(Task):  # type: ignore[misc]
    """Base Celery task configuring automatic retry policies and DB lifecycle hooks."""

    abstract = True
    autoretry_for = (Exception,)
    max_retries = 3
    retry_backoff = True
    retry_backoff_max = 600
    retry_jitter = True

    def before_start(self, task_id: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        """Invoked immediately before the worker begins executing the task body."""
        super().before_start(task_id, args, kwargs)
        update_task_record_sync(
            celery_task_id=task_id,
            status=TaskStatus.RUNNING,
            started=True,
        )

    def on_success(
        self,
        retval: Any,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        """Invoked when the task finishes execution successfully."""
        super().on_success(retval, task_id, args, kwargs)
        update_task_record_sync(
            celery_task_id=task_id,
            status=TaskStatus.SUCCESS,
            result=retval,
            completed=True,
        )

    def on_failure(
        self,
        exc: Exception,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: Any,
    ) -> None:
        """Invoked after retries are exhausted and task officially fails."""
        super().on_failure(exc, task_id, args, kwargs, einfo)
        update_task_record_sync(
            celery_task_id=task_id,
            status=TaskStatus.FAILED,
            error_message=str(exc),
            completed=True,
        )

    def on_retry(
        self,
        exc: Exception,
        task_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        einfo: Any,
    ) -> None:
        """Invoked whenever the task is rescheduled for retry."""
        super().on_retry(exc, task_id, args, kwargs, einfo)
        update_task_record_sync(
            celery_task_id=task_id,
            status=TaskStatus.RETRYING,
            error_message=str(exc),
            retry_count=self.request.retries + 1,
        )
