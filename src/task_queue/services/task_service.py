"""Domain service coordinating task submissions, cancellations, and query lifecycles."""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from celery import chain, group
from sqlalchemy import CursorResult, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.celery_app import celery_app
from task_queue.config import get_settings
from task_queue.exceptions import (
    InvalidTaskPriorityError,
    TaskCancellationError,
    TaskNotFoundError,
    TaskRetryError,
)
from task_queue.models.task_record import (
    TaskRecord,
    TaskStatus,
    TaskType,
)
from task_queue.schemas.task import (
    DataProcessingTaskRequest,
    EmailTaskRequest,
    ReportTaskRequest,
    TaskChainRequest,
    TaskChainStep,
    TaskGroupRequest,
)
from task_queue.tasks.data_processing import aggregate_chain_task, process_data_task
from task_queue.tasks.email import send_email_task
from task_queue.tasks.report import generate_report_task

logger = logging.getLogger(__name__)


class TaskService:
    """Service layer managing task persistence, Celery dispatch, and status manipulation."""

    @staticmethod
    def _resolve_priority_num(priority: str) -> int:
        """Translate string priority level to numerical queue priority.

        Args:
            priority: Priority string (low, medium, high, critical).

        Returns:
            int: Integer priority level (0-9).

        Raises:
            InvalidTaskPriorityError: If priority value is unrecognized.
        """
        settings = get_settings()
        if priority not in settings.priority_map:
            raise InvalidTaskPriorityError(priority)
        return settings.priority_map[priority]

    @staticmethod
    def _calculate_expiration() -> datetime:
        """Calculate record expiration datetime based on configured TTL hours.

        Returns:
            datetime: Expiration timestamp in UTC.
        """
        settings = get_settings()
        return datetime.now(UTC) + timedelta(hours=settings.default_task_ttl_hours)

    @classmethod
    async def submit_email_task(
        cls,
        db: AsyncSession,
        request: EmailTaskRequest,
    ) -> TaskRecord:
        """Dispatch simulated email delivery task and persist initial execution record.

        Args:
            db: Database session.
            request: Validated email task specification.

        Returns:
            TaskRecord: Persisted task entity.
        """
        priority_str = request.priority.value
        priority_num = cls._resolve_priority_num(priority_str)
        expires_at = cls._calculate_expiration()

        async_result = send_email_task.apply_async(
            kwargs={
                "recipient": request.recipient,
                "subject": request.subject,
                "body": request.body,
                "template_name": request.template_name,
                "simulate_failure": request.simulate_failure,
                "delay_seconds": request.delay_seconds,
            },
            priority=priority_num,
            queue=priority_str,
            countdown=request.countdown_seconds,
            eta=request.scheduled_at,
        )

        record = TaskRecord(
            celery_task_id=async_result.id,
            task_name="tasks.send_email",
            task_type=TaskType.EMAIL.value,
            status=TaskStatus.PENDING.value,
            priority=priority_str,
            priority_num=priority_num,
            args=[],
            kwargs={
                "recipient": request.recipient,
                "subject": request.subject,
                "template_name": request.template_name,
                "simulate_failure": request.simulate_failure,
            },
            scheduled_at=request.scheduled_at,
            expires_at=expires_at,
        )

        db.add(record)
        await db.commit()
        await db.refresh(record)
        logger.info("Created and scheduled email task record %s", record.id)
        return record

    @classmethod
    async def submit_report_task(
        cls,
        db: AsyncSession,
        request: ReportTaskRequest,
    ) -> TaskRecord:
        """Dispatch analytical report task and persist record.

        Args:
            db: Database session.
            request: Validated report task payload.

        Returns:
            TaskRecord: Persisted task entity.
        """
        priority_str = request.priority.value
        priority_num = cls._resolve_priority_num(priority_str)
        expires_at = cls._calculate_expiration()

        date_from_str = request.date_from.isoformat() if request.date_from else None
        date_to_str = request.date_to.isoformat() if request.date_to else None

        async_result = generate_report_task.apply_async(
            kwargs={
                "report_type": request.report_type,
                "date_from": date_from_str,
                "date_to": date_to_str,
                "filters": request.filters,
                "format": request.format,
            },
            priority=priority_num,
            queue=priority_str,
            countdown=request.countdown_seconds,
            eta=request.scheduled_at,
        )

        record = TaskRecord(
            celery_task_id=async_result.id,
            task_name="tasks.generate_report",
            task_type=TaskType.REPORT.value,
            status=TaskStatus.PENDING.value,
            priority=priority_str,
            priority_num=priority_num,
            args=[],
            kwargs={
                "report_type": request.report_type,
                "date_from": date_from_str,
                "date_to": date_to_str,
                "filters": request.filters,
                "format": request.format,
            },
            scheduled_at=request.scheduled_at,
            expires_at=expires_at,
        )

        db.add(record)
        await db.commit()
        await db.refresh(record)
        logger.info("Created and scheduled report task record %s", record.id)
        return record

    @classmethod
    async def submit_data_processing_task(
        cls,
        db: AsyncSession,
        request: DataProcessingTaskRequest,
    ) -> TaskRecord:
        """Dispatch batch data processing task and persist record.

        Args:
            db: Database session.
            request: Validated data processing payload.

        Returns:
            TaskRecord: Persisted task entity.
        """
        priority_str = request.priority.value
        priority_num = cls._resolve_priority_num(priority_str)
        expires_at = cls._calculate_expiration()

        async_result = process_data_task.apply_async(
            kwargs={
                "dataset_name": request.dataset_name,
                "records": request.records,
                "operations": request.operations,
                "chunk_size": request.chunk_size,
            },
            priority=priority_num,
            queue=priority_str,
            countdown=request.countdown_seconds,
            eta=request.scheduled_at,
        )

        record = TaskRecord(
            celery_task_id=async_result.id,
            task_name="tasks.process_data",
            task_type=TaskType.DATA_PROCESSING.value,
            status=TaskStatus.PENDING.value,
            priority=priority_str,
            priority_num=priority_num,
            args=[],
            kwargs={
                "dataset_name": request.dataset_name,
                "operations": request.operations,
                "record_count": len(request.records),
            },
            scheduled_at=request.scheduled_at,
            expires_at=expires_at,
        )

        db.add(record)
        await db.commit()
        await db.refresh(record)
        logger.info("Created data processing task record %s", record.id)
        return record

    @classmethod
    def _create_canvas_signature(cls, step: TaskChainStep) -> Any:
        """Convert a canvas step description to a Celery task signature.

        Args:
            step: Step specification containing task type and parameters.

        Returns:
            Any: Celery task signature.
        """
        if step.task_type == TaskType.EMAIL:
            return send_email_task.s(**step.params)
        elif step.task_type == TaskType.REPORT:
            return generate_report_task.s(**step.params)
        elif step.task_type == TaskType.DATA_PROCESSING:
            return process_data_task.s(**step.params)
        else:
            return aggregate_chain_task.s()

    @classmethod
    async def submit_chain(
        cls,
        db: AsyncSession,
        request: TaskChainRequest,
    ) -> TaskRecord:
        """Execute a sequential Celery chain workflow.

        Args:
            db: Database session.
            request: Ordered chain steps.

        Returns:
            TaskRecord: Persisted parent task record for the chain.
        """
        priority_str = request.priority.value
        priority_num = cls._resolve_priority_num(priority_str)
        expires_at = cls._calculate_expiration()

        signatures = [cls._create_canvas_signature(step) for step in request.steps]
        workflow = chain(*signatures)
        async_result = workflow.apply_async(priority=priority_num, queue=priority_str)

        record = TaskRecord(
            celery_task_id=async_result.id,
            task_name="canvas.chain",
            task_type=TaskType.CHAIN.value,
            status=TaskStatus.PENDING.value,
            priority=priority_str,
            priority_num=priority_num,
            args=[],
            kwargs={"steps_count": len(request.steps)},
            expires_at=expires_at,
        )

        db.add(record)
        await db.commit()
        await db.refresh(record)
        logger.info("Dispatched Celery chain workflow %s", record.id)
        return record

    @classmethod
    async def submit_group(
        cls,
        db: AsyncSession,
        request: TaskGroupRequest,
    ) -> TaskRecord:
        """Execute a parallel Celery group workflow.

        Args:
            db: Database session.
            request: Concurrent tasks specification.

        Returns:
            TaskRecord: Persisted parent task record for the group.
        """
        priority_str = request.priority.value
        priority_num = cls._resolve_priority_num(priority_str)
        expires_at = cls._calculate_expiration()

        signatures = [cls._create_canvas_signature(task) for task in request.tasks]
        workflow = group(*signatures)
        async_result = workflow.apply_async(priority=priority_num, queue=priority_str)

        record = TaskRecord(
            celery_task_id=async_result.id,
            task_name="canvas.group",
            task_type=TaskType.GROUP.value,
            status=TaskStatus.PENDING.value,
            priority=priority_str,
            priority_num=priority_num,
            args=[],
            kwargs={"tasks_count": len(request.tasks)},
            expires_at=expires_at,
        )

        db.add(record)
        await db.commit()
        await db.refresh(record)
        logger.info("Dispatched Celery group workflow %s", record.id)
        return record

    @classmethod
    async def get_task(
        cls,
        db: AsyncSession,
        task_id: str,
    ) -> TaskRecord:
        """Retrieve task by internal UUID or Celery task ID.

        Args:
            db: Database session.
            task_id: UUID string or Celery execution ID.

        Returns:
            TaskRecord: Matching entity.

        Raises:
            TaskNotFoundError: If no matching task is found.
        """
        query = select(TaskRecord).where(
            (TaskRecord.id == task_id) | (TaskRecord.celery_task_id == task_id)
        )
        result = await db.execute(query)
        record = result.scalar_one_or_none()
        if not record:
            raise TaskNotFoundError(task_id)
        return record

    @classmethod
    async def list_tasks(
        cls,
        db: AsyncSession,
        status: str | None = None,
        task_type: str | None = None,
        priority: str | None = None,
        page: int = 1,
        limit: int = 20,
    ) -> tuple[list[TaskRecord], int]:
        """Fetch paginated tasks with optional filtering.

        Args:
            db: Database session.
            status: Optional status filter.
            task_type: Optional task type filter.
            priority: Optional priority filter.
            page: 1-indexed page number.
            limit: Page item size.

        Returns:
            tuple[list[TaskRecord], int]: List of records and total count.
        """
        query = select(TaskRecord)
        count_query = select(func.count(TaskRecord.id))

        if status:
            query = query.where(TaskRecord.status == status)
            count_query = count_query.where(TaskRecord.status == status)

        if task_type:
            query = query.where(TaskRecord.task_type == task_type)
            count_query = count_query.where(TaskRecord.task_type == task_type)

        if priority:
            query = query.where(TaskRecord.priority == priority)
            count_query = count_query.where(TaskRecord.priority == priority)

        total = await db.scalar(count_query) or 0

        offset = (page - 1) * limit
        query = query.order_by(TaskRecord.created_at.desc()).offset(offset).limit(limit)

        result = await db.execute(query)
        records = list(result.scalars().all())

        return records, total

    @classmethod
    async def cancel_task(
        cls,
        db: AsyncSession,
        task_id: str,
    ) -> TaskRecord:
        """Cancel an in-flight or pending Celery task and revoke execution.

        Args:
            db: Database session.
            task_id: Internal or Celery task identifier.

        Returns:
            TaskRecord: Updated record in CANCELLED state.

        Raises:
            TaskNotFoundError: If task is missing.
            TaskCancellationError: If task has already finished or failed.
        """
        record = await cls.get_task(db, task_id)

        if record.status in [
            TaskStatus.SUCCESS.value,
            TaskStatus.FAILED.value,
            TaskStatus.CANCELLED.value,
        ]:
            raise TaskCancellationError(task_id, record.status)

        # Revoke task at the Celery worker level (terminate=True interrupts running worker)
        try:
            celery_app.control.revoke(record.celery_task_id, terminate=True, signal="SIGTERM")
        except Exception as exc:
            logger.debug("Failed revoking Celery task execution: %s", exc)

        record.status = TaskStatus.CANCELLED.value
        record.completed_at = datetime.now(UTC)
        record.error_message = "Task execution was cancelled by user request."
        await db.commit()
        await db.refresh(record)

        logger.info("Cancelled task %s (celery_id=%s)", record.id, record.celery_task_id)
        return record

    @classmethod
    async def retry_task(
        cls,
        db: AsyncSession,
        task_id: str,
    ) -> TaskRecord:
        """Manually re-queue a failed or cancelled task.

        Args:
            db: Database session.
            task_id: Internal or Celery task identifier.

        Returns:
            TaskRecord: Re-queued task record.

        Raises:
            TaskNotFoundError: If task does not exist.
            TaskRetryError: If task is already pending or actively running.
        """
        record = await cls.get_task(db, task_id)

        if record.status in [TaskStatus.PENDING.value, TaskStatus.RUNNING.value]:
            raise TaskRetryError(task_id, f"Task is currently in '{record.status}' state.")

        # Re-dispatch Celery task
        try:
            async_result = celery_app.send_task(
                name=record.task_name,
                args=record.args,
                kwargs=record.kwargs,
                priority=record.priority_num,
                queue=record.priority,
            )
            record.celery_task_id = async_result.id
        except Exception as exc:
            logger.debug("Failed dispatching task during retry: %s", exc)
            record.celery_task_id = str(uuid.uuid4())
        record.status = TaskStatus.PENDING.value
        record.retry_count = record.retry_count + 1
        record.error_message = None
        record.started_at = None
        record.completed_at = None
        record.duration_seconds = None

        await db.commit()
        await db.refresh(record)
        logger.info("Manually re-queued task %s as Celery %s", record.id, async_result.id)
        return record

    @classmethod
    async def cleanup_expired_tasks(
        cls,
        db: AsyncSession,
    ) -> int:
        """Purge records whose TTL expiration timestamp has elapsed.

        Args:
            db: Database session.

        Returns:
            int: Number of deleted records.
        """
        now = datetime.now(UTC)
        stmt = delete(TaskRecord).where(
            TaskRecord.expires_at.is_not(None),
            TaskRecord.expires_at <= now,
        )
        result = await db.execute(stmt)
        await db.commit()
        deleted_count = result.rowcount if isinstance(result, CursorResult) else 0
        logger.info("Purged %d expired task records", deleted_count)
        return deleted_count
