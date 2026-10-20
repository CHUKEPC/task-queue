"""Unit tests for the TaskService business logic layer."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.exceptions import (
    InvalidTaskPriorityError,
    TaskCancellationError,
    TaskNotFoundError,
    TaskRetryError,
)
from task_queue.models.task_record import (
    TaskPriority,
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
from task_queue.services.task_service import TaskService


@pytest.mark.asyncio
async def test_submit_email_task(db_session: AsyncSession) -> None:
    """Verify submitting email task persists DB entity with correct fields."""
    request = EmailTaskRequest(
        recipient="test@example.com",
        subject="Invoice #1234",
        body="Your invoice is ready.",
        priority=TaskPriority.HIGH,
        delay_seconds=0.0,
    )

    record = await TaskService.submit_email_task(db_session, request)

    assert record.id is not None
    assert record.celery_task_id is not None
    assert record.task_type == TaskType.EMAIL.value
    assert record.priority == "high"
    assert record.priority_num == 6
    assert record.status in [TaskStatus.PENDING.value, TaskStatus.SUCCESS.value]
    assert record.expires_at is not None


@pytest.mark.asyncio
async def test_submit_report_task(db_session: AsyncSession) -> None:
    """Verify submitting report task persists analytical parameters."""
    request = ReportTaskRequest(
        report_type="sales",
        format="csv",
        priority=TaskPriority.LOW,
        delay_seconds=0.0,
    )

    record = await TaskService.submit_report_task(db_session, request)

    assert record.task_type == TaskType.REPORT.value
    assert record.priority == "low"
    assert record.priority_num == 0
    assert record.kwargs["report_type"] == "sales"


@pytest.mark.asyncio
async def test_submit_data_processing_task(db_session: AsyncSession) -> None:
    """Verify submitting data processing task persists records metadata."""
    request = DataProcessingTaskRequest(
        dataset_name="batch_01",
        records=[{"val": 10}, {"val": 20}],
        priority=TaskPriority.CRITICAL,
        delay_seconds=0.0,
    )

    record = await TaskService.submit_data_processing_task(db_session, request)

    assert record.task_type == TaskType.DATA_PROCESSING.value
    assert record.priority == "critical"
    assert record.priority_num == 9


@pytest.mark.asyncio
async def test_submit_chain_and_group(db_session: AsyncSession) -> None:
    """Verify submitting canvas workflow chains and groups."""
    chain_req = TaskChainRequest(
        steps=[
            TaskChainStep(
                task_type=TaskType.DATA_PROCESSING,
                params={"dataset_name": "chain_data", "records": [{"x": 1}], "delay_seconds": 0.0},
            ),
            TaskChainStep(
                task_type=TaskType.REPORT,
                params={"report_type": "sales", "delay_seconds": 0.0},
            ),
        ],
        priority=TaskPriority.MEDIUM,
    )

    chain_record = await TaskService.submit_chain(db_session, chain_req)
    assert chain_record.task_type == TaskType.CHAIN.value

    group_req = TaskGroupRequest(
        tasks=[
            TaskChainStep(
                task_type=TaskType.EMAIL,
                params={
                    "recipient": "user1@example.com",
                    "subject": "Hi",
                    "body": "1",
                    "delay_seconds": 0.0,
                },
            ),
            TaskChainStep(
                task_type=TaskType.EMAIL,
                params={
                    "recipient": "user2@example.com",
                    "subject": "Hi",
                    "body": "2",
                    "delay_seconds": 0.0,
                },
            ),
        ],
        priority=TaskPriority.MEDIUM,
    )

    group_record = await TaskService.submit_group(db_session, group_req)
    assert group_record.task_type == TaskType.GROUP.value


@pytest.mark.asyncio
async def test_get_task_not_found(db_session: AsyncSession) -> None:
    """Verify TaskNotFoundError is raised when querying missing ID."""
    with pytest.raises(TaskNotFoundError):
        await TaskService.get_task(db_session, "nonexistent-id")


@pytest.mark.asyncio
async def test_list_tasks_and_filtering(db_session: AsyncSession) -> None:
    """Verify task listing and filtering by status and category."""
    r1 = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.email",
        task_type=TaskType.EMAIL.value,
        status=TaskStatus.PENDING.value,
        priority="low",
        priority_num=0,
        args=[],
        kwargs={},
    )
    r2 = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.report",
        task_type=TaskType.REPORT.value,
        status=TaskStatus.SUCCESS.value,
        priority="high",
        priority_num=6,
        args=[],
        kwargs={},
    )
    db_session.add_all([r1, r2])
    await db_session.commit()

    all_tasks, total = await TaskService.list_tasks(db_session)
    assert total >= 2
    assert len(all_tasks) >= 2

    pending_tasks, pending_total = await TaskService.list_tasks(
        db_session, status=TaskStatus.PENDING.value
    )
    assert pending_total >= 1
    assert all(t.status == TaskStatus.PENDING.value for t in pending_tasks)

    email_tasks, email_total = await TaskService.list_tasks(
        db_session, task_type=TaskType.EMAIL.value
    )
    assert email_total >= 1
    assert all(t.task_type == TaskType.EMAIL.value for t in email_tasks)


@pytest.mark.asyncio
async def test_cancel_task(db_session: AsyncSession) -> None:
    """Verify cancelling task sets CANCELLED status."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.long_running",
        task_type="test",
        status=TaskStatus.PENDING.value,
        priority="medium",
        priority_num=3,
        args=[],
        kwargs={},
    )
    db_session.add(record)
    await db_session.commit()

    cancelled = await TaskService.cancel_task(db_session, record.id)
    assert cancelled.status == TaskStatus.CANCELLED.value
    assert cancelled.completed_at is not None

    # Cancelling already cancelled task raises TaskCancellationError
    with pytest.raises(TaskCancellationError):
        await TaskService.cancel_task(db_session, record.id)


@pytest.mark.asyncio
async def test_retry_task(db_session: AsyncSession) -> None:
    """Verify retrying a failed task resets status and increments retry count."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.send_email",
        task_type=TaskType.EMAIL.value,
        status=TaskStatus.FAILED.value,
        priority="medium",
        priority_num=3,
        args=[],
        kwargs={
            "recipient": "retry@example.com",
            "subject": "Retry",
            "body": "Content",
            "delay_seconds": 0.0,
        },
        retry_count=1,
    )
    db_session.add(record)
    await db_session.commit()

    retried = await TaskService.retry_task(db_session, record.id)
    assert retried.retry_count == 2

    # Retrying a pending task should raise TaskRetryError
    with pytest.raises(TaskRetryError):
        await TaskService.retry_task(db_session, record.id)


@pytest.mark.asyncio
async def test_cleanup_expired_tasks(db_session: AsyncSession) -> None:
    """Verify expired records past TTL are purged."""
    expired_record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.old",
        task_type="test",
        status=TaskStatus.SUCCESS.value,
        priority="low",
        priority_num=0,
        args=[],
        kwargs={},
        expires_at=datetime.now(UTC) - timedelta(days=2),
    )
    active_record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.new",
        task_type="test",
        status=TaskStatus.SUCCESS.value,
        priority="low",
        priority_num=0,
        args=[],
        kwargs={},
        expires_at=datetime.now(UTC) + timedelta(days=2),
    )
    db_session.add_all([expired_record, active_record])
    await db_session.commit()

    deleted = await TaskService.cleanup_expired_tasks(db_session)
    assert deleted >= 1


def test_resolve_invalid_priority() -> None:
    """Verify InvalidTaskPriorityError is raised for unsupported priority level."""
    with pytest.raises(InvalidTaskPriorityError):
        TaskService._resolve_priority_num("ultra-mega")
