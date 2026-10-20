"""Unit tests for BaseTask lifecycle hooks and DB sync updates."""

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from task_queue.models.task_record import TaskRecord, TaskStatus
from task_queue.tasks.base import update_task_record_sync


def test_update_task_record_sync(sync_db_session: Session) -> None:
    """Verify that update_task_record_sync correctly updates database fields."""
    celery_id = str(uuid.uuid4())
    record = TaskRecord(
        celery_task_id=celery_id,
        task_name="tasks.test",
        task_type="test",
        status=TaskStatus.PENDING.value,
        priority="medium",
        priority_num=3,
        args=[],
        kwargs={},
    )
    sync_db_session.add(record)
    sync_db_session.commit()

    # 1. Update to RUNNING
    update_task_record_sync(
        celery_task_id=celery_id,
        status=TaskStatus.RUNNING,
        started=True,
    )

    sync_db_session.expire_all()
    updated = sync_db_session.execute(
        select(TaskRecord).where(TaskRecord.celery_task_id == celery_id)
    ).scalar_one()

    assert updated.status == TaskStatus.RUNNING.value
    assert updated.started_at is not None

    # 2. Update to SUCCESS
    update_task_record_sync(
        celery_task_id=celery_id,
        status=TaskStatus.SUCCESS,
        result={"status": "ok"},
        completed=True,
    )

    sync_db_session.expire_all()
    completed = sync_db_session.execute(
        select(TaskRecord).where(TaskRecord.celery_task_id == celery_id)
    ).scalar_one()

    assert completed.status == TaskStatus.SUCCESS.value
    assert completed.result == {"status": "ok"}
    assert completed.completed_at is not None
    assert completed.duration_seconds is not None


def test_update_task_record_sync_nonexistent() -> None:
    """Verify that nonexistent celery_id update completes gracefully without error."""
    # Should not raise exception
    update_task_record_sync(
        celery_task_id="nonexistent-id",
        status=TaskStatus.RUNNING,
    )


def test_update_task_record_sync_retry_and_failure(sync_db_session: Session) -> None:
    """Verify updating record with retry state and failure messages."""
    celery_id = str(uuid.uuid4())
    record = TaskRecord(
        celery_task_id=celery_id,
        task_name="tasks.fail_test",
        task_type="test",
        status=TaskStatus.RUNNING.value,
        priority="high",
        priority_num=6,
        args=[],
        kwargs={},
        started_at=datetime.now(UTC),
    )
    sync_db_session.add(record)
    sync_db_session.commit()

    # Test retry update
    update_task_record_sync(
        celery_task_id=celery_id,
        status=TaskStatus.RETRYING,
        error_message="Temporary network error",
        retry_count=1,
    )

    sync_db_session.expire_all()
    retried = sync_db_session.execute(
        select(TaskRecord).where(TaskRecord.celery_task_id == celery_id)
    ).scalar_one()
    assert retried.status == TaskStatus.RETRYING.value
    assert retried.retry_count == 1
    assert retried.error_message == "Temporary network error"

    # Test final failure update
    update_task_record_sync(
        celery_task_id=celery_id,
        status=TaskStatus.FAILED,
        error_message="Max retries exceeded",
        completed=True,
    )

    sync_db_session.expire_all()
    failed = sync_db_session.execute(
        select(TaskRecord).where(TaskRecord.celery_task_id == celery_id)
    ).scalar_one()
    assert failed.status == TaskStatus.FAILED.value
    assert failed.error_message == "Max retries exceeded"
    assert failed.completed_at is not None
