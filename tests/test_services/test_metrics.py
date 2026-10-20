"""Unit tests for MetricsService and worker health monitoring."""

import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.models.task_record import TaskRecord, TaskStatus
from task_queue.services.metrics import MetricsService


@pytest.mark.asyncio
async def test_get_task_metrics(db_session: AsyncSession) -> None:
    """Verify task metrics computation matches DB statistics."""
    # Seed records: 1 success with duration, 1 failed
    r1 = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.success_task",
        task_type="email",
        status=TaskStatus.SUCCESS.value,
        priority="high",
        priority_num=6,
        args=[],
        kwargs={},
        duration_seconds=1.5,
    )
    r2 = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.fail_task",
        task_type="report",
        status=TaskStatus.FAILED.value,
        priority="low",
        priority_num=0,
        args=[],
        kwargs={},
    )
    db_session.add_all([r1, r2])
    await db_session.commit()

    metrics = await MetricsService.get_task_metrics(db_session)

    assert metrics.total_tasks >= 2
    assert metrics.status_counts[TaskStatus.SUCCESS.value] >= 1
    assert metrics.status_counts[TaskStatus.FAILED.value] >= 1
    assert metrics.failure_rate_percentage > 0.0
    assert metrics.avg_duration_seconds > 0.0
    assert "email" in metrics.type_counts
    assert "report" in metrics.type_counts


def test_get_worker_health_offline() -> None:
    """Verify offline response when inspector returns None or empty."""
    with patch("task_queue.services.metrics.celery_app.control.inspect") as mock_inspect:
        mock_inspect.return_value = None
        health = MetricsService.get_worker_health()
        assert health.status == "offline"
        assert health.workers_count == 0


def test_get_worker_health_healthy() -> None:
    """Verify healthy status when workers respond to ping."""
    with patch("task_queue.services.metrics.celery_app.control.inspect") as mock_inspect:
        mock_instance = MagicMock()
        mock_instance.ping.return_value = {"celery@worker-1": {"ok": "pong"}}
        mock_instance.active.return_value = {"celery@worker-1": [{"id": "task-1"}]}
        mock_instance.registered.return_value = {
            "celery@worker-1": ["tasks.email", "tasks.report"]
        }
        mock_inspect.return_value = mock_instance

        health = MetricsService.get_worker_health()

        assert health.status == "healthy"
        assert health.workers_count == 1
        assert health.workers[0].worker_name == "celery@worker-1"
        assert health.workers[0].active_tasks_count == 1
        assert "tasks.email" in health.workers[0].registered_tasks


def test_get_worker_health_exception_handling() -> None:
    """Verify graceful handling when inspect raises exception."""
    with patch("task_queue.services.metrics.celery_app.control.inspect") as mock_inspect:
        mock_inspect.side_effect = RuntimeError("Broker connection timeout")
        health = MetricsService.get_worker_health()
        assert health.status == "unreachable"
        assert health.workers_count == 0
