"""Integration tests for status querying, results, and metrics endpoints."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.models.task_record import TaskRecord, TaskStatus


@pytest.mark.asyncio
async def test_health_check_endpoint(client: AsyncClient) -> None:
    """Verify GET /health returns operational readiness."""
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_list_tasks_endpoint(client: AsyncClient, db_session: AsyncSession) -> None:
    """Verify GET /api/v1/tasks returns paginated items."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.list_test",
        task_type="email",
        status=TaskStatus.SUCCESS.value,
        priority="low",
        priority_num=0,
        args=[],
        kwargs={},
    )
    db_session.add(record)
    await db_session.commit()

    response = await client.get("/api/v1/tasks?page=1&limit=10")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "total" in data
    assert data["page"] == 1


@pytest.mark.asyncio
async def test_get_task_details_endpoint(client: AsyncClient, db_session: AsyncSession) -> None:
    """Verify GET /api/v1/tasks/{task_id} returns single task."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.detail_test",
        task_type="report",
        status=TaskStatus.RUNNING.value,
        priority="high",
        priority_num=6,
        args=[],
        kwargs={"report_type": "sales"},
    )
    db_session.add(record)
    await db_session.commit()

    response = await client.get(f"/api/v1/tasks/{record.id}")
    assert response.status_code == 200
    assert response.json()["id"] == record.id
    assert response.json()["status"] == "running"

    # Missing task
    missing_res = await client.get("/api/v1/tasks/missing-uuid")
    assert missing_res.status_code == 404


@pytest.mark.asyncio
async def test_get_task_result_endpoint(client: AsyncClient, db_session: AsyncSession) -> None:
    """Verify GET /api/v1/tasks/{task_id}/result returns outcome."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.result_test",
        task_type="data_processing",
        status=TaskStatus.SUCCESS.value,
        priority="medium",
        priority_num=3,
        args=[],
        kwargs={},
        result={"summary": "done", "records": 42},
        duration_seconds=0.75,
    )
    db_session.add(record)
    await db_session.commit()

    response = await client.get(f"/api/v1/tasks/{record.id}/result")
    assert response.status_code == 200
    assert response.json()["result"] == {"summary": "done", "records": 42}
    assert response.json()["duration_seconds"] == 0.75

    missing_res = await client.get("/api/v1/tasks/missing-id/result")
    assert missing_res.status_code == 404


@pytest.mark.asyncio
async def test_get_metrics_endpoint(client: AsyncClient) -> None:
    """Verify GET /api/v1/metrics returns dashboard metrics structure."""
    response = await client.get("/api/v1/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "total_tasks" in data
    assert "status_counts" in data
    assert "throughput_last_hour" in data
    assert "avg_duration_seconds" in data


@pytest.mark.asyncio
async def test_get_workers_health_endpoint(client: AsyncClient) -> None:
    """Verify GET /api/v1/workers/health returns workers overview."""
    response = await client.get("/api/v1/workers/health")
    assert response.status_code == 200
    data = response.json()
    assert "workers_count" in data
    assert "status" in data
    assert isinstance(data["workers"], list)
