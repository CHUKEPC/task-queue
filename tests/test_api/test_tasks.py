"""Integration tests for task submission and mutation endpoints."""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.models.task_record import TaskRecord, TaskStatus


@pytest.mark.asyncio
async def test_submit_email_endpoint(client: AsyncClient) -> None:
    """Verify POST /api/v1/tasks/email creates task and returns 202."""
    payload = {
        "recipient": "dev@example.com",
        "subject": "System Alert",
        "body": "Server CPU load is high",
        "priority": "critical",
        "delay_seconds": 0.0,
    }
    response = await client.post("/api/v1/tasks/email", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["task_type"] == "email"
    assert data["priority"] == "critical"
    assert data["celery_task_id"] is not None


@pytest.mark.asyncio
async def test_submit_report_endpoint(client: AsyncClient) -> None:
    """Verify POST /api/v1/tasks/report returns 202."""
    payload = {
        "report_type": "sales",
        "format": "json",
        "priority": "medium",
        "delay_seconds": 0.0,
    }
    response = await client.post("/api/v1/tasks/report", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["task_type"] == "report"
    assert data["priority"] == "medium"


@pytest.mark.asyncio
async def test_submit_data_processing_endpoint(client: AsyncClient) -> None:
    """Verify POST /api/v1/tasks/data-processing returns 202."""
    payload = {
        "dataset_name": "api_test_data",
        "records": [{"val": 10}, {"val": 20}],
        "operations": ["clean", "aggregate"],
        "priority": "high",
        "delay_seconds": 0.0,
    }
    response = await client.post("/api/v1/tasks/data-processing", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["task_type"] == "data_processing"


@pytest.mark.asyncio
async def test_submit_chain_endpoint(client: AsyncClient) -> None:
    """Verify POST /api/v1/tasks/chain dispatches workflow chain."""
    payload = {
        "steps": [
            {
                "task_type": "data_processing",
                "params": {"dataset_name": "ch", "records": [{"a": 1}], "delay_seconds": 0.0},
            },
            {
                "task_type": "report",
                "params": {"report_type": "sales", "delay_seconds": 0.0},
            },
        ],
        "priority": "high",
    }
    response = await client.post("/api/v1/tasks/chain", json=payload)
    assert response.status_code == 202
    assert response.json()["task_type"] == "chain"


@pytest.mark.asyncio
async def test_submit_group_endpoint(client: AsyncClient) -> None:
    """Verify POST /api/v1/tasks/group dispatches concurrent group."""
    payload = {
        "tasks": [
            {
                "task_type": "email",
                "params": {
                    "recipient": "a@ex.com",
                    "subject": "A",
                    "body": "1",
                    "delay_seconds": 0.0,
                },
            },
            {
                "task_type": "email",
                "params": {
                    "recipient": "b@ex.com",
                    "subject": "B",
                    "body": "2",
                    "delay_seconds": 0.0,
                },
            },
        ],
        "priority": "medium",
    }
    response = await client.post("/api/v1/tasks/group", json=payload)
    assert response.status_code == 202
    assert response.json()["task_type"] == "group"


@pytest.mark.asyncio
async def test_cancel_endpoint(client: AsyncClient, db_session: AsyncSession) -> None:
    """Verify POST /api/v1/tasks/{task_id}/cancel cancels pending task."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.test_cancel",
        task_type="test",
        status=TaskStatus.PENDING.value,
        priority="low",
        priority_num=0,
        args=[],
        kwargs={},
    )
    db_session.add(record)
    await db_session.commit()

    response = await client.post(f"/api/v1/tasks/{record.id}/cancel")
    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"

    # Cancelling again returns 400 Bad Request
    repeat_res = await client.post(f"/api/v1/tasks/{record.id}/cancel")
    assert repeat_res.status_code == 400


@pytest.mark.asyncio
async def test_retry_endpoint(client: AsyncClient, db_session: AsyncSession) -> None:
    """Verify POST /api/v1/tasks/{task_id}/retry re-queues failed task."""
    record = TaskRecord(
        celery_task_id=str(uuid.uuid4()),
        task_name="tasks.send_email",
        task_type="email",
        status=TaskStatus.FAILED.value,
        priority="medium",
        priority_num=3,
        args=[],
        kwargs={
            "recipient": "retry@test.com",
            "subject": "Retry",
            "body": "msg",
            "delay_seconds": 0.0,
        },
    )
    db_session.add(record)
    await db_session.commit()

    response = await client.post(f"/api/v1/tasks/{record.id}/retry")
    assert response.status_code == 200
    assert response.json()["message"] == "Task has been successfully re-queued for execution."


@pytest.mark.asyncio
async def test_purge_expired_endpoint(client: AsyncClient) -> None:
    """Verify DELETE /api/v1/tasks/expired returns deleted count."""
    response = await client.delete("/api/v1/tasks/expired")
    assert response.status_code == 200
    assert "deleted_count" in response.json()


@pytest.mark.asyncio
async def test_endpoint_not_found_handling(client: AsyncClient) -> None:
    """Verify 404 response on non-existent task actions."""
    res_cancel = await client.post("/api/v1/tasks/missing-uuid-1234/cancel")
    assert res_cancel.status_code == 404

    res_retry = await client.post("/api/v1/tasks/missing-uuid-1234/retry")
    assert res_retry.status_code == 404
