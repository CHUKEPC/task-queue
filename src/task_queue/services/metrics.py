"""Metrics calculation and worker health monitoring service."""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from task_queue.celery_app import celery_app
from task_queue.models.task_record import TaskRecord, TaskStatus
from task_queue.schemas.task import TaskMetricsResponse, WorkerHealthResponse, WorkerStat

logger = logging.getLogger(__name__)


class MetricsService:
    """Service responsible for aggregating execution statistics and inspecting worker nodes."""

    @staticmethod
    async def get_task_metrics(db: AsyncSession) -> TaskMetricsResponse:
        """Calculate system-wide throughput, failure rates, and status distributions.

        Args:
            db: Database session.

        Returns:
            TaskMetricsResponse: Computed system analytics.
        """
        # Total tasks
        total_tasks = await db.scalar(select(func.count(TaskRecord.id))) or 0

        # Status distribution
        status_stmt = select(TaskRecord.status, func.count(TaskRecord.id)).group_by(
            TaskRecord.status
        )
        status_rows = (await db.execute(status_stmt)).all()
        status_counts = {
            TaskStatus.PENDING.value: 0,
            TaskStatus.RUNNING.value: 0,
            TaskStatus.SUCCESS.value: 0,
            TaskStatus.FAILED.value: 0,
            TaskStatus.RETRYING.value: 0,
            TaskStatus.CANCELLED.value: 0,
        }
        for status, count in status_rows:
            status_counts[status] = count

        # Type distribution
        type_stmt = select(TaskRecord.task_type, func.count(TaskRecord.id)).group_by(
            TaskRecord.task_type
        )
        type_rows = (await db.execute(type_stmt)).all()
        type_counts = {row[0]: row[1] for row in type_rows}

        # Priority distribution
        priority_stmt = select(TaskRecord.priority, func.count(TaskRecord.id)).group_by(
            TaskRecord.priority
        )
        priority_rows = (await db.execute(priority_stmt)).all()
        priority_counts = {row[0]: row[1] for row in priority_rows}

        # Throughput in the last 1 hour
        one_hour_ago = datetime.now(UTC) - timedelta(hours=1)
        throughput_stmt = select(func.count(TaskRecord.id)).where(
            TaskRecord.created_at >= one_hour_ago
        )
        throughput_last_hour = await db.scalar(throughput_stmt) or 0

        # Average duration of successful tasks
        avg_duration_stmt = select(func.avg(TaskRecord.duration_seconds)).where(
            TaskRecord.status == TaskStatus.SUCCESS.value,
            TaskRecord.duration_seconds.is_not(None),
        )
        avg_duration_raw = await db.scalar(avg_duration_stmt)
        avg_duration_seconds = round(float(avg_duration_raw), 2) if avg_duration_raw else 0.0

        # Failure rate
        failed_tasks = status_counts.get(TaskStatus.FAILED.value, 0)
        failure_rate = round((failed_tasks / total_tasks * 100), 2) if total_tasks > 0 else 0.0

        active_tasks = status_counts.get(TaskStatus.PENDING.value, 0) + status_counts.get(
            TaskStatus.RUNNING.value, 0
        )

        return TaskMetricsResponse(
            total_tasks=total_tasks,
            status_counts=status_counts,
            type_counts=type_counts,
            priority_counts=priority_counts,
            throughput_last_hour=throughput_last_hour,
            avg_duration_seconds=avg_duration_seconds,
            failure_rate_percentage=failure_rate,
            active_tasks=active_tasks,
        )

    @staticmethod
    def get_worker_health() -> WorkerHealthResponse:
        """Inspect Celery cluster to detect active workers, load, and registered tasks.

        Returns:
            WorkerHealthResponse: Summary of cluster availability.
        """
        try:
            inspector = celery_app.control.inspect(timeout=1.0)
            if not inspector:
                return WorkerHealthResponse(
                    workers_count=0,
                    status="offline",
                    workers=[],
                )

            pings: dict[str, Any] | None = inspector.ping()
            if not pings:
                return WorkerHealthResponse(
                    workers_count=0,
                    status="offline",
                    workers=[],
                )

            raw_active = inspector.active()
            active_dict: dict[str, list[Any]] = raw_active if isinstance(raw_active, dict) else {}
            raw_registered = inspector.registered()
            registered_dict: dict[str, list[str]] = (
                raw_registered if isinstance(raw_registered, dict) else {}
            )

            worker_stats: list[WorkerStat] = []
            for worker_name in pings:
                active_tasks = len(active_dict.get(worker_name, []))
                registered_tasks = registered_dict.get(worker_name, [])

                worker_stats.append(
                    WorkerStat(
                        worker_name=worker_name,
                        status="online",
                        active_tasks_count=active_tasks,
                        registered_tasks=registered_tasks,
                    )
                )

            return WorkerHealthResponse(
                workers_count=len(worker_stats),
                status="healthy" if worker_stats else "idle",
                workers=worker_stats,
            )
        except Exception as exc:
            logger.warning("Worker health inspection encountered an error: %s", exc)
            return WorkerHealthResponse(
                workers_count=0,
                status="unreachable",
                workers=[],
            )
