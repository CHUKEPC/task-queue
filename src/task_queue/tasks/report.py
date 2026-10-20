"""Analytical report generation background task."""

import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from task_queue.celery_app import celery_app
from task_queue.tasks.base import BaseTask

logger = logging.getLogger(__name__)


@celery_app.task(base=BaseTask, bind=True, name="tasks.generate_report")
def generate_report_task(
    self: Any,
    report_type: str,
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict[str, Any] | None = None,
    format: str = "json",
    delay_seconds: float = 0.2,
) -> dict[str, Any]:
    """Generate business intelligence or analytical report asynchronously.

    Args:
        self: Bound Celery task context.
        report_type: Category of report (sales, user_activity, financial, etc.).
        date_from: Optional starting date string (YYYY-MM-DD).
        date_to: Optional ending date string (YYYY-MM-DD).
        filters: Custom filter dictionary.
        format: Export file format (json, csv, pdf).
        delay_seconds: Simulated computation latency.

    Returns:
        dict[str, Any]: Summary metrics and report metadata.
    """
    logger.info("Generating %s report (task_id=%s)", report_type, self.request.id)

    if delay_seconds > 0:
        time.sleep(min(delay_seconds, 5.0))

    report_id = f"rep_{uuid.uuid4().hex[:12]}"

    # Simulate metric aggregation based on report type
    metrics: dict[str, Any] = {}
    if report_type == "sales":
        metrics = {
            "total_orders": 1420,
            "gross_revenue": 128500.75,
            "average_order_value": 90.49,
            "currency": "USD",
        }
    elif report_type == "user_activity":
        metrics = {
            "active_users": 5890,
            "new_signups": 340,
            "retention_rate_pct": 74.2,
        }
    elif report_type == "financial":
        metrics = {
            "operating_cost": 42000.00,
            "net_margin_pct": 28.5,
            "ebitda": 36625.21,
        }
    else:
        metrics = {
            "items_processed": 500,
            "status": "completed",
            "score": 98.4,
        }

    return {
        "report_id": report_id,
        "report_type": report_type,
        "format": format,
        "date_from": date_from,
        "date_to": date_to,
        "filters_applied": filters or {},
        "summary": metrics,
        "generated_at": datetime.now(UTC).isoformat(),
        "download_url": f"/api/v1/reports/{report_id}/download.{format}",
    }
