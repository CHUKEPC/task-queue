"""Unit tests for analytical report generation task."""

from task_queue.tasks.report import generate_report_task


def test_generate_sales_report() -> None:
    """Verify sales report generation includes revenue metrics."""
    result = generate_report_task.apply(
        kwargs={
            "report_type": "sales",
            "date_from": "2026-01-01",
            "date_to": "2026-01-31",
            "filters": {"region": "EU"},
            "format": "json",
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["report_type"] == "sales"
    assert result["format"] == "json"
    assert result["date_from"] == "2026-01-01"
    assert result["summary"]["total_orders"] == 1420
    assert result["summary"]["gross_revenue"] == 128500.75
    assert result["filters_applied"] == {"region": "EU"}
    assert "download_url" in result


def test_generate_user_activity_report() -> None:
    """Verify user activity report calculates signups and active users."""
    result = generate_report_task.apply(
        kwargs={
            "report_type": "user_activity",
            "format": "csv",
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["report_type"] == "user_activity"
    assert result["format"] == "csv"
    assert result["summary"]["active_users"] == 5890
    assert result["summary"]["retention_rate_pct"] == 74.2


def test_generate_financial_report() -> None:
    """Verify financial report returns EBITDA and operating cost."""
    result = generate_report_task.apply(
        kwargs={
            "report_type": "financial",
            "format": "pdf",
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["report_type"] == "financial"
    assert result["summary"]["operating_cost"] == 42000.00
    assert result["summary"]["net_margin_pct"] == 28.5


def test_generate_custom_report() -> None:
    """Verify fallback behavior for arbitrary report types."""
    result = generate_report_task.apply(
        kwargs={
            "report_type": "inventory",
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["report_type"] == "inventory"
    assert result["summary"]["items_processed"] == 500
    assert result["summary"]["status"] == "completed"
