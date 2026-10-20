"""Unit tests for data processing and workflow aggregation tasks."""

from task_queue.tasks.data_processing import aggregate_chain_task, process_data_task


def test_process_data_cleaning_and_deduplication() -> None:
    """Verify that records are stripped, deduplicated, and summarized."""
    raw_records = [
        {"id": 1, "name": " Alice ", "amount": 100},
        {"id": 2, "name": "Bob", "amount": 200, "note": None},
        {"id": 1, "name": " Alice ", "amount": 100},  # duplicate
    ]

    result = process_data_task.apply(
        kwargs={
            "dataset_name": "users_test",
            "records": raw_records,
            "operations": ["clean", "deduplicate", "aggregate", "summarize"],
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["dataset_name"] == "users_test"
    assert result["input_count"] == 3
    assert result["output_count"] == 2
    assert result["duplicates_removed"] == 1
    assert result["cleaned_count"] == 3
    assert result["aggregations"]["amount_sum"] == 300.0
    assert result["aggregations"]["amount_avg"] == 150.0
    assert len(result["checksum"]) == 16


def test_process_data_empty_records() -> None:
    """Verify handling when empty record list is provided."""
    result = process_data_task.apply(
        kwargs={
            "dataset_name": "empty_set",
            "records": [],
            "operations": ["clean", "aggregate"],
            "delay_seconds": 0.0,
        }
    ).get()

    assert result["input_count"] == 0
    assert result["output_count"] == 0
    assert result["aggregations"] == {}


def test_aggregate_chain_task() -> None:
    """Verify canvas chain/group outcome aggregation."""
    step_results = [
        {"step": 1, "data": "output_one"},
        {"step": 2, "data": "output_two"},
    ]

    summary = aggregate_chain_task.apply(args=[step_results]).get()

    assert summary["step_count"] == 2
    assert len(summary["steps_summary"]) == 2
    assert summary["steps_summary"][0]["status"] == "success"
    assert "completed_at" in summary
