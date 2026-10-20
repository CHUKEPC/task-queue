"""Batch data processing, cleaning, and aggregation background tasks."""

import hashlib
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from task_queue.celery_app import celery_app
from task_queue.tasks.base import BaseTask

logger = logging.getLogger(__name__)


@celery_app.task(base=BaseTask, bind=True, name="tasks.process_data")
def process_data_task(
    self: Any,
    dataset_name: str,
    records: list[dict[str, Any]],
    operations: list[str] | None = None,
    chunk_size: int = 50,
    delay_seconds: float = 0.2,
) -> dict[str, Any]:
    """Process, clean, and aggregate a batch of unstructured/structured records.

    Args:
        self: Bound Celery task context.
        dataset_name: Identifier for data lineage tracking.
        records: Collection of raw dictionaries to process.
        operations: List of operations ('clean', 'deduplicate', 'aggregate', 'summarize').
        chunk_size: Processing batch chunk size.
        delay_seconds: Simulated processing latency.

    Returns:
        dict[str, Any]: Processing statistics, checksum, and summary.
    """
    logger.info(
        "Processing dataset '%s' with %d records (task_id=%s)",
        dataset_name,
        len(records),
        self.request.id,
    )

    if delay_seconds > 0:
        time.sleep(min(delay_seconds, 5.0))

    ops = operations or ["clean", "aggregate", "summarize"]
    current_records = [dict(r) for r in records]
    cleaned_count = 0
    duplicates_removed = 0

    # 1. Clean operation: strip strings and discard nulls
    if "clean" in ops:
        cleaned_records = []
        for item in current_records:
            cleaned_item = {
                k: v.strip() if isinstance(v, str) else v for k, v in item.items() if v is not None
            }
            cleaned_records.append(cleaned_item)
            cleaned_count += 1
        current_records = cleaned_records

    # 2. Deduplicate operation: unique JSON representations
    if "deduplicate" in ops:
        unique_hashes = set()
        unique_records = []
        for item in current_records:
            item_hash = hashlib.md5(json.dumps(item, sort_keys=True).encode("utf-8")).hexdigest()
            if item_hash not in unique_hashes:
                unique_hashes.add(item_hash)
                unique_records.append(item)
            else:
                duplicates_removed += 1
        current_records = unique_records

    # 3. Numeric aggregation
    aggregations: dict[str, float] = {}
    if "aggregate" in ops and current_records:
        numeric_sums: dict[str, float] = {}
        numeric_counts: dict[str, int] = {}
        for r in current_records:
            for k, v in r.items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    numeric_sums[k] = numeric_sums.get(k, 0.0) + float(v)
                    numeric_counts[k] = numeric_counts.get(k, 0) + 1
        for k, total in numeric_sums.items():
            aggregations[f"{k}_sum"] = round(total, 2)
            aggregations[f"{k}_avg"] = round(total / numeric_counts[k], 2)

    # Compute checksum of resulting data
    dataset_digest = hashlib.sha256(
        json.dumps(current_records, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]

    return {
        "dataset_name": dataset_name,
        "input_count": len(records),
        "output_count": len(current_records),
        "cleaned_count": cleaned_count,
        "duplicates_removed": duplicates_removed,
        "operations_applied": ops,
        "aggregations": aggregations,
        "checksum": dataset_digest,
        "processed_at": datetime.now(UTC).isoformat(),
    }


@celery_app.task(base=BaseTask, bind=True, name="tasks.aggregate_chain")
def aggregate_chain_task(self: Any, results: list[Any] | Any) -> dict[str, Any]:
    """Aggregate outcomes from multiple preceding tasks in a Celery chord or chain.

    Args:
        self: Bound Celery task context.
        results: Single outcome or list of outcomes from upstream tasks.

    Returns:
        dict[str, Any]: Consolidated summary of workflow execution.
    """
    logger.info("Aggregating canvas workflow outcomes in task %s", self.request.id)

    raw_list = results if isinstance(results, list) else [results]

    return {
        "workflow_task_id": self.request.id,
        "step_count": len(raw_list),
        "steps_summary": [
            {
                "index": idx,
                "status": "success",
                "preview": str(item)[:120],
            }
            for idx, item in enumerate(raw_list)
        ],
        "completed_at": datetime.now(UTC).isoformat(),
    }
