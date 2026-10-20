"""SQLAlchemy model for persistent task execution records."""

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from task_queue.db.session import Base


class TaskStatus(StrEnum):
    """Lifecycle statuses for asynchronous tasks."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class TaskPriority(StrEnum):
    """Priority classifications for task scheduling."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TaskType(StrEnum):
    """Functional categories of background tasks."""

    EMAIL = "email"
    REPORT = "report"
    DATA_PROCESSING = "data_processing"
    CHAIN = "chain"
    GROUP = "group"


class TaskRecord(Base):
    """Database entity tracking task lifecycle, metadata, and execution outcomes."""

    __tablename__ = "task_records"

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        doc="Unique internal task UUID.",
    )
    celery_task_id: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        index=True,
        nullable=False,
        doc="ID assigned by Celery broker canvas.",
    )
    task_name: Mapped[str] = mapped_column(
        String(100),
        index=True,
        nullable=False,
        doc="Registered Celery task name.",
    )
    task_type: Mapped[str] = mapped_column(
        String(50),
        index=True,
        nullable=False,
        doc="Task categorization (email, report, data_processing, etc.).",
    )
    status: Mapped[str] = mapped_column(
        String(30),
        default=TaskStatus.PENDING.value,
        index=True,
        nullable=False,
        doc="Current lifecycle state of the task.",
    )
    priority: Mapped[str] = mapped_column(
        String(20),
        default=TaskPriority.MEDIUM.value,
        index=True,
        nullable=False,
        doc="Human-readable priority string.",
    )
    priority_num: Mapped[int] = mapped_column(
        Integer,
        default=3,
        nullable=False,
        doc="Numerical priority level for Redis queue routing (0-9).",
    )
    args: Mapped[list[Any]] = mapped_column(
        JSON,
        default=list,
        nullable=False,
        doc="Positional arguments supplied to task.",
    )
    kwargs: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        default=dict,
        nullable=False,
        doc="Keyword arguments supplied to task.",
    )
    result: Mapped[Any | None] = mapped_column(
        JSON,
        nullable=True,
        doc="Serialized result output upon successful completion.",
    )
    error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        doc="Captured stack trace or error message upon failure.",
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        default=0,
        nullable=False,
        doc="Number of times task has been retried.",
    )
    max_retries: Mapped[int] = mapped_column(
        Integer,
        default=3,
        nullable=False,
        doc="Maximum permitted retries before marking as failed.",
    )
    scheduled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when task is scheduled to run (ETA).",
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when task execution began.",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when task completed or terminated.",
    )
    duration_seconds: Mapped[float | None] = mapped_column(
        Float,
        nullable=True,
        doc="Total runtime duration in seconds.",
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp after which task record can be purged.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Creation timestamp in DB.",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="Last modification timestamp.",
    )
