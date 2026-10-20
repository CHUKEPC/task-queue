"""Pydantic schemas for request validation and response serialization."""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from task_queue.models.task_record import TaskPriority, TaskStatus, TaskType


class TaskScheduleParams(BaseModel):
    """Optional parameters for delayed or scheduled task execution."""

    priority: TaskPriority = Field(
        default=TaskPriority.MEDIUM,
        description="Execution priority level (low, medium, high, critical).",
    )
    scheduled_at: datetime | None = Field(
        default=None,
        description="Explicit UTC timestamp to execute the task (ETA).",
    )
    countdown_seconds: int | None = Field(
        default=None,
        ge=0,
        description="Number of seconds to delay task execution.",
    )


class EmailTaskRequest(TaskScheduleParams):
    """Payload for submitting simulated transactional or bulk email tasks."""

    recipient: EmailStr = Field(description="Recipient email address.")
    subject: str = Field(min_length=1, max_length=255, description="Email subject line.")
    body: str = Field(min_length=1, description="Email body content (text or HTML).")
    template_name: str | None = Field(
        default=None,
        description="Optional template identifier (e.g. welcome_email).",
    )
    simulate_failure: bool = Field(
        default=False,
        description="Intentionally trigger an error to demonstrate retry logic.",
    )
    delay_seconds: float = Field(
        default=0.2,
        ge=0.0,
        le=10.0,
        description="Simulated network transport delay in seconds.",
    )


class ReportTaskRequest(TaskScheduleParams):
    """Payload for submitting asynchronous analytical report generation tasks."""

    report_type: str = Field(
        min_length=1,
        max_length=50,
        description="Type of report (e.g. sales, user_activity, financial).",
    )
    date_from: date | None = Field(
        default=None,
        description="Start date for report data aggregation.",
    )
    date_to: date | None = Field(
        default=None,
        description="End date for report data aggregation.",
    )
    filters: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary query filter parameters.",
    )
    format: str = Field(
        default="json",
        pattern="^(json|csv|pdf)$",
        description="Desired export format (json, csv, pdf).",
    )


class DataProcessingTaskRequest(TaskScheduleParams):
    """Payload for batch data processing and transformation tasks."""

    dataset_name: str = Field(
        min_length=1,
        max_length=100,
        description="Identifying label for dataset.",
    )
    records: list[dict[str, Any]] = Field(
        min_length=1,
        description="Collection of records to process.",
    )
    operations: list[str] = Field(
        default_factory=lambda: ["clean", "aggregate", "summarize"],
        description="Pipeline of operations to apply to records.",
    )
    chunk_size: int = Field(
        default=50,
        ge=1,
        le=1000,
        description="Batch size for segmented processing.",
    )


class TaskChainStep(BaseModel):
    """Definition of an individual step in a Celery canvas chain or group."""

    task_type: TaskType = Field(description="Category of task to invoke.")
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters tailored to the specified task type.",
    )


class TaskChainRequest(BaseModel):
    """Request to execute a sequential pipeline (Celery chain) of tasks."""

    steps: list[TaskChainStep] = Field(
        min_length=2,
        description="Ordered sequence of tasks where output flows downstream.",
    )
    priority: TaskPriority = Field(
        default=TaskPriority.MEDIUM,
        description="Priority assigned to chain execution.",
    )


class TaskGroupRequest(BaseModel):
    """Request to execute a parallel batch (Celery group) of tasks."""

    tasks: list[TaskChainStep] = Field(
        min_length=1,
        description="List of tasks to execute concurrently.",
    )
    priority: TaskPriority = Field(
        default=TaskPriority.MEDIUM,
        description="Priority assigned to group execution.",
    )


class TaskResponse(BaseModel):
    """Detailed view of a task execution record."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    celery_task_id: str
    task_name: str
    task_type: str
    status: str
    priority: str
    priority_num: int
    args: list[Any]
    kwargs: dict[str, Any]
    result: Any | None = None
    error_message: str | None = None
    retry_count: int
    max_retries: int
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    expires_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class TaskListResponse(BaseModel):
    """Paginated collection of task records."""

    items: list[TaskResponse]
    total: int
    page: int
    limit: int
    total_pages: int


class TaskResultResponse(BaseModel):
    """Lightweight response containing task outcome and timing."""

    id: str
    celery_task_id: str
    status: str
    result: Any | None = None
    error_message: str | None = None
    duration_seconds: float | None = None
    completed_at: datetime | None = None


class TaskCancelResponse(BaseModel):
    """Response returned upon attempting task cancellation."""

    task_id: str
    celery_task_id: str
    status: str
    message: str


class TaskRetryResponse(BaseModel):
    """Response returned upon attempting manual task re-queue."""

    task_id: str
    celery_task_id: str
    status: str
    message: str


class TaskMetricsResponse(BaseModel):
    """Analytical metrics covering task throughput, errors, and timing."""

    total_tasks: int
    status_counts: dict[str, int]
    type_counts: dict[str, int]
    priority_counts: dict[str, int]
    throughput_last_hour: int
    avg_duration_seconds: float
    failure_rate_percentage: float
    active_tasks: int


class WorkerStat(BaseModel):
    """Status summary for an active or discovered Celery worker."""

    worker_name: str
    status: str
    active_tasks_count: int
    registered_tasks: list[str] = Field(default_factory=list)


class WorkerHealthResponse(BaseModel):
    """Overview of worker nodes cluster availability."""

    workers_count: int
    status: str
    workers: list[WorkerStat] = Field(default_factory=list)


__all__ = [
    "DataProcessingTaskRequest",
    "EmailTaskRequest",
    "ReportTaskRequest",
    "TaskCancelResponse",
    "TaskChainRequest",
    "TaskChainStep",
    "TaskGroupRequest",
    "TaskListResponse",
    "TaskMetricsResponse",
    "TaskPriority",
    "TaskResponse",
    "TaskResultResponse",
    "TaskRetryResponse",
    "TaskScheduleParams",
    "TaskStatus",
    "TaskType",
    "WorkerHealthResponse",
    "WorkerStat",
]
