"""Custom domain exceptions for the task queue system."""


class TaskQueueError(Exception):
    """Base exception for all task queue related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class TaskNotFoundError(TaskQueueError):
    """Raised when a requested task does not exist in the database."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task with ID '{task_id}' was not found.")
        self.task_id = task_id


class TaskAlreadyExistsError(TaskQueueError):
    """Raised when attempting to create a task with an existing ID."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Task with ID '{task_id}' already exists.")
        self.task_id = task_id


class InvalidTaskPriorityError(TaskQueueError):
    """Raised when an invalid task priority is provided."""

    def __init__(self, priority: str) -> None:
        super().__init__(
            f"Invalid priority '{priority}'. Allowed: 'low', 'medium', 'high', 'critical'."
        )
        self.priority = priority


class InvalidTaskTypeError(TaskQueueError):
    """Raised when an unsupported task type is specified."""

    def __init__(self, task_type: str) -> None:
        super().__init__(f"Unsupported task type '{task_type}'.")
        self.task_type = task_type


class TaskCancellationError(TaskQueueError):
    """Raised when a task cannot be cancelled (e.g. already completed or failed)."""

    def __init__(self, task_id: str, current_status: str) -> None:
        super().__init__(
            f"Cannot cancel task '{task_id}' in state '{current_status}'. Only pending or running tasks can be cancelled."
        )
        self.task_id = task_id
        self.current_status = current_status


class TaskRetryError(TaskQueueError):
    """Raised when a task cannot be retried (e.g. already active)."""

    def __init__(self, task_id: str, reason: str) -> None:
        super().__init__(f"Cannot retry task '{task_id}': {reason}")
        self.task_id = task_id
        self.reason = reason


class BrokerConnectionError(TaskQueueError):
    """Raised when communication with Redis or Celery broker fails."""

    def __init__(self, message: str = "Failed to connect to message broker.") -> None:
        super().__init__(message)


class WorkerUnavailableError(TaskQueueError):
    """Raised when no active Celery workers are detected."""

    def __init__(self, message: str = "No Celery workers available to process tasks.") -> None:
        super().__init__(message)
