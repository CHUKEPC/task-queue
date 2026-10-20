"""Unit tests for configuration, database session helper functions, and custom exceptions."""

from sqlalchemy.orm import Session

from task_queue.config import Settings, get_settings
from task_queue.db.session import (
    async_session_factory,
    get_async_engine,
    get_sync_db,
    get_sync_engine,
    sync_session_factory,
)
from task_queue.exceptions import (
    BrokerConnectionError,
    InvalidTaskPriorityError,
    InvalidTaskTypeError,
    TaskAlreadyExistsError,
    TaskCancellationError,
    TaskNotFoundError,
    TaskQueueError,
    TaskRetryError,
    WorkerUnavailableError,
)


def test_settings_properties() -> None:
    """Verify Settings property calculations for environment checks."""
    s_dev = Settings(environment="development")
    assert not s_dev.is_production
    assert not s_dev.is_testing

    s_prod = Settings(environment="production")
    assert s_prod.is_production
    assert not s_prod.is_testing

    s_test = Settings(environment="testing")
    assert not s_test.is_production
    assert s_test.is_testing

    cached = get_settings()
    assert cached is not None


def test_all_custom_exceptions() -> None:
    """Verify all domain exceptions instantiate with descriptive error messages."""
    assert "Base error" in str(TaskQueueError("Base error"))
    assert "t1" in str(TaskNotFoundError("t1"))
    assert "t2" in str(TaskAlreadyExistsError("t2"))
    assert "invalid" in str(InvalidTaskPriorityError("invalid"))
    assert "unsupported" in str(InvalidTaskTypeError("unsupported"))
    assert "t3" in str(TaskCancellationError("t3", "completed"))
    assert "busy" in str(TaskRetryError("t4", "busy"))
    assert "broker" in str(BrokerConnectionError("broker issue"))
    assert "worker" in str(WorkerUnavailableError("no workers"))


def test_db_session_engines() -> None:
    """Verify async and sync engine factory initializers."""
    a_engine = get_async_engine()
    assert a_engine is not None

    s_engine = get_sync_engine()
    assert s_engine is not None

    a_factory = async_session_factory()
    assert a_factory is not None

    s_factory = sync_session_factory()
    assert s_factory is not None


def test_get_sync_db_context_manager() -> None:
    """Verify get_sync_db provides session and commits."""
    with get_sync_db() as db:
        assert isinstance(db, Session)
