"""Pytest configuration, database fixtures, and test client providers."""

import contextlib
import os
import tempfile
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

# Configure test environment variables prior to importing application modules
TEST_DB_PATH = os.path.join(tempfile.gettempdir(), f"task_queue_test_{os.getpid()}.db")
TEST_ASYNC_DB_URL = f"sqlite+aiosqlite:///{TEST_DB_PATH}"
TEST_SYNC_DB_URL = f"sqlite:///{TEST_DB_PATH}"

os.environ["ENVIRONMENT"] = "testing"
os.environ["DEBUG"] = "true"
os.environ["DATABASE_URL"] = TEST_ASYNC_DB_URL
os.environ["DATABASE_SYNC_URL"] = TEST_SYNC_DB_URL
os.environ["CELERY_TASK_ALWAYS_EAGER"] = "true"

from task_queue.celery_app import celery_app  # noqa: E402
from task_queue.db.session import Base, get_db  # noqa: E402
from task_queue.main import app  # noqa: E402

# Ensure Celery runs in eager mode during testing
celery_app.conf.update(
    broker_url="memory://",
    result_backend="cache+memory://",
    task_always_eager=True,
    task_eager_propagates=True,
)


@pytest.fixture(scope="session", autouse=True)
def init_test_database() -> Generator[None, None, None]:
    """Create test SQLite database file and initialize schema."""
    sync_engine = create_engine(TEST_SYNC_DB_URL, poolclass=NullPool)
    Base.metadata.create_all(bind=sync_engine)
    sync_engine.dispose()

    yield

    if os.path.exists(TEST_DB_PATH):
        with contextlib.suppress(OSError):
            os.remove(TEST_DB_PATH)


@pytest_asyncio.fixture
async def async_engine() -> AsyncGenerator[AsyncEngine, None]:
    """Provide an asynchronous test engine."""
    engine = create_async_engine(TEST_ASYNC_DB_URL, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(async_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Provide an isolated asynchronous database session."""
    session_factory = async_sessionmaker(
        bind=async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
def sync_db_session() -> Generator[Session, None, None]:
    """Provide an isolated synchronous database session."""
    sync_engine = create_engine(TEST_SYNC_DB_URL, poolclass=NullPool)
    session_factory = sessionmaker(bind=sync_engine, class_=Session, expire_on_commit=False)
    session = session_factory()
    yield session
    session.rollback()
    session.close()
    sync_engine.dispose()


@pytest_asyncio.fixture
async def client(db_session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    """Provide an asynchronous HTTP test client for the FastAPI application."""

    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db_session

    app.dependency_overrides[get_db] = _override_get_db

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    app.dependency_overrides.clear()
