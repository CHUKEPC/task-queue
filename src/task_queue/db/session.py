"""Database engines, session factories, and dependency providers."""

from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import NullPool, StaticPool

from task_queue.config import get_settings


class Base(DeclarativeBase):
    """Base declarative class for all SQLAlchemy domain models."""

    pass


def get_async_engine() -> AsyncEngine:
    """Create and return configured asynchronous SQLAlchemy engine.

    Returns:
        AsyncEngine: Configured async engine.
    """
    settings = get_settings()
    url = settings.database_url

    if "sqlite" in url:
        return create_async_engine(
            url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool if ":memory:" in url else NullPool,
        )

    return create_async_engine(
        url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
    )


def get_sync_engine() -> Engine:
    """Create and return configured synchronous SQLAlchemy engine.

    This engine is primarily used by Celery task lifecycle hooks and workers
    that run outside the asyncio event loop.

    Returns:
        Engine: Configured synchronous engine.
    """
    settings = get_settings()
    url = settings.database_sync_url

    # Convert async sqlite URL if used during tests
    if "sqlite" in settings.database_url and "sqlite" not in url:
        url = settings.database_url.replace("+aiosqlite", "")

    if "sqlite" in url:
        return create_engine(
            url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool if ":memory:" in url else NullPool,
        )

    return create_engine(
        url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
    )


def async_session_factory() -> async_sessionmaker[AsyncSession]:
    """Create a configured async session factory.

    Returns:
        async_sessionmaker[AsyncSession]: Callable returning an async session.
    """
    engine = get_async_engine()
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


def sync_session_factory() -> sessionmaker[Session]:
    """Create a configured synchronous session factory.

    Returns:
        sessionmaker[Session]: Callable returning a sync session.
    """
    engine = get_sync_engine()
    return sessionmaker(
        bind=engine,
        class_=Session,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for yielding asynchronous database sessions.

    Yields:
        AsyncSession: Active asynchronous database session.
    """
    session_factory = async_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@contextmanager
def get_sync_db() -> Generator[Session, None, None]:
    """Context manager for yielding synchronous database sessions.

    Used by Celery task workers to update execution status and results.

    Yields:
        Session: Active synchronous database session.
    """
    session_factory = sync_session_factory()
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
