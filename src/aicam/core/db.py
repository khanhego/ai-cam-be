"""SQLAlchemy async: Base, engine, session, hook sau commit (02a §2, §6)."""

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime
from typing import Any, ClassVar

from sqlalchemy import CheckConstraint, DateTime, MetaData, event
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from aicam.core import clock
from aicam.core.ids import uuid7

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map: ClassVar[dict[Any, Any]] = {
        uuid.UUID: PgUUID(as_uuid=True),
        datetime: DateTime(timezone=True),
    }


class UUIDPk:
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid7)


def utcnow() -> datetime:
    return clock.now()


def enum_check(column: str, values: Sequence[str], name: str | None = None) -> CheckConstraint:
    """CHECK column IN (...) — enum lưu text để dễ thêm giá trị bằng migration (02a §3)."""
    quoted = ", ".join(f"'{v}'" for v in values)
    return CheckConstraint(f"{column} IN ({quoted})", name=name or f"{column}_enum")


_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def init_engine(database_url: str, **kwargs: Any) -> AsyncEngine:
    global _engine, _sessionmaker
    _engine = create_async_engine(database_url, pool_pre_ping=True, **kwargs)
    _sessionmaker = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def dispose_engine() -> None:
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None


def sessionmaker() -> async_sessionmaker[AsyncSession]:
    if _sessionmaker is None:
        raise RuntimeError("Chưa gọi init_engine()")
    return _sessionmaker


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: một session / request, rollback khi lỗi."""
    async with sessionmaker()() as session:
        try:
            yield session
        except Exception:
            await rollback(session)
            raise


AfterCommit = Callable[[], Awaitable[None]]
_AFTER_COMMIT_KEY = "aicam_after_commit"


def after_commit(session: AsyncSession, callback: AfterCommit) -> None:
    """Đăng ký việc chạy sau khi transaction commit thành công (vd publish WS — 02a §6)."""
    session.sync_session.info.setdefault(_AFTER_COMMIT_KEY, []).append(callback)


def pop_after_commit(session: AsyncSession) -> list[AfterCommit]:
    callbacks: list[AfterCommit] = session.sync_session.info.pop(_AFTER_COMMIT_KEY, [])
    return callbacks


async def rollback(session: AsyncSession) -> None:
    """Rollback và bỏ mọi callback after_commit đang chờ (kể cả khi session chưa mở transaction)."""
    pop_after_commit(session)
    await session.rollback()


async def commit(session: AsyncSession) -> None:
    """Commit rồi chạy các callback after_commit theo thứ tự đăng ký."""
    await session.commit()
    for callback in pop_after_commit(session):
        await callback()


@event.listens_for(Session, "after_soft_rollback")
def _drop_callbacks_on_rollback(session: Session, previous_transaction: Any) -> None:
    # after_soft_rollback chạy cho mọi session.rollback(), kể cả khi chỉ rollback savepoint.
    session.info.pop(_AFTER_COMMIT_KEY, None)
