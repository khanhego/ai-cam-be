"""Integration test với PostgreSQL thật (DEC-39: compose dev local, service container trên CI).

`TEST_DATABASE_URL` mặc định trỏ DB `aicam_test` trên Postgres của compose dev (port 55432).
DB được tạo nếu chưa có, migrate tới head một lần mỗi phiên test.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from aicam.core.settings import Settings, get_settings

ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://aicam:aicam@localhost:55432/aicam_test"
)


async def _reset_schema(url: str) -> None:
    """Xóa sạch schema để migrate từ đầu. Không dùng `downgrade base`: downgrade 0003 chép dữ liệu Phase 2
    do test đồng thời commit sang `phase2_archive` (T-120) thay vì bỏ; đường downgrade kiểm ở
    `test_migration_rollback.py`."""
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text("DROP SCHEMA IF EXISTS phase2_archive CASCADE"))
            await conn.execute(text("DROP SCHEMA IF EXISTS phase3_archive CASCADE"))  # 0006 downgrade (T-202)
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
    finally:
        await engine.dispose()


async def _ensure_database(url: str) -> None:
    target = make_url(url)
    admin = create_async_engine(target.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": target.database}
            )
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{target.database}"'))
    finally:
        await admin.dispose()


def alembic_config() -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    return cfg


@pytest.fixture(scope="session")
def migrated_database_url() -> str:
    asyncio.run(_ensure_database(TEST_DATABASE_URL))
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
    get_settings.cache_clear()
    asyncio.run(_reset_schema(TEST_DATABASE_URL))
    command.upgrade(alembic_config(), "head")
    return TEST_DATABASE_URL


@pytest.fixture
async def engine(migrated_database_url: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_database_url)
    yield eng
    await eng.dispose()


@pytest.fixture
async def db(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """Session trong một transaction ngoài; rollback sau mỗi test để dữ liệu không rò sang test khác."""
    async with engine.connect() as conn:
        trans = await conn.begin()
        session = AsyncSession(bind=conn, expire_on_commit=False, join_transaction_mode="create_savepoint")
        try:
            yield session
        finally:
            await session.close()
            await trans.rollback()


# ---------------------------------------------------------------- API client (T-7+)

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:56379/15")


@pytest.fixture
async def redis_client() -> AsyncIterator[object]:
    from aicam.core.redis import close_redis, init_redis

    client = init_redis(TEST_REDIS_URL)
    await client.flushdb()
    yield client
    await client.flushdb()
    await close_redis()


@pytest.fixture
def test_settings() -> Settings:
    return Settings(app_env="test", log_json=False, database_url=TEST_DATABASE_URL, redis_url=TEST_REDIS_URL)


@pytest.fixture
async def api(db: AsyncSession, redis_client: object, test_settings: Settings) -> AsyncIterator[AsyncClient]:
    """httpx client gọi app thật; DB là session của test (rollback sau test).

    Base https để cookie Secure được gửi kèm.
    """
    from aicam.core.db import get_session
    from aicam.main import create_app

    app = create_app(test_settings)
    app.dependency_overrides[get_session] = lambda: db
    app.dependency_overrides[get_settings] = lambda: test_settings
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver") as client:
        yield client


# Sao lưu cloud (M15): kho MemoryStore, settings có khóa, client API (tests/integration/backup_fixtures.py).
from .backup_fixtures import backup_api, backup_settings, memory_store, world  # noqa: E402, F401
