"""G3 M-F1 (b), DEC-336: tiến trình thoát khi `alembic_version` lệch head image; J-02 không xóa khi lệch."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aicam.core import schema_guard
from aicam.core.settings import Settings
from aicam.modules.media import service as media

from .conftest import TEST_DATABASE_URL

pytestmark = pytest.mark.integration


def _settings(strict: bool | None) -> Settings:
    return Settings(app_env="test", database_url=TEST_DATABASE_URL, schema_check_strict=strict)


async def test_enforce_passes_on_head(migrated_database_url: str) -> None:
    await schema_guard.enforce(_settings(True), "api")


async def test_enforce_exits_on_mismatch(migrated_database_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(schema_guard, "SCHEMA_HEAD", "9999")
    with pytest.raises(SystemExit) as exc:
        await schema_guard.enforce(_settings(True), "worker")
    assert exc.value.code == schema_guard.EXIT_CODE
    await schema_guard.enforce(_settings(False), "worker")  # dev / test: chỉ log


def test_enforce_blocking_exits_for_celery(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`worker_init` / `beat_init`: SystemExit không bị Celery nuốt như Exception."""
    from aicam.workers import celery_app

    monkeypatch.setattr(schema_guard, "SCHEMA_HEAD", "0004")
    monkeypatch.setattr(celery_app, "get_settings", lambda: _settings(True))
    with pytest.raises(SystemExit):
        celery_app._check_schema(sender=None)


async def test_enforce_unreachable_db_does_not_block() -> None:
    settings = Settings(app_env="production", database_url="postgresql+asyncpg://x:y@127.0.0.1:1/none",
                        jwt_secret="x" * 40, media_signing_key="y" * 40, platform_adapter="shopee",
                        fernet_key="Zm9vYmFyYmF6cXV4cXV1eGNvcmdlZ3JhdWx0Z2FycGx5PQ==")  # fmt: skip
    await schema_guard.enforce(settings, "vision")


async def test_j02_skips_when_schema_mismatch(db: AsyncSession, migrated_database_url: str) -> None:
    await db.execute(text("UPDATE alembic_version SET version_num = '0004'"))
    result = await media.enforce_retention(db, _settings(None))
    assert result == {"skipped_schema_mismatch": 1}


async def test_db_revision_reads_version(migrated_database_url: str) -> None:
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as conn:
            assert await schema_guard.db_revision(conn) == schema_guard.SCHEMA_HEAD
    finally:
        await engine.dispose()
