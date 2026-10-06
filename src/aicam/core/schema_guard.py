"""Kiểm phiên bản schema lúc khởi động (G3 M-F1 b, DEC-336).

Mỗi image đóng gói một head Alembic (`SCHEMA_HEAD`, test đơn vị so với `alembic/versions`). Tiến trình api /
worker / beat / vision so `alembic_version` của DB với head này lúc khởi động:

- Khớp → chạy bình thường.
- Lệch (DB cũ hơn — chưa migrate; DB mới hơn — image cũ chạy trên DB đã nâng cấp, vd. worker Phase 1 còn sống
  khi 0004 bỏ cờ `held`) → log `schema_version_mismatch` rõ và **thoát** (mã 78) ở staging / production; dev /
  test chỉ log (stack dev migrate bằng `exec api alembic upgrade head` khi api đã chạy — qa-reset.sh).
- Không đọc được DB (chưa sẵn sàng) → log cảnh báo, chạy tiếp; J-02 kiểm lại ngay trước khi xóa (`matches`).
"""

import asyncio
from typing import Any

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from aicam.core.settings import Settings

# Head Alembic đóng gói trong image — cập nhật cùng migration mới (tests/unit/test_schema_guard.py so với
# thư mục `alembic/versions`).
SCHEMA_HEAD = "0006"
EXIT_CODE = 78  # EX_CONFIG

log = structlog.get_logger()


class SchemaMismatch(SystemExit):
    """Thoát tiến trình (SystemExit đi qua `except Exception` của Celery signal / uvicorn lifespan)."""


async def db_revision(conn: AsyncConnection | AsyncSession) -> str | None:
    exists = await conn.scalar(text("SELECT to_regclass('alembic_version') IS NOT NULL"))
    if not exists:
        return None
    rows = (await conn.execute(text("SELECT version_num FROM alembic_version"))).scalars().all()
    return ",".join(sorted(rows)) or None


async def matches(db: AsyncSession) -> bool:
    """Dùng trong job xóa dữ liệu (J-02): schema khớp image mới được xóa."""
    revision = await db_revision(db)
    if revision != SCHEMA_HEAD:
        log.error("schema_version_mismatch", component="J-02", db_revision=revision, image_head=SCHEMA_HEAD)
        return False
    return True


def is_strict(settings: Settings) -> bool:
    if settings.schema_check_strict is not None:
        return settings.schema_check_strict
    return settings.app_env in ("staging", "production")


def _fail(settings: Settings, component: str, revision: str | None) -> None:
    fields: dict[str, Any] = {"component": component, "db_revision": revision, "image_head": SCHEMA_HEAD}
    hint = (
        "Chạy migrate (`alembic upgrade head`) bằng ĐÚNG image này trước khi khởi động service; DB mới hơn "
        "image → image cũ đang chạy trên DB đã nâng cấp: dừng service này, triển khai image mới "
        "(docs/ops.md §7.1)."
    )
    if is_strict(settings):
        log.critical("schema_version_mismatch", action="exit", hint=hint, **fields)
        raise SchemaMismatch(EXIT_CODE)
    log.error("schema_version_mismatch", action="continue (dev/test)", hint=hint, **fields)


async def enforce(settings: Settings, component: str) -> None:
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            revision = await db_revision(conn)
    except Exception as exc:  # DB chưa sẵn sàng: không chặn khởi động (J-02 kiểm lại trước khi xóa)
        log.warning("schema_version_check_skipped", component=component, error=type(exc).__name__)
        return
    finally:
        await engine.dispose()
    if revision != SCHEMA_HEAD:
        _fail(settings, component, revision)
        return
    log.info("schema_version_ok", component=component, revision=revision)


def enforce_blocking(settings: Settings, component: str) -> None:
    """Cho Celery `worker_init` / `beat_init` (đồng bộ, chưa có event loop)."""
    asyncio.run(enforce(settings, component))
