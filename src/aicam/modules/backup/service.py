"""Sao lưu cloud — API-180..188, trạng thái `backup.state` (02 §6.2, 02a §4, ADR-010)."""

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.backup.schemas import TestOut
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import UNREACHABLE, CloudError

log = structlog.get_logger()

PROBE_TIMEOUT_S = 10.0  # FR-02.17: kết quả ≤ 10 giây
_HTTP = {UNREACHABLE: 504}


def key_configured(settings: Settings) -> bool:
    return bool(settings.backup_encryption_key.strip())


def configured(settings: Settings) -> bool:
    """`configured` (API-180) — `NOT_CONFIGURED` khi thiếu `S3_*` hoặc `BACKUP_ENCRYPTION_KEY` (EX-K1)."""
    return cloud.is_configured(settings) and key_configured(settings)


def not_configured() -> AppError:
    return AppError(
        "BACKUP_NOT_CONFIGURED",
        "Chưa cấu hình kho lưu cloud hoặc khóa sao lưu — liên hệ IT (tài liệu vận hành, mục Sao lưu cloud).",
        503,
    )


async def test_connection(db: AsyncSession, settings: Settings, p: Principal) -> TestOut:
    """API-183 (FR-02.17): `store.probe()` trong 10 giây; lỗi → 502 `CLOUD_AUTH_FAILED` / `CLOUD_ERROR`, 504
    `CLOUD_UNREACHABLE`. Audit `BACKUP_TEST {ok, code}` ghi cả khi lỗi (commit trước khi trả lỗi)."""
    if not configured(settings):
        raise not_configured()
    store = cloud.backup_store(settings)
    error: CloudError | None = None
    elapsed = 0
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_S):
            elapsed = await asyncio.to_thread(store.probe)
    except TimeoutError:
        error = CloudError(UNREACHABLE)
    except CloudError as exc:
        error = exc
    except Exception as exc:  # lỗi lạ của thư viện: không 500, báo như lỗi kho lưu
        log.exception("backup_probe_failed")
        error = CloudError("CLOUD_ERROR", f"Kho lưu báo lỗi: {type(exc).__name__}")
    audit.record(db, "BACKUP_TEST", user_id=p.user_id, object_type="BACKUP", ip=p.ip,
                 data={"ok": error is None, "code": error.code if error else None,
                       "elapsed_ms": elapsed})  # fmt: skip
    await commit(db)
    if error is not None:
        log.warning("backup_probe", ok=False, code=error.code)
        raise AppError(error.code, error.message, _HTTP.get(error.code, 502))
    log.info("backup_probe", ok=True, elapsed_ms=elapsed)
    return TestOut(ok=True, elapsed_ms=elapsed)
