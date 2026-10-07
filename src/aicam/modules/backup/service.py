"""Sao lưu cloud — API-180..188, trạng thái `backup.state` (02 §6.2, 02a §4, ADR-010)."""

import asyncio
from datetime import datetime

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.backup.schemas import TestOut
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import UNREACHABLE, CloudError
from aicam.modules.settings.models import Setting

log = structlog.get_logger()

PROBE_TIMEOUT_S = 10.0  # FR-02.17: kết quả ≤ 10 giây
_HTTP = {UNREACHABLE: 504}


def key_configured(settings: Settings) -> bool:
    return bool(settings.backup_encryption_key.strip())


def configured(settings: Settings) -> bool:
    """`configured` (API-180) — `NOT_CONFIGURED` khi thiếu `S3_*` hoặc `BACKUP_ENCRYPTION_KEY` (EX-K1)."""
    return cloud.is_configured(settings) and key_configured(settings)


def current_fingerprint(settings: Settings) -> str | None:
    """Dấu vân tay `BACKUP_ENCRYPTION_KEY` (không bao giờ trả / log khóa — FR-02.13)."""
    if not key_configured(settings):
        return None
    return crypto.fingerprint(crypto.parse_key(settings.backup_encryption_key))


def current_key(settings: Settings) -> bytes:
    return crypto.parse_key(settings.backup_encryption_key)


def keyring(settings: Settings, extra: list[bytes] | None = None) -> dict[str, bytes]:
    """Khóa giải mã: hiện tại + `BACKUP_OLD_KEYS` (+ `--key-file`) — DEC-495."""
    return crypto.keyring(settings.backup_encryption_key, settings.backup_old_keys, extra)


# `backup.state` (02 §5.2, §6.2 API-180) — ưu tiên từ trên xuống.
NOT_CONFIGURED = "NOT_CONFIGURED"
RESTORE_PENDING = "RESTORE_PENDING"
KEY_UNCONFIRMED = "KEY_UNCONFIRMED"
KEY_CHANGED = "KEY_CHANGED"
DISABLED = "DISABLED"
ON = "ON"


def state(cfg: Setting, settings: Settings) -> str:
    """Chỉ `ON` thì J-20..J-23 chạy (EX-K1, K2, K7, K8)."""
    if not configured(settings):
        return NOT_CONFIGURED
    if cfg.backup_restore_pending:
        return RESTORE_PENDING
    if not cfg.backup_confirmed_fingerprint:
        return KEY_UNCONFIRMED
    if cfg.backup_confirmed_fingerprint != current_fingerprint(settings):
        return KEY_CHANGED
    if not cfg.backup_enabled:
        return DISABLED
    return ON


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


# ---------------------------------------------------------------- số liệu chung (API-180, API-81, WS)

EVIDENCE_KINDS = ("CLIP", "SNAPSHOT")
PENDING_STATUSES = (
    "PENDING",
    "UPLOADING",
    "FAILED",
)  # `pending` không tính SOURCE_DELETED / IGNORED (DEC-496)


async def last_db_success_at(db: AsyncSession) -> datetime | None:
    return await db.scalar(
        select(func.max(BackupRun.finished_at)).where(BackupRun.kind == "DB", BackupRun.status == "SUCCESS")
    )


async def pending_count(db: AsyncSession) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(BackupObject)
            .where(BackupObject.kind.in_(EVIDENCE_KINDS), BackupObject.status.in_(("PENDING", "UPLOADING")))
        )
        or 0
    )


async def publish_updated(db: AsyncSession, settings: Settings) -> None:
    """WS `backup.updated` `{state, pending, last_db_success_at}` trên `ws:admin` (02 §6.2 WS-02). Lỗi Redis /
    chưa khởi tạo → chỉ log (sự kiện là gợi ý tải lại, API-180 vẫn đúng)."""
    from aicam.modules.settings import service as settings_service  # settings → backup: import muộn
    from aicam.realtime import publish

    try:
        cfg = await settings_service.get(db)
        last = await last_db_success_at(db)
        data = {
            "state": state(cfg, settings),
            "pending": await pending_count(db),
            "last_db_success_at": clock.iso_z(last) if last else None,
        }
        await db.commit()
        await publish.to_admin("backup.updated", data)
    except Exception as exc:
        log.warning("backup_ws_publish_failed", error=type(exc).__name__)
