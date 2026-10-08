"""Đặt / gỡ `MISSING` cho clip / ảnh nguồn từ sao lưu (02a §5.2 #16, EX-K9 v0.5; DEC-530, T-291).

- J-22: nguồn `READY` không thấy tệp ở lần thử liền thứ `BACKUP_SOURCE_MISSING_MARK_AFTER` → `MISSING`
  (audit `MEDIA_MARK_MISSING` `cause = SOURCE_MISSING`, người dùng null); API-188 `IGNORE` cho
  `SOURCE_MISSING` → `MISSING` ngay (`cause = BACKUP_IGNORE`, người dùng = Admin).
- J-22: nguồn `MISSING` có lại tệp, băm khớp (hoặc lệch đã chấp nhận `UPLOAD_ANYWAY`) → `READY` (audit
  `MEDIA_MISSING_RECOVERED`, người dùng null); `clip.sha256` giữ giá trị gốc.

Thứ tự khóa: `backup_object` (người gọi) → nguồn `FOR UPDATE`; luôn đọc lại trạng thái + kiểm tệp
**dưới khóa** trước khi đổi (J-02 / lệnh khôi phục có thể vừa đổi). Không bao giờ đổi nguồn `DELETED`.
"""

import asyncio
import uuid
from pathlib import Path
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.core.settings import Settings
from aicam.modules.backup.models import BackupObject

log = structlog.get_logger()


def _model(obj: BackupObject) -> tuple[Any, uuid.UUID | None, str]:
    from aicam.modules.media.models import Clip, Snapshot

    if obj.kind == "CLIP":
        return Clip, obj.clip_id, "clip_id"
    return Snapshot, obj.snapshot_id, "snapshot_id"


async def lock_source(db: AsyncSession, obj: BackupObject) -> Any:
    model, source_id, _ = _model(obj)
    return await db.scalar(
        select(model).where(model.id == source_id).with_for_update().execution_options(populate_existing=True)
    )


def file_of(settings: Settings, source: Any) -> Path | None:
    from aicam.modules.media import service as media

    if not source.path:
        return None
    try:
        path = media.absolute(settings, source.path)
    except ValueError:
        return None
    return path if path.is_file() else None


async def mark_missing(
    db: AsyncSession,
    obj: BackupObject,
    settings: Settings,
    *,
    cause: str,
    user_id: uuid.UUID | None,
    ip: str | None = None,
) -> bool:
    """`READY` → `MISSING` dưới khóa nguồn khi tệp vẫn không có. Không commit. Trả True nếu đã đổi."""
    source = await lock_source(db, obj)
    if source is None or source.status != "READY":
        return False
    if await asyncio.to_thread(file_of, settings, source) is not None:
        return False  # tệp vừa có lại — không đặt MISSING
    source.status = "MISSING"
    _, source_id, field = _model(obj)
    audit.record(
        db,
        "MEDIA_MARK_MISSING",
        user_id=user_id,
        object_type=obj.kind,
        object_id=source_id,
        ip=ip,
        data={field: str(source_id), "cause": cause, "object_id": str(obj.id)},
    )
    log.warning("media_mark_missing", kind=obj.kind, source_id=str(source_id), cause=cause)
    return True


async def recover(db: AsyncSession, obj: BackupObject, *, accepted_mismatch: bool) -> bool:
    """`MISSING` → `READY` dưới khóa nguồn (người gọi đã kiểm tệp có + băm khớp / lệch đã chấp nhận).
    Không commit."""
    source = await lock_source(db, obj)
    if source is None or source.status != "MISSING":
        return False
    source.status = "READY"
    _, source_id, field = _model(obj)
    audit.record(
        db,
        "MEDIA_MISSING_RECOVERED",
        user_id=None,
        object_type=obj.kind,
        object_id=source_id,
        data={field: str(source_id), "object_id": str(obj.id), "accepted_mismatch": accepted_mismatch},
    )
    log.info("media_missing_recovered", kind=obj.kind, source_id=str(source_id), accepted=accepted_mismatch)
    return True
