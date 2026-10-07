"""J-25 `shares.cleanup` — hết hạn / thu hồi / treo (02a §7 J-25; BR-34, FR-07.08, EX-S7, NFR-42).

1. `ACTIVE` có `expires_at ≤ now` → `EXPIRED` + audit `SHARE_EXPIRE` (người dùng null).
2. `CREATING` quá 15 phút (worker chết giữa chừng) → `FAILED TIMEOUT`.
3. `REVOKED` / `EXPIRED` / `FAILED` chưa `cloud_deleted_at` → xóa **mọi** đối tượng (và mọi phiên bản nếu nhà
   cung cấp buộc versioning) dưới `share/{token}/`, kiểm danh sách rỗng → `cloud_deleted_at`. Mất mạng → để
   lượt sau (beat mỗi phút; `revoke_pending` = "Đang thu hồi — chờ Internet").

Gọi ngay sau API-163 với `share_id` (chỉ xử lý link đó) và theo beat 60 giây (toàn bộ).
"""

import asyncio
import uuid
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.settings import Settings
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import CloudError, ObjectStore
from aicam.modules.shares.build import MESSAGES
from aicam.modules.shares.models import ShareLink
from aicam.modules.shares.service import publish_updated

log = structlog.get_logger()

STUCK_CREATING = timedelta(minutes=15)
DONE = ("REVOKED", "EXPIRED", "FAILED")


async def _locked(db: AsyncSession, share_id: uuid.UUID) -> ShareLink | None:
    link: ShareLink | None = await db.scalar(
        select(ShareLink)
        .where(ShareLink.id == share_id)
        .with_for_update(skip_locked=True)
        .execution_options(populate_existing=True)
    )
    return link


async def expire_due(db: AsyncSession, share_id: uuid.UUID | None = None) -> int:
    now = clock.now()
    query = select(ShareLink.id).where(ShareLink.status == "ACTIVE", ShareLink.expires_at <= now)
    if share_id is not None:
        query = query.where(ShareLink.id == share_id)
    count = 0
    for sid in (await db.scalars(query)).all():
        link = await _locked(db, sid)
        if link is None or link.status != "ACTIVE" or link.expires_at > now:
            await db.rollback()
            continue
        link.status = "EXPIRED"
        audit.record(
            db,
            "SHARE_EXPIRE",
            user_id=None,
            object_type="SHARE",
            object_id=link.id,
            data={"expires_at": clock.iso_z(link.expires_at), "package_id": str(link.package_id),
                  "claim_id": str(link.claim_id) if link.claim_id else None},
        )  # fmt: skip
        publish_updated(db, link)
        await commit(db)
        count += 1
    return count


async def fail_stuck(db: AsyncSession) -> int:
    now = clock.now()
    ids = (
        await db.scalars(
            select(ShareLink.id).where(
                ShareLink.status == "CREATING", ShareLink.created_at < now - STUCK_CREATING
            )
        )
    ).all()
    count = 0
    for sid in ids:
        link = await _locked(db, sid)  # J-24 đang công bố giữ khóa → lượt sau
        if link is None or link.status != "CREATING":
            await db.rollback()
            continue
        link.status, link.error_code, link.error_message = "FAILED", "TIMEOUT", MESSAGES["TIMEOUT"]
        link.step = link.step_index = link.step_total = None
        publish_updated(db, link)
        await commit(db)
        count += 1
        log.warning("share_stuck_failed", share_id=str(sid))
    return count


def _purge(store: ObjectStore, prefix: str) -> tuple[int, bool]:
    deleted = store.delete_prefix(prefix, all_versions=True)
    remaining = any(True for _ in store.list(prefix))
    return deleted, not remaining


async def purge(
    db: AsyncSession, settings: Settings, share_id: uuid.UUID | None, store: ObjectStore
) -> dict[str, int]:
    query = select(ShareLink.id).where(ShareLink.status.in_(DONE), ShareLink.cloud_deleted_at.is_(None))
    if share_id is not None:
        query = query.where(ShareLink.id == share_id)
    out = {"deleted": 0, "pending": 0}
    for sid in (await db.scalars(query.order_by(ShareLink.revoked_at.desc().nulls_last()))).all():
        link = await _locked(db, sid)
        if link is None or link.status not in DONE or link.cloud_deleted_at is not None:
            await db.rollback()
            continue
        prefix = link.object_prefix
        if not prefix.startswith("share/") or len(prefix) < len("share/") + 40:
            # Lưới an toàn: không bao giờ xóa theo tiền tố rỗng / ngắn (xóa nhầm link khác).
            log.error("share_cleanup_bad_prefix", share_id=str(sid))
            await db.rollback()
            continue
        await db.commit()  # không giữ khóa dòng khi gọi mạng (02a §6)
        try:
            deleted, empty = await asyncio.to_thread(_purge, store, prefix)
        except CloudError as exc:
            out["pending"] += 1
            log.warning("share_cleanup", share_id=str(sid), deleted=0, pending=True, error=exc.code)
            continue
        if not empty:
            out["pending"] += 1
            log.warning("share_cleanup", share_id=str(sid), deleted=deleted, pending=True)
            continue
        link = await _locked(db, sid)
        if link is None:
            await db.rollback()
            continue
        link.cloud_deleted_at = clock.now()
        publish_updated(db, link)
        await commit(db)
        out["deleted"] += 1
        log.info("share_cleanup", share_id=str(sid), deleted=deleted, pending=False)
    return out


async def cleanup(
    db: AsyncSession,
    settings: Settings,
    share_id: uuid.UUID | None = None,
    *,
    store: ObjectStore | None = None,
) -> dict[str, Any]:
    counts: dict[str, Any] = {"expired": await expire_due(db, share_id)}
    if share_id is None:
        counts["stuck_failed"] = await fail_stuck(db)
    if not cloud.share_configured(settings):
        counts["skipped"] = "NOT_CONFIGURED"
        return counts
    counts.update(await purge(db, settings, share_id, store or cloud.share_store(settings)))
    return counts
