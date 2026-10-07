"""API-32 báo cáo ngày (FR-09.01, AC-18) — định nghĩa số liệu theo 02 §6 API-32, ngày theo giờ VN.

Cache Redis 5 giây theo ngày (02a §8). `attention` là tình trạng hiện tại, giống nhau mọi ngày.
"""

import shutil
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel
from sqlalchemy import and_, any_, func, literal, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.settings import Settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.claims.models import Claim
from aicam.modules.claims.service import DUE_STATUSES
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package, Shop
from aicam.modules.reconciliation import service as recon
from aicam.modules.returns.models import ReturnCase
from aicam.modules.returns.queries import refund_pending_filter, response_due_sql
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession
from aicam.modules.sessions.queries import dropped_return_filter
from aicam.modules.settings import service as settings_service
from aicam.modules.stations.models import Camera, Station
from aicam.realtime.publish import daily_report_key

CACHE_TTL_S = 5
DISK_WARN_PERCENT = 80  # NFR-30 / 02a §10 `aicam_disk_used_ratio` > 0.8
CLOCK_DRIFT_MS = 1000  # BR-15
_STATE = {
    "OPEN": "PACKING",
    "MISMATCH": "MISMATCH",
    "WAITING_APPROVAL": "WAITING_APPROVAL",
    "INSPECTING": "INSPECTING",  # phiên RETURN đang mở (R2)
}
RECENT_RETURN_WINDOW = timedelta(days=7)  # attention RETURN_SESSION_ABANDONED / RETURN_FORCE_NEW


class ReconOpen(BaseModel):
    HIGH: int = 0
    MEDIUM: int = 0
    LOW: int = 0


class Counts(BaseModel):
    packed: int
    had_mismatch: int
    abandoned: int
    cancelled: int
    packed_not_handed_over: int
    cancelled_after_pack: int
    # Phase 2 (02 §6.2 API-32 mở rộng, §6.5 #9) — theo ngày (giờ VN) trừ khi ghi "hiện tại".
    returns_received: int  # phiên hoàn COMPLETED trong ngày (không tính kiện tạm)
    returns_received_issue: int  # trong đó kết luận ≠ Nguyên vẹn
    returns_unidentified: int  # phiên hoàn COMPLETED trong ngày trên kiện tạm (chưa xác định đơn)
    returns_expected: int  # hiện tại: hồ sơ EXPECTED + PARTIALLY_RECEIVED
    returns_missing: int  # hiện tại: hồ sơ MISSING
    recon_open: ReconOpen  # hiện tại
    claims_open: int  # hiện tại: hồ sơ khiếu nại chưa CLOSED
    claims_due_soon: int  # hiện tại: NEW / SUBMITTED / WAITING, hạn trong `claim_due_soon_hours`
    label_on_tray: int  # phiên PACK COMPLETED kết thúc trong ngày có cờ LABEL_ON_TRAY
    cam2_unverified: int  # ... có cờ CAM2_UNVERIFIED
    # Phase 3 (02 §6.2 API-32 — T-215), hiện tại (không theo ngày).
    returns_dropped_7d: int = 0  # phiên RETURN hủy / bỏ dở 7 ngày, trừ phiên bị loại (BR-39)
    refund_only_pending: int = 0  # BR-40
    claims_overdue_unsent: int = 0  # BR-42: hồ sơ NEW quá hạn


class CameraBrief(BaseModel):
    role: str
    status: str


class StationDaily(BaseModel):
    id: uuid.UUID
    name: str
    state: str
    cameras: list[CameraBrief]
    last_scan_at: datetime | None
    tracking_number: str | None  # thêm cho FE DEC-72 ("Đang đóng gói SPX…")
    work_mode: str  # Phase 2: PACK | RETURN
    operator_name: str | None


class DailyOut(BaseModel):
    date: date
    counts: Counts
    stations: list[StationDaily]
    attention: list[dict[str, Any]]


def today(tz: str) -> date:
    return clock.now().astimezone(ZoneInfo(tz)).date()


def _bounds(day: date, tz: str) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))
    return start, start + timedelta(days=1)


async def _count(db: AsyncSession, *where: Any) -> int:
    return int(await db.scalar(select(func.count()).select_from(PackSession).where(*where)) or 0)


async def _counts(db: AsyncSession, start: datetime, end: datetime) -> Counts:
    ended_in = and_(PackSession.ended_at >= start, PackSession.ended_at < end)
    started_in = and_(PackSession.started_at >= start, PackSession.started_at < end)
    is_pack = PackSession.type == "PACK"  # số Phase 1 chỉ tính phiên đóng gói (phiên hoàn có số riêng)
    returned = (
        await db.execute(
            select(
                func.count().filter(Package.is_placeholder.is_(False)),
                func.count().filter(
                    Package.is_placeholder.is_(False), PackSession.inspection_conclusion != "OK"
                ),
                func.count().filter(Package.is_placeholder.is_(True)),
            )
            .select_from(PackSession)
            .join(Package, Package.id == PackSession.package_id)
            .where(PackSession.type == "RETURN", PackSession.status == "COMPLETED", ended_in)
        )
    ).one()
    cases = dict(
        (
            await db.execute(
                select(ReturnCase.status, func.count())
                .where(ReturnCase.status.in_(("EXPECTED", "PARTIALLY_RECEIVED", "MISSING")))
                .group_by(ReturnCase.status)
            )
        ).all()
    )
    cfg = await settings_service.get(db)
    now = clock.now()
    claims_open, claims_due_soon = (
        await db.execute(
            select(
                func.count().filter(Claim.status != "CLOSED"),
                func.count().filter(
                    Claim.status.in_(DUE_STATUSES),
                    Claim.deadline_at >= now,
                    Claim.deadline_at <= now + timedelta(hours=cfg.claim_due_soon_hours),
                ),
            ).select_from(Claim)
        )
    ).one()
    packages = dict(
        (
            await db.execute(
                select(Package.warehouse_status, func.count())
                .where(Package.warehouse_status.in_(("PACKED", "CANCELLED_AFTER_PACK")))
                .group_by(Package.warehouse_status)
            )
        ).all()
    )
    pack_done = and_(is_pack, PackSession.status == "COMPLETED", ended_in)
    overdue_unsent = int(
        await db.scalar(
            select(func.count()).select_from(Claim).where(Claim.status == "NEW", Claim.deadline_at < now)
        )
        or 0
    )
    return Counts(
        packed=await _count(db, PackSession.status == "COMPLETED", ended_in, is_pack),
        had_mismatch=await _count(
            db, started_in, literal("HAD_MISMATCH") == any_(PackSession.flags), is_pack
        ),
        abandoned=await _count(db, PackSession.status == "ABANDONED", ended_in, is_pack),
        cancelled=await _count(db, PackSession.status == "CANCELLED", ended_in, is_pack),
        packed_not_handed_over=int(packages.get("PACKED", 0)),
        cancelled_after_pack=int(packages.get("CANCELLED_AFTER_PACK", 0)),
        returns_received=int(returned[0]),
        returns_received_issue=int(returned[1]),
        returns_unidentified=int(returned[2]),
        returns_expected=int(cases.get("EXPECTED", 0)) + int(cases.get("PARTIALLY_RECEIVED", 0)),
        returns_missing=int(cases.get("MISSING", 0)),
        recon_open=ReconOpen(**(await recon.summary(db)).model_dump()),
        claims_open=int(claims_open),
        claims_due_soon=int(claims_due_soon),
        label_on_tray=await _count(db, pack_done, literal("LABEL_ON_TRAY") == any_(PackSession.flags)),
        cam2_unverified=await _count(db, pack_done, literal("CAM2_UNVERIFIED") == any_(PackSession.flags)),
        returns_dropped_7d=await _count(
            db, dropped_return_filter(), PackSession.ended_at >= now - RECENT_RETURN_WINDOW
        ),
        refund_only_pending=int(
            await db.scalar(select(func.count()).select_from(ReturnCase).where(refund_pending_filter())) or 0
        ),
        claims_overdue_unsent=overdue_unsent,
    )


async def _stations(db: AsyncSession) -> list[StationDaily]:
    stations = (
        await db.scalars(select(Station).where(Station.is_active.is_(True)).order_by(Station.name))
    ).all()
    cams = (await db.scalars(select(Camera).order_by(Camera.role))).all()
    active = {
        s.station_id: ("INSPECTING" if s.type == "RETURN" and s.status == "OPEN" else s.status, code)
        for s, code in (
            await db.execute(
                select(PackSession, Package.tracking_number)
                .join(Package, Package.id == PackSession.package_id)
                .where(PackSession.status.in_(ACTIVE_STATUSES))
            )
        ).all()
    }
    pending = set(
        (
            await db.scalars(select(ApprovalRequest.station_id).where(ApprovalRequest.status == "PENDING"))
        ).all()
    )
    last: dict[uuid.UUID, datetime | None] = dict(
        (
            await db.execute(
                select(
                    PackSession.station_id,
                    func.greatest(func.max(PackSession.started_at), func.max(PackSession.ended_at)),
                ).group_by(PackSession.station_id)
            )
        ).all()
    )
    out = []
    for st in stations:
        status, code = active.get(st.id, (None, None))
        state = "WAITING_APPROVAL" if st.id in pending else _STATE.get(status or "", "READY")
        out.append(
            StationDaily(
                id=st.id,
                name=st.name,
                state=state,
                cameras=[CameraBrief(role=c.role, status=c.status) for c in cams if c.station_id == st.id],
                last_scan_at=last.get(st.id),
                tracking_number=code,
                work_mode=st.work_mode,
                operator_name=st.operator_name,
            )
        )
    return out


def disk_usage(settings: Settings) -> dict[str, int] | None:
    try:
        usage = shutil.disk_usage(settings.video_root)
    except OSError:
        return None
    percent = round(usage.used * 100 / usage.total) if usage.total else 0
    return {"total_bytes": usage.total, "used_bytes": usage.used, "percent": percent}


async def _attention(db: AsyncSession, counts: Counts, settings: Settings) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if counts.cancelled_after_pack:
        items.append({"kind": "CANCELLED_AFTER_PACK", "count": counts.cancelled_after_pack})
    rows = (
        await db.execute(
            select(Camera, Station.name)
            .join(Station, Station.id == Camera.station_id)
            .where(Station.is_active.is_(True))
            .order_by(Station.name, Camera.role)
        )
    ).all()
    for cam, station_name in rows:
        if cam.status == "OFFLINE":
            items.append({"kind": "CAMERA_OFFLINE", "camera_id": str(cam.id), "station_name": station_name,
                          "role": cam.role})  # fmt: skip
    for cam, station_name in rows:
        if cam.clock_offset_ms is not None and abs(cam.clock_offset_ms) > CLOCK_DRIFT_MS:
            items.append({"kind": "CLOCK_DRIFT", "camera_id": str(cam.id), "offset_ms": cam.clock_offset_ms,
                          "station_name": station_name, "role": cam.role})  # fmt: skip
    pending = await db.scalar(
        select(func.count()).select_from(ApprovalRequest).where(ApprovalRequest.status == "PENDING")
    )
    if pending:
        items.append({"kind": "APPROVAL_PENDING", "count": int(pending)})
    for shop in (await db.scalars(select(Shop).where(Shop.last_error.is_not(None)))).all():
        at = (shop.last_error or {}).get("at") or (
            clock.iso_z(shop.last_synced_at) if shop.last_synced_at else None
        )
        items.append({"kind": "SYNC_ERROR", "shop_id": str(shop.id), "at": at, "shop_name": shop.name,
                      "platform": shop.platform, "code": (shop.last_error or {}).get("code")})  # fmt: skip
    failed = await db.scalar(
        select(func.count())
        .select_from(Clip)
        .where(Clip.status == "FAILED", Clip.created_at >= clock.now() - timedelta(days=7))
    )
    if failed:  # 02a J-01 "lỗi cuối → attention" (kind mới, DEC-105)
        items.append({"kind": "CLIP_FAILED", "count": int(failed)})
    items.extend(await _return_attention(db, counts, settings))
    from aicam.modules.backup import service as backup_service  # backup → settings → reports: import muộn

    items.extend(await backup_service.stale_attention(db, settings))  # chỉ ADMIN (ADMIN_ONLY_KINDS)
    disk = disk_usage(settings)
    if disk and disk["percent"] >= DISK_WARN_PERCENT:
        items.append({"kind": "DISK_USAGE", "percent": disk["percent"]})
    return items


async def _return_attention(db: AsyncSession, counts: Counts, settings: Settings) -> list[dict[str, Any]]:
    """Phase 2 (02 §6.2 API-32, §6.3 #13, §6.5 #1): mục "Cần xử lý" hàng hoàn / đối soát / hồ sơ.

    Phase 3 (T-215): `RETURN_SESSION_ABANDONED` chỉ còn phiên RETURN mở có cờ `AUTO_CLOSE_BLOCKED`; phiên
    hủy / bỏ dở 7 ngày chuyển sang `RETURN_SESSION_DROPPED` (BR-39 — trừ phiên bị loại); thêm
    `REFUND_ONLY_PENDING` (BR-40), `CLAIM_OVERDUE` (BR-42)."""
    since = clock.now() - RECENT_RETURN_WINDOW
    blocked = int(
        await db.scalar(
            select(func.count())
            .select_from(PackSession)
            .where(
                PackSession.type == "RETURN",
                # G3 J-07 (DEC-340): quá hạn bỏ dở nhưng kết luận đã lưu chưa đủ → giữ phiên, cần quản lý xem.
                PackSession.status.in_(("OPEN", "MISMATCH")),
                PackSession.flags.contains(["AUTO_CLOSE_BLOCKED"]),
            )
        )
        or 0
    )
    unidentified, force_new = (
        await db.execute(
            select(
                func.count(),
                func.count().filter(ReturnCase.manual_link_only.is_(True), ReturnCase.created_at >= since),
            ).where(
                ReturnCase.kind == "UNIDENTIFIED",
                ReturnCase.order_id.is_(None),
                ReturnCase.status != "CANCELLED",
            )
        )
    ).one()
    candidates = (
        ("RETURN_MISSING", counts.returns_missing),
        ("RECON_HIGH", counts.recon_open.HIGH),
        ("CLAIM_DUE_SOON", counts.claims_due_soon),
        ("RETURN_UNIDENTIFIED", int(unidentified)),
        ("RETURN_SESSION_ABANDONED", blocked),
        ("RETURN_FORCE_NEW", int(force_new)),
        ("CLAIM_OVERDUE", counts.claims_overdue_unsent),
        ("RETURN_SESSION_DROPPED", counts.returns_dropped_7d),
    )
    items: list[dict[str, Any]] = [{"kind": kind, "count": count} for kind, count in candidates if count]
    if counts.refund_only_pending:
        hours = (await settings_service.get(db)).refund_only_default_hours
        nearest = await db.scalar(
            select(func.min(response_due_sql(hours))).select_from(ReturnCase).where(refund_pending_filter())
        )
        items.append(
            {
                "kind": "REFUND_ONLY_PENDING",
                "count": counts.refund_only_pending,
                "nearest_due_at": clock.iso_z(nearest) if nearest else None,
            }
        )
    return items


# 02 §6.2 API-32: mục chỉ ADMIN thấy (lọc theo vai **sau** cache).
ADMIN_ONLY_KINDS = frozenset({"SYNC_ERROR", "BACKUP_STALE"})


def for_role(out: "DailyOut", role: str) -> "DailyOut":
    if role == "ADMIN":
        return out
    return out.model_copy(
        update={"attention": [a for a in out.attention if a["kind"] not in ADMIN_ONLY_KINDS]}
    )


async def daily(db: AsyncSession, day: date | None, settings: Settings) -> DailyOut:
    tz = settings.tz_display
    day = day or today(tz)
    if day > today(tz):
        raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422,
                       {"fields": {"date": "Không chọn ngày trong tương lai"}})  # fmt: skip
    key = daily_report_key(day.isoformat())
    redis = get_redis()
    cached = await redis.get(key)
    if cached:
        return DailyOut.model_validate_json(cached)
    start, end = _bounds(day, tz)
    counts = await _counts(db, start, end)
    out = DailyOut(
        date=day,
        counts=counts,
        stations=await _stations(db),
        attention=await _attention(db, counts, settings),
    )
    await redis.set(key, out.model_dump_json(), ex=CACHE_TTL_S)
    return out
