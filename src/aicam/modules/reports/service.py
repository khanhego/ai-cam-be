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
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package, Shop
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession
from aicam.modules.stations.models import Camera, Station
from aicam.realtime.publish import daily_report_key

CACHE_TTL_S = 5
DISK_WARN_PERCENT = 80  # NFR-30 / 02a §10 `aicam_disk_used_ratio` > 0.8
CLOCK_DRIFT_MS = 1000  # BR-15
_STATE = {"OPEN": "PACKING", "MISMATCH": "MISMATCH", "WAITING_APPROVAL": "WAITING_APPROVAL"}


class Counts(BaseModel):
    packed: int
    had_mismatch: int
    abandoned: int
    cancelled: int
    packed_not_handed_over: int
    cancelled_after_pack: int


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
    packages = dict(
        (
            await db.execute(
                select(Package.warehouse_status, func.count())
                .where(Package.warehouse_status.in_(("PACKED", "CANCELLED_AFTER_PACK")))
                .group_by(Package.warehouse_status)
            )
        ).all()
    )
    return Counts(
        packed=await _count(db, PackSession.status == "COMPLETED", ended_in),
        had_mismatch=await _count(db, started_in, literal("HAD_MISMATCH") == any_(PackSession.flags)),
        abandoned=await _count(db, PackSession.status == "ABANDONED", ended_in),
        cancelled=await _count(db, PackSession.status == "CANCELLED", ended_in),
        packed_not_handed_over=int(packages.get("PACKED", 0)),
        cancelled_after_pack=int(packages.get("CANCELLED_AFTER_PACK", 0)),
    )


async def _stations(db: AsyncSession) -> list[StationDaily]:
    stations = (
        await db.scalars(select(Station).where(Station.is_active.is_(True)).order_by(Station.name))
    ).all()
    cams = (await db.scalars(select(Camera).order_by(Camera.role))).all()
    active = {
        s.station_id: (s.status, code)
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
            shop.last_synced_at.isoformat() if shop.last_synced_at else None
        )
        items.append({"kind": "SYNC_ERROR", "shop_id": str(shop.id), "at": at})
    failed = await db.scalar(
        select(func.count())
        .select_from(Clip)
        .where(Clip.status == "FAILED", Clip.created_at >= clock.now() - timedelta(days=7))
    )
    if failed:  # 02a J-01 "lỗi cuối → attention" (kind mới, DEC-105)
        items.append({"kind": "CLIP_FAILED", "count": int(failed)})
    disk = disk_usage(settings)
    if disk and disk["percent"] >= DISK_WARN_PERCENT:
        items.append({"kind": "DISK_USAGE", "percent": disk["percent"]})
    return items


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
