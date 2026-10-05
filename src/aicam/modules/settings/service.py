"""Bảng `setting` một dòng (02a §3), API-80 cài đặt, API-81 sức khỏe hệ thống."""

import asyncio
from typing import Any

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from aicam.core import audit
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.modules.settings.models import Setting
from aicam.modules.settings.schemas import (
    CameraHealth,
    DiskOut,
    HealthOut,
    SettingsIn,
    SettingsOut,
    SyncHealth,
)

log = structlog.get_logger()

FIELDS = ("retention_raw_days", "retention_clip_days", "session_warn_minutes", "session_abandon_minutes")


async def get(session: AsyncSession) -> Setting:
    row = await session.get(Setting, 1)
    if row is None:  # migration luôn seed; phòng DB test bị xóa tay
        row = Setting(id=1)
        session.add(row)
        await session.flush()
    return row


def to_out(row: Setting) -> SettingsOut:
    return SettingsOut(**{f: getattr(row, f) for f in FIELDS}, updated_at=row.updated_at)


async def update(session: AsyncSession, data: SettingsIn, p: Principal) -> SettingsOut:
    """API-80 PUT: ràng buộc chéo theo 02 (clip ≥ thô; bỏ dở > cảnh báo). Áp ngay cho J-02, J-07 (DEC-30)."""
    fields: dict[str, str] = {}
    if data.retention_clip_days < data.retention_raw_days:
        fields["retention_clip_days"] = "Số ngày giữ clip phải lớn hơn hoặc bằng video thô."
    if data.session_abandon_minutes <= data.session_warn_minutes:
        fields["session_abandon_minutes"] = "Thời gian bỏ dở phải lớn hơn thời gian cảnh báo."
    if fields:
        raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})
    row = await session.scalar(select(Setting).where(Setting.id == 1).with_for_update())
    if row is None:
        row = await get(session)
    before = {f: getattr(row, f) for f in FIELDS}
    for f in FIELDS:
        setattr(row, f, getattr(data, f))
    audit.record(session, "SETTINGS_UPDATE", user_id=p.user_id, object_type="SETTING", object_id="1", ip=p.ip,
                 data={"before": before, "after": data.model_dump()})  # fmt: skip
    await session.flush()
    await session.refresh(row)
    out = to_out(row)
    await commit(session)
    return out


async def _check(coro: Any, limit_s: float = 3.0) -> str:
    try:
        await asyncio.wait_for(coro, limit_s)
    except Exception as exc:  # thành phần lỗi không làm hỏng cả API-81
        log.warning("health_component_failed", error=str(exc))
        return "ERROR"
    return "OK"


async def _ping_db(session: AsyncSession) -> None:
    """Ping DB trên connection riêng (G3-N11): `wait_for` hủy giữa câu lệnh trên session dùng chung làm hỏng
    transaction của các truy vấn sau trong API-81."""
    bind = session.bind
    engine = bind.engine if isinstance(bind, AsyncConnection) else bind
    if not isinstance(engine, AsyncEngine):
        raise TypeError("Session chưa gắn engine")
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def health(session: AsyncSession, *, mediamtx_check: Any, disk: dict[str, int] | None) -> HealthOut:
    """API-81: DB, Redis, MediaMTX, ổ đĩa video, camera, đồng bộ sàn. Luôn 200, từng phần OK / ERROR."""
    from aicam.modules.orders.models import Shop
    from aicam.modules.stations.models import Camera, Station

    db_status = await _check(_ping_db(session))
    redis_status = await _check(get_redis().ping())
    mediamtx_status = await _check(mediamtx_check)
    cameras: list[CameraHealth] = []
    sync: list[SyncHealth] = []
    if db_status == "OK":
        rows = (
            await session.execute(
                select(Camera, Station.name)
                .join(Station, Station.id == Camera.station_id)
                .order_by(Station.name, Camera.role)
            )
        ).all()
        cameras = [
            CameraHealth(
                id=c.id,
                station_name=name,
                role=c.role,
                status=c.status,
                clock_offset_ms=c.clock_offset_ms,
                last_seen_at=c.last_seen_at,
            )
            for c, name in rows
        ]
        sync = [
            SyncHealth(shop_id=s.id, last_success_at=s.last_synced_at, last_error=s.last_error)
            for s in (await session.scalars(select(Shop).order_by(Shop.created_at))).all()
        ]
    return HealthOut(
        db=db_status,
        redis=redis_status,
        mediamtx=mediamtx_status,
        disk=DiskOut(**disk) if disk else None,
        cameras=cameras,
        sync=sync,
    )
