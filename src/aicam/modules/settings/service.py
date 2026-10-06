"""Bảng `setting` một dòng (02a §3), API-80 cài đặt, API-81 sức khỏe hệ thống."""

import asyncio
from typing import Any

import structlog
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from aicam.core import audit
from aicam.core.db import after_commit, commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.settings import Settings
from aicam.modules.settings.models import Setting
from aicam.modules.settings.schemas import (
    THRESHOLD_FIELDS,
    CameraHealth,
    DiskOut,
    HealthOut,
    RetentionImpactOut,
    SettingsIn,
    SettingsOut,
    SyncHealth,
)

log = structlog.get_logger()

FIELDS = ("retention_raw_days", "retention_clip_days", "session_warn_minutes", "session_abandon_minutes")
ALL_FIELDS = (*FIELDS, *THRESHOLD_FIELDS)


async def get(session: AsyncSession) -> Setting:
    row = await session.get(Setting, 1)
    if row is None:  # migration luôn seed; phòng DB test bị xóa tay
        row = Setting(id=1)
        session.add(row)
        await session.flush()
    return row


def to_out(row: Setting, settings: Settings) -> SettingsOut:
    return SettingsOut(
        **{f: getattr(row, f) for f in ALL_FIELDS},
        packer_name_required=row.packer_name_required,
        retention_clip_min_days=settings.retention_clip_min_days,
        updated_at=row.updated_at,
    )


async def retention_impact(
    session: AsyncSession, raw_days: int, clip_days: int, settings: Settings
) -> RetentionImpactOut:
    """API-82 (FR-02.10): số clip / giờ video thô lần dọn kế tiếp sẽ xóa nếu đổi retention."""
    from aicam.modules.media import service as media  # media → settings: import muộn tránh vòng

    return RetentionImpactOut(**await media.retention_impact(session, raw_days, clip_days, settings))


async def update(session: AsyncSession, data: SettingsIn, p: Principal, settings: Settings) -> SettingsOut:
    """API-80 PUT (02 §6.2, 02a §4): 4 trường Phase 1 bắt buộc + 6 ngưỡng tùy chọn (thiếu = giữ cũ).

    Khóa dòng setting → ràng buộc chéo (clip ≥ thô; bỏ dở > cảnh báo, cả phiên hoàn) → 422 `VALIDATION_ERROR`;
    clip < sàn `RETENTION_CLIP_MIN_DAYS` → 422 `RETENTION_BELOW_MINIMUM` (`details.min`); giảm số ngày giữ
    clip / video thô mà chưa `confirm_reduction` → 409 `RETENTION_REDUCTION_UNCONFIRMED` (`details.impact`
    như API-82); đã xác nhận → lưu + audit `RETENTION_REDUCED` (cũ, mới, impact). Áp ngay cho J-02, J-07,
    J-14 (DEC-30)."""
    row = await session.scalar(
        select(Setting).where(Setting.id == 1).with_for_update().execution_options(populate_existing=True)
    )
    if row is None:
        row = await get(session)
    after: dict[str, int] = {f: getattr(data, f) for f in FIELDS}
    for f in THRESHOLD_FIELDS:
        value = getattr(data, f)
        after[f] = value if value is not None else getattr(row, f)
    fields: dict[str, str] = {}
    if after["retention_clip_days"] < after["retention_raw_days"]:
        fields["retention_clip_days"] = "Số ngày giữ clip phải lớn hơn hoặc bằng video thô."
    if after["session_abandon_minutes"] <= after["session_warn_minutes"]:
        fields["session_abandon_minutes"] = "Thời gian bỏ dở phải lớn hơn thời gian cảnh báo."
    if after["return_abandon_minutes"] <= after["return_warn_minutes"]:
        fields["return_abandon_minutes"] = "Thời gian tự đóng phải lớn hơn thời gian cảnh báo."
    if fields:
        raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})
    minimum = settings.retention_clip_min_days
    if after["retention_clip_days"] < minimum:
        message = f"Số ngày giữ clip không được thấp hơn {minimum}."
        raise AppError(
            "RETENTION_BELOW_MINIMUM",
            message,
            422,
            {"min": minimum, "fields": {"retention_clip_days": message}},
        )
    before = {f: getattr(row, f) for f in ALL_FIELDS}
    reduced = (
        after["retention_clip_days"] < row.retention_clip_days
        or after["retention_raw_days"] < row.retention_raw_days
    )
    impact: RetentionImpactOut | None = None
    if reduced:
        impact = await retention_impact(
            session, after["retention_raw_days"], after["retention_clip_days"], settings
        )
        if not data.confirm_reduction:
            raise AppError(
                "RETENTION_REDUCTION_UNCONFIRMED",
                "Giảm thời gian lưu cần xác nhận.",
                409,
                {"impact": impact.model_dump(mode="json")},
            )
    for f, value in after.items():
        setattr(row, f, value)
    flags_before = {"packer_name_required": row.packer_name_required}
    flags_after = {
        "packer_name_required": (
            row.packer_name_required if data.packer_name_required is None else data.packer_name_required
        )
    }
    row.packer_name_required = flags_after["packer_name_required"]
    audit.record(session, "SETTINGS_UPDATE", user_id=p.user_id, object_type="SETTING", object_id="1", ip=p.ip,
                 data={"before": {**before, **flags_before}, "after": {**after, **flags_after}})  # fmt: skip
    if flags_before != flags_after:
        after_commit(session, lambda: _publish_station_states(session, settings))
    if impact is not None:
        audit.record(
            session, "RETENTION_REDUCED", user_id=p.user_id, object_type="SETTING", object_id="1", ip=p.ip,
            data={
                "before": {k: before[k] for k in ("retention_raw_days", "retention_clip_days")},
                "after": {k: after[k] for k in ("retention_raw_days", "retention_clip_days")},
                "impact": impact.model_dump(mode="json"),
            },
        )  # fmt: skip
    await session.flush()
    await session.refresh(row)
    out = to_out(row, settings)
    await commit(session)
    return out


async def _publish_station_states(session: AsyncSession, settings: Settings) -> None:
    """`packer_name_required` đổi → WS `station.state` cho mọi station đang bật (02 API-80, FR-03.16)."""
    from aicam.modules.sessions.service import publish_state  # sessions → settings: import muộn tránh vòng
    from aicam.modules.stations.models import Station

    ids = (await session.scalars(select(Station.id).where(Station.is_active.is_(True)))).all()
    for station_id in ids:
        try:
            await publish_state(session, station_id, settings)
        except Exception:  # một station lỗi không chặn station khác; state tới ở lần đổi kế tiếp / poll
            log.exception("publish_station_state_failed", station_id=str(station_id))
    await session.commit()


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
