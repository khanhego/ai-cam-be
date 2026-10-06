"""API-100 đổi chế độ bàn, API-101 người kiểm hàng hoàn (02 §6.2, FR-01.07, FR-04.10, UC-14, BR-28).

Dưới khóa station (DEC-11) — tuần tự với quét; đọc lại station sau khóa (`populate_existing`).
"""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.core.db import commit
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.sessions.schemas import StationStateOut
from aicam.modules.sessions.service import _publish_state_after_commit, build_state, lock_station
from aicam.modules.stations import service as stations
from aicam.modules.stations.models import Station

OPERATOR_MIN, OPERATOR_MAX = 2, 40


async def _locked(session: AsyncSession, station_id: uuid.UUID) -> Station:
    await lock_station(session, station_id)
    station = await session.scalar(
        select(Station).where(Station.id == station_id).execution_options(populate_existing=True)
    )
    if station is None:  # require_station đã kiểm trước đó
        raise AppError("FORBIDDEN", "Tài khoản không gắn station.", 403)
    return station


async def _ensure_idle(session: AsyncSession, station_id: uuid.UUID) -> None:
    if await stations.is_busy(session, station_id):
        raise AppError("SESSION_ACTIVE", "Đóng phiên trước khi đổi.", 409)


async def _finish(session: AsyncSession, station: Station, settings: Settings) -> StationStateOut:
    await session.flush()
    state = await build_state(session, station, settings)
    _publish_state_after_commit(session, station.id, state)
    await commit(session)
    return state


async def set_work_mode(
    session: AsyncSession,
    station: Station,
    work_mode: str,
    *,
    actor: uuid.UUID,
    ip: str | None,
    settings: Settings,
) -> StationStateOut:
    """API-100: chỉ station loại BOTH; không có phiên hoạt động / yêu cầu duyệt chờ."""
    station = await _locked(session, station.id)
    if station.kind != "BOTH":
        raise AppError("MODE_NOT_ALLOWED", "Station này không đổi được chế độ.", 409, {"kind": station.kind})
    await _ensure_idle(session, station.id)
    if station.work_mode != work_mode:
        audit.record(session, "STATION_WORK_MODE", user_id=actor, object_type="STATION", object_id=station.id,
                     ip=ip, data={"old": station.work_mode, "new": work_mode})  # fmt: skip
        station.work_mode = work_mode
    return await _finish(session, station, settings)


async def set_operator(
    session: AsyncSession,
    station: Station,
    name: str,
    *,
    actor: uuid.UUID,
    ip: str | None,
    settings: Settings,
) -> StationStateOut:
    """API-101: đặt / đổi tên người kiểm (strip, 2–40); không đổi khi có phiên hoạt động."""
    clean = " ".join(name.split())
    if not OPERATOR_MIN <= len(clean) <= OPERATOR_MAX:
        raise AppError(
            "VALIDATION_ERROR",
            "Dữ liệu không hợp lệ.",
            422,
            {"fields": {"name": f"Nhập tên người kiểm {OPERATOR_MIN}–{OPERATOR_MAX} ký tự"}},
        )
    station = await _locked(session, station.id)
    await _ensure_idle(session, station.id)
    if station.operator_name != clean:
        audit.record(session, "STATION_OPERATOR", user_id=actor, object_type="STATION", object_id=station.id,
                     ip=ip, data={"old": station.operator_name, "new": clean})  # fmt: skip
        station.operator_name = clean
    return await _finish(session, station, settings)
