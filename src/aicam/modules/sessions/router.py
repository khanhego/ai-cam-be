"""API-10, API-11, API-100, API-101 — 02 §6.2."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.platforms.base import PlatformAdapter
from aicam.modules.platforms.router import get_platform_adapter
from aicam.modules.sessions import service, station_config
from aicam.modules.sessions.schemas import (
    CancelIn,
    OperatorIn,
    RecentOut,
    ScanIn,
    ScanOut,
    StateOnlyOut,
    StationStateOut,
    WorkModeIn,
)

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
StationOnly = Annotated[Principal, Depends(require_roles("STATION"))]


router = APIRouter(prefix="/station", tags=["station"])


@router.get("/state", response_model=StationStateOut)
async def station_state(p: StationOnly, db: DbSession, settings: AppSettings) -> StationStateOut:
    station = await service.require_station(db, p.station_id, p.user_id)
    return await service.build_state(db, station, settings)


@router.post("/scan", response_model=ScanOut)
async def scan(
    body: ScanIn,
    p: StationOnly,
    db: DbSession,
    settings: AppSettings,
    adapter: Annotated[PlatformAdapter, Depends(get_platform_adapter)],
) -> ScanOut:
    station = await service.require_station(db, p.station_id, p.user_id)
    return await service.scan(
        db, station, code=body.code, client_scan_id=body.client_scan_id, adapter=adapter, settings=settings
    )


@router.post("/sessions/{session_id}/cancel", response_model=StateOnlyOut)
async def cancel_session(
    session_id: uuid.UUID, body: CancelIn, p: StationOnly, db: DbSession, settings: AppSettings
) -> StateOnlyOut:
    station = await service.require_station(db, p.station_id, p.user_id)
    state = await service.cancel(
        db, station, session_id, reason=body.reason, note=body.note, settings=settings
    )
    return StateOnlyOut(state=state)


@router.get("/sessions/recent", response_model=RecentOut)
async def recent_sessions(p: StationOnly, db: DbSession, settings: AppSettings) -> RecentOut:
    station = await service.require_station(db, p.station_id, p.user_id)
    return await service.recent(db, station, settings)


@router.put("/work-mode", response_model=StateOnlyOut)
async def set_work_mode(
    body: WorkModeIn, p: StationOnly, db: DbSession, settings: AppSettings
) -> StateOnlyOut:
    """API-100: đổi chế độ bàn (station loại "Cả hai") — FR-01.07."""
    station = await service.require_station(db, p.station_id, p.user_id)
    state = await station_config.set_work_mode(
        db, station, body.work_mode, actor=p.user_id, ip=p.ip, settings=settings
    )
    return StateOnlyOut(state=state)


@router.put("/operator", response_model=StateOnlyOut)
async def set_operator(
    body: OperatorIn, p: StationOnly, db: DbSession, settings: AppSettings
) -> StateOnlyOut:
    """API-101: tên người kiểm hàng hoàn — FR-04.10, BR-28."""
    station = await service.require_station(db, p.station_id, p.user_id)
    state = await station_config.set_operator(
        db, station, body.name, actor=p.user_id, ip=p.ip, settings=settings
    )
    return StateOnlyOut(state=state)
