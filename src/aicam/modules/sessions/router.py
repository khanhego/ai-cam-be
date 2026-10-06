"""API-10, API-11, API-100, API-101 — 02 §6.2."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.platforms.base import PlatformAdapter
from aicam.modules.platforms.router import get_platform_adapter
from aicam.modules.sessions import return_lookup, service, station_config
from aicam.modules.sessions.schemas import (
    CancelIn,
    InspectionIn,
    InspectionSavedOut,
    OperatorIn,
    RecentOut,
    ReturnLookupOut,
    ReturnSessionIn,
    ScanIn,
    ScanOut,
    SnapshotCreatedOut,
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


@router.get("/return-lookup", response_model=ReturnLookupOut)
async def return_lookup_api(
    p: StationOnly,
    db: DbSession,
    settings: AppSettings,
    adapter: Annotated[PlatformAdapter, Depends(get_platform_adapter)],
    q: Annotated[str, Query(max_length=64)] = "",
) -> ReturnLookupOut:
    """API-104: tìm kiện hoàn thủ công (FR-04.07)."""
    station = await service.require_station(db, p.station_id, p.user_id)
    return await return_lookup.lookup(db, station, q, adapter, settings)


@router.put("/sessions/{session_id}/inspection", response_model=InspectionSavedOut)
async def save_inspection(
    session_id: uuid.UUID, body: InspectionIn, p: StationOnly, db: DbSession, settings: AppSettings
) -> InspectionSavedOut:
    """API-102: lưu kết luận + dòng kiểm phiên hoàn (FR-04.03, 04.09; BR-22)."""
    station = await service.require_station(db, p.station_id, p.user_id)
    return await service.save_inspection(db, station, session_id, body, settings)


@router.post("/sessions/{session_id}/snapshots", response_model=SnapshotCreatedOut, status_code=201)
async def take_snapshot(
    session_id: uuid.UUID, p: StationOnly, db: DbSession, settings: AppSettings
) -> SnapshotCreatedOut:
    """API-103: chụp ảnh Cam 1 (server lấy khung từ relay) cho phiên hoàn đang mở (FR-04.04)."""
    station = await service.require_station(db, p.station_id, p.user_id)
    return await service.take_snapshot(db, station, session_id, settings)


@router.post("/return-sessions", response_model=ScanOut)
async def open_return_session(
    body: ReturnSessionIn, p: StationOnly, db: DbSession, settings: AppSettings
) -> ScanOut:
    """API-105: mở phiên hoàn từ kết quả tìm / mở phiên chưa xác định (FR-04.07, 04.13)."""
    station = await service.require_station(db, p.station_id, p.user_id)
    return await service.open_return_by_request(
        db, station, body, actor=p.user_id, ip=p.ip, settings=settings
    )
