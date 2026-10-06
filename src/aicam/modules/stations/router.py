"""API-60..65 — 02 §6."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Response
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.errors import AppError
from aicam.core.settings import Settings, get_settings
from aicam.modules.stations import service
from aicam.modules.stations.mediamtx import HttpMediaMTX, MediaMTX
from aicam.modules.stations.schemas import (
    CameraIn,
    CameraOut,
    CameraRole,
    CameraTestOut,
    LiveOut,
    Roi,
    StationCreateIn,
    StationList,
    StationOut,
    StationPatchIn,
)

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
AdminOnly = Annotated[Principal, Depends(require_roles("ADMIN"))]
AdminOrSupervisor = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]


def get_mediamtx(settings: AppSettings) -> MediaMTX:
    return HttpMediaMTX(settings.mediamtx_api_url)


router = APIRouter(tags=["stations"])


@router.get("/stations", response_model=StationList)
async def list_stations(_: AdminOnly, db: DbSession) -> StationList:
    return StationList(items=await service.list_stations(db))


@router.get("/stations/{station_id}", response_model=StationOut)
async def get_station(station_id: uuid.UUID, _: AdminOnly, db: DbSession) -> StationOut:
    station = await service.get_station(db, station_id)
    if station is None:
        raise AppError("NOT_FOUND", "Không tìm thấy station.", 404)
    return await service.station_out(db, station)


@router.post("/stations", response_model=StationOut, status_code=201)
async def create_station(body: StationCreateIn, p: AdminOnly, db: DbSession) -> StationOut:
    return await service.create_station(db, body, p.user_id, p.ip)


@router.patch("/stations/{station_id}", response_model=StationOut)
async def patch_station(
    station_id: uuid.UUID, body: StationPatchIn, p: AdminOnly, db: DbSession, settings: AppSettings
) -> StationOut:
    out, mode_changed = await service.patch_station(db, station_id, body, p.user_id, p.ip)
    if mode_changed:  # station đang mở màn hình chuyển S1 ⇄ R1 ngay (WS-01)
        from aicam.modules.sessions.service import publish_state

        await publish_state(db, station_id, settings)
    return out


@router.put("/stations/{station_id}/cameras/{role}", response_model=CameraOut)
async def set_camera(
    station_id: uuid.UUID,
    role: CameraRole,
    body: CameraIn,
    p: AdminOnly,
    db: DbSession,
    settings: AppSettings,
    mediamtx: Annotated[MediaMTX, Depends(get_mediamtx)],
) -> CameraOut:
    return await service.set_camera(
        db, station_id, role, body, mediamtx=mediamtx, settings=settings, actor=p.user_id, ip=p.ip
    )


@router.post("/cameras/test", response_model=CameraTestOut)
async def probe_camera(body: CameraIn, _: AdminOnly) -> CameraTestOut:
    return await service.probe_camera(body)


@router.get("/cameras/{camera_id}/snapshot", response_class=Response)
async def snapshot(
    camera_id: uuid.UUID, _: AdminOrSupervisor, db: DbSession, settings: AppSettings
) -> Response:
    return Response(await service.snapshot(db, camera_id, settings), media_type="image/jpeg")


@router.put("/cameras/{camera_id}/roi", response_model=CameraOut)
async def set_roi(camera_id: uuid.UUID, body: Roi, p: AdminOnly, db: DbSession) -> CameraOut:
    return await service.set_roi(db, camera_id, body, p.user_id, p.ip)


@router.get("/live", response_model=LiveOut)
async def live(_: AdminOrSupervisor, db: DbSession) -> LiveOut:
    return await service.live(db)
