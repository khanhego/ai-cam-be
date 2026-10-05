"""API-80, API-81 — 02 §6.2."""

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.reports.service import disk_usage
from aicam.modules.settings import service
from aicam.modules.settings.schemas import HealthOut, SettingsIn, SettingsOut
from aicam.modules.stations.mediamtx import MediaMTX
from aicam.modules.stations.router import get_mediamtx

DbSession = Annotated[AsyncSession, Depends(get_session)]
Manager = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]
AdminOnly = Annotated[Principal, Depends(require_roles("ADMIN"))]

router = APIRouter(tags=["settings"])


@router.get("/settings", response_model=SettingsOut)
async def get_settings_api(_: Manager, db: DbSession) -> SettingsOut:
    """API-80 GET: retention, ngưỡng phiên."""
    return service.to_out(await service.get(db))


@router.put("/settings", response_model=SettingsOut)
async def put_settings(body: SettingsIn, p: AdminOnly, db: DbSession) -> SettingsOut:
    """API-80 PUT (FR-02.06, BR-16)."""
    return await service.update(db, body, p)


@router.get("/system/health", response_model=HealthOut)
async def system_health(
    _: Manager,
    db: DbSession,
    settings: Annotated[Settings, Depends(get_settings)],
    mediamtx: Annotated[MediaMTX, Depends(get_mediamtx)],
) -> HealthOut:
    """API-81 (FR-01.02, 01.06, NFR-30)."""
    return await service.health(db, mediamtx_check=mediamtx.list_paths(), disk=disk_usage(settings))
