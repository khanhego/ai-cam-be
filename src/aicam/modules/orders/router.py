"""API-30, API-31 — 02 §6.2."""

import uuid
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.pagination import Page
from aicam.core.settings import Settings, get_settings
from aicam.modules.orders import packages
from aicam.modules.orders.models import WAREHOUSE_STATUSES
from aicam.modules.sessions.models import SESSION_FLAGS, SESSION_STATUSES

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]

WarehouseStatus = Literal[WAREHOUSE_STATUSES]  # type: ignore[valid-type]
SessionStatus = Literal[SESSION_STATUSES]  # type: ignore[valid-type]
SessionFlag = Literal[SESSION_FLAGS]  # type: ignore[valid-type]

router = APIRouter(tags=["packages"])


@router.get("/packages", response_model=Page[packages.PackageItem])
async def search_packages(
    _: Staff,
    db: DbSession,
    settings: AppSettings,
    q: Annotated[str | None, Query(max_length=64)] = None,
    date_from: date | None = None,
    date_to: date | None = None,
    station_id: uuid.UUID | None = None,
    warehouse_status: WarehouseStatus | None = None,
    session_status: SessionStatus | None = None,
    session_flag: SessionFlag | None = None,
    source: Literal["API", "CSV"] | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> Page[packages.PackageItem]:
    """API-30: tra cứu kiện (FR-07.01, 07.03)."""
    return await packages.search(
        db, tz=settings.tz_display, page=page, page_size=page_size, q=q, date_from=date_from,
        date_to=date_to, station_id=station_id, warehouse_status=warehouse_status,
        session_status=session_status, session_flag=session_flag, source=source,
    )  # fmt: skip


@router.get("/packages/{package_id}", response_model=packages.PackageDetail)
async def package_detail(package_id: uuid.UUID, _: Staff, db: DbSession) -> packages.PackageDetail:
    """API-31: chi tiết kiện, phiên, clip, dòng thời gian (FR-07.02)."""
    return await packages.detail(db, package_id)
