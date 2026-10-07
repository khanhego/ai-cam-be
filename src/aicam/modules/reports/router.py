"""API-32, API-150..152 — 02 §6.2."""

import uuid
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.reports import analytics, service
from aicam.modules.reports import schemas as s

router = APIRouter(tags=["reports"])

ALL_ROLES = ("ADMIN", "SUPERVISOR", "CSKH")
MANAGER_ROLES = ("ADMIN", "SUPERVISOR")  # tab Năng suất (DEC-414)
FromQuery = Annotated[
    date | None, Query(alias="from", description="YYYY-MM-DD giờ VN; mặc định to − 29 ngày")
]
ToQuery = Annotated[date | None, Query(description="YYYY-MM-DD giờ VN, ≤ hôm nay; mặc định hôm nay")]
PlatformQuery = Annotated[Literal["SHOPEE", "TIKTOK"] | None, Query()]


@router.get("/reports/daily", response_model=service.DailyOut)
async def daily(
    p: Annotated[Principal, Depends(require_roles(*ALL_ROLES))],
    db: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    date: date | None = None,
) -> service.DailyOut:
    """Số liệu ngày (giờ VN) + trạng thái station + mục cần xử lý (FR-09.01)."""
    return service.for_role(await service.daily(db, date, settings), p.role)


@router.get("/reports/returns", response_model=s.ReturnsReportOut)
async def returns_report(
    _: Annotated[Principal, Depends(require_roles(*ALL_ROLES))],
    db: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    from_: FromQuery = None,
    to: ToQuery = None,
    platform: PlatformQuery = None,
    shop_id: uuid.UUID | None = None,
) -> s.ReturnsReportOut:
    """API-150 báo cáo hàng hoàn (FR-09.03, 09.05; BR-41)."""
    f = analytics.make_filters(from_, to, platform, shop_id, None, settings.tz_display)
    return await analytics.get_report(db, "returns", f, settings.tz_display, s.ReturnsReportOut)


@router.get("/reports/claims", response_model=s.ClaimsReportOut)
async def claims_report(
    _: Annotated[Principal, Depends(require_roles(*ALL_ROLES))],
    db: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    from_: FromQuery = None,
    to: ToQuery = None,
    platform: PlatformQuery = None,
    shop_id: uuid.UUID | None = None,
) -> s.ClaimsReportOut:
    """API-151 báo cáo khiếu nại (FR-09.04, 09.05; BR-41)."""
    f = analytics.make_filters(from_, to, platform, shop_id, None, settings.tz_display)
    return await analytics.get_report(db, "claims", f, settings.tz_display, s.ClaimsReportOut)


@router.get("/reports/productivity", response_model=s.ProductivityReportOut)
async def productivity_report(
    _: Annotated[Principal, Depends(require_roles(*MANAGER_ROLES))],
    db: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    from_: FromQuery = None,
    to: ToQuery = None,
    platform: PlatformQuery = None,
    shop_id: uuid.UUID | None = None,
    station_id: uuid.UUID | None = None,
) -> s.ProductivityReportOut:
    """API-152 báo cáo năng suất (FR-09.02, 09.05, FR-03.16; BR-41) — CSKH 403."""
    f = analytics.make_filters(from_, to, platform, shop_id, station_id, settings.tz_display)
    return await analytics.get_report(db, "productivity", f, settings.tz_display, s.ProductivityReportOut)
