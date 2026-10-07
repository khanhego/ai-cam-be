"""API-32, API-150..152 — 02 §6.2."""

import uuid
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.core.db import commit, get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.errors import AppError
from aicam.core.settings import Settings, get_settings
from aicam.modules.reports import analytics, csv_export, service
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


@router.get(
    "/reports/{report}/export",
    response_class=StreamingResponse,
    responses={
        200: {"content": {"text/csv": {"schema": {"type": "string"}}}, "description": "CSV UTF-8 có BOM"}
    },
)
async def export_report(
    report: str,
    p: Annotated[Principal, Depends(require_roles(*ALL_ROLES))],
    db: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    from_: FromQuery = None,
    to: ToQuery = None,
    platform: PlatformQuery = None,
    shop_id: uuid.UUID | None = None,
    station_id: uuid.UUID | None = None,
) -> StreamingResponse:
    """API-153 xuất CSV tab đang xem (FR-09.06): `report` = returns | claims | productivity.

    Audit `REPORT_EXPORT`; quyền như API tương ứng (CSKH + productivity → 403); `report` lạ → 404."""
    if report not in analytics.REPORTS:
        raise AppError("NOT_FOUND", "Không tìm thấy báo cáo.", 404)
    if report == "productivity" and p.role not in MANAGER_ROLES:
        raise AppError("FORBIDDEN", "Tài khoản không có quyền thực hiện thao tác này.", 403)
    tz = settings.tz_display
    f = analytics.make_filters(
        from_, to, platform, shop_id, station_id if report == "productivity" else None, tz
    )
    out = await analytics.get_report(db, report, f, tz, analytics.MODELS[report])
    shop_name, station_name = await analytics.filter_names(db, f)
    body = csv_export.render(report, out, shop_name=shop_name, station_name=station_name)
    audit.record(
        db,
        "REPORT_EXPORT",
        user_id=p.user_id,
        ip=p.ip,
        data={
            "report": report,
            "from": f.from_.isoformat(),
            "to": f.to.isoformat(),
            "platform": platform,
            "shop_id": str(shop_id) if shop_id else None,
            "station_id": str(station_id) if station_id else None,
        },
    )
    await commit(db)
    return StreamingResponse(
        iter([body]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{csv_export.filename(report, f)}"'},
    )
