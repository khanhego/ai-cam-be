"""API-30, API-31, API-113, API-122 — 02 §6.2."""

import uuid
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import commit, get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.pagination import Page
from aicam.core.settings import Settings, get_settings
from aicam.modules.orders import adjust, packages
from aicam.modules.orders.models import WAREHOUSE_STATUSES
from aicam.modules.sessions import correction
from aicam.modules.sessions.models import SESSION_FLAGS, SESSION_STATUSES
from aicam.modules.users.queries import get_user_ref

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]
Lead = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]

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
    session_type: Literal["PACK", "RETURN"] | None = None,
    source: Literal["API", "CSV"] | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> Page[packages.PackageItem]:
    """API-30: tra cứu kiện (FR-07.01, 07.03)."""
    return await packages.search(
        db, tz=settings.tz_display, page=page, page_size=page_size, q=q, date_from=date_from,
        date_to=date_to, station_id=station_id, warehouse_status=warehouse_status,
        session_status=session_status, session_flag=session_flag, session_type=session_type, source=source,
    )  # fmt: skip


@router.get("/packages/{package_id}", response_model=packages.PackageDetail)
async def package_detail(
    package_id: uuid.UUID, p: Staff, db: DbSession, settings: AppSettings
) -> packages.PackageDetail:
    """API-31: chi tiết kiện, phiên (+ phiên hoàn: kết luận, ảnh), clip (+ `protection` — ADR-009), hồ sơ hàng
    hoàn / cảnh báo / hồ sơ khiếu nại, dòng thời gian (FR-07.02, FR-02.09, 02.11)."""
    return await packages.detail(db, package_id, settings, viewer=p.user_id, role=p.role)


@router.post("/packages/{package_id}/warehouse-status", response_model=adjust.AdjustOut)
async def adjust_warehouse_status(
    package_id: uuid.UUID, body: adjust.AdjustIn, p: Lead, db: DbSession, settings: AppSettings
) -> adjust.AdjustOut:
    """API-122: điều chỉnh trạng thái kho thủ công (FR-06.05)."""
    return await adjust.adjust_status(db, package_id, body, actor=p.user_id, ip=p.ip, tz=settings.tz_display)


@router.put("/sessions/{session_id}/inspection", response_model=packages.SessionDetail)
async def correct_inspection(
    session_id: uuid.UUID, body: correction.CorrectIn, p: Lead, db: DbSession, settings: AppSettings
) -> packages.SessionDetail:
    """API-113: sửa kết luận phiên hoàn đã đóng ≤ 7 ngày (FR-04.11) → phiên như API-31 `sessions[]`."""
    user = await get_user_ref(db, p.user_id)
    await correction.correct(
        db, session_id, body, actor=p.user_id, actor_name=user.display_name if user else "Quản lý", ip=p.ip,
        tz=settings.tz_display,
    )  # fmt: skip
    out = await packages.session_detail(db, session_id, settings, viewer=p.user_id, role=p.role)
    await commit(db)
    return out
