"""API-110, API-111 — 02 §6.2 (API-112 gắn đơn ở T-119)."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.returns import views
from aicam.modules.returns.schemas import ReturnCaseDetail, ReturnCasePage, ReturnKind, ReturnTab

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]

router = APIRouter(tags=["returns"])


@router.get("/returns", response_model=ReturnCasePage)
async def list_returns(
    _: Staff,
    db: DbSession,
    settings: AppSettings,
    tab: ReturnTab = "EXPECTED",
    kind: ReturnKind | None = None,
    q: Annotated[str | None, Query(max_length=64)] = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ReturnCasePage:
    """API-110: danh sách hồ sơ hàng hoàn (D14)."""
    return await views.list_cases(
        db, tz=settings.tz_display, tab=tab, kind=kind, q=q, date_from=date_from, date_to=date_to,
        page=page, page_size=page_size,
    )  # fmt: skip


@router.get("/returns/{case_id}", response_model=ReturnCaseDetail)
async def return_detail(
    case_id: uuid.UUID, _: Staff, db: DbSession, settings: AppSettings
) -> ReturnCaseDetail:
    """API-111: chi tiết hồ sơ hàng hoàn."""
    return await views.case_detail(db, case_id, settings.tz_display)
