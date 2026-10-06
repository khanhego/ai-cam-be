"""API-110, API-111, API-112 — 02 §6.2."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import commit, get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.returns import service, views
from aicam.modules.returns.schemas import (
    CaseRef,
    LinkOrderIn,
    LinkOrderOut,
    MergedClaimRef,
    ReturnCaseDetail,
    ReturnCasePage,
    ReturnKind,
    ReturnTab,
)
from aicam.modules.users.queries import get_user_ref

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]
Lead = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]

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


@router.post("/returns/{case_id}/link-order", response_model=LinkOrderOut, response_model_by_alias=True)
async def link_order(
    case_id: uuid.UUID, body: LinkOrderIn, p: Lead, db: DbSession, settings: AppSettings
) -> LinkOrderOut:
    """API-112: gắn đơn cho hồ sơ chưa xác định (FR-04.13, UC-13)."""
    user = await get_user_ref(db, p.user_id)
    result = await service.link_order(
        db, case_id, body.package_id, actor_user_id=p.user_id,
        actor_label=user.display_name if user else "Quản lý", ip=p.ip,
    )  # fmt: skip
    detail = await views.case_detail(db, result.destination.id, settings.tz_display)
    out = LinkOrderOut(
        **{
            **detail.model_dump(),
            "merged_into": CaseRef(id=result.merged_into.id, code=result.merged_into.code)
            if result.merged_into
            else None,
        },
        merged_claims=[MergedClaimRef(from_=m.from_code, into=m.into_code) for m in result.merged_claims],
    )
    await commit(db)
    return out
