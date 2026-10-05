"""API-13, 14 (station) và API-20, 21 (dashboard) — 02 §6.2."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.pagination import Page
from aicam.core.settings import Settings, get_settings
from aicam.modules.approvals import service
from aicam.modules.approvals.schemas import (
    ApprovalCreatedOut,
    ApprovalItem,
    ApprovalRequestIn,
    ApprovalStatus,
    DecisionIn,
    DecisionOut,
)
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.schemas import StateOnlyOut

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
StationOnly = Annotated[Principal, Depends(require_roles("STATION"))]
Approver = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]

router = APIRouter(tags=["approvals"])


@router.post("/station/approval-requests", response_model=ApprovalCreatedOut, status_code=201)
async def create_request(
    body: ApprovalRequestIn, p: StationOnly, db: DbSession, settings: AppSettings
) -> ApprovalCreatedOut:
    """API-13: gửi yêu cầu duyệt MISMATCH / ASSIST / REPACK (FR-03.10, 03.12)."""
    station = await sessions.require_station(db, p.station_id, p.user_id)
    return await service.request(db, station, body, settings)


@router.post("/station/approval-requests/{approval_id}/withdraw", response_model=StateOnlyOut)
async def withdraw_request(
    approval_id: uuid.UUID, p: StationOnly, db: DbSession, settings: AppSettings
) -> StateOnlyOut:
    """API-14: rút yêu cầu của station mình."""
    station = await sessions.require_station(db, p.station_id, p.user_id)
    return StateOnlyOut(state=await service.withdraw(db, station, approval_id, settings))


@router.get("/approval-requests", response_model=Page[ApprovalItem])
async def list_requests(
    _: Approver,
    db: DbSession,
    status: ApprovalStatus | None = "PENDING",
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> Page[ApprovalItem]:
    """API-20: danh sách yêu cầu (mặc định PENDING), cũ nhất trước."""
    return await service.list_requests(db, status=status, page=page, page_size=page_size)


@router.post("/approval-requests/{approval_id}/decision", response_model=DecisionOut)
async def decide(
    approval_id: uuid.UUID, body: DecisionIn, p: Approver, db: DbSession, settings: AppSettings
) -> DecisionOut:
    """API-21: duyệt / từ chối (UC-08, AC-19)."""
    return await service.decide(db, approval_id, body, p, settings)
