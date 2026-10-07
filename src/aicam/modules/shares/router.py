"""API-160..164 — link chia sẻ bằng chứng (02 §6.2). ADMIN, SUPERVISOR, CSKH (thu hồi: CSKH chỉ link mình)."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.shares import schemas as s
from aicam.modules.shares import service

router = APIRouter(tags=["shares"])

Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]
DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.get("/shares/options", response_model=s.ShareOptions)
async def share_options(
    _: Staff,
    db: DbSession,
    settings: AppSettings,
    claim_id: uuid.UUID | None = None,
    session_id: uuid.UUID | None = None,
) -> s.ShareOptions:
    """API-164 (FR-07.05, BR-35, EX-S3): phiên / ảnh chọn được cho ShareLinkDialog."""
    return await service.options(db, claim_id, session_id, settings)


@router.post("/shares", response_model=s.ShareCreated, status_code=202)
async def create_share(
    body: s.ShareCreateIn, p: Staff, db: DbSession, settings: AppSettings
) -> s.ShareCreated:
    """API-160 (FR-07.05): tạo link chia sẻ — dựng nền J-24, tiến độ qua WS `share.updated` + API-162."""
    return await service.create(db, body, p, settings)


@router.get("/shares", response_model=s.SharePage)
async def list_shares(
    p: Staff,
    db: DbSession,
    settings: AppSettings,
    status: s.ShareListStatus = "ACTIVE",
    q: Annotated[str | None, Query(max_length=64)] = None,
    mine: bool = False,
    created_by: uuid.UUID | None = None,
    claim_id: uuid.UUID | None = None,
    package_id: uuid.UUID | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> s.SharePage:
    """API-161 (FR-07.09): danh sách link + số theo trạng thái (D21)."""
    return await service.list_shares(
        db, p, settings, status=status, q=q, mine=mine, created_by=created_by, claim_id=claim_id,
        package_id=package_id, page=page, page_size=page_size,
    )  # fmt: skip


@router.get("/shares/{share_id}", response_model=s.ShareOut)
async def get_share(share_id: uuid.UUID, p: Staff, db: DbSession, settings: AppSettings) -> s.ShareOut:
    """API-162 (FR-07.05, 07.09): trạng thái / chi tiết một link."""
    return await service.get_share(db, share_id, p, settings)


@router.post("/shares/{share_id}/revoke", response_model=s.ShareOut)
async def revoke_share(share_id: uuid.UUID, p: Staff, db: DbSession, settings: AppSettings) -> s.ShareOut:
    """API-163 (FR-07.08): thu hồi — J-25 xóa thư mục link trên cloud ngay (≤ 60 giây)."""
    return await service.revoke(db, share_id, p, settings)
