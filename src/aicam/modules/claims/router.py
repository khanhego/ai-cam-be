"""API-130..135 — 02 §6.2 "API-130..135" (hồ sơ khiếu nại)."""

import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import commit, get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.errors import AppError
from aicam.core.settings import Settings, get_settings
from aicam.modules.claims import service, views
from aicam.modules.claims.models import Claim
from aicam.modules.claims.schemas import (
    ClaimCreateIn,
    ClaimDetail,
    ClaimPage,
    ClaimPatchIn,
    ClaimStatus,
    ClaimType,
    Counterparty,
    EvidenceIn,
    NoteIn,
    NoteOut,
    UserBrief,
)
from aicam.modules.users.queries import get_user_ref

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]

router = APIRouter(tags=["claims"])


@router.get("/claims", response_model=ClaimPage)
async def list_claims(
    p: Staff,
    db: DbSession,
    status: ClaimStatus | None = None,
    type: ClaimType | None = None,
    counterparty: Counterparty | None = None,
    owner: Annotated[str | None, Query(max_length=64)] = None,
    due: Literal["soon", "overdue"] | None = None,
    q: Annotated[str | None, Query(max_length=64)] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ClaimPage:
    """API-130: danh sách hồ sơ (D16)."""
    return await views.list_claims(
        db, viewer=p.user_id, status=status, claim_type=type, counterparty=counterparty, owner=owner, due=due,
        q=q, page=page, page_size=page_size,
    )  # fmt: skip


@router.post("/claims", response_model=ClaimDetail, status_code=201)
async def create_claim(body: ClaimCreateIn, p: Staff, db: DbSession, settings: AppSettings) -> ClaimDetail:
    """API-131: tạo hồ sơ thủ công (FR-08.01)."""
    claim = await service.create_manual(db, body, p)
    out = await views.claim_detail(db, claim.id, p.user_id, settings)
    await commit(db)
    return out


@router.get("/claims/{claim_id}", response_model=ClaimDetail)
async def claim_detail(claim_id: uuid.UUID, p: Staff, db: DbSession, settings: AppSettings) -> ClaimDetail:
    """API-132: chi tiết hồ sơ (D17)."""
    return await views.claim_detail(db, claim_id, p.user_id, settings)


async def _locked(
    db: AsyncSession, claim_id: uuid.UUID, version: int, p: Principal, settings: Settings
) -> Claim:
    """Khóa hồ sơ + kiểm `version` (khóa lạc quan — 02a §4 API-133/134)."""
    claim = await service.lock_claim(db, claim_id)
    if claim is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
    if claim.version != version:
        current = await views.claim_detail(db, claim_id, p.user_id, settings)
        raise AppError(
            "VERSION_CONFLICT",
            "Hồ sơ vừa được người khác cập nhật. Tải lại để xem bản mới.",
            409,
            {"current": current.model_dump(mode="json")},
        )
    return claim


@router.patch("/claims/{claim_id}", response_model=ClaimDetail)
async def patch_claim(
    claim_id: uuid.UUID, body: ClaimPatchIn, p: Staff, db: DbSession, settings: AppSettings
) -> ClaimDetail:
    """API-133: đổi trạng thái / phụ trách / mã sàn / số tiền / hạn (FR-08.02, 08.03)."""
    claim = await _locked(db, claim_id, body.version, p, settings)
    await service.patch(db, claim, body, p)
    await db.flush()
    out = await views.claim_detail(db, claim_id, p.user_id, settings)
    await commit(db)
    return out


@router.put("/claims/{claim_id}/evidence", response_model=ClaimDetail)
async def set_evidence(
    claim_id: uuid.UUID, body: EvidenceIn, p: Staff, db: DbSession, settings: AppSettings
) -> ClaimDetail:
    """API-134: đặt danh sách bằng chứng (FR-08.06)."""
    claim = await _locked(db, claim_id, body.version, p, settings)
    await service.set_evidence(db, claim, body, p)
    await db.flush()
    out = await views.claim_detail(db, claim_id, p.user_id, settings)
    await commit(db)
    return out


@router.post("/claims/{claim_id}/notes", response_model=NoteOut, status_code=201)
async def add_note(claim_id: uuid.UUID, body: NoteIn, p: Staff, db: DbSession) -> NoteOut:
    """API-135: thêm ghi chú (hồ sơ đã đóng vẫn thêm được)."""
    text_ = body.text.strip()
    if not text_:
        raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"text": "Nhập ghi chú"}})
    claim = await db.get(Claim, claim_id)
    if claim is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
    note = service.add_note(db, claim, "NOTE", text_, p.user_id)
    await db.flush()
    user = await get_user_ref(db, p.user_id)
    service.notify(db, claim)
    out = views.note_out(note, UserBrief(id=user.id, display_name=user.display_name) if user else None)
    await commit(db)
    return out
