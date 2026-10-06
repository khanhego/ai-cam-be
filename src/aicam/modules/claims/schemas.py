"""Schema API-130..135 (02 §6.2 "API-130..135" — hồ sơ khiếu nại; §5.1 CLAIM, CLAIM_EVIDENCE, CLAIM_NOTE)."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from aicam.core.pagination import Page

ClaimType = Literal[
    "DAMAGED", "MISSING_ITEM", "WRONG_ITEM", "EMPTY_BOX", "OTHER", "BUYER_CLAIM", "LOST_IN_TRANSIT"
]
ClaimStatus = Literal["NEW", "SUBMITTED", "WAITING", "WON", "LOST", "CLOSED"]
Counterparty = Literal["PLATFORM", "CARRIER"]
ClaimSource = Literal["AUTO_RETURN", "MANUAL", "RECON", "LEGACY_HOLD"]
DeadlineSource = Literal["PLATFORM", "DEFAULT", "MANUAL"]
NoteKind = Literal["NOTE", "STATUS_CHANGE", "SYSTEM"]
Missing = Literal["NO_PACK_CLIP", "PACK_CLIP_DELETED", "RETURN_CLIP_PENDING"]


class UserBrief(BaseModel):
    id: uuid.UUID
    display_name: str


class ClaimPackageBrief(BaseModel):
    id: uuid.UUID
    tracking_number: str


class ClaimPackage(ClaimPackageBrief):
    warehouse_status: str


class ClaimOrderBrief(BaseModel):
    platform_order_sn: str


class ClaimOrder(ClaimOrderBrief):
    id: uuid.UUID


class ClaimReturnCase(BaseModel):
    id: uuid.UUID
    code: str
    kind: Literal["FAILED_DELIVERY", "BUYER_RETURN", "REFUND_ONLY", "UNANNOUNCED", "UNIDENTIFIED"]
    return_tracking_number: str | None


class ClaimListItem(BaseModel):
    id: uuid.UUID
    code: str
    type: ClaimType
    counterparty: Counterparty
    status: ClaimStatus
    source: ClaimSource
    package: ClaimPackageBrief
    order: ClaimOrderBrief | None
    owner: UserBrief | None
    deadline_at: datetime | None
    due_soon: bool
    overdue: bool
    created_at: datetime


class StatusCounts(BaseModel):
    NEW: int = 0
    SUBMITTED: int = 0
    WAITING: int = 0
    WON: int = 0
    LOST: int = 0
    CLOSED: int = 0


class ClaimPage(Page[ClaimListItem]):
    status_counts: StatusCounts


class EvidenceClip(BaseModel):
    id: uuid.UUID
    camera_role: Literal["CAM1", "CAM2"]
    status: Literal["PENDING", "READY", "FAILED", "DELETED"]
    sha256: str | None
    deleted_at: datetime | None


class EvidenceSession(BaseModel):
    id: uuid.UUID
    type: Literal["PACK", "RETURN"]
    status: str
    station_name: str
    operator_name: str | None
    started_at: datetime
    ended_at: datetime | None
    flags: list[str]
    clips: list[EvidenceClip]


class EvidenceSnapshot(BaseModel):
    id: uuid.UUID
    kind: Literal["MANUAL", "PACK_CLOSE"]
    taken_at: datetime
    url: str | None  # null khi ảnh đã bị retention xóa
    status: Literal["READY", "DELETED"]


class EvidenceOut(BaseModel):
    id: uuid.UUID
    kind: Literal["SESSION", "SNAPSHOT"]
    auto: bool
    session: EvidenceSession | None = None
    snapshot: EvidenceSnapshot | None = None
    # BR-39 (Phase 3, DEC-448 — suy ra lúc đọc): phiên mở hoàn trước đã hủy / bỏ dở; phiên chính (đúng một).
    prior_return: bool = False
    primary: bool = False


class PriorReturnSession(BaseModel):
    """API-132 `prior_return_sessions[]` (BR-39): phiên mở hoàn trước có clip của kiện / hồ sơ hàng hoàn."""

    session_id: uuid.UUID
    status: str
    started_at: datetime
    in_evidence: bool


class OtherSession(BaseModel):
    id: uuid.UUID
    type: Literal["PACK", "RETURN"]
    status: str
    started_at: datetime


class NoteOut(BaseModel):
    id: uuid.UUID
    kind: NoteKind
    text: str
    author: UserBrief | None
    at: datetime


class ClaimDetail(BaseModel):
    id: uuid.UUID
    code: str
    type: ClaimType
    counterparty: Counterparty
    status: ClaimStatus
    source: ClaimSource
    version: int
    package: ClaimPackage
    order: ClaimOrder | None
    return_case: ClaimReturnCase | None
    owner: UserBrief | None
    deadline_at: datetime | None
    deadline_source: DeadlineSource | None
    platform_claim_ref: str | None
    recovered_amount: int | None
    close_reason: str | None
    created_at: datetime
    closed_at: datetime | None
    evidence: list[EvidenceOut]
    other_sessions: list[OtherSession]
    prior_return_sessions: list[PriorReturnSession] = []
    missing: list[Missing]
    notes: list[NoteOut]
    allowed_transitions: list[ClaimStatus]


class ClaimCreateIn(BaseModel):
    """API-131."""

    package_id: uuid.UUID
    type: ClaimType
    counterparty: Counterparty
    note: str | None = Field(default=None, max_length=1000)
    return_case_id: uuid.UUID | None = None
    recon_alert_id: uuid.UUID | None = None


class ClaimPatchIn(BaseModel):
    """API-133: `version` bắt buộc; trường để null / bỏ = không đổi (02b-admin `ClaimPatch`)."""

    version: int = Field(ge=1)
    status: ClaimStatus | None = None
    platform_claim_ref: str | None = None  # ≤ 64 ký tự kiểm ở service (G3 C-05)
    owner_user_id: uuid.UUID | None = None
    deadline_at: datetime | None = None
    recovered_amount: int | None = Field(default=None, ge=0, le=10_000_000_000)
    reason: str | None = None  # 5–500 ký tự kiểm ở service


class EvidenceIn(BaseModel):
    """API-134: thay tập bằng chứng; bỏ bằng chứng tự chọn cần `note` 5–500 (02 §6.3 #10)."""

    version: int = Field(ge=1)
    session_ids: list[uuid.UUID] = Field(default_factory=list, max_length=50)
    snapshot_ids: list[uuid.UUID] = Field(default_factory=list, max_length=200)
    note: str | None = Field(default=None, max_length=500)


class NoteIn(BaseModel):
    """API-135 — 1–1000 ký tự (kiểm sau khi strip ở service)."""

    text: str = Field(max_length=1000)


PackStatus = Literal["QUEUED", "RUNNING", "READY", "FAILED"]


class EvidencePackCreated(BaseModel):
    """API-136 — 202."""

    id: uuid.UUID
    status: PackStatus
    progress: int


class PackFiles(BaseModel):
    zip: str


class PackMissing(BaseModel):
    session_id: uuid.UUID
    camera_role: Literal["CAM1", "CAM2"]
    reason: str  # CLIP_DELETED | CLIP_NOT_READY | CLIP_FAILED | CLIP_MISSING | SNAPSHOT_DELETED | …
    snapshot_id: uuid.UUID | None = None


class EvidencePackOut(BaseModel):
    """API-137 / WS-02 `evidence_pack.updated`."""

    id: uuid.UUID
    claim_id: uuid.UUID
    status: PackStatus
    progress: int
    sha256: str | None
    size_bytes: int | None
    missing: list[PackMissing]
    files: PackFiles | None
    expires_at: datetime | None
