"""Schema API-160..164 + `shares[]` của API-31 / API-132 (02 §6.2 "API-160..164")."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from aicam.core.pagination import Page

ShareStatus = Literal["CREATING", "ACTIVE", "FAILED", "REVOKED", "EXPIRED"]
ShareLayout = Literal["SIDE_BY_SIDE", "CAM1"]
ShareSourceType = Literal["CLAIM", "SESSION"]
ShareStep = Literal["RENDERING", "UPLOADING", "PUBLISHING"]
ShareErrorCode = Literal["RENDER_FAILED", "UPLOAD_FAILED", "TIMEOUT"]
ShareListStatus = Literal["ACTIVE", "REVOKED", "EXPIRED", "ALL"]
UnavailableReason = Literal["CLIP_PENDING", "CLIP_FAILED", "CLIP_DELETED", "CLIP_MISSING"]


class UserBrief(BaseModel):
    id: uuid.UUID
    display_name: str


class ShareCreateIn(BaseModel):
    """API-160. Giới hạn BR-35 (1–4 phiên, ≤ 1.800 giây), `recipient` 3–100, `expires_days` 1 / 3 / 7 kiểm ở
    service (422 `fields` tiếng Việt)."""

    source_type: ShareSourceType
    claim_id: uuid.UUID | None = None
    session_id: uuid.UUID | None = None
    session_ids: list[uuid.UUID] = Field(default_factory=list, max_length=20)
    layout: ShareLayout = "SIDE_BY_SIDE"
    include_snapshots: bool = True
    recipient: str = Field(default="", max_length=300)
    expires_days: int = 7


class ShareCreated(BaseModel):
    id: uuid.UUID
    status: Literal["CREATING"]


class ShareSource(BaseModel):
    type: ShareSourceType
    claim_id: uuid.UUID | None
    claim_code: str | None
    package_id: uuid.UUID
    tracking_number: str
    platform: str | None = None
    shop_name: str | None = None


class SourceSha(BaseModel):
    CAM1: str | None = None
    CAM2: str | None = None


class ShareItemOut(BaseModel):
    session_id: uuid.UUID
    order: int
    video_sha256: str | None
    size_bytes: int | None
    source_sha256: SourceSha
    snapshot_count: int


class ShareError(BaseModel):
    code: ShareErrorCode
    message: str


class ShareOut(BaseModel):
    """API-162 (= item API-161 không có `items`)."""

    id: uuid.UUID
    status: ShareStatus
    progress: int
    step: ShareStep | None
    step_index: int | None
    step_total: int | None
    url: str | None  # chỉ khi ACTIVE (giải mã Fernet)
    recipient: str
    source: ShareSource
    session_count: int
    layout: ShareLayout
    include_snapshots: bool
    expires_at: datetime
    created_at: datetime
    created_by: UserBrief | None
    revoked_at: datetime | None
    revoked_by: UserBrief | None
    revoke_pending: bool  # đã thu hồi nhưng chưa xóa xong đối tượng trên cloud (EX-S7)
    error: ShareError | None
    items: list[ShareItemOut] | None = None
    can_revoke: bool


class ShareCounts(BaseModel):
    ACTIVE: int
    REVOKED: int
    EXPIRED: int
    ALL: int


class SharePage(Page[ShareOut]):
    counts: ShareCounts


class ShareBrief(BaseModel):
    """`shares[]` của API-31 / API-132."""

    id: uuid.UUID
    status: ShareStatus
    recipient: str
    expires_at: datetime
    session_count: int
    url: str | None
    can_revoke: bool
    revoke_pending: bool
    created_at: datetime


class AffectedShare(BaseModel):
    """API-189 `affected_shares[]` (v0.4 — DEC-531)."""

    id: uuid.UUID
    recipient: str
    status: ShareStatus
    expires_at: datetime
    created_by: UserBrief | None
    can_revoke: bool


class OptionSession(BaseModel):
    id: uuid.UUID
    type: Literal["PACK", "RETURN"]
    status: str
    started_at: datetime
    ended_at: datetime | None
    station_name: str | None
    operator_name: str | None
    conclusion: str | None
    duration_s: int | None
    prior_return: bool
    primary: bool
    default_selected: bool
    selectable: bool
    unavailable_reason: UnavailableReason | None
    unavailable_at: datetime | None
    cameras: list[Literal["CAM1", "CAM2"]]
    review_needed: bool = False
    excluded: bool = False  # phiên bị loại theo BR-39 nhưng có trong bằng chứng (thêm tay) — không chọn sẵn
    snapshot_count: int = 0  # ảnh READY của phiên trong bằng chứng (đi theo phiên khi chọn — DEC-667)


class OptionLimits(BaseModel):
    max_sessions: int
    max_total_seconds: int
    max_snapshots: int


class ShareOptions(BaseModel):
    """API-164."""

    storage_configured: bool
    source: ShareSource
    sessions: list[OptionSession]
    snapshot_count: int
    # v0.4 (DEC-531): số phiên "Cần soát" của hồ sơ (= API-132 `review_sessions`); nguồn `SESSION` → 0.
    review_pending_count: int = 0
    limits: OptionLimits
    default_expires_days: int
