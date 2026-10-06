"""Schema API-13, 14, 20, 21 (02 §6.2 "API-13 / API-14", "API-20 / API-21")."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from aicam.modules.sessions.schemas import StationRef, StationStateOut

ApprovalType = Literal["MISMATCH", "ASSIST", "REPACK"]
ApprovalStatus = Literal["PENDING", "RESOLVED", "WITHDRAWN"]
Action = Literal["CONTINUE", "CLOSE_WITH_NOTE", "CANCEL_SESSION", "APPROVE_REPACK", "REJECT"]

ACTIONS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "MISMATCH": ("CONTINUE", "CLOSE_WITH_NOTE", "CANCEL_SESSION"),
    "ASSIST": ("CONTINUE", "CLOSE_WITH_NOTE", "CANCEL_SESSION"),
    "REPACK": ("APPROVE_REPACK", "REJECT"),
}


class ApprovalRequestIn(BaseModel):
    """MISMATCH / ASSIST cần `session_id`; REPACK cần `tracking_number`."""

    type: ApprovalType
    session_id: uuid.UUID | None = None
    tracking_number: str | None = Field(default=None, min_length=1, max_length=64)


class ApprovalCreated(BaseModel):
    id: uuid.UUID
    type: ApprovalType
    status: ApprovalStatus
    tracking_number: str
    created_at: datetime


class ApprovalCreatedOut(BaseModel):
    approval_request: ApprovalCreated
    state: StationStateOut


class UserBrief(BaseModel):
    id: uuid.UUID
    display_name: str


class ReturnSummary(BaseModel):
    """API-20 (Phase 3, L11): tóm tắt phiên RETURN để Supervisor quyết hủy — D13 "Đã có kết luận: Hộp rỗng ·
    3 ảnh · mở 4 phút"."""

    conclusion: str | None
    snapshot_count: int
    opened_at: datetime


class ApprovalItem(BaseModel):
    """Item API-20, cũng là `data` của WS-02 `approval.*`."""

    id: uuid.UUID
    type: ApprovalType
    status: ApprovalStatus
    station: StationRef
    session_id: uuid.UUID | None
    tracking_number: str
    # { expected, actual, source, tray_match } — tray_match cập nhật khi khay đổi trong lúc chờ (DEC-112)
    context: dict[str, Any] | None
    created_at: datetime
    decision: Action | None = None
    decided_by: UserBrief | None = None
    decided_at: datetime | None = None
    note: str | None = None
    # Phase 2 (02 API-20): loại phiên của yêu cầu, người kiểm (phiên RETURN).
    session_type: Literal["PACK", "RETURN"] | None = None
    operator_name: str | None = None
    return_summary: ReturnSummary | None = None  # chỉ `session_type = RETURN`


class DecisionIn(BaseModel):
    action: Action
    note: str | None = Field(default=None, max_length=500)


class DecisionResult(BaseModel):
    id: uuid.UUID
    status: Literal["RESOLVED"]
    decision: Action
    decided_by: UserBrief
    decided_at: datetime


class DecisionOut(BaseModel):
    approval_request: DecisionResult
