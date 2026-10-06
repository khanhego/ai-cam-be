"""Schema API-80, API-81, API-82 (02 §6.2)."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

Days = Field(ge=1, le=365)
Minutes = Field(ge=1, le=1440)
# Phase 2 (02 §6.2 API-80): 6 ngưỡng mới — tùy chọn khi PUT (thiếu = giữ giá trị cũ).
THRESHOLD_FIELDS = (
    "return_warn_minutes",
    "return_abandon_minutes",
    "return_missing_days",
    "handover_warn_hours",
    "claim_deadline_days",
    "claim_due_soon_hours",
)


class SettingsIn(BaseModel):
    retention_raw_days: int = Days
    retention_clip_days: int = Days
    session_warn_minutes: int = Minutes
    session_abandon_minutes: int = Minutes
    return_warn_minutes: int | None = Field(None, ge=1, le=1440)
    return_abandon_minutes: int | None = Field(None, ge=1, le=1440)
    return_missing_days: int | None = Field(None, ge=1, le=60)
    handover_warn_hours: int | None = Field(None, ge=1, le=168)
    claim_deadline_days: int | None = Field(None, ge=1, le=90)
    claim_due_soon_hours: int | None = Field(None, ge=1, le=168)
    # Phase 3 (FR-03.16): tùy chọn khi PUT (thiếu = giữ).
    packer_name_required: bool | None = None
    # Phase 3 (FR-08.08, BR-40): giờ mặc định hạn phản hồi Chỉ hoàn tiền khi sàn không có hạn.
    refund_only_default_hours: int | None = Field(None, ge=1, le=168)
    # Giảm `retention_clip_days` / `retention_raw_days` cần xác nhận (FR-02.10) — thiếu → 409.
    confirm_reduction: bool = False


class SettingsOut(BaseModel):
    retention_raw_days: int
    retention_clip_days: int
    session_warn_minutes: int
    session_abandon_minutes: int
    return_warn_minutes: int
    return_abandon_minutes: int
    return_missing_days: int
    handover_warn_hours: int
    claim_deadline_days: int
    claim_due_soon_hours: int
    packer_name_required: bool = False
    refund_only_default_hours: int = 48
    # Sàn giữ clip (BR-25) — chỉ đọc, từ biến môi trường `RETENTION_CLIP_MIN_DAYS`.
    retention_clip_min_days: int
    updated_at: datetime


class RetentionImpactOut(BaseModel):
    """API-82 (cũng là `details.impact` của 409 `RETENTION_REDUCTION_UNCONFIRMED`)."""

    clips: int
    clip_bytes: int
    raw_hours: int
    raw_bytes: int
    protected_clips: int
    next_run_at: datetime


ComponentStatus = Literal["OK", "ERROR"]


class DiskOut(BaseModel):
    total_bytes: int
    used_bytes: int
    percent: int


class CameraHealth(BaseModel):
    id: uuid.UUID
    station_name: str
    role: str
    status: str
    clock_offset_ms: int | None
    last_seen_at: datetime | None


class SyncHealth(BaseModel):
    shop_id: uuid.UUID
    last_success_at: datetime | None
    last_error: dict[str, Any] | None


class HealthOut(BaseModel):
    db: ComponentStatus
    redis: ComponentStatus
    mediamtx: ComponentStatus
    disk: DiskOut | None
    cameras: list[CameraHealth]
    sync: list[SyncHealth]
