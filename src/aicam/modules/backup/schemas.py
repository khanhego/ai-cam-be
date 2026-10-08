"""Schema API-180..188 (02 §6.2 "sao lưu"), API-81 `backup`."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

BackupState = Literal["ON", "NOT_CONFIGURED", "KEY_UNCONFIRMED", "KEY_CHANGED", "DISABLED", "RESTORE_PENDING"]
ObjectStatus = Literal[
    "PENDING",
    "UPLOADING",
    "UPLOADED",
    "FAILED",
    "HASH_MISMATCH",
    "SOURCE_DELETED",
    "IGNORED",
    "CLOUD_DELETED",
]
ResolutionAction = Literal["UPLOAD_ANYWAY", "IGNORE", "RETRY", "ACCEPT_RESTORED"]
IssueKind = Literal["HASH_MISMATCH", "UPLOAD_FAILED", "SOURCE_MISSING"]


class TestOut(BaseModel):
    """API-183: ghi → đọc → xóa một đối tượng 1 KB dưới `backup/_probe/` (≤ 10 giây)."""

    ok: bool
    elapsed_ms: int


class UserRefOut(BaseModel):
    id: uuid.UUID
    display_name: str


class StorageOut(BaseModel):
    endpoint_host: str | None
    bucket: str


class OldKeyOut(BaseModel):
    """EX-K7 (DEC-495, 522): dấu vân tay cũ còn bản trên cloud (`cloud_present`)."""

    fingerprint: str
    evidence_objects: int
    db_runs: int
    reuploadable: int
    reuploadable_bytes: int


class KeyOut(BaseModel):
    configured: bool
    fingerprint: str | None
    confirmed_fingerprint: str | None
    confirmed_at: datetime | None
    confirmed_by: UserRefOut | None
    old_keys: list[OldKeyOut] = []


class DbOut(BaseModel):
    last_success_at: datetime | None
    last_size_bytes: int | None
    next_run_at: datetime | None
    hours_since_success: float | None
    late: bool
    running: bool
    consecutive_failures: int


class EvidenceOut(BaseModel):
    uploaded: int
    pending: int
    failed: int
    oldest_pending_at: datetime | None
    late_count: int
    hash_mismatch: int
    ignored: int
    source_deleted: int
    source_missing: int


class ErrorOut(BaseModel):
    code: str
    message: str
    at: datetime


class SettingsOut(BaseModel):
    upload_mbps: int
    all_pack_clips: bool
    all_pack_clips_estimate_gb_per_day: float


class HistoryOut(BaseModel):
    id: uuid.UUID
    kind: Literal["DB"]
    started_at: datetime
    finished_at: datetime | None
    status: Literal["RUNNING", "SUCCESS", "FAILED"]
    size_bytes: int | None
    error: str | None
    key_fingerprint: str | None


class BackupStatusOut(BaseModel):
    """API-180 `GET /backup`."""

    configured: bool
    storage: StorageOut | None
    key: KeyOut
    state: BackupState
    enabled: bool
    db: DbOut
    evidence: EvidenceOut
    cloud_bytes: int
    last_error: ErrorOut | None
    settings: SettingsOut
    history: list[HistoryOut]


class BackupSettingsIn(BaseModel):
    """API-181 — trường thiếu = giữ."""

    enabled: bool | None = None
    upload_mbps: int | None = Field(None, ge=1, le=1000)
    all_pack_clips: bool | None = None


class ConfirmKeyIn(BaseModel):
    fingerprint: str = Field(min_length=1, max_length=40)


class RunNowOut(BaseModel):
    run_id: uuid.UUID


class ReuploadOut(BaseModel):
    queued: int
    bytes: int


class ResolutionOut(BaseModel):
    action: ResolutionAction
    note: str | None
    by: UserRefOut | None
    at: datetime


class IssueOut(BaseModel):
    """API-185 item (cũng là response API-188)."""

    object_id: uuid.UUID
    kind: Literal["CLIP", "SNAPSHOT"]
    status: ObjectStatus
    session_id: uuid.UUID | None
    package_id: uuid.UUID | None
    tracking_number: str | None
    detected_at: datetime
    detail: str | None
    attempts: int
    sha256_expected: str | None
    sha256_actual: str | None
    resolution: ResolutionOut | None


class IssuesPage(BaseModel):
    items: list[IssueOut]
    page: int
    page_size: int
    total: int


class ResolveIn(BaseModel):
    """API-188."""

    action: Literal["UPLOAD_ANYWAY", "IGNORE", "RETRY"]
    note: str  # 5–500 sau trim — kiểm ở service (`fields.note` tiếng Việt, C-05)


class HealthBackupOut(BaseModel):
    """API-81 `backup` (02 §6.2)."""

    state: BackupState
    last_db_success_at: datetime | None
    pending: int
    late: bool
    last_error: ErrorOut | None
