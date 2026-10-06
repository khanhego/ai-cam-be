"""Item cảnh báo lệch — dùng chung API-120 / 121 / 122 (02 §6.2)."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel

Rule = Literal[
    "SHIPPED_NOT_PACKED",
    "CANCELLED_AFTER_PACK",
    "RETURN_OVERDUE",
    "RETURN_UNANNOUNCED",
    "PACKED_NOT_HANDED_OVER",
    "RETURN_DONE_NOT_RECEIVED",
    "UNVERIFIED_STALE",
]
Severity = Literal["HIGH", "MEDIUM", "LOW"]
AlertStatus = Literal["OPEN", "RESOLVED", "AUTO_RESOLVED"]


class AlertPackage(BaseModel):
    id: uuid.UUID
    tracking_number: str
    warehouse_status: str
    platform_status: str | None


class ResolvedBy(BaseModel):
    id: uuid.UUID
    display_name: str


class Resolution(BaseModel):
    action: Literal["RESOLVE", "ADJUST_STATUS", "OPEN_CLAIM"]
    note: str | None
    by: ResolvedBy | None
    at: datetime | None
    to_status: str | None
    claim_id: uuid.UUID | None


class ReconAlertOut(BaseModel):
    id: uuid.UUID
    rule: Rule
    br: str
    severity: Severity
    status: AlertStatus
    package: AlertPackage
    context: dict[str, Any]
    detected_at: datetime
    closed_at: datetime | None
    resolution: Resolution | None
    allowed_status_targets: list[str]


class OpenSummary(BaseModel):
    HIGH: int = 0
    MEDIUM: int = 0
    LOW: int = 0
