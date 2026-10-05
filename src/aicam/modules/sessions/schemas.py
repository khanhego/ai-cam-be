"""Schema API-10, API-11 (02 §6.2)."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

StationStateName = Literal["READY", "PACKING", "MISMATCH", "WAITING_APPROVAL"]
Outcome = Literal["SESSION_OPENED", "SESSION_COMPLETED", "MISMATCH", "ALERT", "IGNORED"]


class StationRef(BaseModel):
    id: uuid.UUID
    name: str


class CameraState(BaseModel):
    role: Literal["CAM1", "CAM2"]
    status: Literal["ONLINE", "OFFLINE"]


class TrayOut(BaseModel):
    codes: list[str]
    match: Literal["MATCH", "NOT_SEEN", "DIFFERENT", "MULTIPLE", "UNAVAILABLE"]
    updated_at: datetime | None


class OrderBrief(BaseModel):
    platform: str
    platform_order_sn: str
    buyer_note: str | None


class ItemOut(BaseModel):
    product_name: str
    variation: str | None
    quantity: int
    image_url: str | None


class PackageBrief(BaseModel):
    id: uuid.UUID
    tracking_number: str
    order: OrderBrief | None
    items: list[ItemOut]


class MismatchOut(BaseModel):
    source: Literal["SCAN", "CAM2"]
    expected: str
    actual: str


class SessionOut(BaseModel):
    id: uuid.UUID
    status: str
    started_at: datetime
    flags: list[str]
    package: PackageBrief
    mismatch: MismatchOut | None
    warn_at: datetime
    abandon_at: datetime


class ApprovalBrief(BaseModel):
    id: uuid.UUID
    type: Literal["MISMATCH", "ASSIST", "REPACK"]
    tracking_number: str
    created_at: datetime


class StationStateOut(BaseModel):
    station: StationRef
    state: StationStateName
    cameras: list[CameraState]
    tray: TrayOut
    session: SessionOut | None
    approval_request: ApprovalBrief | None
    today_count: int
    server_time: datetime


class ScanIn(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    client_scan_id: uuid.UUID


class AlertOut(BaseModel):
    code: Literal[
        "ORDER_CANCELLED",
        "ALREADY_PACKED",
        "ALREADY_HANDED_OVER",
        "INVALID_CODE",
        "PACKED_ELSEWHERE_IN_PROGRESS",
    ]
    message: str
    data: dict[str, Any] = {}


class ScanOut(BaseModel):
    outcome: Outcome
    alert: AlertOut | None
    state: StationStateOut


CancelReason = Literal["OUT_OF_STOCK", "WRONG_SCAN", "OTHER"]


class CancelIn(BaseModel):
    reason: CancelReason
    note: str | None = Field(default=None, max_length=200)


class StateOnlyOut(BaseModel):
    state: StationStateOut


class RecentClip(BaseModel):
    id: uuid.UUID
    camera_role: Literal["CAM1", "CAM2"]
    status: str


class RecentSession(BaseModel):
    id: uuid.UUID
    tracking_number: str
    status: str
    flags: list[str]
    started_at: datetime
    ended_at: datetime | None
    clips: list[RecentClip]


class RecentOut(BaseModel):
    items: list[RecentSession]
