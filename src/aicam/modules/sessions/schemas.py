"""Schema API-10, API-11 (02 §6.2)."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

StationStateName = Literal["READY", "PACKING", "MISMATCH", "WAITING_APPROVAL", "INSPECTING"]
Conclusion = Literal["OK", "DAMAGED", "MISSING_ITEM", "WRONG_ITEM", "EMPTY_BOX", "OTHER"]
Outcome = Literal["SESSION_OPENED", "SESSION_COMPLETED", "MISMATCH", "ALERT", "IGNORED"]


class StationRef(BaseModel):
    id: uuid.UUID
    name: str


class StationStateRef(StationRef):
    """Khối `station` của API-10 / WS `station.state` (Phase 2, 02 §6.2)."""

    kind: Literal["PACK", "RETURN", "BOTH"]
    work_mode: Literal["PACK", "RETURN"]
    operator_name: str | None
    # Phase 3 (FR-03.16): Admin bật `packer_name_required` ∧ `work_mode = PACK` → quét cần tên người đóng gói.
    operator_required: bool = False


class WorkModeIn(BaseModel):
    """API-100."""

    work_mode: Literal["PACK", "RETURN"]


class OperatorIn(BaseModel):
    """API-101 — strip rồi 2–40 ký tự (kiểm ở service để trả `fields.name`)."""

    name: str  # 2–40 ký tự sau strip kiểm ở service (G3 C-05)


class CameraState(BaseModel):
    role: Literal["CAM1", "CAM2"]
    status: Literal["ONLINE", "OFFLINE"]


class TrayOut(BaseModel):
    codes: list[str]
    match: Literal["MATCH", "NOT_SEEN", "DIFFERENT", "MULTIPLE", "UNAVAILABLE"]
    updated_at: datetime | None


class MergedOrderRef(BaseModel):
    platform_order_sn: str


class OrderBrief(BaseModel):
    # Phase 3: `null` khi đơn chưa gắn shop (đơn nhập file — chưa rõ sàn, DEC-541).
    platform: str | None
    shop_name: str | None = None
    platform_order_sn: str
    buyer_note: str | None
    # Kiện gộp (FR-05.22): đơn thêm cùng mã vận đơn; rỗng khi không gộp.
    merged_orders: list[MergedOrderRef] = []


class ItemOut(BaseModel):
    order_item_id: uuid.UUID | None = None
    product_name: str
    variation: str | None
    quantity: int
    image_url: str | None
    # Đơn của dòng (kiện gộp có dòng của nhiều đơn) — luôn có khi kiện có đơn.
    platform_order_sn: str | None = None


class PackageBrief(BaseModel):
    id: uuid.UUID
    tracking_number: str
    order: OrderBrief | None
    items: list[ItemOut]


class MismatchOut(BaseModel):
    source: Literal["SCAN", "CAM2"]
    expected: str
    actual: str


class ReturnCaseState(BaseModel):
    """`session.return_case` của API-10 (phiên RETURN)."""

    id: uuid.UUID
    code: str
    kind: Literal["FAILED_DELIVERY", "BUYER_RETURN", "REFUND_ONLY", "UNANNOUNCED", "UNIDENTIFIED"]
    status: str
    platform_return_sn: str | None
    return_tracking_number: str | None
    reason: str | None
    reason_text: str | None
    reason_label: str | None
    package_count: int
    received_count: int


class InspectionLineOut(BaseModel):
    order_item_id: uuid.UUID | None
    product_name: str
    variation: str | None
    image_url: str | None
    quantity_sent: int
    quantity_requested: int
    quantity_received: int
    condition: Conclusion | None
    note: str | None


class InspectionOut(BaseModel):
    conclusion: Conclusion | None
    note: str
    saved_at: datetime | None
    lines_mode: Literal["FULL", "REFERENCE"]
    lines: list[InspectionLineOut]


class SnapshotOut(BaseModel):
    id: uuid.UUID
    kind: Literal["MANUAL", "PACK_CLOSE"]
    taken_at: datetime
    url: str


class SnapshotCreated(BaseModel):
    id: uuid.UUID
    kind: Literal["MANUAL", "PACK_CLOSE"]
    camera_role: Literal["CAM1"]
    taken_at: datetime
    sha256: str
    url: str


class SnapshotCreatedOut(BaseModel):
    """API-103 (02 §6.2) — 201."""

    snapshot: SnapshotCreated


class SnapshotRef(BaseModel):
    id: uuid.UUID
    url: str


class PackReferenceClip(BaseModel):
    id: uuid.UUID
    camera_role: Literal["CAM1", "CAM2"]
    status: str


class PackReference(BaseModel):
    """Phiên PACK hiệu lực của kiện (02 API-10 `pack_reference`); null → cờ `NO_PACK_CLIP`."""

    session_id: uuid.UUID
    ended_at: datetime | None
    station_name: str
    clips: list[PackReferenceClip]
    snapshot: SnapshotRef | None


class SessionOut(BaseModel):
    id: uuid.UUID
    type: Literal["PACK", "RETURN"] = "PACK"
    status: str
    started_at: datetime
    flags: list[str]
    operator_name: str | None = None
    package: PackageBrief
    mismatch: MismatchOut | None
    warn_at: datetime
    abandon_at: datetime
    return_case: ReturnCaseState | None = None
    inspection: InspectionOut | None = None
    snapshots: list[SnapshotOut] | None = None
    pack_reference: PackReference | None = None
    # BR-37 (Phase 3): hạn station tự hủy phiên RETURN `OPEN`; null = không còn tự hủy (Gọi quản lý).
    self_cancel_until: datetime | None = None


class ApprovalBrief(BaseModel):
    id: uuid.UUID
    type: Literal["MISMATCH", "ASSIST", "REPACK"]
    tracking_number: str
    created_at: datetime


class StationStateOut(BaseModel):
    station: StationStateRef
    state: StationStateName
    cameras: list[CameraState]
    tray: TrayOut
    session: SessionOut | None
    approval_request: ApprovalBrief | None
    today_count: int
    today_return_count: int = 0
    today_return_issue_count: int = 0
    server_time: datetime


class ScanIn(BaseModel):
    code: str = Field(min_length=1, max_length=64)
    client_scan_id: uuid.UUID


class ReturnSessionIn(BaseModel):
    """API-105: đúng một trong `package_id` / `unidentified_code`; `force_new` cần `note` 5–200 (02 §6.4)."""

    package_id: uuid.UUID | None = None
    unidentified_code: str | None = Field(default=None, max_length=64)
    client_scan_id: uuid.UUID
    force_new: bool = False
    note: str | None = Field(default=None, max_length=500)


class AlertOut(BaseModel):
    code: Literal[
        "ORDER_CANCELLED",
        "ALREADY_PACKED",
        "ALREADY_HANDED_OVER",
        "INVALID_CODE",
        "PACKED_ELSEWHERE_IN_PROGRESS",
        # Phase 2 — bàn hoàn (02 §6.2 API-11).
        "OPERATOR_REQUIRED",
        "RETURN_NOT_FOUND",
        "RETURN_ALREADY_RECEIVED",
        "RETURN_MULTIPLE_PACKAGES",
        "NOT_SHIPPED",
        "RETURN_IN_PROGRESS_ELSEWHERE",
        "INSPECTION_REQUIRED",
        "RETURN_CODE_DIFFERENT",
        # Phase 3 (02 §6.2 API-11, DEC-455, DEC-492).
        "ORDER_CANCEL_REQUESTED",
        "RETURN_MULTIPLE_ORDERS",
    ]
    message: str
    data: dict[str, Any] = {}


class ClosedSessionOut(BaseModel):
    """`closed_session` của API-11 `SESSION_COMPLETED` (02 §6.2, FR-03.14) / WS `SESSION_AUTO_CLOSED`."""

    id: uuid.UUID
    type: Literal["PACK", "RETURN"]
    tracking_number: str
    flags: list[str]
    conclusion: Conclusion | None
    claim_code: str | None
    package_status: str
    return_case_status: str | None = None


class ScanOut(BaseModel):
    outcome: Outcome
    alert: AlertOut | None
    state: StationStateOut
    closed_session: ClosedSessionOut | None = None


# Phiên PACK: OUT_OF_STOCK / WRONG_SCAN / OTHER; phiên RETURN: WRONG_SCAN / NOT_A_RETURN / OTHER (02 API-12).
CancelReason = Literal["OUT_OF_STOCK", "WRONG_SCAN", "OTHER", "NOT_A_RETURN"]


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
    type: Literal["PACK", "RETURN"] = "PACK"
    tracking_number: str
    status: str
    flags: list[str]
    started_at: datetime
    ended_at: datetime | None
    conclusion: Conclusion | None = None
    claim_code: str | None = None
    clips: list[RecentClip]


class RecentOut(BaseModel):
    items: list[RecentSession]


class ReturnLookupCase(BaseModel):
    id: uuid.UUID
    code: str
    kind: str
    status: str
    return_tracking_number: str | None


class ReturnLookupItem(BaseModel):
    package_id: uuid.UUID
    tracking_number: str
    platform_order_sn: str | None
    warehouse_status: str
    return_case: ReturnLookupCase | None
    can_open: bool
    blocked_reason: str | None
    # Phase 3 (02 §6.2 API-104, §5.1 #11): mã trùng giữa shop → chip sàn · shop phân biệt; null khi chưa gắn
    # đơn.
    platform: str | None = None
    shop_name: str | None = None


class ReturnLookupOut(BaseModel):
    """API-104 (02 §6.2)."""

    items: list[ReturnLookupItem]
    platform_checked: bool


class InspectionLineIn(BaseModel):
    order_item_id: uuid.UUID | None
    quantity_received: int
    condition: Conclusion | None = None
    note: str | None = Field(default=None, max_length=2000)


class InspectionIn(BaseModel):
    """API-102 — ghi đè toàn bộ (kiểm độ dài / BR-22 ở service để trả `details.fields`)."""

    conclusion: Conclusion | None = None
    note: str | None = Field(default=None, max_length=2000)
    lines: list[InspectionLineIn] = Field(default_factory=list, max_length=200)


class InspectionSavedOut(BaseModel):
    inspection: InspectionOut
