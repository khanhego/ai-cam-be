"""Schema API-110, API-111, API-112 (02 §6.2 "API-110 / API-111 / API-112")."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from aicam.core.pagination import Page
from aicam.modules.orders.refs import ShopRef

ReturnKind = Literal["FAILED_DELIVERY", "BUYER_RETURN", "REFUND_ONLY", "UNANNOUNCED", "UNIDENTIFIED"]
ReturnStatus = Literal[
    "EXPECTED",
    "INSPECTING",
    "PARTIALLY_RECEIVED",
    "RECEIVED_OK",
    "RECEIVED_ISSUE",
    "MISSING",
    "CANCELLED",
    "NO_PARCEL",
]
ReturnTab = Literal["EXPECTED", "MISSING", "RECEIVED", "NO_PARCEL", "UNIDENTIFIED", "ALL"]
ReturnSort = Literal["due_asc", "created_desc"]


class ReturnOrderBrief(BaseModel):
    id: uuid.UUID
    platform_order_sn: str


class ReturnPackageBrief(BaseModel):
    id: uuid.UUID
    tracking_number: str
    warehouse_status: str


class ClaimBrief(BaseModel):
    id: uuid.UUID
    code: str
    status: str


class CaseRef(BaseModel):
    id: uuid.UUID
    code: str


class ReturnCaseItem(BaseModel):
    id: uuid.UUID
    code: str
    kind: ReturnKind
    status: ReturnStatus
    order: ReturnOrderBrief | None
    packages: list[ReturnPackageBrief]
    return_tracking_number: str | None
    reason_label: str | None
    reported_at: datetime | None
    expected_since: datetime | None
    waiting_days: int | None
    received_at: datetime | None
    conclusion: str | None
    claims: list[ClaimBrief]
    merged_into: CaseRef | None
    # Phase 3 (02 §6.2 API-110 — T-215).
    platform: str | None = None
    shop: ShopRef | None = None
    platform_status_group: str | None = None
    response_due_at: datetime | None = None  # chỉ REFUND_ONLY (BR-40)
    response_due_source: Literal["PLATFORM", "DEFAULT"] | None = None
    claim: CaseRef | None = None  # hồ sơ khiếu nại chưa đóng mới nhất của đơn


class TabCounts(BaseModel):
    EXPECTED: int = 0
    MISSING: int = 0
    RECEIVED: int = 0
    NO_PARCEL: int = 0
    UNIDENTIFIED: int = 0


class ReturnCasePage(Page[ReturnCaseItem]):
    tab_counts: TabCounts


class RequestedItem(BaseModel):
    order_item_id: uuid.UUID | None
    product_name: str
    variation: str | None
    quantity: int


class ReturnSessionBrief(BaseModel):
    id: uuid.UUID
    package_id: uuid.UUID
    status: str
    station_name: str
    operator_name: str | None
    started_at: datetime
    ended_at: datetime | None
    conclusion: str | None


class ReturnCaseDetail(ReturnCaseItem):
    platform_return_sn: str | None
    platform_status: str | None
    needs_parcel: bool | None
    reason: str | None
    reason_text: str | None
    seller_due_at: datetime | None
    source: Literal["PLATFORM", "WAREHOUSE"]
    requested_items: list[RequestedItem]
    sessions: list[ReturnSessionBrief]


class LinkOrderIn(BaseModel):
    """API-112."""

    package_id: uuid.UUID


class MergedClaimRef(BaseModel):
    from_: str = Field(alias="from", serialization_alias="from")
    into: str

    model_config = ConfigDict(populate_by_name=True)


class LinkOrderOut(ReturnCaseDetail):
    """API-112: chi tiết hồ sơ đích (như API-111); `merged_into` = hồ sơ đích khi hồ sơ chưa xác định được gộp
    vào hồ sơ mở của đơn; `merged_claims` = hồ sơ khiếu nại gộp do trùng BR-27."""

    merged_claims: list[MergedClaimRef]
