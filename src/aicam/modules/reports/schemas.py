"""Response API-150..152 (02 §6.2) — số theo BR-41; tỷ lệ `{numerator, denominator, value}`, mẫu 0 → null."""

import uuid
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PlatformCode = Literal["SHOPEE", "TIKTOK"]


class PeriodOut(BaseModel):
    """Kỳ giờ VN, gồm cả hai đầu. `from` là từ khóa Python → alias (FastAPI xuất theo alias)."""

    model_config = ConfigDict(populate_by_name=True)

    from_: date = Field(alias="from")
    to: date


class Filters(BaseModel):
    platform: PlatformCode | None
    shop_id: uuid.UUID | None


class ProductivityFilters(Filters):
    station_id: uuid.UUID | None


class Ratio(BaseModel):
    numerator: int
    denominator: int
    value: float | None


# ---------------------------------------------------------------- API-150


class RefundOnlyCard(BaseModel):
    count: int
    rate_of_handed_over: float | None


class ReturnCards(BaseModel):
    return_rate: Ratio
    issue_rate: Ratio
    refund_only: RefundOnlyCard
    expected_now: int


class KindRow(BaseModel):
    kind: Literal["BUYER_RETURN", "FAILED_DELIVERY", "UNANNOUNCED", "UNIDENTIFIED", "REFUND_ONLY"]
    count: int
    share: float | None


class ReasonRow(BaseModel):
    reason: str | None
    reason_label: str
    counts: dict[str, int]
    total: int


class ReasonByConclusion(BaseModel):
    conclusions: list[str]
    rows: list[ReasonRow]


class ProductRow(BaseModel):
    sku: str | None
    product_name: str
    variation: str | None
    shipped: int
    return_requests: int
    rate: float | None
    issue: int


class ReturnShopRow(BaseModel):
    platform: PlatformCode | None
    shop_id: uuid.UUID | None
    shop_name: str | None
    handed_over: int
    return_cases: int
    rate: float | None


class SeriesRow(BaseModel):
    """FR-09.07 (C): một cột biểu đồ — `bucket` = ngày đầu của ngày / tuần / tháng (cắt theo đầu kỳ)."""

    bucket: date
    packed: int
    return_cases: int
    claims: int


Granularity = Literal["day", "week", "month"]


class ReturnsReportOut(BaseModel):
    period: PeriodOut
    filters: Filters
    generated_at: datetime
    cards: ReturnCards
    by_kind: list[KindRow]
    reason_by_conclusion: ReasonByConclusion
    top_products: list[ProductRow]
    by_shop: list[ReturnShopRow]
    series: list[SeriesRow]
    series_granularity: Granularity


# ---------------------------------------------------------------- API-151


class ClaimCards(BaseModel):
    created: int
    win_rate: Ratio
    recovered_amount: int
    submitted_before_deadline: Ratio
    overdue_unsent_now: int


class ClaimStatusRow(BaseModel):
    status: Literal["NEW", "SUBMITTED", "WAITING", "WON", "LOST", "CLOSED"]
    count: int


class ClaimTypeRow(BaseModel):
    type: str
    won: int
    lost: int
    pending: int


class ClaimCounterpartyRow(BaseModel):
    counterparty: Literal["PLATFORM", "CARRIER"]
    count: int
    won: int
    lost: int
    recovered_amount: int


class ClaimShopRow(BaseModel):
    platform: PlatformCode | None
    shop_id: uuid.UUID | None
    shop_name: str | None
    count: int
    won: int
    lost: int
    recovered_amount: int


class ClaimsReportOut(BaseModel):
    period: PeriodOut
    filters: Filters
    generated_at: datetime
    cards: ClaimCards
    by_status: list[ClaimStatusRow]
    by_type_result: list[ClaimTypeRow]
    by_counterparty: list[ClaimCounterpartyRow]
    by_shop: list[ClaimShopRow]
    series: list[SeriesRow]
    series_granularity: Granularity


# ---------------------------------------------------------------- API-152


class ProductivityCards(BaseModel):
    packed: int
    pack_avg_seconds: int | None
    returns_inspected: int
    return_avg_seconds: int | None


class StationRow(BaseModel):
    station_id: uuid.UUID
    station_name: str
    packed: int
    avg_seconds: int | None
    mismatch: int
    abandoned: int
    cancelled: int
    repacked: int


class OperatorRow(BaseModel):
    operator_name: str | None
    packed: int
    avg_seconds: int | None
    mismatch: int
    abandoned: int
    cancelled: int
    repacked: int


class ReturnOperatorRow(BaseModel):
    operator_name: str | None
    inspected: int
    avg_seconds: int | None
    issue_rate: Ratio


class ProductivityReportOut(BaseModel):
    period: PeriodOut
    filters: ProductivityFilters
    generated_at: datetime
    cards: ProductivityCards
    by_station: list[StationRow]
    by_operator: list[OperatorRow]
    return_by_operator: list[ReturnOperatorRow]
