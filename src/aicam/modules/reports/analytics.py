"""Báo cáo M09 API-150..152 (FR-09.02..05, BR-41, 02 §6.2, 02a §8) — SQL theo tập trên bảng hiện có.

Kỳ = `from`..`to` giờ VN gồm cả hai đầu → `[start, end)` UTC (tính ở Python để dùng index cột thời gian).
Mỗi bảng con một truy vấn gộp; cache Redis 60 giây theo bộ lọc; `SET LOCAL statement_timeout = '15s'`
(quá → 503 `REPORT_TIMEOUT`). Shop của dòng: kiện / phiên → đơn của kiện; hồ sơ hàng hoàn →
`COALESCE(return_case.shop_id, đơn.shop_id)`; hồ sơ khiếu nại → `COALESCE(đơn của hồ sơ, đơn của kiện)`.
Không có shop (đơn file, kiện chưa xác minh, kiện tạm) → chỉ có mặt khi không lọc sàn / shop
(02 §6.2 API-152).
"""

import hashlib
import json
import time
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from pydantic import BaseModel
from sqlalchemy import Text, and_, case, cast, func, literal, not_, or_, select, text, type_coerce
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.claims.models import CLAIM_STATUSES, CLAIM_TYPES, COUNTERPARTIES, Claim
from aicam.modules.orders.models import Order, OrderItem, Package, Shop, StatusHistory
from aicam.modules.orders.refs import shop_conditions, shops_by_id
from aicam.modules.reports import schemas as s
from aicam.modules.returns.models import ReturnCase
from aicam.modules.returns.views import reason_label
from aicam.modules.sessions.models import INSPECTION_CONCLUSIONS, PackSession
from aicam.modules.sessions.queries import excluded_return_sql
from aicam.modules.stations.models import Station

log = structlog.get_logger()

MAX_DAYS = 366
DEFAULT_DAYS = 30  # 01 §4.2 B2: mặc định 30 ngày gần nhất
CACHE_TTL_S = 60
STATEMENT_TIMEOUT = "15s"
QUERY_CANCELED = "57014"  # Postgres `query_canceled` (hết `statement_timeout`)
SLOW_REPORT_S = 3.0  # NFR-37 (kỳ ≤ 92 ngày) — log cảnh báo, metric `aicam_report_seconds{report}` (02a §10)
TOP_PRODUCTS = 20

# BR-41: "hồ sơ hàng hoàn có kiện về" = Khách trả + Giao thất bại + Về trước khi sàn báo.
RETURN_RATE_KINDS = ("BUYER_RETURN", "FAILED_DELIVERY", "UNANNOUNCED")
SHARE_KINDS = ("BUYER_RETURN", "FAILED_DELIVERY")  # 02 §6.2: `share` chỉ 2 loại có tín hiệu sàn
KIND_ORDER = ("BUYER_RETURN", "FAILED_DELIVERY", "UNANNOUNCED", "UNIDENTIFIED", "REFUND_ONLY")
PRODUCT_KINDS = (*RETURN_RATE_KINDS, "REFUND_ONLY")  # "yêu cầu trả" theo sản phẩm (DEC-571)
RECEIVED_STATUSES = ("RECEIVED_OK", "RECEIVED_ISSUE")
EXPECTED_STATUSES = ("EXPECTED", "PARTIALLY_RECEIVED")  # "Đang về" = API-32 `returns_expected`
NO_REASON_LABEL = "Không có lý do"
PENDING_CLAIM_STATUSES = ("NEW", "SUBMITTED", "WAITING")


# ---------------------------------------------------------------- bộ lọc · kỳ · công thức


@dataclass(frozen=True)
class ReportFilters:
    from_: date
    to: date
    platform: str | None = None
    shop_id: uuid.UUID | None = None
    station_id: uuid.UUID | None = None

    def params(self) -> dict[str, str | None]:
        return {
            "from": self.from_.isoformat(),
            "to": self.to.isoformat(),
            "platform": self.platform,
            "shop_id": str(self.shop_id) if self.shop_id else None,
            "station_id": str(self.station_id) if self.station_id else None,
        }

    @property
    def days(self) -> int:
        return (self.to - self.from_).days + 1


def today_vn(tz: str) -> date:
    return clock.now().astimezone(ZoneInfo(tz)).date()


def make_filters(
    from_: date | None,
    to: date | None,
    platform: str | None,
    shop_id: uuid.UUID | None,
    station_id: uuid.UUID | None,
    tz: str,
) -> ReportFilters:
    """Kiểm kỳ (02 §6 "Kỳ báo cáo", EX-B1) — mọi lỗi trả một 422 `fields`.

    Thiếu ngày → 30 ngày tới hôm nay (DEC-571)."""
    today = today_vn(tz)
    to = to or today
    from_ = from_ or (to - timedelta(days=DEFAULT_DAYS - 1))
    fields: dict[str, str] = {}
    if to < from_:
        fields["to"] = "Ngày đến phải sau ngày từ."
    elif (to - from_).days + 1 > MAX_DAYS:
        fields["from"] = "Chọn tối đa 366 ngày."
    if to > today:
        fields["to"] = "Không chọn ngày trong tương lai."
    if fields:
        raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})
    return ReportFilters(from_, to, platform, shop_id, station_id)


def bounds(f: ReportFilters, tz: str) -> tuple[datetime, datetime]:
    zone = ZoneInfo(tz)
    start = datetime(f.from_.year, f.from_.month, f.from_.day, tzinfo=zone)
    end_day = f.to + timedelta(days=1)
    end = datetime(end_day.year, end_day.month, end_day.day, tzinfo=zone)
    return start.astimezone(UTC), end.astimezone(UTC)


def rate(numerator: int, denominator: int) -> float | None:
    """Mẫu số 0 → `None` (FE "—", EX-B2); làm tròn nửa lên 4 chữ số (FE hiện 1 chữ số sau dấu phẩy %)."""
    if not denominator:
        return None
    return float((Decimal(numerator) / denominator).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def ratio(numerator: int, denominator: int) -> s.Ratio:
    return s.Ratio(numerator=numerator, denominator=denominator, value=rate(numerator, denominator))


def round_seconds(total: float | Decimal | None, count: int) -> int | None:
    """Trung bình giây, làm tròn nửa lên (BR-41 "làm tròn giây"); không có phiên → `None`."""
    if not count or total is None:
        return None
    return int((Decimal(str(total)) / count).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def net_seconds(started_at: datetime, ended_at: datetime, waits: Iterable[float] = ()) -> float:
    """BR-41: thời gian một phiên = đóng − mở − tổng thời gian chờ duyệt (không âm).

    Bản Python của biểu thức `duration` trong `productivity_report`."""
    return max((ended_at - started_at).total_seconds() - sum(waits), 0.0)


def _in(col: Any, start: datetime, end: datetime) -> Any:
    return and_(col >= start, col < end)


def _shop(col: Any, f: ReportFilters) -> list[Any]:
    return shop_conditions(col, f.platform, f.shop_id)


def _platform_order(platform: str | None) -> int:
    return {"SHOPEE": 0, "TIKTOK": 1}.get(platform or "", 2)


async def _shop_meta(
    db: AsyncSession, ids: Iterable[uuid.UUID | None]
) -> Callable[[uuid.UUID | None], tuple[str | None, str | None]]:
    shops: dict[uuid.UUID, Shop] = await shops_by_id(db, ids)

    def meta(shop_id: uuid.UUID | None) -> tuple[str | None, str | None]:
        shop = shops.get(shop_id) if shop_id else None
        return (shop.platform, shop.name) if shop else (None, None)

    return meta


def _shop_sort_key(meta: Callable[[uuid.UUID | None], tuple[str | None, str | None]]) -> Any:
    def key(shop_id: uuid.UUID | None) -> tuple[int, int, str]:
        platform, name = meta(shop_id)
        return (shop_id is None, _platform_order(platform), (name or "").lower())

    return key


# ---------------------------------------------------------------- API-150 hàng hoàn


def _product_key() -> Any:
    """Gộp sản phẩm theo SKU; không có SKU → tên + phân loại (không phân biệt hoa thường, bỏ khoảng trắng
    thừa)."""
    sku = type_coerce(func.nullif(func.trim(OrderItem.sku), ""), Text)
    by_name = (
        literal("N:", Text)
        .concat(func.lower(func.trim(OrderItem.product_name)))
        .concat("|")
        .concat(func.lower(func.coalesce(func.trim(OrderItem.variation), "")))
    )
    return func.coalesce(literal("S:", Text).concat(sku), by_name)


async def returns_report(db: AsyncSession, f: ReportFilters, tz: str) -> s.ReturnsReportOut:
    start, end = bounds(f, tz)
    o = aliased(Order)
    case_shop = func.coalesce(ReturnCase.shop_id, o.shop_id)

    # Kiện chuyển `HANDED_OVER` trong kỳ (status_history), theo shop của đơn.
    handed: dict[uuid.UUID | None, int] = {
        shop_id: int(n)
        for shop_id, n in (
            await db.execute(
                select(o.shop_id, func.count(func.distinct(StatusHistory.package_id)))
                .select_from(StatusHistory)
                .join(Package, Package.id == StatusHistory.package_id)
                .outerjoin(o, o.id == Package.order_id)
                .where(
                    StatusHistory.to_status == "HANDED_OVER",
                    _in(StatusHistory.at, start, end),
                    *_shop(o.shop_id, f),
                )
                .group_by(o.shop_id)
            )
        ).all()
    }
    # Hồ sơ tạo trong kỳ theo (shop, loại); hồ sơ đã hủy / gộp (`CANCELLED`) không tính (DEC-571).
    created: dict[tuple[uuid.UUID | None, str], int] = {
        (shop_id, kind): int(n)
        for shop_id, kind, n in (
            await db.execute(
                select(case_shop, ReturnCase.kind, func.count())
                .select_from(ReturnCase)
                .outerjoin(o, o.id == ReturnCase.order_id)
                .where(
                    _in(ReturnCase.created_at, start, end),
                    ReturnCase.status != "CANCELLED",
                    *_shop(case_shop, f),
                )
                .group_by(case_shop, ReturnCase.kind)
            )
        ).all()
    }
    # Hồ sơ đã nhận theo giờ nhận: lý do khách × kết luận kho.
    received = (
        await db.execute(
            select(
                ReturnCase.reason,
                ReturnCase.conclusion,
                func.count(),
                func.count().filter(ReturnCase.status == "RECEIVED_ISSUE"),
            )
            .select_from(ReturnCase)
            .outerjoin(o, o.id == ReturnCase.order_id)
            .where(
                ReturnCase.status.in_(RECEIVED_STATUSES),
                _in(ReturnCase.received_at, start, end),
                *_shop(case_shop, f),
            )
            .group_by(ReturnCase.reason, ReturnCase.conclusion)
        )
    ).all()
    expected_now = int(
        await db.scalar(
            select(func.count())
            .select_from(ReturnCase)
            .outerjoin(o, o.id == ReturnCase.order_id)
            .where(ReturnCase.status.in_(EXPECTED_STATUSES), *_shop(case_shop, f))
        )
        or 0
    )

    handed_total = sum(handed.values())
    by_kind_count: dict[str, int] = defaultdict(int)
    shop_cases: dict[uuid.UUID | None, int] = defaultdict(int)
    for (shop_id, kind), n in created.items():
        by_kind_count[kind] += n
        if kind in RETURN_RATE_KINDS:
            shop_cases[shop_id] += n
    returned = sum(by_kind_count[k] for k in RETURN_RATE_KINDS)
    share_base = sum(by_kind_count[k] for k in SHARE_KINDS)
    refund_only = by_kind_count["REFUND_ONLY"]
    received_total = sum(int(r[2]) for r in received)
    issue_total = sum(int(r[3]) for r in received)

    rows: dict[str | None, dict[str, int]] = {}
    for reason, conclusion, n, _issue in received:
        counts = rows.setdefault(reason, dict.fromkeys(INSPECTION_CONCLUSIONS, 0))
        counts[conclusion if conclusion in counts else "OTHER"] += int(n)  # thiếu kết luận → "Khác"
    reason_rows = [
        s.ReasonRow(
            reason=reason,
            reason_label=reason_label(reason) or NO_REASON_LABEL,
            counts=counts,
            total=sum(counts.values()),
        )
        for reason, counts in rows.items()
    ]
    reason_rows.sort(key=lambda r: (r.reason is None, -r.total, r.reason_label))

    shop_ids = set(handed) | set(shop_cases)
    meta = await _shop_meta(db, shop_ids)
    by_shop = []
    for shop_id in sorted(shop_ids, key=_shop_sort_key(meta)):
        platform, name = meta(shop_id)
        n_handed, n_cases = handed.get(shop_id, 0), shop_cases.get(shop_id, 0)
        by_shop.append(
            s.ReturnShopRow(
                platform=platform,
                shop_id=shop_id,
                shop_name=name,
                handed_over=n_handed,
                return_cases=n_cases,
                rate=rate(n_cases, n_handed),
            )
        )

    return s.ReturnsReportOut(
        period=s.PeriodOut(from_=f.from_, to=f.to),
        filters=s.Filters(platform=f.platform, shop_id=f.shop_id),
        generated_at=clock.now(),
        cards=s.ReturnCards(
            return_rate=ratio(returned, handed_total),
            issue_rate=ratio(issue_total, received_total),
            refund_only=s.RefundOnlyCard(
                count=refund_only, rate_of_handed_over=rate(refund_only, handed_total)
            ),
            expected_now=expected_now,
        ),
        by_kind=[
            s.KindRow(
                kind=k,
                count=by_kind_count[k],
                share=rate(by_kind_count[k], share_base) if k in SHARE_KINDS else None,
            )
            for k in KIND_ORDER
            if by_kind_count[k]
        ],
        reason_by_conclusion=s.ReasonByConclusion(conclusions=list(INSPECTION_CONCLUSIONS), rows=reason_rows),
        top_products=await _top_products(db, f, start, end),
        by_shop=by_shop,
    )


async def _top_products(
    db: AsyncSession, f: ReportFilters, start: datetime, end: datetime
) -> list[s.ProductRow]:
    """Top 20 theo số hồ sơ yêu cầu trả tạo trong kỳ có sản phẩm trong đơn (DEC-571); "Đã gửi" = số đơn có sản
    phẩm, có kiện bàn giao trong kỳ."""
    o = aliased(Order)
    key = _product_key()
    case_shop = func.coalesce(ReturnCase.shop_id, o.shop_id)
    case_count = func.count(func.distinct(ReturnCase.id))
    top = (
        await db.execute(
            select(
                key,
                case_count,
                func.count(func.distinct(ReturnCase.id)).filter(ReturnCase.status == "RECEIVED_ISSUE"),
                func.min(func.trim(OrderItem.sku)),
                func.min(OrderItem.product_name),
                func.min(OrderItem.variation),
            )
            .select_from(ReturnCase)
            .join(OrderItem, OrderItem.order_id == ReturnCase.order_id)
            .outerjoin(o, o.id == ReturnCase.order_id)
            .where(
                ReturnCase.kind.in_(PRODUCT_KINDS),
                ReturnCase.status != "CANCELLED",
                _in(ReturnCase.created_at, start, end),
                *_shop(case_shop, f),
            )
            .group_by(key)
            .order_by(case_count.desc(), key)
            .limit(TOP_PRODUCTS)
        )
    ).all()
    if not top:
        return []
    keys = [row[0] for row in top]
    shipped: dict[str, int] = dict(
        (
            await db.execute(
                select(key, func.count(func.distinct(Package.order_id)))
                .select_from(StatusHistory)
                .join(Package, Package.id == StatusHistory.package_id)
                .join(OrderItem, OrderItem.order_id == Package.order_id)
                .outerjoin(o, o.id == Package.order_id)
                .where(
                    StatusHistory.to_status == "HANDED_OVER",
                    _in(StatusHistory.at, start, end),
                    key.in_(keys),
                    *_shop(o.shop_id, f),
                )
                .group_by(key)
            )
        ).all()
    )
    out = []
    for k, n_cases, n_issue, sku, name, variation in top:
        n_shipped = int(shipped.get(k, 0))
        out.append(
            s.ProductRow(
                sku=sku if str(k).startswith("S:") else None,
                product_name=name,
                variation=variation,
                shipped=n_shipped,
                return_requests=int(n_cases),
                rate=rate(int(n_cases), n_shipped),
                issue=int(n_issue),
            )
        )
    return out


# ---------------------------------------------------------------- API-151 khiếu nại


def _claim_outcome() -> Any:
    """Kết quả hồ sơ: `WON` / `LOST` / `PENDING` (đang xử lý) / `None` (đóng không có kết quả).

    Hồ sơ đã đóng sau khi có kết quả (`WON` → `CLOSED`) không còn trạng thái kết quả → lấy từ audit
    `CLAIM_UPDATE` gần nhất có `after.status` ∈ {WON, LOST} (cùng nguồn backfill 0006, DEC-461) — DEC-572."""
    after_status = AuditLog.data["after"]["status"].astext
    from_audit = (
        select(after_status)
        .where(
            AuditLog.object_type == "CLAIM",
            AuditLog.object_id == cast(Claim.id, Text),
            AuditLog.action == "CLAIM_UPDATE",
            after_status.in_(("WON", "LOST")),
        )
        .order_by(AuditLog.at.desc(), AuditLog.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    return case(
        (Claim.status.in_(("WON", "LOST")), Claim.status),
        (and_(Claim.status == "CLOSED", Claim.result_at.is_not(None)), from_audit),
        (Claim.status.in_(PENDING_CLAIM_STATUSES), literal("PENDING")),
        else_=None,
    )


def _claim_rows(f: ReportFilters, *where: Any) -> Any:
    """Dòng hồ sơ (không `LEGACY_HOLD`) kèm shop + kết quả — subquery để gộp theo `outcome`."""
    by_claim, by_package = aliased(Order), aliased(Order)
    claim_shop = func.coalesce(by_claim.shop_id, by_package.shop_id)
    return (
        select(
            claim_shop.label("shop_id"),
            Claim.status,
            Claim.type,
            Claim.counterparty,
            Claim.recovered_amount,
            Claim.submitted_at,
            Claim.deadline_at,
            _claim_outcome().label("outcome"),
        )
        .select_from(Claim)
        .join(Package, Package.id == Claim.package_id)
        .outerjoin(by_claim, by_claim.id == Claim.order_id)
        .outerjoin(by_package, by_package.id == Package.order_id)
        .where(Claim.source != "LEGACY_HOLD", *where, *_shop(claim_shop, f))
        .subquery()
    )


async def claims_report(db: AsyncSession, f: ReportFilters, tz: str) -> s.ClaimsReportOut:
    start, end = bounds(f, tz)
    now = clock.now()

    c = _claim_rows(f, _in(Claim.created_at, start, end))
    won_amount = func.coalesce(func.sum(c.c.recovered_amount).filter(c.c.outcome == "WON"), 0)
    created = (
        await db.execute(
            select(
                c.c.shop_id, c.c.status, c.c.type, c.c.counterparty, c.c.outcome, func.count(), won_amount
            ).group_by(c.c.shop_id, c.c.status, c.c.type, c.c.counterparty, c.c.outcome)
        )
    ).all()

    r = _claim_rows(f, _in(Claim.result_at, start, end))
    won, lost, recovered = (
        await db.execute(
            select(
                func.count().filter(r.c.outcome == "WON"),
                func.count().filter(r.c.outcome == "LOST"),
                func.coalesce(func.sum(r.c.recovered_amount).filter(r.c.outcome == "WON"), 0),
            )
        )
    ).one()

    sub = _claim_rows(f, _in(Claim.submitted_at, start, end))
    submitted, on_time = (
        await db.execute(
            select(
                func.count(),
                # Không có hạn → không trễ (DEC-572).
                func.count().filter(
                    or_(sub.c.deadline_at.is_(None), sub.c.submitted_at <= sub.c.deadline_at)
                ),
            )
        )
    ).one()

    od = _claim_rows(
        f, Claim.status == "NEW", Claim.deadline_at < now
    )  # BR-42 = API-32 `claims_overdue_unsent`
    overdue = int(await db.scalar(select(func.count()).select_from(od)) or 0)

    status_n: dict[str, int] = defaultdict(int)
    type_n: dict[str, dict[str, int]] = defaultdict(lambda: {"WON": 0, "LOST": 0, "PENDING": 0})
    party_n: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "WON": 0, "LOST": 0, "amount": 0})
    shop_n: dict[uuid.UUID | None, dict[str, int]] = defaultdict(
        lambda: {"count": 0, "WON": 0, "LOST": 0, "amount": 0}
    )
    for shop_id, status, type_, party, outcome, n, amount in created:
        n, amount = int(n), int(amount)
        status_n[status] += n
        per_type = type_n[type_]  # loại chỉ có hồ sơ đóng không kết quả vẫn có dòng (0 / 0 / 0)
        if outcome in per_type:
            per_type[outcome] += n
        for bucket in (party_n[party], shop_n[shop_id]):
            bucket["count"] += n
            bucket["amount"] += amount
            if outcome in ("WON", "LOST"):
                bucket[outcome] += n

    meta = await _shop_meta(db, shop_n)
    return s.ClaimsReportOut(
        period=s.PeriodOut(from_=f.from_, to=f.to),
        filters=s.Filters(platform=f.platform, shop_id=f.shop_id),
        generated_at=now,
        cards=s.ClaimCards(
            created=sum(status_n.values()),
            win_rate=ratio(int(won), int(won) + int(lost)),
            recovered_amount=int(recovered),
            submitted_before_deadline=ratio(int(on_time), int(submitted)),
            overdue_unsent_now=overdue,
        ),
        by_status=[s.ClaimStatusRow(status=st, count=status_n[st]) for st in CLAIM_STATUSES if status_n[st]],
        by_type_result=[
            s.ClaimTypeRow(type=t, won=type_n[t]["WON"], lost=type_n[t]["LOST"], pending=type_n[t]["PENDING"])
            for t in CLAIM_TYPES
            if t in type_n
        ],
        by_counterparty=[
            s.ClaimCounterpartyRow(
                counterparty=p,
                count=v["count"],
                won=v["WON"],
                lost=v["LOST"],
                recovered_amount=v["amount"],
            )
            for p in COUNTERPARTIES
            if (v := party_n.get(p))
        ],
        by_shop=[
            s.ClaimShopRow(
                platform=meta(sid)[0],
                shop_id=sid,
                shop_name=meta(sid)[1],
                count=v["count"],
                won=v["WON"],
                lost=v["LOST"],
                recovered_amount=v["amount"],
            )
            for sid in sorted(shop_n, key=_shop_sort_key(meta))
            if (v := shop_n[sid])
        ],
    )


# ---------------------------------------------------------------- API-152 năng suất


def _waits() -> Any:
    """Tổng thời gian chờ duyệt mỗi phiên (giây) — yêu cầu đã quyết định."""
    return (
        select(
            ApprovalRequest.session_id.label("session_id"),
            func.sum(func.extract("epoch", ApprovalRequest.decided_at - ApprovalRequest.created_at)).label(
                "wait"
            ),
        )
        .where(ApprovalRequest.session_id.is_not(None), ApprovalRequest.decided_at.is_not(None))
        .group_by(ApprovalRequest.session_id)
        .subquery()
    )


def _operator_key() -> Any:
    return func.nullif(func.lower(func.trim(PackSession.operator_name)), "")


def _first_name() -> Any:
    """Tên hiển thị = tên đầu tiên gặp (theo giờ mở phiên) của nhóm."""
    order = (PackSession.started_at, PackSession.id)
    return func.array_agg(aggregate_order_by(func.trim(PackSession.operator_name), *order))


async def productivity_report(db: AsyncSession, f: ReportFilters, tz: str) -> s.ProductivityReportOut:
    start, end = bounds(f, tz)
    o = aliased(Order)
    waits = _waits()
    duration = func.greatest(
        func.extract("epoch", PackSession.ended_at - PackSession.started_at) - func.coalesce(waits.c.wait, 0),
        0,
    )
    op_key = _operator_key()
    done = PackSession.status == "COMPLETED"
    scope = [
        _in(PackSession.ended_at, start, end),
        *([PackSession.station_id == f.station_id] if f.station_id else []),
        *_shop(o.shop_id, f),
    ]

    def base(*cols: Any) -> Any:
        return (
            select(*cols)
            .select_from(PackSession)
            .join(Package, Package.id == PackSession.package_id)
            .outerjoin(o, o.id == Package.order_id)
            .outerjoin(waits, waits.c.session_id == PackSession.id)
        )

    pack_rows = (
        await db.execute(
            base(
                PackSession.station_id,
                op_key,
                func.count().filter(done),
                func.coalesce(func.sum(duration).filter(done), 0),
                func.count().filter(PackSession.flags.contains(["HAD_MISMATCH"])),
                func.count().filter(PackSession.status == "ABANDONED"),
                func.count().filter(PackSession.status == "CANCELLED"),
                func.count().filter(done, PackSession.flags.contains(["REPACK"])),
                func.min(PackSession.started_at),
                _first_name(),
            )
            .where(
                PackSession.type == "PACK",
                PackSession.status.in_(("COMPLETED", "ABANDONED", "CANCELLED")),
                *scope,
            )
            .group_by(PackSession.station_id, op_key)
        )
    ).all()
    return_rows = (
        await db.execute(
            base(
                op_key,
                func.count(),
                func.coalesce(func.sum(duration), 0),
                func.count().filter(PackSession.inspection_conclusion != "OK"),
                func.min(PackSession.started_at),
                _first_name(),
            )
            # Phiên hoàn bị loại (quét nhầm / không phải hàng hoàn — BR-39) không tính năng suất bàn hoàn.
            .where(PackSession.type == "RETURN", done, not_(excluded_return_sql()), *scope)
            .group_by(op_key)
        )
    ).all()

    zero = {"packed": 0, "dur": 0.0, "mismatch": 0, "abandoned": 0, "cancelled": 0, "repacked": 0}
    stations: dict[uuid.UUID, dict[str, float]] = defaultdict(lambda: dict(zero))
    operators: dict[str | None, dict[str, float]] = defaultdict(lambda: dict(zero))
    names: dict[str | None, tuple[datetime, str | None]] = {}
    for station_id, key, packed, dur, mismatch, abandoned, cancelled, repacked, first_at, first in pack_rows:
        for bucket in (stations[station_id], operators[key]):
            bucket["packed"] += int(packed)
            bucket["dur"] += float(dur)
            bucket["mismatch"] += int(mismatch)
            bucket["abandoned"] += int(abandoned)
            bucket["cancelled"] += int(cancelled)
            bucket["repacked"] += int(repacked)
        if key not in names or first_at < names[key][0]:
            names[key] = (first_at, first[0] if key is not None and first else None)

    station_names = (
        dict((await db.execute(select(Station.id, Station.name).where(Station.id.in_(list(stations))))).all())
        if stations
        else {}
    )
    by_station = sorted(
        (
            s.StationRow(
                station_id=sid,
                station_name=station_names.get(sid, ""),
                packed=int(v["packed"]),
                avg_seconds=round_seconds(v["dur"], int(v["packed"])),
                mismatch=int(v["mismatch"]),
                abandoned=int(v["abandoned"]),
                cancelled=int(v["cancelled"]),
                repacked=int(v["repacked"]),
            )
            for sid, v in stations.items()
        ),
        key=lambda r: r.station_name.lower(),
    )
    by_operator = sorted(
        (
            s.OperatorRow(
                operator_name=names[key][1],
                packed=int(v["packed"]),
                avg_seconds=round_seconds(v["dur"], int(v["packed"])),
                mismatch=int(v["mismatch"]),
                abandoned=int(v["abandoned"]),
                cancelled=int(v["cancelled"]),
                repacked=int(v["repacked"]),
            )
            for key, v in operators.items()
        ),
        # "(Không ghi tên)" cuối bảng; còn lại nhiều kiện trước.
        key=lambda r: (r.operator_name is None, -r.packed, (r.operator_name or "").lower()),
    )
    return_by_operator = sorted(
        (
            s.ReturnOperatorRow(
                operator_name=first[0] if key is not None and first else None,
                inspected=int(n),
                avg_seconds=round_seconds(dur, int(n)),
                issue_rate=ratio(int(issue), int(n)),
            )
            for key, n, dur, issue, _first_at, first in return_rows
        ),
        key=lambda r: (r.operator_name is None, -r.inspected, (r.operator_name or "").lower()),
    )
    packed_total = sum(int(v["packed"]) for v in stations.values())
    inspected_total = sum(int(r[1]) for r in return_rows)
    return s.ProductivityReportOut(
        period=s.PeriodOut(from_=f.from_, to=f.to),
        filters=s.ProductivityFilters(platform=f.platform, shop_id=f.shop_id, station_id=f.station_id),
        generated_at=clock.now(),
        cards=s.ProductivityCards(
            packed=packed_total,
            pack_avg_seconds=round_seconds(sum(v["dur"] for v in stations.values()), packed_total),
            returns_inspected=inspected_total,
            return_avg_seconds=round_seconds(sum(float(r[2]) for r in return_rows), inspected_total),
        ),
        by_station=by_station,
        by_operator=by_operator,
        return_by_operator=return_by_operator,
    )


# ---------------------------------------------------------------- chạy: cache + statement_timeout


REPORTS: dict[str, Callable[[AsyncSession, ReportFilters, str], Awaitable[BaseModel]]] = {
    "returns": returns_report,
    "claims": claims_report,
    "productivity": productivity_report,
}
MODELS: dict[str, type[BaseModel]] = {
    "returns": s.ReturnsReportOut,
    "claims": s.ClaimsReportOut,
    "productivity": s.ProductivityReportOut,
}


def cache_key(name: str, f: ReportFilters) -> str:
    """`report:{name}:{sha1(bộ lọc)}`; `station_id` chỉ có nghĩa với năng suất."""
    params = f.params() if name == "productivity" else {**f.params(), "station_id": None}
    digest = hashlib.sha1(json.dumps(params, sort_keys=True).encode(), usedforsecurity=False).hexdigest()
    return f"report:{name}:{digest}"


def _is_timeout(exc: DBAPIError) -> bool:
    orig = exc.orig
    return (getattr(orig, "pgcode", None) or getattr(orig, "sqlstate", None)) == QUERY_CANCELED


async def build(db: AsyncSession, name: str, f: ReportFilters, tz: str) -> BaseModel:
    """Tính trong savepoint có `statement_timeout`; quá hạn → 503 `REPORT_TIMEOUT` (rollback savepoint)."""
    began = time.monotonic()
    try:
        async with db.begin_nested():
            await db.execute(text(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'"))
            out = await REPORTS[name](db, f, tz)
    except DBAPIError as exc:
        if not _is_timeout(exc):
            raise
        log.warning("report_timeout", report=name, days=f.days, timeout=STATEMENT_TIMEOUT)
        raise AppError("REPORT_TIMEOUT", "Không tải được báo cáo.", 503) from exc
    took = time.monotonic() - began
    # metric `aicam_report_seconds{report}` (02a §10) — log có cấu trúc; cảnh báo khi kỳ ≤ 92 ngày > 3 giây.
    (log.warning if f.days <= 92 and took > SLOW_REPORT_S else log.info)(
        "report_built", report=name, days=f.days, seconds=round(took, 3)
    )
    return out


async def get_report[M: BaseModel](
    db: AsyncSession, name: str, f: ReportFilters, tz: str, model: type[M]
) -> M:
    """Cache Redis 60 giây theo bộ lọc (02a §8) — số mới nhất sau tối đa 60 giây, không job tổng hợp."""
    key = cache_key(name, f)
    redis = get_redis()
    cached = await redis.get(key)
    if cached:
        return model.model_validate_json(cached)
    out = model.model_validate(await build(db, name, f, tz), from_attributes=True)
    await redis.set(key, out.model_dump_json(by_alias=True), ex=CACHE_TTL_S)
    return out
