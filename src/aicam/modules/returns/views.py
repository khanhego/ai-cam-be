"""API-110 danh sách / API-111 chi tiết hồ sơ hàng hoàn (02 §6.2, 02a §4, §8: `tab_counts` một `GROUP BY`)."""

import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import ColumnElement, and_, case, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.errors import AppError
from aicam.modules.claims.models import Claim
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms.shopee.returns_mapping import REASON_LABELS
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.returns.schemas import (
    CaseRef,
    ClaimBrief,
    RequestedItem,
    ReturnCaseDetail,
    ReturnCaseItem,
    ReturnCasePage,
    ReturnOrderBrief,
    ReturnPackageBrief,
    ReturnSessionBrief,
    TabCounts,
)
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

MAX_RANGE_DAYS = 92
TAB_STATUSES: dict[str, tuple[str, ...]] = {
    "EXPECTED": ("EXPECTED", "INSPECTING", "PARTIALLY_RECEIVED"),
    "MISSING": ("MISSING",),
    "RECEIVED": ("RECEIVED_OK", "RECEIVED_ISSUE"),
    "NO_PARCEL": ("NO_PARCEL",),
}


def reason_label(reason: str | None) -> str | None:
    """Mã lý do sàn → chữ tiếng Việt; mã lạ → giữ chữ gốc (02 §6.2 API-110)."""
    if not reason:
        return None
    return REASON_LABELS.get(reason, reason)


def waiting_days(expected_since: datetime | None, received_at: datetime | None, tz: str) -> int | None:
    """Số ngày (giờ VN) từ lúc vào "Đang về" tới hôm nay; đã nhận → null."""
    if expected_since is None or received_at is not None:
        return None
    zone = ZoneInfo(tz)
    return (clock.now().astimezone(zone).date() - expected_since.astimezone(zone).date()).days


def _tab_condition(tab: str) -> ColumnElement[bool] | None:
    if tab == "ALL":
        return None
    if tab == "UNIDENTIFIED":
        return and_(ReturnCase.kind == "UNIDENTIFIED", ReturnCase.status != "CANCELLED")
    return ReturnCase.status.in_(TAB_STATUSES[tab])


def _validate_range(date_from: date | None, date_to: date | None) -> None:
    if date_from and date_to:
        if date_from > date_to:
            raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422,
                           {"fields": {"date_to": "Ngày kết thúc phải sau ngày bắt đầu"}})  # fmt: skip
        if (date_to - date_from).days + 1 > MAX_RANGE_DAYS:
            raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422,
                           {"fields": {"date_to": f"Khoảng ngày tối đa {MAX_RANGE_DAYS} ngày"}})  # fmt: skip


def _day_start(day: date, tz: str) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))


async def _items(db: AsyncSession, cases: list[ReturnCase], tz: str) -> list[ReturnCaseItem]:
    ids = [c.id for c in cases]
    if not ids:
        return []
    packages: dict[uuid.UUID, list[ReturnPackageBrief]] = defaultdict(list)
    for case_id, package in (
        await db.execute(
            select(ReturnCasePackage.return_case_id, Package)
            .join(Package, Package.id == ReturnCasePackage.package_id)
            .where(ReturnCasePackage.return_case_id.in_(ids))
            .order_by(Package.tracking_number)
        )
    ).all():
        packages[case_id].append(
            ReturnPackageBrief(
                id=package.id,
                tracking_number=package.tracking_number,
                warehouse_status=package.warehouse_status,
            )
        )
    claims: dict[uuid.UUID, list[ClaimBrief]] = defaultdict(list)
    for claim in (
        await db.scalars(select(Claim).where(Claim.return_case_id.in_(ids)).order_by(Claim.created_at))
    ).all():
        if claim.return_case_id is not None:
            claims[claim.return_case_id].append(ClaimBrief(id=claim.id, code=claim.code, status=claim.status))
    order_ids = {c.order_id for c in cases if c.order_id}
    orders: dict[uuid.UUID, Order] = {}
    if order_ids:
        orders = {o.id: o for o in (await db.scalars(select(Order).where(Order.id.in_(order_ids)))).all()}
    merged_ids = {c.merged_into_id for c in cases if c.merged_into_id}
    merged: dict[uuid.UUID, str] = {}
    if merged_ids:
        rows = await db.scalars(select(ReturnCase).where(ReturnCase.id.in_(merged_ids)))
        merged = {r.id: r.code for r in rows.all()}
    out = []
    for c in cases:
        order = orders.get(c.order_id) if c.order_id else None
        out.append(
            ReturnCaseItem(
                id=c.id,
                code=c.code,
                kind=c.kind,
                status=c.status,
                order=ReturnOrderBrief(id=order.id, platform_order_sn=order.platform_order_sn)
                if order
                else None,
                packages=packages.get(c.id, []),
                return_tracking_number=c.return_tracking_number,
                reason_label=reason_label(c.reason),
                reported_at=c.reported_at,
                expected_since=c.expected_since,
                waiting_days=waiting_days(c.expected_since, c.received_at, tz),
                received_at=c.received_at,
                conclusion=c.conclusion,
                claims=claims.get(c.id, []),
                merged_into=CaseRef(id=c.merged_into_id, code=merged[c.merged_into_id])
                if c.merged_into_id and c.merged_into_id in merged
                else None,
            )
        )
    return out


async def list_cases(
    db: AsyncSession,
    *,
    tz: str,
    tab: str,
    kind: str | None,
    q: str | None,
    date_from: date | None,
    date_to: date | None,
    page: int,
    page_size: int,
) -> ReturnCasePage:
    """API-110 (FR-05.05, 05.11, 05.12): lọc theo tab / loại / mã / ngày (theo `reported_at`, hồ sơ do kho tạo
    theo lúc tạo); `tab_counts` một truy vấn `GROUP BY` trên cùng bộ lọc (trừ tab)."""
    _validate_range(date_from, date_to)
    conds: list[ColumnElement[bool]] = []
    if kind:
        conds.append(ReturnCase.kind == kind)
    if q and q.strip():
        code = q.strip().upper()
        by_package = exists().where(
            ReturnCasePackage.return_case_id == ReturnCase.id,
            ReturnCasePackage.package_id == Package.id,
            func.upper(Package.tracking_number) == code,
        )
        by_order = exists().where(
            Order.id == ReturnCase.order_id, func.upper(Order.platform_order_sn) == code
        )
        conds.append(
            or_(
                func.upper(ReturnCase.return_tracking_number) == code,
                func.upper(ReturnCase.platform_return_sn) == code,
                func.upper(ReturnCase.code) == code,
                by_package,
                by_order,
            )
        )
    day = func.coalesce(ReturnCase.reported_at, ReturnCase.created_at)
    if date_from:
        conds.append(day >= _day_start(date_from, tz))
    if date_to:
        conds.append(day < _day_start(date_to + timedelta(days=1), tz))

    counts_row = (
        await db.execute(
            select(
                *(
                    func.count(case((cond, 1))).label(name)
                    for name in ("EXPECTED", "MISSING", "RECEIVED", "NO_PARCEL", "UNIDENTIFIED")
                    if (cond := _tab_condition(name)) is not None
                )
            ).where(*conds)
        )
    ).one()
    tab_counts = TabCounts(**counts_row._asdict())

    tab_cond = _tab_condition(tab)
    where = [*conds, *([tab_cond] if tab_cond is not None else [])]
    total = await db.scalar(select(func.count()).select_from(ReturnCase).where(*where)) or 0
    order_by = (
        (ReturnCase.expected_since.asc().nulls_last(), ReturnCase.id)
        if tab in ("EXPECTED", "MISSING")
        else (ReturnCase.created_at.desc(), ReturnCase.id.desc())
    )
    cases = list(
        (
            await db.scalars(
                select(ReturnCase)
                .where(*where)
                .order_by(*order_by)
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
        ).all()
    )
    return ReturnCasePage(
        items=await _items(db, cases, tz), page=page, page_size=page_size, total=total, tab_counts=tab_counts
    )


async def case_detail(db: AsyncSession, case_id: uuid.UUID, tz: str) -> ReturnCaseDetail:
    """API-111 (FR-07.02): hồ sơ + kiện + phiên RETURN + hồ sơ khiếu nại (brief)."""
    found = await db.get(ReturnCase, case_id)
    if found is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ hàng hoàn.", 404)
    item = (await _items(db, [found], tz))[0]
    rows = (
        await db.execute(
            select(PackSession, Station.name)
            .join(Station, Station.id == PackSession.station_id)
            .where(PackSession.return_case_id == found.id, PackSession.type == "RETURN")
            .order_by(PackSession.started_at)
        )
    ).all()
    sessions = [
        ReturnSessionBrief(
            id=s.id,
            package_id=s.package_id,
            status=s.status,
            station_name=name,
            operator_name=s.operator_name,
            started_at=s.started_at,
            ended_at=s.ended_at,
            conclusion=s.inspection_conclusion,
        )
        for s, name in rows
    ]
    return ReturnCaseDetail(
        **item.model_dump(),
        platform_return_sn=found.platform_return_sn,
        platform_status=found.platform_status,
        needs_parcel=found.needs_parcel,
        reason=found.reason,
        reason_text=found.reason_text,
        seller_due_at=found.seller_due_at,
        source=found.source,
        requested_items=[
            RequestedItem(
                order_item_id=uuid.UUID(r["order_item_id"]) if r.get("order_item_id") else None,
                product_name=str(r.get("product_name") or ""),
                variation=r.get("variation"),
                quantity=int(r.get("quantity") or 0),
            )
            for r in found.requested_items or []
        ],
        sessions=sessions,
    )
