"""API-30 tra cứu kiện, API-31 chi tiết kiện (02 §6.2; FR-07.01..03). Chỉ đọc."""

import uuid
from datetime import date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel
from sqlalchemy import ColumnElement, and_, any_, exists, func, literal, or_, select
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.errors import AppError
from aicam.core.pagination import Page
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Order, OrderItem, Package, Shop, StatusHistory
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings import service as settings_service
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

MAX_RANGE_DAYS = 92


# ---------------------------------------------------------------- schema


class LastSession(BaseModel):
    station_name: str
    ended_at: datetime | None


class PackageItem(BaseModel):
    id: uuid.UUID
    tracking_number: str
    platform_order_sn: str | None
    warehouse_status: str
    platform_status: str | None
    source: Literal["API", "CSV"] | None
    last_session: LastSession | None
    has_clip: bool


class ItemDetail(BaseModel):
    product_name: str
    variation: str | None
    quantity: int
    image_url: str | None


class OrderDetail(BaseModel):
    id: uuid.UUID
    platform: str
    platform_order_sn: str
    platform_status: str | None
    buyer_note: str | None
    source: str
    items: list[ItemDetail]


class ClipDetail(BaseModel):
    id: uuid.UUID
    camera_role: str
    status: str
    sha256: str | None
    duration_s: float | None
    held: bool
    retention_until: datetime | None
    deleted_at: datetime | None
    flags: list[str]


class SessionDetail(BaseModel):
    id: uuid.UUID
    status: str
    station_name: str
    started_at: datetime
    ended_at: datetime | None
    duration_s: int | None
    flags: list[str]
    cancel_reason: str | None
    note: str | None
    clips: list[ClipDetail]


class TimelineItem(BaseModel):
    at: datetime
    source: str
    from_status: str | None
    to_status: str
    actor: str | None


class PackageDetail(BaseModel):
    id: uuid.UUID
    tracking_number: str
    warehouse_status: str
    platform_logistics_status: str | None
    verified: bool
    order: OrderDetail | None
    sessions: list[SessionDetail]
    timeline: list[TimelineItem]


# ---------------------------------------------------------------- API-30


def _day_start(day: date, tz: str) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))


def _validate_range(date_from: date | None, date_to: date | None) -> None:
    if date_from and date_to:
        if date_from > date_to:
            raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422,
                           {"fields": {"date_to": "Ngày kết thúc phải sau ngày bắt đầu"}})  # fmt: skip
        if (date_to - date_from).days + 1 > MAX_RANGE_DAYS:
            raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422,
                           {"fields": {"date_to": f"Khoảng ngày tối đa {MAX_RANGE_DAYS} ngày"}})  # fmt: skip


async def search(
    db: AsyncSession,
    *,
    tz: str,
    page: int,
    page_size: int,
    q: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    station_id: uuid.UUID | None = None,
    warehouse_status: str | None = None,
    session_status: str | None = None,
    session_flag: str | None = None,
    source: str | None = None,
) -> Page[PackageItem]:
    """Lọc theo phiên (EXISTS): `session_status` theo ngày kết thúc, `session_flag` theo ngày bắt đầu (khớp
    định nghĩa thẻ API-32); không lọc phiên thì ngày theo lúc phiên kết thúc (hoặc bắt đầu nếu còn mở)."""
    _validate_range(date_from, date_to)
    conditions: list[ColumnElement[bool]] = []
    if q and q.strip():
        code = q.strip().upper()
        conditions.append(
            or_(func.upper(Package.tracking_number) == code, func.upper(Order.platform_order_sn) == code)
        )
    if warehouse_status:
        conditions.append(Package.warehouse_status == warehouse_status)
    if source:
        conditions.append(Order.source == source)

    session_conds: list[ColumnElement[bool]] = [PackSession.package_id == Package.id]
    if station_id:
        session_conds.append(PackSession.station_id == station_id)
    if session_status:
        session_conds.append(PackSession.status == session_status)
    if session_flag:
        session_conds.append(literal(session_flag) == any_(PackSession.flags))
    if date_from or date_to:
        when = (
            PackSession.ended_at
            if session_status
            else PackSession.started_at
            if session_flag
            else func.coalesce(PackSession.ended_at, PackSession.started_at)
        )
        if date_from:
            session_conds.append(when >= _day_start(date_from, tz))
        if date_to:
            session_conds.append(when < _day_start(date_to + timedelta(days=1), tz))
    if len(session_conds) > 1:
        conditions.append(exists().where(and_(*session_conds)))

    base = select(Package, Order).outerjoin(Order, Order.id == Package.order_id).where(*conditions)
    total = await db.scalar(select(func.count()).select_from(base.subquery())) or 0
    rows = (
        await db.execute(
            base.order_by(Package.updated_at.desc(), Package.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    ids = [p.id for p, _ in rows]
    last: dict[uuid.UUID, LastSession] = {}
    with_clip: set[uuid.UUID] = set()
    if ids:
        latest = (
            await db.execute(
                select(PackSession.package_id, Station.name, PackSession.ended_at)
                .join(Station, Station.id == PackSession.station_id)
                .where(PackSession.package_id.in_(ids))
                .order_by(PackSession.package_id, PackSession.started_at.desc())
                .ext(distinct_on(PackSession.package_id))
            )
        ).all()
        last = {pid: LastSession(station_name=name, ended_at=ended) for pid, name, ended in latest}
        with_clip = set(
            (
                await db.scalars(
                    select(PackSession.package_id)
                    .join(Clip, Clip.session_id == PackSession.id)
                    .where(PackSession.package_id.in_(ids), Clip.status == "READY")
                    .distinct()
                )
            ).all()
        )
    items = [
        PackageItem(
            id=p.id,
            tracking_number=p.tracking_number,
            platform_order_sn=o.platform_order_sn if o else None,
            warehouse_status=p.warehouse_status,
            platform_status=o.platform_status if o else None,
            source=o.source if o else None,
            last_session=last.get(p.id),
            has_clip=p.id in with_clip,
        )
        for p, o in rows
    ]
    return Page(items=items, page=page, page_size=page_size, total=total)


# ---------------------------------------------------------------- API-31


async def detail(db: AsyncSession, package_id: uuid.UUID) -> PackageDetail:
    package = await db.get(Package, package_id)
    if package is None:
        raise AppError("NOT_FOUND", "Không tìm thấy kiện hàng.", 404)
    order_out = None
    if package.order_id:
        order = await db.get(Order, package.order_id)
        if order is not None:
            shop = await db.get(Shop, order.shop_id) if order.shop_id else None
            items = (await db.scalars(select(OrderItem).where(OrderItem.order_id == order.id))).all()
            order_out = OrderDetail(
                id=order.id,
                platform=shop.platform if shop else "SHOPEE",
                platform_order_sn=order.platform_order_sn,
                platform_status=order.platform_status,
                buyer_note=order.buyer_note,
                source=order.source,
                items=[
                    ItemDetail(
                        product_name=i.product_name,
                        variation=i.variation,
                        quantity=i.quantity,
                        image_url=i.image_url,
                    )
                    for i in items
                ],
            )
    cfg = await settings_service.get(db)
    rows = (
        await db.execute(
            select(PackSession, Station.name)
            .join(Station, Station.id == PackSession.station_id)
            .where(PackSession.package_id == package.id)
            .order_by(PackSession.started_at.desc())
        )
    ).all()
    session_ids = [s.id for s, _ in rows]
    clips_by_session: dict[uuid.UUID, list[ClipDetail]] = {sid: [] for sid in session_ids}
    if session_ids:
        clips = (
            await db.scalars(select(Clip).where(Clip.session_id.in_(session_ids)).order_by(Clip.camera_role))
        ).all()
        for c in clips:
            # Clip đã xóa: `retention_until` = ngày bị xóa (FE DEC-76 đọc cho câu "Clip đã bị xóa ngày …").
            if c.status == "DELETED":
                until = c.deleted_at
            elif c.held:
                until = None
            else:
                until = c.end_at + timedelta(days=cfg.retention_clip_days)
            clips_by_session[c.session_id].append(
                ClipDetail(
                    id=c.id,
                    camera_role=c.camera_role,
                    status=c.status,
                    sha256=c.sha256,
                    duration_s=float(c.duration_s) if c.duration_s is not None else None,
                    held=c.held,
                    retention_until=until,
                    deleted_at=c.deleted_at,
                    flags=list(c.flags),
                )
            )
    sessions = [
        SessionDetail(
            id=s.id,
            status=s.status,
            station_name=name,
            started_at=s.started_at,
            ended_at=s.ended_at,
            duration_s=int((s.ended_at - s.started_at).total_seconds()) if s.ended_at else None,
            flags=list(s.flags),
            cancel_reason=s.cancel_reason,
            note=s.note,
            clips=clips_by_session[s.id],
        )
        for s, name in rows
    ]
    history = (
        await db.execute(
            select(StatusHistory, User.display_name)
            .outerjoin(User, User.id == StatusHistory.actor_user_id)
            .where(StatusHistory.package_id == package.id)
            .order_by(StatusHistory.at, StatusHistory.id)
        )
    ).all()
    timeline = [
        TimelineItem(
            at=h.at,
            source=h.source,
            from_status=h.from_status,
            to_status=h.to_status,
            actor=h.actor_label or display,
        )
        for h, display in history
    ]
    return PackageDetail(
        id=package.id,
        tracking_number=package.tracking_number,
        warehouse_status=package.warehouse_status,
        platform_logistics_status=package.platform_logistics_status,
        verified=package.verified,
        order=order_out,
        sessions=sessions,
        timeline=timeline,
    )
