"""API-30 tra cứu kiện, API-31 chi tiết kiện (02 §6.2; FR-07.01..03; Phase 2 mở rộng FR-07.01, 07.02,
FR-02.09, 02.11). Chỉ đọc."""

import uuid
from datetime import date, datetime, time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel
from sqlalchemy import ColumnElement, and_, any_, exists, func, literal, or_, select
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.errors import AppError
from aicam.core.pagination import Page
from aicam.core.settings import Settings
from aicam.modules.claims.models import Claim
from aicam.modules.media import protection
from aicam.modules.media import snapshots as snapshot_media
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Order, OrderItem, Package, Shop, StatusHistory
from aicam.modules.orders.refs import ShopRef, shop_conditions, shop_ref, shops_by_id
from aicam.modules.orders.service import MANUAL_TRANSITIONS, merged_orders
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.reconciliation.service import RULE_BR
from aicam.modules.returns import views as return_views
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase, ReturnCasePackage
from aicam.modules.returns.schemas import ReturnCaseItem
from aicam.modules.sessions import inspection
from aicam.modules.sessions.models import SESSION_STATUSES, PackSession, SessionEvent
from aicam.modules.sessions.queries import dropped_return_filter
from aicam.modules.sessions.schemas import InspectionLineOut, InspectionOut
from aicam.modules.settings import service as settings_service
from aicam.modules.shares import queries as share_queries
from aicam.modules.shares.schemas import ShareBrief
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

MAX_RANGE_DAYS = 92
CORRECTION_WINDOW = timedelta(days=7)  # API-113 (FR-04.11)
CORRECTORS = ("ADMIN", "SUPERVISOR")


# ---------------------------------------------------------------- schema


class LastSession(BaseModel):
    station_name: str
    ended_at: datetime | None


class ReturnCaseBrief(BaseModel):
    id: uuid.UUID
    code: str
    kind: str
    status: str


class PackageItem(BaseModel):
    id: uuid.UUID
    tracking_number: str
    platform_order_sn: str | None
    # Phase 3 (02 §6.2 API-30 — T-215): null = chưa gắn shop.
    platform: str | None = None
    shop: ShopRef | None = None
    warehouse_status: str
    platform_status: str | None
    source: Literal["API", "CSV"] | None
    last_session: LastSession | None
    has_clip: bool
    is_placeholder: bool  # kiện tạm của hàng hoàn chưa xác định (02 §6.2 API-30, DEC-260) — FE hiện chip
    return_case: ReturnCaseBrief | None  # 02 §6.2 API-30 mở rộng: hồ sơ hàng hoàn của kiện


class ItemDetail(BaseModel):
    product_name: str
    variation: str | None
    quantity: int
    image_url: str | None


class MergedOrderOut(BaseModel):
    platform_order_sn: str


class OrderDetail(BaseModel):
    id: uuid.UUID
    # Phase 3: `null` = đơn chưa gắn shop (đơn file — "Chưa rõ sàn", DEC-541).
    platform: str | None
    shop: ShopRef | None = None
    platform_order_sn: str
    platform_status: str | None
    platform_status_group: str = "UNKNOWN"
    merged_orders: list[MergedOrderOut] = []
    buyer_note: str | None
    source: str
    items: list[ItemDetail]


class Protection(BaseModel):
    """02 §6.2 API-31 v0.2 (DEC-245, ADR-009): lý do clip / ảnh không bị retention xóa."""

    reasons: list[Literal["CLAIM", "RETURN_CASE", "HELD"]]
    claims: list[str]
    return_cases: list[str]
    until: datetime | None


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
    protected_by_claim: bool
    protection: Protection | None


class ClaimRef(BaseModel):
    id: uuid.UUID
    code: str


class CorrectionBy(BaseModel):
    id: uuid.UUID | None
    display_name: str


class CorrectionBefore(BaseModel):
    conclusion: str | None
    note: str
    lines: list[InspectionLineOut]


class InspectionCorrection(BaseModel):
    """Một lần sửa kết luận (API-113, 02 §6.3 #4, DEC-261)."""

    at: datetime
    by: CorrectionBy
    reason: str
    before: CorrectionBefore


class SessionInspection(InspectionOut):
    corrections: list[InspectionCorrection]
    corrected: InspectionCorrection | None  # lần sửa gần nhất (tương thích 02 §6.2 v0.1 `corrected`)


class SessionSnapshot(BaseModel):
    id: uuid.UUID
    kind: Literal["MANUAL", "PACK_CLOSE"]
    taken_at: datetime
    url: str | None  # null khi ảnh đã xóa theo lưu trữ / thiếu tệp
    status: Literal["READY", "DELETED", "MISSING"]  # MISSING (v0.3 — DEC-524): url, protection = null
    protection: Protection | None


class PackSnapshot(BaseModel):
    id: uuid.UUID
    url: str | None
    status: Literal["READY", "DELETED", "MISSING"]


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
    protected_by_claims: list[ClaimRef]
    # Phase 2 (02 §6.2 API-31 mở rộng).
    type: Literal["PACK", "RETURN"]
    operator_name: str | None
    return_case_id: uuid.UUID | None
    inspection: SessionInspection | None  # chỉ phiên RETURN
    can_correct: bool  # API-113: phiên RETURN COMPLETED ≤ 7 ngày, người xem ADMIN / SUPERVISOR
    snapshots: list[SessionSnapshot]  # ảnh chụp tay (phiên RETURN)
    pack_snapshot: PackSnapshot | None  # phiên PACK: ảnh Cam 1 lúc đóng gói (J-17)


class ReconAlertBrief(BaseModel):
    id: uuid.UUID
    rule: str
    br: str
    severity: str
    status: str
    detected_at: datetime
    closed_at: datetime | None


class PackageClaimBrief(BaseModel):
    id: uuid.UUID
    code: str
    type: str
    status: str


class TimelineShop(BaseModel):
    platform: str
    name: str | None


class TimelineItem(BaseModel):
    at: datetime
    source: str
    from_status: str | None
    to_status: str
    actor: str | None
    # Phase 3 (02 §6.2 API-31, BR-32, DEC-561): dòng của sự kiện phiên `AMBIGUOUS_SHOP` — mã có ở ≥ 2 shop khi
    # quét (`source = WAREHOUSE`, `to_status = PACKING`, `actor` = station); dòng trạng thái thường → null.
    shops: list[TimelineShop] | None = None


class PackageDetail(BaseModel):
    id: uuid.UUID
    tracking_number: str
    warehouse_status: str
    platform_logistics_status: str | None
    verified: bool
    order: OrderDetail | None
    sessions: list[SessionDetail]
    timeline: list[TimelineItem]
    # Phase 2 (02 §6.2 API-31 mở rộng).
    is_placeholder: bool
    return_cases: list[ReturnCaseItem]
    recon_alerts: list[ReconAlertBrief]
    claims: list[PackageClaimBrief]
    allowed_status_targets: list[str]  # đích "Điều chỉnh trạng thái" (API-122); rỗng → FE ẩn menu
    # Phase 3 link chia sẻ (02 §6.2 API-31, FR-07.09): ≤ 3 link mới nhất có phiên của kiện (trừ `FAILED`).
    shares: list[ShareBrief] = []
    shares_active_count: int = 0


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


SESSION_STATUS_MAX = 4


def parse_session_statuses(raw: str | None) -> list[str] | None:
    """API-30 `session_status` (Phase 3): một hoặc nhiều giá trị cách dấu phẩy (≤ 4) — sai → 422."""
    if raw is None or not raw.strip():
        return None
    values = list(dict.fromkeys(v.strip().upper() for v in raw.split(",") if v.strip()))
    if not values or len(values) > SESSION_STATUS_MAX or any(v not in SESSION_STATUSES for v in values):
        message = f"Tối đa {SESSION_STATUS_MAX} trạng thái phiên hợp lệ, cách nhau dấu phẩy"
        raise AppError(
            "VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"session_status": message}}
        )
    return values


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
    session_status: str | list[str] | None = None,
    session_flag: str | None = None,
    session_type: str | None = None,
    source: str | None = None,
    platform: str | None = None,
    shop_id: uuid.UUID | None = None,
    return_dropped: bool = False,
) -> Page[PackageItem]:
    """Lọc theo phiên (EXISTS): `session_status` theo ngày kết thúc, `session_flag` theo ngày bắt đầu (khớp
    định nghĩa thẻ API-32); không lọc phiên thì ngày theo lúc phiên kết thúc (hoặc bắt đầu nếu còn mở)."""
    _validate_range(date_from, date_to)
    conditions: list[ColumnElement[bool]] = []
    if q and q.strip():
        code = q.strip().upper()
        # Phase 2: mã vận đơn chiều về / mã hồ sơ `HH-` của hồ sơ hàng hoàn chứa kiện (02 §6.2 API-30).
        by_case = exists().where(
            ReturnCasePackage.package_id == Package.id,
            ReturnCase.id == ReturnCasePackage.return_case_id,
            or_(func.upper(ReturnCase.return_tracking_number) == code, func.upper(ReturnCase.code) == code),
        )
        conditions.append(
            or_(
                func.upper(Package.tracking_number) == code,
                func.upper(Order.platform_order_sn) == code,
                by_case,
            )
        )
    if warehouse_status:
        conditions.append(Package.warehouse_status == warehouse_status)
    if source:
        conditions.append(Order.source == source)
    conditions += shop_conditions(Order.shop_id, platform, shop_id)
    statuses = [session_status] if isinstance(session_status, str) else session_status

    session_conds: list[ColumnElement[bool]] = [PackSession.package_id == Package.id]
    if station_id:
        session_conds.append(PackSession.station_id == station_id)
    if statuses:
        session_conds.append(PackSession.status.in_(statuses))
    if return_dropped:
        # BR-39 v0.4: phiên mở hoàn hủy / bỏ dở **trừ** phiên bị loại (quét nhầm) — cùng luật thẻ D2, N03.
        session_conds.append(dropped_return_filter())
    if session_flag:
        session_conds.append(literal(session_flag) == any_(PackSession.flags))
    if session_type:
        session_conds.append(PackSession.type == session_type)
    if date_from or date_to:
        when = (
            PackSession.ended_at
            if statuses or return_dropped
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
    shops = await shops_by_id(db, [o.shop_id for _, o in rows if o is not None])
    last: dict[uuid.UUID, LastSession] = {}
    with_clip: set[uuid.UUID] = set()
    cases = await _case_briefs(db, ids)
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
            platform=(shop.platform if (shop := shops.get(o.shop_id) if o and o.shop_id else None) else None),
            shop=shop_ref(shop),
            warehouse_status=p.warehouse_status,
            platform_status=o.platform_status if o else None,
            source=o.source if o else None,
            last_session=last.get(p.id),
            has_clip=p.id in with_clip,
            is_placeholder=p.is_placeholder,
            return_case=cases.get(p.id),
        )
        for p, o in rows
    ]
    return Page(items=items, page=page, page_size=page_size, total=total)


async def _case_briefs(db: AsyncSession, package_ids: list[uuid.UUID]) -> dict[uuid.UUID, ReturnCaseBrief]:
    """Hồ sơ hàng hoàn đại diện mỗi kiện: hồ sơ mở trước, rồi hồ sơ mới nhất (bỏ hồ sơ đã gộp / hủy nếu còn
    hồ sơ khác)."""
    if not package_ids:
        return {}
    rows = (
        await db.execute(
            select(ReturnCasePackage.package_id, ReturnCase)
            .join(ReturnCase, ReturnCase.id == ReturnCasePackage.return_case_id)
            .where(ReturnCasePackage.package_id.in_(package_ids))
            .order_by(
                ReturnCasePackage.package_id,
                ReturnCase.status.in_(OPEN_CASE_STATUSES).desc(),
                (ReturnCase.status == "CANCELLED").asc(),
                ReturnCase.created_at.desc(),
            )
            .ext(distinct_on(ReturnCasePackage.package_id))
        )
    ).all()
    return {pid: ReturnCaseBrief(id=c.id, code=c.code, kind=c.kind, status=c.status) for pid, c in rows}


# ---------------------------------------------------------------- API-31


def _snapshot_protection(info: protection.SessionProtection | None) -> Protection | None:
    """Ảnh theo bảo vệ của phiên (02 §6.2 API-31 "snapshots[] có cùng protection")."""
    if info is None or not (info.claims or info.return_cases):
        return None
    reasons: list[Literal["CLAIM", "RETURN_CASE", "HELD"]] = []
    if info.claims:
        reasons.append("CLAIM")
    if info.return_cases:
        reasons.append("RETURN_CASE")
    forever = bool(info.claims) or info.case_forever
    return Protection(
        reasons=reasons,
        claims=[code for _, code in info.claims],
        return_cases=list(info.return_cases),
        until=None if forever else info.case_until,
    )


def _corrections(raw: list[dict[str, Any]] | None) -> list[InspectionCorrection]:
    out = []
    for entry in raw or []:
        by = entry.get("by") or {}
        before = entry.get("before") or {}
        out.append(
            InspectionCorrection(
                at=datetime.fromisoformat(str(entry["at"]).replace("Z", "+00:00")),
                by=CorrectionBy(id=by.get("id"), display_name=str(by.get("display_name") or "")),
                reason=str(entry.get("reason") or ""),
                before=CorrectionBefore(
                    conclusion=before.get("conclusion"),
                    note=str(before.get("note") or ""),
                    lines=[InspectionLineOut.model_validate(line) for line in before.get("lines") or []],
                ),
            )
        )
    return out


def can_correct(pack: PackSession, role: str) -> bool:
    """API-113 (FR-04.11): phiên RETURN `COMPLETED`, kết thúc ≤ 7 ngày, người xem ADMIN / SUPERVISOR."""
    return (
        role in CORRECTORS
        and pack.type == "RETURN"
        and pack.status == "COMPLETED"
        and pack.ended_at is not None
        and pack.ended_at >= clock.now() - CORRECTION_WINDOW
    )


async def sessions_out(
    db: AsyncSession,
    rows: list[tuple[PackSession, str]],
    settings: Settings,
    *,
    viewer: uuid.UUID,
    role: str,
) -> list[SessionDetail]:
    """`sessions[]` của API-31 (dùng lại cho response API-113): clip + bảo vệ (ADR-009), phiên RETURN có kết
    luận / lịch sử sửa / ảnh, phiên PACK có ảnh lúc đóng gói. URL ảnh ký theo người xem (10 phút)."""
    session_ids = [s.id for s, _ in rows]
    cfg = await settings_service.get(db)
    clips_by_session: dict[uuid.UUID, list[ClipDetail]] = {sid: [] for sid in session_ids}
    guarded = await protection.sessions_protection(db, session_ids, clock.now())
    days = protection.clip_days(cfg.retention_clip_days, settings.retention_clip_min_days)
    snaps: dict[uuid.UUID, list[Snapshot]] = {sid: [] for sid in session_ids}
    if session_ids:
        clips = (
            await db.scalars(select(Clip).where(Clip.session_id.in_(session_ids)).order_by(Clip.camera_role))
        ).all()
        for c in clips:
            # Clip đã xóa: `retention_until` = ngày bị xóa (FE DEC-76 đọc cho câu "Clip đã bị xóa ngày …");
            # được bảo vệ vô hạn → null; còn lại theo ADR-009 (DEC-245, DEC-268).
            info = protection.clip_protection(c, guarded.get(c.session_id), days)
            clips_by_session[c.session_id].append(
                ClipDetail(
                    id=c.id,
                    camera_role=c.camera_role,
                    status=c.status,
                    sha256=c.sha256,
                    duration_s=float(c.duration_s) if c.duration_s is not None else None,
                    held=c.held,
                    retention_until=info.retention_until,
                    deleted_at=c.deleted_at,
                    flags=list(c.flags),
                    protected_by_claim="CLAIM" in info.reasons,
                    protection=Protection(
                        reasons=info.reasons,
                        claims=info.claims,
                        return_cases=info.return_cases,
                        until=info.until,
                    )
                    if info.reasons
                    else None,
                )
            )
        for snap in (
            await db.scalars(
                select(Snapshot)
                .where(Snapshot.session_id.in_(session_ids))
                .order_by(Snapshot.taken_at, Snapshot.id)
            )
        ).all():
            snaps[snap.session_id].append(snap)

    def _url(snap: Snapshot) -> str | None:
        return snapshot_media.url_for(settings, snap.id, viewer) if snap.status == "READY" else None

    out = []
    for s, name in rows:
        manual = [x for x in snaps[s.id] if x.kind == "MANUAL"]
        pack_close = next((x for x in snaps[s.id] if x.kind == "PACK_CLOSE"), None)
        inspection_out = None
        if s.type == "RETURN":
            base = inspection.inspection_out(s, await inspection.lines_of(db, s.id))
            history = _corrections(s.inspection_corrections)
            inspection_out = SessionInspection(
                **base.model_dump(), corrections=history, corrected=history[-1] if history else None
            )
        session_guard = guarded.get(s.id)
        out.append(
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
                protected_by_claims=[ClaimRef(id=cid, code=code) for cid, code in guarded[s.id].claims],
                type=s.type,
                operator_name=s.operator_name,
                return_case_id=s.return_case_id,
                inspection=inspection_out,
                can_correct=can_correct(s, role),
                snapshots=[
                    SessionSnapshot(
                        id=x.id,
                        kind=x.kind,
                        taken_at=x.taken_at,
                        url=_url(x),
                        status=x.status,
                        protection=_snapshot_protection(session_guard) if x.status == "READY" else None,
                    )
                    for x in manual
                ],
                pack_snapshot=PackSnapshot(id=pack_close.id, url=_url(pack_close), status=pack_close.status)
                if pack_close is not None and s.type == "PACK"
                else None,
            )
        )
    return out


async def session_detail(
    db: AsyncSession, session_id: uuid.UUID, settings: Settings, *, viewer: uuid.UUID, role: str
) -> SessionDetail:
    rows = (
        await db.execute(
            select(PackSession, Station.name)
            .join(Station, Station.id == PackSession.station_id)
            .where(PackSession.id == session_id)
            .execution_options(populate_existing=True)
        )
    ).all()
    if not rows:
        raise AppError("NOT_FOUND", "Không tìm thấy phiên.", 404)
    return (await sessions_out(db, [(s, n) for s, n in rows], settings, viewer=viewer, role=role))[0]


async def detail(
    db: AsyncSession, package_id: uuid.UUID, settings: Settings, *, viewer: uuid.UUID, role: str
) -> PackageDetail:
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
                platform=shop.platform if shop else None,
                shop=ShopRef(id=shop.id, name=shop.name) if shop else None,
                platform_order_sn=order.platform_order_sn,
                platform_status=order.platform_status,
                platform_status_group=order.platform_status_group,
                merged_orders=[
                    MergedOrderOut(platform_order_sn=m.platform_order_sn)
                    for m in await merged_orders(db, package.id)
                ],
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
    rows = (
        await db.execute(
            select(PackSession, Station.name)
            .join(Station, Station.id == PackSession.station_id)
            .where(PackSession.package_id == package.id)
            .order_by(PackSession.started_at.desc())
        )
    ).all()
    sessions = await sessions_out(db, [(s, n) for s, n in rows], settings, viewer=viewer, role=role)
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
    # BR-32 (DEC-561): mỗi sự kiện phiên `AMBIGUOUS_SHOP` thêm một dòng `shops` (D4 "Mã có ở 2 shop: …").
    station_of = {s.id: name for s, name in rows}
    ambiguous = (
        await db.scalars(
            select(SessionEvent)
            .where(
                SessionEvent.session_id.in_(list(station_of)),
                SessionEvent.type == "AMBIGUOUS_SHOP",
            )
            .order_by(SessionEvent.at, SessionEvent.id)
        )
    ).all()
    for ev in ambiguous:
        timeline.append(
            TimelineItem(
                at=ev.at, source="WAREHOUSE", from_status=None, to_status="PACKING",
                actor=station_of.get(ev.session_id),
                shops=[
                    TimelineShop(platform=str(x.get("platform")), name=x.get("name"))
                    for x in (ev.payload or {}).get("shops") or []
                ],
            )
        )  # fmt: skip
    timeline.sort(key=lambda t: t.at)
    case_ids = set(
        (
            await db.scalars(
                select(ReturnCasePackage.return_case_id).where(ReturnCasePackage.package_id == package.id)
            )
        ).all()
    )
    case_ids |= {s.return_case_id for s, _ in rows if s.return_case_id is not None}
    cases = (
        list(
            (
                await db.scalars(
                    select(ReturnCase)
                    .where(ReturnCase.id.in_(case_ids))
                    .order_by(ReturnCase.created_at.desc())
                )
            ).all()
        )
        if case_ids
        else []
    )
    alerts = (
        await db.scalars(
            select(ReconAlert)
            .where(ReconAlert.package_id == package.id)
            .order_by(ReconAlert.detected_at.desc())
        )
    ).all()
    claims = (
        await db.scalars(
            select(Claim).where(Claim.package_id == package.id).order_by(Claim.created_at.desc())
        )
    ).all()
    shares, shares_active = await share_queries.package_shares(
        db, package.id, viewer=viewer, role=role, settings=settings
    )
    return PackageDetail(
        id=package.id,
        tracking_number=package.tracking_number,
        warehouse_status=package.warehouse_status,
        platform_logistics_status=package.platform_logistics_status,
        verified=package.verified,
        order=order_out,
        sessions=sessions,
        timeline=timeline,
        is_placeholder=package.is_placeholder,
        return_cases=await return_views.items_of(db, cases, settings.tz_display),
        recon_alerts=[
            ReconAlertBrief(
                id=a.id,
                rule=a.rule,
                br=RULE_BR[a.rule],
                severity=a.severity,
                status=a.status,
                detected_at=a.detected_at,
                closed_at=a.closed_at,
            )
            for a in alerts
        ],
        claims=[PackageClaimBrief(id=c.id, code=c.code, type=c.type, status=c.status) for c in claims],
        allowed_status_targets=list(MANUAL_TRANSITIONS.get(package.warehouse_status, ())),
        shares=shares,
        shares_active_count=shares_active,
    )
