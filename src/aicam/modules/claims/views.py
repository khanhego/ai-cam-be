"""API-130 danh sách / API-132 chi tiết hồ sơ khiếu nại (02 §6.2; 02a §8: `status_counts` một `GROUP BY`)."""

import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import ColumnElement, and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.claims import evidence_rules
from aicam.modules.claims import service as claims_service
from aicam.modules.claims.evidence_rules import primary_session as primary_session
from aicam.modules.claims.models import CLAIM_STATUSES, Claim, ClaimEvidence, ClaimNote
from aicam.modules.claims.schemas import (
    ClaimDetail,
    ClaimListItem,
    ClaimOrder,
    ClaimOrderBrief,
    ClaimPackage,
    ClaimPackageBrief,
    ClaimPage,
    ClaimReturnCase,
    EvidenceClip,
    EvidenceOut,
    EvidenceSession,
    EvidenceSnapshot,
    ExcludedReturnSession,
    NoteOut,
    OtherSession,
    PriorReturnSession,
    RemovedInfo,
    ReviewSession,
    SessionMark,
    StatusCounts,
    UserBrief,
)
from aicam.modules.claims.service import (
    DUE_STATUSES,
    allowed_sessions,
    allowed_transitions,
    effective_pack_session,
)
from aicam.modules.media import snapshots as media_snapshots
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Order, Package
from aicam.modules.orders.refs import shop_conditions, shop_ref, shops_by_id
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings import service as settings_service
from aicam.modules.shares import queries as share_queries
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User


def due_flags(claim: Claim, now: datetime, soon_hours: int) -> tuple[bool, bool]:
    """(sắp hết hạn, quá hạn) — chỉ hồ sơ còn phải gửi / chờ (FR-08.04)."""
    if claim.status not in DUE_STATUSES or claim.deadline_at is None:
        return False, False
    if claim.deadline_at < now:
        return False, True
    return claim.deadline_at <= now + timedelta(hours=soon_hours), False


async def _users(db: AsyncSession, ids: set[uuid.UUID]) -> dict[uuid.UUID, UserBrief]:
    if not ids:
        return {}
    rows = (await db.scalars(select(User).where(User.id.in_(sorted(ids))))).all()
    return {u.id: UserBrief(id=u.id, display_name=u.display_name) for u in rows}


async def list_claims(
    db: AsyncSession,
    *,
    viewer: uuid.UUID,
    status: str | None,
    claim_type: str | None,
    counterparty: str | None,
    owner: str | None,
    due: str | None,
    q: str | None,
    page: int,
    page_size: int,
    platform: str | None = None,
    shop_id: uuid.UUID | None = None,
) -> ClaimPage:
    """API-130 (FR-08.03, 08.04): lọc; `status_counts` cùng bộ lọc (trừ `status`), một truy vấn. Phase 3:
    `platform`, `shop_id` theo shop của đơn (`claim.order_id`, không có → đơn của kiện)."""
    cfg = await settings_service.get(db)
    now = clock.now()
    conds: list[ColumnElement[bool]] = []
    if claim_type:
        conds.append(Claim.type == claim_type)
    if counterparty:
        conds.append(Claim.counterparty == counterparty)
    if owner:
        if owner == "me":
            conds.append(Claim.owner_user_id == viewer)
        else:
            try:
                conds.append(Claim.owner_user_id == uuid.UUID(owner))
            except ValueError as exc:
                raise AppError(
                    "VALIDATION_ERROR",
                    "Dữ liệu không hợp lệ.",
                    422,
                    {"fields": {"owner": "owner = me hoặc id người dùng"}},
                ) from exc
    if due in ("soon", "overdue"):
        conds.append(Claim.status.in_(DUE_STATUSES))
        if due == "soon":
            conds.append(
                and_(
                    Claim.deadline_at >= now,
                    Claim.deadline_at <= now + timedelta(hours=cfg.claim_due_soon_hours),
                )
            )
        else:
            conds.append(Claim.deadline_at < now)
    if q and q.strip():
        code = q.strip().upper()
        conds.append(
            or_(
                func.upper(Claim.code) == code,
                Claim.package_id.in_(select(Package.id).where(func.upper(Package.tracking_number) == code)),
                Claim.order_id.in_(select(Order.id).where(func.upper(Order.platform_order_sn) == code)),
            )
        )
    if platform or shop_id:
        claim_shop = (
            select(Order.shop_id)
            .select_from(Package)
            .join(Order, Order.id == func.coalesce(Claim.order_id, Package.order_id))
            .where(Package.id == Claim.package_id)
            .scalar_subquery()
        )
        conds += shop_conditions(claim_shop, platform, shop_id)
    counts_row = (
        await db.execute(
            select(*(func.count(case((Claim.status == s, 1))).label(s) for s in CLAIM_STATUSES)).where(*conds)
        )
    ).one()
    where = [*conds, *([Claim.status == status] if status else [])]
    total = await db.scalar(select(func.count()).select_from(Claim).where(*where)) or 0
    order_by = (
        (Claim.deadline_at.asc().nulls_last(), Claim.id)
        if due
        else (Claim.created_at.desc(), Claim.id.desc())
    )
    rows = (
        await db.execute(
            select(Claim, Package.tracking_number, Order.platform_order_sn, Order.shop_id)
            .join(Package, Package.id == Claim.package_id)
            .outerjoin(Order, Order.id == func.coalesce(Claim.order_id, Package.order_id))
            .where(*where)
            .order_by(*order_by)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    owners = await _users(db, {c.owner_user_id for c, _, _, _ in rows if c.owner_user_id})
    shops = await shops_by_id(db, [shop for _, _, _, shop in rows])
    items = []
    for c, tracking, order_sn, shop_key in rows:
        shop = shops.get(shop_key) if shop_key else None
        soon, overdue = due_flags(c, now, cfg.claim_due_soon_hours)
        items.append(
            ClaimListItem(
                id=c.id,
                code=c.code,
                type=c.type,
                counterparty=c.counterparty,
                status=c.status,
                source=c.source,
                package=ClaimPackageBrief(id=c.package_id, tracking_number=tracking),
                order=ClaimOrderBrief(platform_order_sn=order_sn) if order_sn else None,
                owner=owners.get(c.owner_user_id) if c.owner_user_id else None,
                deadline_at=c.deadline_at,
                due_soon=soon,
                overdue=overdue,
                created_at=c.created_at,
                platform=shop.platform if shop else None,
                shop=shop_ref(shop),
            )
        )
    return ClaimPage(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        status_counts=StatusCounts(**counts_row._asdict()),
    )


def _mark(
    at: datetime | None,
    by: uuid.UUID | None,
    note: str | None,
    users: dict[uuid.UUID, UserBrief],
    code: str | None = None,
) -> SessionMark | None:
    if at is None:
        return None
    return SessionMark(at=at, by=users.get(by) if by else None, code=code, note=note)


def exclusion_fields(s: PackSession, users: dict[uuid.UUID, UserBrief]) -> dict[str, Any]:
    """`session.{cancel_reason, cancel_cause, wrong_scan, review_needed, evidence_exclusion,
    return_confirmed}` (02 §5.1 SESSION — BR-39 v0.3–v0.5)."""
    return {
        "cancel_reason": s.cancel_reason,
        "cancel_cause": s.cancel_cause,
        "wrong_scan": _mark(s.wrong_scan_at, s.wrong_scan_by, s.wrong_scan_note, users, s.wrong_scan_code),
        "review_needed": evidence_rules.review_needed(s),
        "evidence_exclusion": evidence_rules.evidence_exclusion(s),
        "return_confirmed": _mark(
            s.review_confirmed_at, s.review_confirmed_by, s.review_confirmed_note, users
        ),
    }


def _missing(evidence: list[EvidenceOut]) -> list[str]:
    """`NO_PACK_CLIP` (không có phiên đóng gói), `PACK_CLIP_DELETED`, `RETURN_CLIP_PENDING` (02 API-132)."""
    sessions = [e.session for e in evidence if e.session is not None]
    packs = [s for s in sessions if s.type == "PACK"]
    out: list[str] = []
    if not packs:
        out.append("NO_PACK_CLIP")
    elif any(c.status == "DELETED" for s in packs for c in s.clips):
        out.append("PACK_CLIP_DELETED")
    if any(
        not s.clips or any(c.status == "PENDING" for c in s.clips) for s in sessions if s.type == "RETURN"
    ):
        out.append("RETURN_CLIP_PENDING")
    return out


async def claim_detail(
    db: AsyncSession, claim_id: uuid.UUID, viewer: uuid.UUID, settings: Settings, *, role: str | None = None
) -> ClaimDetail:
    """API-132 (FR-08.02, 08.06): hồ sơ, bằng chứng (phiên, clip, ảnh ký URL), phiên khác, thiếu, ghi chú."""
    claim = await db.get(Claim, claim_id, populate_existing=True)
    if claim is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
    package = await db.get(Package, claim.package_id)
    if package is None:  # FK RESTRICT
        raise AppError("NOT_FOUND", "Không tìm thấy kiện hàng.", 404)
    order = await db.get(Order, claim.order_id) if claim.order_id else None
    return_case = await db.get(ReturnCase, claim.return_case_id) if claim.return_case_id else None

    evidence_rows = (
        await db.scalars(
            select(ClaimEvidence)
            .where(ClaimEvidence.claim_id == claim.id)
            .order_by(ClaimEvidence.added_at, ClaimEvidence.id)
        )
    ).all()
    session_ids = [e.session_id for e in evidence_rows if e.session_id]
    snapshot_ids = [e.snapshot_id for e in evidence_rows if e.snapshot_id]
    sessions: dict[uuid.UUID, tuple[PackSession, str]] = {}
    clips: dict[uuid.UUID, list[EvidenceClip]] = defaultdict(list)
    if session_ids:
        for s, name in (
            await db.execute(
                select(PackSession, Station.name)
                .join(Station, Station.id == PackSession.station_id)
                .where(PackSession.id.in_(session_ids))
            )
        ).all():
            sessions[s.id] = (s, name)
        for c in (
            await db.scalars(select(Clip).where(Clip.session_id.in_(session_ids)).order_by(Clip.camera_role))
        ).all():
            clips[c.session_id].append(
                EvidenceClip(
                    id=c.id,
                    camera_role=c.camera_role,
                    status=c.status,
                    sha256=c.sha256,
                    deleted_at=c.deleted_at,
                )
            )
    snaps: dict[uuid.UUID, Snapshot] = {}
    if snapshot_ids:
        snaps = {
            s.id: s for s in (await db.scalars(select(Snapshot).where(Snapshot.id.in_(snapshot_ids)))).all()
        }

    excluded_sessions = await evidence_rules.excluded_return_sessions(
        db, claim.package_id, claim.return_case_id
    )
    review_list = await evidence_rules.review_sessions(db, claim.package_id, claim.return_case_id)
    marks_by = await _users(
        db,
        {
            uid
            for x in [*(s for s, _ in sessions.values()), *excluded_sessions]
            for uid in (x.wrong_scan_by, x.review_confirmed_by)
            if uid is not None
        },
    )
    evidence: list[EvidenceOut] = []
    removed_evidence: list[EvidenceOut] = []
    removed_rows: dict[uuid.UUID, ClaimEvidence] = {}
    for e in evidence_rows:
        item: EvidenceOut | None = None
        if e.session_id and e.session_id in sessions:
            s, name = sessions[e.session_id]
            item = EvidenceOut(
                id=e.id,
                kind="SESSION",
                auto=e.auto,
                session=EvidenceSession(
                    id=s.id,
                    type=s.type,
                    status=s.status,
                    station_name=name,
                    operator_name=s.operator_name,
                    started_at=s.started_at,
                    ended_at=s.ended_at,
                    flags=list(s.flags),
                    clips=clips[s.id],
                    **exclusion_fields(s, marks_by),
                ),
            )
        elif e.snapshot_id and e.snapshot_id in snaps:
            snap = snaps[e.snapshot_id]
            item = EvidenceOut(
                id=e.id,
                kind="SNAPSHOT",
                auto=e.auto,
                snapshot=EvidenceSnapshot(
                    id=snap.id,
                    kind=snap.kind,
                    taken_at=snap.taken_at,
                    status=snap.status,
                    url=media_snapshots.url_for(settings, snap.id, viewer)
                    if snap.status == "READY"
                    else None,
                ),
            )
        if item is None:
            continue
        if e.removed_at is None:
            evidence.append(item)
        else:  # BR-38: bằng chứng đã bỏ — không vào gói / link, hiện riêng
            removed_evidence.append(item)
            removed_rows[e.id] = e
    # BR-38 (L15): hạn giữ nếu bỏ bây giờ (dòng đang dùng) / theo lúc đã bỏ (dòng đã bỏ).
    days = await claims_service.keep_days(db)
    keep_now = await evidence_rules.keep_until(
        db,
        [x.session.id for x in evidence if x.session],
        [x.snapshot.id for x in evidence if x.snapshot],
        clock.now(),
        days,
    )
    removed_at = {
        (r.session_id or r.snapshot_id): r.removed_at
        for r in removed_rows.values()
        if r.removed_at is not None
    }
    keep_removed = await evidence_rules.keep_until(
        db,
        [r.session_id for r in removed_rows.values() if r.session_id],
        [r.snapshot_id for r in removed_rows.values() if r.snapshot_id],
        removed_at,  # type: ignore[arg-type]
        days,
    )
    for x in evidence:
        x.removal_keep_until = keep_now.get(
            x.session.id if x.session else x.snapshot.id if x.snapshot else x.id
        )
    removed_by = await _users(db, {r.removed_by for r in removed_rows.values() if r.removed_by})
    for x in removed_evidence:
        row = removed_rows[x.id]
        key = row.session_id or row.snapshot_id
        if row.removed_at is None or key is None:  # chỉ dòng đã bỏ có trong `removed_rows`
            continue
        x.removed = RemovedInfo(
            at=row.removed_at,
            by=removed_by.get(row.removed_by) if row.removed_by else None,
            reason=row.removed_reason or "",
            keep_until=keep_removed[key],
        )
    active_sessions = {x.session.id for x in evidence if x.session}
    # BR-39 (DEC-448): phiên mở hoàn trước, phiên chính — suy ra lúc đọc (chỉ bằng chứng đang dùng).
    in_evidence = [sessions[sid][0] for sid in active_sessions]
    with_clip = await evidence_rules.live_clip_sessions(db, active_sessions)
    effective = await effective_pack_session(db, claim.package_id)
    primary = primary_session(in_evidence, with_clip, effective.id if effective else None)
    primary_reason = await evidence_rules.primary_unavailable_reason(db, primary)
    latest_done = await evidence_rules.latest_completed_return_start(
        db, claim.package_id, claim.return_case_id
    )
    for item in evidence:
        if item.session is not None:
            s, _ = sessions[item.session.id]
            item.primary = s.id == primary
            item.prior_return = s.id in with_clip and evidence_rules.is_prior_return(s, latest_done)
    priors = await evidence_rules.prior_return_sessions(db, claim.package_id, claim.return_case_id)
    # Phân loại bằng chứng: phiên đóng gói trước, phiên chính, phiên mở hoàn (sớm trước), rồi ảnh.
    evidence.sort(
        key=lambda x: (
            x.kind != "SESSION",
            x.session.type != "PACK" if x.session else False,
            not x.primary,
            x.session.started_at if x.session else x.snapshot.taken_at if x.snapshot else clock.now(),
        )
    )

    candidates = await allowed_sessions(db, claim)
    other_ids = [sid for sid in candidates if sid not in sessions]
    others: list[OtherSession] = []
    if other_ids:
        others = [
            OtherSession(id=s.id, type=s.type, status=s.status, started_at=s.started_at)
            for s in (
                await db.scalars(
                    select(PackSession)
                    .where(PackSession.id.in_(other_ids))
                    .order_by(PackSession.started_at.desc())
                )
            ).all()
        ]
    notes = (
        await db.scalars(
            select(ClaimNote).where(ClaimNote.claim_id == claim.id).order_by(ClaimNote.at, ClaimNote.id)
        )
    ).all()
    users = await _users(
        db,
        {n.author_user_id for n in notes if n.author_user_id} | ({claim.owner_user_id} - {None}),  # type: ignore[operator]
    )
    shares, shares_active = await share_queries.claim_shares(
        db, claim.id, viewer=viewer, role=role, settings=settings
    )
    return ClaimDetail(
        id=claim.id,
        code=claim.code,
        type=claim.type,
        counterparty=claim.counterparty,
        status=claim.status,
        source=claim.source,
        version=claim.version,
        package=ClaimPackage(
            id=package.id, tracking_number=package.tracking_number, warehouse_status=package.warehouse_status
        ),
        order=ClaimOrder(id=order.id, platform_order_sn=order.platform_order_sn) if order else None,
        return_case=ClaimReturnCase(
            id=return_case.id,
            code=return_case.code,
            kind=return_case.kind,
            return_tracking_number=return_case.return_tracking_number,
        )
        if return_case
        else None,
        owner=users.get(claim.owner_user_id) if claim.owner_user_id else None,
        deadline_at=claim.deadline_at,
        deadline_source=claim.deadline_source,
        platform_claim_ref=claim.platform_claim_ref,
        recovered_amount=claim.recovered_amount,
        close_reason=claim.close_reason,
        created_at=claim.created_at,
        closed_at=claim.closed_at,
        evidence=evidence,
        primary_unavailable=primary_reason is not None,
        primary_unavailable_reason=primary_reason,
        other_sessions=others,
        removed_evidence=removed_evidence,
        prior_return_sessions=[
            PriorReturnSession(
                session_id=s.id, status=s.status, started_at=s.started_at, in_evidence=s.id in active_sessions
            )
            for s in priors
        ],
        excluded_return_sessions=[
            ExcludedReturnSession(
                session_id=s.id,
                status=s.status,
                cancel_reason=s.cancel_reason,
                cancel_cause=s.cancel_cause,
                evidence_exclusion=evidence_rules.evidence_exclusion(s),
                wrong_scan=_mark(
                    s.wrong_scan_at, s.wrong_scan_by, s.wrong_scan_note, marks_by, s.wrong_scan_code
                ),
                started_at=s.started_at,
                has_clip=True,
                in_evidence=s.id in active_sessions,
            )
            for s in excluded_sessions
        ],
        review_sessions=[
            ReviewSession(
                session_id=s.id, status=s.status, started_at=s.started_at, in_evidence=s.id in active_sessions
            )
            for s in review_list
        ],
        shares=shares,
        shares_active_count=shares_active,
        missing=_missing(evidence),
        notes=[
            NoteOut(
                id=n.id,
                kind=n.kind,
                text=n.text,
                author=users.get(n.author_user_id) if n.author_user_id else None,
                at=n.at,
            )
            for n in notes
        ],
        allowed_transitions=allowed_transitions(claim.status),
    )


def note_out(note: ClaimNote, author: UserBrief | None) -> NoteOut:
    return NoteOut(id=note.id, kind=note.kind, text=note.text, author=author, at=note.at)
