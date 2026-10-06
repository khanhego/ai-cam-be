"""Bảo vệ bằng chứng theo hồ sơ (ADR-009, BR-09, DEC-245, DEC-251, DEC-268) — dùng chung cho J-02, API-31,
API-42, API-82.

Clip / ảnh của một phiên **không bị retention xóa** khi:
(a) phiên là bằng chứng của hồ sơ khiếu nại chưa `CLOSED`, hoặc đã đóng nhưng `closed_at` ≥ mốc cắt (hạn
    = max(`end_at`, `closed_at`) + số ngày giữ);
(b) phiên PACK hiệu lực / mọi phiên RETURN của kiện thuộc hồ sơ hàng hoàn `EXPECTED`/`INSPECTING`/
    `PARTIALLY_RECEIVED`/`MISSING`, hoặc `RECEIVED_*` trong 7 ngày sau `received_at`;
(c) như (b) với `NO_PARCEL` trong 30 ngày từ lúc sàn báo;
(d) clip `held` (đường khẩn cấp, API-42 chỉ ADMIN).
BR-38 (Phase 3, L15, DEC-449): bằng chứng **đã bỏ** khỏi hồ sơ (`claim_evidence.removed_at`) giữ như hồ sơ đã
đóng lúc bỏ — (a) thành `(removed_at IS NULL AND (status <> 'CLOSED' OR closed_at >= cutoff)) OR removed_at >=
cutoff` (hạn = max(`end_at`, `removed_at`) + số ngày giữ).
Ảnh còn được bảo vệ khi chính nó là bằng chứng (a). Số ngày giữ = max(`retention_clip_days`,
`RETENTION_CLIP_MIN_DAYS`) (BR-25, DEC-257).

Chỉ import model (không import service) để `claims`, `returns`, `orders` dùng được mà không vòng import.
"""

import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import ColumnElement, CompoundSelect, Select, and_, exists, or_, select, union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.media.models import Clip
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase, ReturnCasePackage
from aicam.modules.sessions.models import PackSession

RECEIVED_CASE_STATUSES = ("RECEIVED_OK", "RECEIVED_ISSUE")
RECEIVED_GRACE = timedelta(days=7)  # khớp hạn sửa kết luận API-113 (DEC-268)
NO_PARCEL_GRACE = timedelta(days=30)  # BR-09 (c)


def clip_days(retention_clip_days: int, minimum: int) -> int:
    """Số ngày giữ clip thực dùng: không thấp hơn sàn (BR-25, DEC-257)."""
    return max(retention_clip_days, minimum)


# ---------------------------------------------------------------- SQL dùng chung (J-02, API-82)


def _effective_pack() -> ColumnElement[bool]:
    """Phiên PACK hiệu lực: `COMPLETED` không có phiên PACK `COMPLETED` nào của kiện kết thúc sau nó."""
    later = aliased(PackSession)
    return and_(
        PackSession.type == "PACK",
        PackSession.status == "COMPLETED",
        ~exists().where(
            later.package_id == PackSession.package_id,
            later.type == "PACK",
            later.status == "COMPLETED",
            or_(
                later.ended_at > PackSession.ended_at,
                and_(later.ended_at == PackSession.ended_at, later.id > PackSession.id),
            ),
        ),
    )


def _case_protects(now: datetime) -> ColumnElement[bool]:
    return or_(
        ReturnCase.status.in_(OPEN_CASE_STATUSES),
        and_(
            ReturnCase.status.in_(RECEIVED_CASE_STATUSES),
            ReturnCase.received_at.is_not(None),
            ReturnCase.received_at > now - RECEIVED_GRACE,
        ),
        and_(
            ReturnCase.status == "NO_PARCEL",
            (ReturnCase.reported_at.is_(None) & (ReturnCase.created_at > now - NO_PARCEL_GRACE))
            | (ReturnCase.reported_at > now - NO_PARCEL_GRACE),
        ),
    )


def _evidence_protects(cutoff: datetime) -> ColumnElement[bool]:
    """(a) + BR-38: dòng đang dùng của hồ sơ chưa đóng / đóng chưa quá hạn; dòng đã bỏ chưa quá hạn giữ."""
    return or_(
        and_(ClaimEvidence.removed_at.is_(None), or_(Claim.status != "CLOSED", Claim.closed_at >= cutoff)),
        ClaimEvidence.removed_at >= cutoff,
    )


def claim_session_ids(cutoff: datetime) -> Select[uuid.UUID | None]:
    """(a) phiên làm bằng chứng của hồ sơ chưa đóng / đóng chưa quá hạn giữ / đã bỏ chưa quá hạn (BR-38)."""
    return (
        select(ClaimEvidence.session_id)
        .join(Claim, Claim.id == ClaimEvidence.claim_id)
        .where(ClaimEvidence.kind == "SESSION", _evidence_protects(cutoff))
    )


def claim_snapshot_ids(cutoff: datetime) -> Select[uuid.UUID | None]:
    """(a) ảnh tự là bằng chứng (BR-38 như phiên)."""
    return (
        select(ClaimEvidence.snapshot_id)
        .join(Claim, Claim.id == ClaimEvidence.claim_id)
        .where(ClaimEvidence.kind == "SNAPSHOT", _evidence_protects(cutoff))
    )


def case_session_ids(now: datetime) -> CompoundSelect[uuid.UUID]:
    """(b) + (c): phiên PACK hiệu lực + phiên RETURN của kiện thuộc hồ sơ hàng hoàn còn bảo vệ."""
    cases = (
        select(ReturnCase.id.label("case_id"), ReturnCasePackage.package_id)
        .join(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
        .where(_case_protects(now))
        .subquery()
    )
    by_package = select(PackSession.id).where(
        PackSession.package_id.in_(select(cases.c.package_id)),
        or_(PackSession.type == "RETURN", _effective_pack()),
    )
    by_case = select(PackSession.id).where(PackSession.return_case_id.in_(select(cases.c.case_id)))
    return union(by_package, by_case)


def protected_sessions_sql(now: datetime, cutoff: datetime) -> CompoundSelect[uuid.UUID | None]:
    """Phiên được bảo vệ (a) + (b) + (c) — (d) `held` xét theo clip."""
    return union(claim_session_ids(cutoff), case_session_ids(now))


def session_not_protected(column: Any, now: datetime, cutoff: datetime) -> ColumnElement[bool]:
    """`NOT EXISTS` (G3 B-6): `NOT IN (subquery)` thành NULL khi tập con có NULL → J-02 không xóa gì, và
    planner không dùng anti-join hiệu quả."""
    protected = protected_sessions_sql(now, cutoff).subquery()
    return ~exists().where(protected.c[0] == column)


def snapshot_not_evidence(column: Any, cutoff: datetime) -> ColumnElement[bool]:
    evidence = claim_snapshot_ids(cutoff).subquery()
    return ~exists().where(evidence.c[0] == column)


async def is_session_protected(
    session: AsyncSession, session_id: uuid.UUID, now: datetime, cutoff: datetime
) -> bool:
    """Kiểm lại dưới khóa dòng clip / ảnh (DEC-251)."""
    protected = protected_sessions_sql(now, cutoff).subquery()
    found = await session.scalar(select(protected.c[0]).where(protected.c[0] == session_id).limit(1))
    return found is not None


async def is_snapshot_evidence(session: AsyncSession, snapshot_id: uuid.UUID, cutoff: datetime) -> bool:
    found = await session.scalar(
        claim_snapshot_ids(cutoff).where(ClaimEvidence.snapshot_id == snapshot_id).limit(1)
    )
    return found is not None


# ---------------------------------------------------------------- khóa khi gắn bằng chứng (DEC-251)


async def lock_session_clips(session: AsyncSession, session_ids: Iterable[uuid.UUID]) -> list[Clip]:
    """DEC-251 (R-7): khóa clip của các phiên `ORDER BY id FOR UPDATE` trước khi ghi bằng chứng.

    J-02 khóa từng clip rồi kiểm lại bảo vệ dưới khóa → ai khóa trước thắng: hồ sơ ghi xong thì J-02 thấy
    bằng chứng và bỏ qua; J-02 xóa trước thì hồ sơ thấy clip `DELETED` (hiện ở `missing`)."""
    ids = sorted(set(session_ids))
    if not ids:
        return []
    rows = await session.scalars(
        select(Clip)
        .where(Clip.session_id.in_(ids))
        .order_by(Clip.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


# ---------------------------------------------------------------- lý do bảo vệ theo phiên (API-31, API-42)


@dataclass
class SessionProtection:
    """Lý do bảo vệ của một phiên (chưa gồm `HELD` — theo clip)."""

    claims: list[tuple[uuid.UUID, str]] = field(default_factory=list)  # hồ sơ khiếu nại chưa đóng
    return_cases: list[str] = field(default_factory=list)
    case_until: datetime | None = None  # hạn của lý do hồ sơ hàng hoàn có hạn (+7 ngày / 30 ngày)
    case_forever: bool = False  # có hồ sơ hàng hoàn chưa kết thúc
    closed_claim_at: datetime | None = None  # `closed_at` muộn nhất của hồ sơ đã đóng có phiên này


@dataclass(frozen=True)
class ClipProtection:
    reasons: list[str]
    claims: list[str]
    return_cases: list[str]
    until: datetime | None
    retention_until: datetime | None


async def sessions_protection(
    session: AsyncSession, session_ids: Sequence[uuid.UUID], now: datetime
) -> dict[uuid.UUID, SessionProtection]:
    """Lý do bảo vệ cho các phiên (2–3 truy vấn cho mọi phiên của một kiện — 02a §4 API-31)."""
    out: dict[uuid.UUID, SessionProtection] = {sid: SessionProtection() for sid in session_ids}
    if not session_ids:
        return out
    ids = list(session_ids)
    for sid, claim_id, code, status, closed_at, removed_at in (
        await session.execute(
            select(
                ClaimEvidence.session_id,
                Claim.id,
                Claim.code,
                Claim.status,
                Claim.closed_at,
                ClaimEvidence.removed_at,
            )
            .join(Claim, Claim.id == ClaimEvidence.claim_id)
            .where(ClaimEvidence.kind == "SESSION", ClaimEvidence.session_id.in_(ids))
            .order_by(Claim.created_at)
        )
    ).all():
        if sid is None:  # CHECK: bằng chứng SESSION luôn có session_id
            continue
        info = out[sid]
        # BR-38: dòng đã bỏ giữ như hồ sơ đóng lúc bỏ; dòng đang dùng theo trạng thái hồ sơ.
        ended = removed_at if removed_at is not None else (closed_at if status == "CLOSED" else None)
        if removed_at is None and status != "CLOSED":
            info.claims.append((claim_id, code))
        elif ended is not None and (info.closed_claim_at is None or ended > info.closed_claim_at):
            info.closed_claim_at = ended

    sessions = (await session.scalars(select(PackSession).where(PackSession.id.in_(ids)))).all()
    package_ids = {s.package_id for s in sessions}
    case_ids = {s.return_case_id for s in sessions if s.return_case_id}
    rows = (
        await session.execute(
            select(ReturnCase, ReturnCasePackage.package_id)
            .outerjoin(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
            .where(
                or_(ReturnCasePackage.package_id.in_(package_ids), ReturnCase.id.in_(case_ids)),
                _case_protects(now),
            )
        )
    ).all()
    if not rows:
        return out
    effective: dict[uuid.UUID, uuid.UUID] = {}
    for pid, sid in (
        await session.execute(
            select(PackSession.package_id, PackSession.id).where(
                PackSession.package_id.in_(package_ids), _effective_pack()
            )
        )
    ).all():
        effective[pid] = sid
    cases_by_package: dict[uuid.UUID, list[ReturnCase]] = defaultdict(list)
    cases_by_id: dict[uuid.UUID, ReturnCase] = {}
    for case, pid in rows:
        cases_by_id[case.id] = case
        if pid is not None:
            cases_by_package[pid].append(case)
    for s in sessions:
        covered: dict[uuid.UUID, ReturnCase] = {}
        if s.type == "RETURN" or effective.get(s.package_id) == s.id:
            covered.update({c.id: c for c in cases_by_package.get(s.package_id, [])})
        if s.return_case_id in cases_by_id:
            covered[s.return_case_id] = cases_by_id[s.return_case_id]
        info = out[s.id]
        for case in sorted(covered.values(), key=lambda c: c.code):
            info.return_cases.append(case.code)
            until = _case_until(case)
            if until is None:
                info.case_forever = True
            elif info.case_until is None or until > info.case_until:
                info.case_until = until
    return out


def _case_until(case: ReturnCase) -> datetime | None:
    if case.status in RECEIVED_CASE_STATUSES and case.received_at is not None:
        return case.received_at + RECEIVED_GRACE
    if case.status == "NO_PARCEL":
        return (case.reported_at or case.created_at) + NO_PARCEL_GRACE
    return None  # hồ sơ chưa kết thúc: giữ tới khi kết thúc


def clip_protection(clip: Clip, info: SessionProtection | None, days: int) -> ClipProtection:
    """`protection` + `retention_until` của một clip (02 §6.2 API-31 v0.2, DEC-245).

    Clip đã xóa → không bảo vệ, `retention_until` = lúc xóa (DEC-76 item 01). Lý do vô hạn (hồ sơ khiếu nại
    chưa đóng, hồ sơ hàng hoàn chưa kết thúc, `held`) → `retention_until = null`; chỉ lý do có hạn → hạn muộn
    nhất của: `end_at` + ngày giữ, hạn lý do, `closed_at` hồ sơ khiếu nại đã đóng + ngày giữ."""
    keep = timedelta(days=days)
    if clip.status == "DELETED":
        return ClipProtection([], [], [], None, clip.deleted_at)
    info = info or SessionProtection()
    reasons: list[str] = []
    if info.claims:
        reasons.append("CLAIM")
    if info.return_cases:
        reasons.append("RETURN_CASE")
    if clip.held:
        reasons.append("HELD")
    forever = bool(info.claims) or info.case_forever or clip.held
    until = None if forever else info.case_until
    candidates = [clip.end_at + keep]
    if info.closed_claim_at is not None:
        candidates.append(info.closed_claim_at + keep)
    if until is not None:
        candidates.append(until)
    return ClipProtection(
        reasons=reasons,
        claims=[code for _, code in info.claims],
        return_cases=list(info.return_cases),
        until=until,
        retention_until=None if forever else max(candidates),
    )
