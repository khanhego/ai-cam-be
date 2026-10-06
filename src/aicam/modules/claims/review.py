"""API-189 — soát phiên mở hoàn trên D17 (BR-39 v0.4–v0.5, EX-R21; DEC-515, 516, 529).

- `MARK_WRONG_SCAN`: đánh dấu quét nhầm phiên RETURN `CANCELLED` / `ABANDONED` chưa bị loại → phiên bị
  loại (không tự vào bằng chứng, không phiên chính, không tính N03 / D2 / D3); **mọi** hồ sơ chưa đóng đang
  có phiên (hoặc ảnh của phiên) trong bằng chứng → bỏ mềm (BR-38 — video giữ tới `keep_until`); hồ sơ đã
  đóng giữ nguyên.
- `UNMARK_WRONG_SCAN`: xóa đánh dấu; **không** tự thêm lại (CSKH dùng API-134 "Thêm lại").
- `CONFIRM_RETURN`: xác nhận "Là phiên hoàn thật" cho phiên cần soát (Supervisor hủy trước Phase 3) và
  (v0.5, T-290) phiên bị loại theo lý do hủy — chỉ ADMIN / SUPERVISOR.

Thứ tự khóa (02a §4 API-189, DEC-251): hồ sơ (id tăng, gồm `{id}`) → `session` → `claim_evidence`.
"""

import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

import structlog
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.modules.claims import evidence_rules
from aicam.modules.claims import service as claims
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.claims.schemas import ReviewIn
from aicam.modules.media.models import Snapshot
from aicam.modules.media.protection import lock_session_clips
from aicam.modules.sessions.models import PackSession

log = structlog.get_logger()

NOTE_MIN, NOTE_MAX = 5, 500
ACTIONS = ("MARK_WRONG_SCAN", "UNMARK_WRONG_SCAN", "CONFIRM_RETURN")
MARK_CODES = ("WRONG_SCAN", "NOT_A_RETURN")
_REVIEWABLE = ("CANCELLED", "ABANDONED")


def _validate(body: ReviewIn) -> str:
    fields: dict[str, str] = {}
    if body.action not in ACTIONS:
        fields["action"] = "Thao tác không hợp lệ."
    if body.action == "MARK_WRONG_SCAN" and body.reason_code not in MARK_CODES:
        fields["reason_code"] = "Chọn lý do."
    note = (body.note or "").strip()
    if not NOTE_MIN <= len(note) <= NOTE_MAX:
        fields["note"] = "Nhập ghi chú (5–500 ký tự)."
    if fields:
        raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})
    return note


def _not_eligible(message: str) -> AppError:
    return AppError("SESSION_NOT_ELIGIBLE", message, 409)


async def _lock_claims(db: AsyncSession, ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, Claim]:
    rows = (
        await db.scalars(
            select(Claim)
            .where(Claim.id.in_(sorted(set(ids))))
            .order_by(Claim.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    return {c.id: c for c in rows}


def _evidence_of_session(session_id: uuid.UUID) -> object:
    """Dòng bằng chứng của phiên hoặc của ảnh thuộc phiên."""
    return or_(
        ClaimEvidence.session_id == session_id,
        ClaimEvidence.snapshot_id.in_(select(Snapshot.id).where(Snapshot.session_id == session_id)),
    )


@dataclass
class ReviewResult:
    claim: Claim
    touched_claims: list[uuid.UUID]


async def review_return_session(
    db: AsyncSession,
    claim_id: uuid.UUID,
    session_id: uuid.UUID,
    body: ReviewIn,
    p: Principal,
    *,
    version_conflict: Callable[[uuid.UUID], Awaitable[AppError]],
) -> ReviewResult:
    note = _validate(body)
    claim = await db.get(Claim, claim_id)
    if claim is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
    # Ứng viên bỏ mềm (MARK): hồ sơ khác đang dùng phiên — đọc trước (không khóa) để khóa cả nhóm theo id
    # tăng.
    others: list[uuid.UUID] = []
    if body.action == "MARK_WRONG_SCAN":
        others = list(
            (
                await db.scalars(
                    select(ClaimEvidence.claim_id)
                    .join(Claim, Claim.id == ClaimEvidence.claim_id)
                    .where(
                        _evidence_of_session(session_id),  # type: ignore[arg-type]
                        ClaimEvidence.removed_at.is_(None),
                        Claim.status != "CLOSED",
                    )
                    .distinct()
                )
            ).all()
        )
    locked = await _lock_claims(db, [claim_id, *others])
    claim = locked.get(claim_id)
    if claim is None:
        raise AppError("NOT_FOUND", "Không tìm thấy hồ sơ khiếu nại.", 404)
    if claim.version != body.version:
        raise await version_conflict(claim_id)
    if claim.status == "CLOSED":
        raise AppError("CLAIM_CLOSED", "Hồ sơ đã đóng, chỉ thêm được ghi chú.", 409)
    scope = await evidence_rules.scope_condition(db, claim.package_id, claim.return_case_id)
    pack: PackSession | None = await db.scalar(
        select(PackSession)
        .where(PackSession.id == session_id, PackSession.type == "RETURN", scope)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if pack is None:
        raise AppError("NOT_FOUND", "Phiên không thuộc kiện / hồ sơ hàng hoàn của hồ sơ này.", 404)

    touched: set[uuid.UUID] = {claim.id}
    if body.action == "MARK_WRONG_SCAN":
        touched |= await _mark(db, pack, claim, locked, body.reason_code or "", note, p)
    elif body.action == "UNMARK_WRONG_SCAN":
        _unmark(db, pack, claim, note, p)
    else:
        await confirm_return(db, pack, claim, note, p)
    now = clock.now()
    for cid in sorted(touched):
        target = locked.get(cid)
        if target is None:
            continue
        target.version += 1
        target.updated_at = now
        claims.notify(db, target)
    await db.flush()
    log.info("return_session_review", claim_id=str(claim.id), session_id=str(pack.id), action=body.action,
             touched=len(touched))  # fmt: skip
    return ReviewResult(claim=claim, touched_claims=sorted(touched))


async def _mark(
    db: AsyncSession,
    pack: PackSession,
    claim: Claim,
    locked: dict[uuid.UUID, Claim],
    code: str,
    note: str,
    p: Principal,
) -> set[uuid.UUID]:
    if pack.status not in _REVIEWABLE:
        raise _not_eligible("Phiên đã có kết luận — sửa ở chi tiết đơn.")
    if evidence_rules.excluded(pack):
        raise _not_eligible("Phiên đã được loại khỏi bằng chứng.")
    now = clock.now()
    pack.wrong_scan_at, pack.wrong_scan_by = now, p.user_id
    pack.wrong_scan_code, pack.wrong_scan_note = code, note
    # Bỏ mềm ở mọi hồ sơ chưa đóng (đã khóa) — đọc lại dưới khóa: hồ sơ vừa đóng / dòng vừa bỏ → bỏ qua.
    rows = (
        await db.scalars(
            select(ClaimEvidence)
            .where(
                ClaimEvidence.claim_id.in_(sorted(c for c, x in locked.items() if x.status != "CLOSED")),
                _evidence_of_session(pack.id),  # type: ignore[arg-type]
                ClaimEvidence.removed_at.is_(None),
            )
            .order_by(ClaimEvidence.claim_id, ClaimEvidence.id)
            .with_for_update()
        )
    ).all()
    reason = f"Đánh dấu quét nhầm: {note}"
    days = await claims.keep_days(db)
    until = await evidence_rules.keep_until(
        db,
        [r.session_id for r in rows if r.session_id],
        [r.snapshot_id for r in rows if r.snapshot_id],
        now,
        days,
    )
    by_claim: dict[uuid.UUID, int] = {}
    for row in rows:
        row.removed_at, row.removed_by, row.removed_reason = now, p.user_id, reason
        by_claim[row.claim_id] = by_claim.get(row.claim_id, 0) + 1
        target = row.session_id or row.snapshot_id
        audit.record(
            db, "CLAIM_EVIDENCE_REMOVE", user_id=p.user_id, object_type="CLAIM", object_id=row.claim_id,
            ip=p.ip,
            data={
                "evidence_id": str(row.id), "kind": row.kind,
                "session_id": str(row.session_id) if row.session_id else None,
                "snapshot_id": str(row.snapshot_id) if row.snapshot_id else None,
                "reason": reason, "keep_until": clock.iso_z(until[target]) if target in until else None,
            },
        )  # fmt: skip
    for cid, count in by_claim.items():
        claims.add_note(
            db, locked[cid], "NOTE", f"Đánh dấu phiên mở hoàn quét nhầm — bỏ {count} bằng chứng. {reason}",
            p.user_id,
        )  # fmt: skip
    audit.record(
        db, "SESSION_WRONG_SCAN_MARK", user_id=p.user_id, object_type="SESSION", object_id=pack.id, ip=p.ip,
        data={
            "session_id": str(pack.id), "claim_id": str(claim.id), "reason_code": code, "note": note,
            "removed_from_claims": [str(c) for c in sorted(by_claim)],
            "active_shares": [],  # link chia sẻ chứa phiên: T-292 (sau `shares`, T-224)
        },
    )  # fmt: skip
    return set(by_claim)


def _unmark(db: AsyncSession, pack: PackSession, claim: Claim, note: str, p: Principal) -> None:
    if pack.wrong_scan_at is None:
        raise _not_eligible("Phiên chưa được đánh dấu quét nhầm.")
    pack.wrong_scan_at = pack.wrong_scan_by = pack.wrong_scan_code = pack.wrong_scan_note = None
    audit.record(
        db, "SESSION_WRONG_SCAN_UNMARK", user_id=p.user_id, object_type="SESSION", object_id=pack.id, ip=p.ip,
        data={"session_id": str(pack.id), "claim_id": str(claim.id), "note": note},
    )  # fmt: skip


def excluded_by_cause(s: PackSession) -> bool:
    """Bị loại **theo lý do hủy** (station / Supervisor chọn quét nhầm / không phải hàng hoàn), chưa xác nhận,
    chưa đánh dấu quét nhầm — đối tượng gỡ loại của `CONFIRM_RETURN` v0.5 (DEC-529)."""
    return (
        s.type == "RETURN"
        and s.wrong_scan_at is None
        and s.review_confirmed_at is None
        and evidence_rules.excluded(s)
    )


async def confirm_return(db: AsyncSession, pack: PackSession, claim: Claim, note: str, p: Principal) -> None:
    """`CONFIRM_RETURN` (DEC-516, DEC-529): phiên cần soát → phiên thường; phiên bị loại theo lý do hủy
    (chọn nhầm lý do) → gỡ loại (chỉ ADMIN / SUPERVISOR), vào bằng chứng hồ sơ `{id}` (thêm `auto = false`
    hoặc thêm lại dòng đã bỏ) — hồ sơ khác không tự thêm. Phiên đã đánh dấu quét nhầm: dùng `UNMARK` trước
    (đánh dấu thắng)."""
    overridden: str | None = None
    if evidence_rules.review_needed(pack):
        pass
    elif excluded_by_cause(pack):
        if p.role not in ("ADMIN", "SUPERVISOR"):
            raise AppError("FORBIDDEN", "Chỉ Admin / Supervisor gỡ lý do hủy của phiên.", 403)
        overridden = evidence_rules.effective_cancel_reason(pack)
    else:
        raise _not_eligible("Phiên không cần xác nhận.")
    pack.review_confirmed_at, pack.review_confirmed_by, pack.review_confirmed_note = (
        clock.now(),
        p.user_id,
        note,
    )
    if overridden is not None:
        row: ClaimEvidence | None = await db.scalar(
            select(ClaimEvidence)
            .where(ClaimEvidence.claim_id == claim.id, ClaimEvidence.session_id == pack.id)
            .with_for_update()
        )
        if row is None:
            await claims.add_evidence(db, claim, [pack.id], [], auto=False, added_by=p.user_id)
        elif row.removed_at is not None:
            await lock_session_clips(db, [pack.id])  # như thêm lại ở API-134 (DEC-251, DEC-543)
            row.removed_at = row.removed_by = row.removed_reason = None
        claims.add_note(
            db, claim, "NOTE", f"Xác nhận phiên mở hoàn là phiên hoàn thật (gỡ lý do hủy). Ghi chú: {note}",
            p.user_id,
        )  # fmt: skip
    audit.record(
        db, "SESSION_RETURN_CONFIRM", user_id=p.user_id, object_type="SESSION", object_id=pack.id, ip=p.ip,
        data={"session_id": str(pack.id), "claim_id": str(claim.id), "note": note,
              "overridden_cause": overridden},
    )  # fmt: skip
