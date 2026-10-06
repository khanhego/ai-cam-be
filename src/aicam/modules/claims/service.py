"""Hồ sơ khiếu nại (02a §2 `claims`, §4 API-131..135, §5 BR-08, BR-27, "Chuyển trạng thái hồ sơ"; J-15).

- `create_from_return(pack, case)`: BR-08 — phiên RETURN đóng, kết luận ≠ Nguyên vẹn (API-11, J-07, API-113).
- `create_manual(...)`: API-131 (tay / từ cảnh báo lệch / từ hồ sơ "Chỉ hoàn tiền").
- `auto_evidence(...)`: FR-08.06 — phiên PACK hiệu lực + phiên mở hoàn + ảnh; khóa clip (DEC-251).
- `patch` / `set_evidence` / `add_note`: API-133..135 (khóa lạc quan `version`).
- `move_claims_to_package(...)`: gộp hồ sơ chưa xác định (DEC-269) / API-112 — hồ sơ khiếu nại của kiện tạm
  sang kiện thật, trùng BR-27 thì gộp bằng chứng + ghi chú rồi đóng hồ sơ cũ.
- `check_deadlines()`: J-15.

Thứ tự khóa (DEC-266 + DEC-251): … → kiện → `claim:{kiện}:{loại}` (advisory, chỉ khi tạo) → hồ sơ (FOR UPDATE)
→ clip (FOR UPDATE, id tăng). Hàm ở đây không commit; router / người gọi commit.
"""

import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import ColumnElement, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import get_settings
from aicam.modules.claims import evidence_rules
from aicam.modules.claims.models import Claim, ClaimEvidence, ClaimNote
from aicam.modules.claims.schemas import ClaimCreateIn, ClaimPatchIn, EvidenceIn
from aicam.modules.media.models import Snapshot
from aicam.modules.media.protection import lock_session_clips
from aicam.modules.orders.models import Package
from aicam.modules.reconciliation import service as recon
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings import service as settings_service
from aicam.modules.users.models import User

log = structlog.get_logger()

# 02a §5 "Chuyển trạng thái hồ sơ khiếu nại".
TRANSITIONS: dict[str, tuple[str, ...]] = {
    "NEW": ("SUBMITTED", "CLOSED"),
    "SUBMITTED": ("WAITING", "WON", "LOST", "CLOSED"),
    "WAITING": ("WON", "LOST", "CLOSED"),
    "WON": ("CLOSED",),
    "LOST": ("CLOSED",),
    "CLOSED": (),
}
DUE_STATUSES = ("NEW", "SUBMITTED", "WAITING")  # còn phải gửi / chờ — tính hạn (FR-08.04)
OWNER_ROLES = ("ADMIN", "SUPERVISOR", "CSKH")
ISSUE_CONCLUSIONS = ("DAMAGED", "MISSING_ITEM", "WRONG_ITEM", "EMPTY_BOX", "OTHER")
CONCLUSION_LABELS = {
    "OK": "Nguyên vẹn",
    "DAMAGED": "Hư hỏng",
    "MISSING_ITEM": "Thiếu hàng",
    "WRONG_ITEM": "Sai hàng / bị tráo",
    "EMPTY_BOX": "Hộp rỗng",
    "OTHER": "Khác",
}
STATUS_LABELS = {
    "NEW": "Mới",
    "SUBMITTED": "Đã gửi",
    "WAITING": "Đang chờ phản hồi",
    "WON": "Thắng",
    "LOST": "Thua",
    "CLOSED": "Đóng",
}
RULE_LABELS = {
    "SHIPPED_NOT_PACKED": "Sàn đã giao nhưng kho chưa đóng gói",
    "CANCELLED_AFTER_PACK": "Đơn hủy sau khi đóng gói",
    "RETURN_OVERDUE": "Hàng hoàn quá hạn chưa về",
    "RETURN_UNANNOUNCED": "Hàng hoàn về trước khi sàn báo",
    "PACKED_NOT_HANDED_OVER": "Đóng gói nhưng chưa bàn giao",
    "RETURN_DONE_NOT_RECEIVED": "Sàn đã hoàn tiền nhưng kho chưa nhận",
    "UNVERIFIED_STALE": "Kiện chưa xác minh quá lâu",
}


def _validation(field: str, message: str) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {field: message}})


def _vn(at: datetime) -> str:
    return at.astimezone(ZoneInfo(get_settings().tz_display)).strftime("%H:%M %d/%m/%Y")


def allowed_transitions(status: str) -> list[str]:
    return list(TRANSITIONS.get(status, ()))


# ---------------------------------------------------------------- đọc / khóa


async def lock_claim(session: AsyncSession, claim_id: uuid.UUID) -> Claim | None:
    result: Claim | None = await session.scalar(
        select(Claim).where(Claim.id == claim_id).with_for_update().execution_options(populate_existing=True)
    )
    return result


async def _lock_create(session: AsyncSession, package_id: uuid.UUID, claim_type: str) -> None:
    """Tuần tự hóa việc tạo hồ sơ cùng (kiện, loại) — BR-27 không cần savepoint (rollback savepoint làm mất
    callback sau commit đã đăng ký của phiên đang đóng)."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"claim:{package_id}:{claim_type}"}
    )


async def find_open(
    session: AsyncSession, package_id: uuid.UUID, claim_type: str, *, for_update: bool = False
) -> Claim | None:
    """Hồ sơ chưa đóng cùng (kiện, loại) — BR-27 (không tính `LEGACY_HOLD`)."""
    query = select(Claim).where(
        Claim.package_id == package_id,
        Claim.type == claim_type,
        Claim.status != "CLOSED",
        Claim.source != "LEGACY_HOLD",
    )
    if for_update:
        query = query.with_for_update().execution_options(populate_existing=True)
    result: Claim | None = await session.scalar(query.order_by(Claim.created_at).limit(1))
    return result


async def effective_pack_session(session: AsyncSession, package_id: uuid.UUID) -> PackSession | None:
    """Phiên PACK hiệu lực: `COMPLETED` mới nhất (phiên đóng gói lại thay phiên cũ → `SUPERSEDED`)."""
    result: PackSession | None = await session.scalar(
        select(PackSession)
        .where(
            PackSession.package_id == package_id,
            PackSession.type == "PACK",
            PackSession.status == "COMPLETED",
        )
        .order_by(PackSession.ended_at.desc(), PackSession.id.desc())
        .limit(1)
    )
    return result


async def completed_return_sessions(
    session: AsyncSession, package_id: uuid.UUID, case_id: uuid.UUID | None = None
) -> list[PackSession]:
    query = select(PackSession).where(
        PackSession.package_id == package_id,
        PackSession.type == "RETURN",
        PackSession.status == "COMPLETED",
    )
    if case_id is not None:
        query = query.where(PackSession.return_case_id == case_id)
    rows = await session.scalars(query.order_by(PackSession.ended_at, PackSession.id))
    return list(rows.all())


async def code_for_session(session: AsyncSession, session_id: uuid.UUID) -> str | None:
    """Mã hồ sơ khiếu nại tự tạo từ phiên RETURN (API-15 `claim_code`)."""
    code: str | None = await session.scalar(
        select(Claim.code)
        .join(ClaimEvidence, ClaimEvidence.claim_id == Claim.id)
        .where(ClaimEvidence.session_id == session_id, Claim.source != "LEGACY_HOLD")
        .order_by(Claim.created_at)
        .limit(1)
    )
    return code


# ---------------------------------------------------------------- bằng chứng (FR-08.06, DEC-251)


async def _snapshot_ids(session: AsyncSession, sessions: Sequence[PackSession]) -> list[uuid.UUID]:
    """Ảnh lúc đóng gói của phiên PACK + ảnh chụp tay của phiên RETURN (còn `READY`)."""
    pack_ids = [s.id for s in sessions if s.type == "PACK"]
    return_ids = [s.id for s in sessions if s.type == "RETURN"]
    conds = []
    if pack_ids:
        conds.append((Snapshot.session_id.in_(pack_ids)) & (Snapshot.kind == "PACK_CLOSE"))
    if return_ids:
        conds.append((Snapshot.session_id.in_(return_ids)) & (Snapshot.kind == "MANUAL"))
    if not conds:
        return []
    cond = conds[0] if len(conds) == 1 else conds[0] | conds[1]
    rows = await session.scalars(
        select(Snapshot.id).where(cond, Snapshot.status == "READY").order_by(Snapshot.taken_at, Snapshot.id)
    )
    return list(rows.all())


async def add_evidence(
    session: AsyncSession,
    claim: Claim,
    session_ids: Iterable[uuid.UUID],
    snapshot_ids: Iterable[uuid.UUID],
    *,
    auto: bool,
    added_by: uuid.UUID | None,
) -> int:
    """Thêm bằng chứng (bỏ qua cái đã có). Khóa clip của phiên rồi ảnh (của phiên + ảnh được chọn) trước
    khi ghi (DEC-251; G3 B-1, DEC-339): J-02 `_expire_snapshots` khóa dòng ảnh rồi kiểm lại bảo vệ — phải
    chờ hồ sơ này commit, không xóa ảnh vừa thành bằng chứng. Ảnh đã `DELETED` khi lấy được khóa → bỏ qua.
    Trả số dòng thêm."""
    sids = list(dict.fromkeys(session_ids))
    snaps = list(dict.fromkeys(snapshot_ids))
    await lock_session_clips(session, sids)
    if snaps or sids:
        locked = (
            await session.execute(
                select(Snapshot.id, Snapshot.status)
                .where(or_(Snapshot.id.in_(snaps), Snapshot.session_id.in_(sids)))
                .order_by(Snapshot.id)
                .with_for_update()
            )
        ).all()
        gone = {sid for sid, status in locked if status != "READY"}
        if gone & set(snaps):
            log.info(
                "claim_evidence_snapshot_deleted", claim_id=str(claim.id), snapshot_ids=sorted(map(str, gone))
            )
        snaps = [s for s in snaps if s not in gone]
    now = clock.now()
    added = 0
    for sid in sids:
        res = await session.execute(
            insert(ClaimEvidence)
            .values(
                id=uuid.uuid4(),
                claim_id=claim.id,
                kind="SESSION",
                session_id=sid,
                auto=auto,
                added_by=added_by,
                added_at=now,
            )
            .on_conflict_do_nothing(index_elements=["claim_id", "session_id"])
        )
        added += res.rowcount or 0  # type: ignore[attr-defined]
    for snap in snaps:
        res = await session.execute(
            insert(ClaimEvidence)
            .values(
                id=uuid.uuid4(),
                claim_id=claim.id,
                kind="SNAPSHOT",
                snapshot_id=snap,
                auto=auto,
                added_by=added_by,
                added_at=now,
            )
            .on_conflict_do_nothing(index_elements=["claim_id", "snapshot_id"])
        )
        added += res.rowcount or 0  # type: ignore[attr-defined]
    return added


async def auto_evidence(
    session: AsyncSession,
    claim: Claim,
    package_id: uuid.UUID,
    return_sessions: Sequence[PackSession],
    *,
    prior: bool = False,
) -> int:
    """FR-08.06: phiên PACK hiệu lực của kiện + phiên mở hoàn + ảnh (bằng chứng tự chọn, `auto = true`).

    `prior` (BR-39, L11 — khi tạo hồ sơ): thêm mọi phiên mở hoàn **trước** đã hủy / bỏ dở có clip của kiện /
    hồ sơ hàng hoàn (`evidence_rules.prior_return_sessions`)."""
    sessions: list[PackSession] = []
    pack = await effective_pack_session(session, package_id)
    if pack is not None:
        sessions.append(pack)
    if prior:
        sessions.extend(
            await evidence_rules.prior_return_sessions(session, claim.package_id, claim.return_case_id)
        )
    sessions.extend(return_sessions)
    sessions = list({s.id: s for s in sessions}.values())
    return await add_evidence(
        session,
        claim,
        [s.id for s in sessions],
        await _snapshot_ids(session, sessions),
        auto=True,
        added_by=None,
    )


# ---------------------------------------------------------------- tạo


def _deadline(case: ReturnCase | None, claim_deadline_days: int) -> tuple[datetime, str]:
    """BR-27: hạn người bán do sàn trả; không có → ngày tạo + `claim_deadline_days`."""
    if case is not None and case.seller_due_at is not None:
        return case.seller_due_at, "PLATFORM"
    return clock.now() + timedelta(days=claim_deadline_days), "DEFAULT"


def add_note(
    session: AsyncSession, claim: Claim, kind: str, body: str, author: uuid.UUID | None = None
) -> ClaimNote:
    note = ClaimNote(claim_id=claim.id, kind=kind, text=body, author_user_id=author, at=clock.now())
    session.add(note)
    return note


def notify(session: AsyncSession, claim: Claim) -> None:
    """WS-02 `claim.updated` (D16, D17, D4) + `report.updated` (D2: hồ sơ mở / sắp hết hạn) sau commit."""
    from aicam.realtime import publish

    data = {"claim_id": str(claim.id), "status": claim.status, "version": claim.version}
    day = clock.now().astimezone(ZoneInfo(get_settings().tz_display)).date().isoformat()

    async def _send() -> None:
        await publish.to_dashboard("claim.updated", data)
        await publish.to_dashboard("report.updated", {"date": day})

    after_commit(session, _send)


async def _insert_claim(session: AsyncSession, claim: Claim) -> Claim:
    session.add(claim)
    await session.flush()
    await session.refresh(claim, ["code"])  # mã `KN-` do sequence (server default)
    return claim


@dataclass
class CreatedFromReturn:
    claim: Claim
    created: bool


async def create_from_return(
    session: AsyncSession, pack: PackSession, case: ReturnCase | None
) -> CreatedFromReturn | None:
    """BR-08: phiên RETURN `COMPLETED` có kết luận ≠ Nguyên vẹn → hồ sơ khiếu nại tự tạo (loại = kết luận).

    Bên nhận: ĐVVC khi hồ sơ hàng hoàn "Giao thất bại", còn lại Sàn. Đã có hồ sơ cùng (kiện, loại) chưa đóng
    (BR-27) → thêm phiên + ảnh vào hồ sơ đó + ghi chú. Người gọi giữ khóa station → hồ sơ hàng hoàn → kiện.
    """
    conclusion = pack.inspection_conclusion
    if conclusion not in ISSUE_CONCLUSIONS:
        return None
    label = CONCLUSION_LABELS[conclusion]
    package = await session.get(Package, pack.package_id)
    if package is None:  # FK RESTRICT — không xảy ra
        return None
    await _lock_create(session, package.id, conclusion)
    existing = await find_open(session, package.id, conclusion, for_update=True)
    if existing is not None:
        added = await auto_evidence(session, existing, package.id, [pack])
        if added:
            add_note(
                session,
                existing,
                "SYSTEM",
                f"Thêm phiên mở hoàn lúc {_vn(pack.ended_at or clock.now())} ({label}).",
            )
            existing.version += 1
        notify(session, existing)
        log.info("claim_evidence_from_return", claim_id=str(existing.id), session_id=str(pack.id))
        return CreatedFromReturn(existing, created=False)
    cfg = await settings_service.get(session)
    deadline, deadline_source = _deadline(case, cfg.claim_deadline_days)
    claim = await _insert_claim(
        session,
        Claim(
            package_id=package.id,
            order_id=package.order_id,
            return_case_id=case.id if case else None,
            type=conclusion,
            counterparty="CARRIER" if case is not None and case.kind == "FAILED_DELIVERY" else "PLATFORM",
            status="NEW",
            source="AUTO_RETURN",
            deadline_at=deadline,
            deadline_source=deadline_source,
            created_by=None,
            version=1,
        ),
    )
    await auto_evidence(session, claim, package.id, [pack], prior=True)
    add_note(session, claim, "SYSTEM", f"Tạo tự động từ phiên mở hoàn ({label}).")
    audit.record(
        session,
        "CLAIM_CREATE",
        user_id=None,
        object_type="CLAIM",
        object_id=claim.id,
        data={
            "code": claim.code,
            "source": "AUTO_RETURN",
            "type": conclusion,
            "session_id": str(pack.id),
            "package_id": str(package.id),
        },
    )
    notify(session, claim)
    log.info("claim_created", claim_id=str(claim.id), source="AUTO_RETURN", session_id=str(pack.id))
    return CreatedFromReturn(claim, created=True)


async def retype_auto_on_correct(
    session: AsyncSession, pack: PackSession, case: ReturnCase | None, old: str
) -> tuple[list[str], CreatedFromReturn | None]:
    """API-113 lỗi → lỗi khác (G3 SM-F9, DEC-344 — quyết định PO theo ủy quyền). Hồ sơ `AUTO_RETURN` của phiên
    loại `old`: còn `NEW` → đổi loại theo kết luận mới + ghi chú hệ thống (đã có hồ sơ mở loại mới cho kiện →
    gộp: thêm phiên vào hồ sơ đó, đóng hồ sơ cũ); đã qua `NEW` (đã gửi sàn) → giữ hồ sơ cũ + ghi chú "kết luận
    đã sửa thành …" và tạo / gộp hồ sơ loại mới theo BR-27. Trả (mã hồ sơ đổi loại, hồ sơ loại mới)."""
    new = pack.inspection_conclusion or "OTHER"
    label = CONCLUSION_LABELS.get(new, new)
    await _lock_create(session, pack.package_id, new)
    rows = (
        await session.scalars(
            select(Claim)
            .join(ClaimEvidence, ClaimEvidence.claim_id == Claim.id)
            .where(
                ClaimEvidence.session_id == pack.id, Claim.source == "AUTO_RETURN", Claim.type == old,
                Claim.status != "CLOSED",
            )
            .order_by(Claim.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()  # fmt: skip
    retyped: list[str] = []
    for claim in rows:
        if claim.status == "NEW" and await find_open(session, claim.package_id, new) is None:
            claim.type = new
            claim.version += 1
            add_note(
                session,
                claim,
                "SYSTEM",
                f"Loại hồ sơ đổi theo kết luận đã sửa: {CONCLUSION_LABELS.get(old, old)} → {label}.",
            )
            notify(session, claim)
            retyped.append(claim.code)
            continue
        if claim.status == "NEW":  # đã có hồ sơ mở loại mới: gộp vào đó, đóng hồ sơ này
            claim.status, claim.closed_at = "CLOSED", clock.now()
            claim.close_reason = f"Kết luận đã sửa thành {label} — gộp vào hồ sơ cùng loại"
            add_note(session, claim, "STATUS_CHANGE", f"Mới → Đóng: kết luận đã sửa thành {label}.")
        else:
            add_note(session, claim, "SYSTEM", f"Kết luận phiên mở hoàn đã sửa thành {label}.")
        claim.version += 1
        notify(session, claim)
    if retyped:
        return retyped, None
    return retyped, await create_from_return(session, pack, case)


async def close_auto_on_correct_ok(session: AsyncSession, pack: PackSession) -> list[Claim]:
    """API-113 ISSUE → OK (T-115): hồ sơ `AUTO_RETURN` đang `NEW` của phiên → `CLOSED` (lý do hệ thống)."""
    rows = await session.scalars(
        select(Claim)
        .join(ClaimEvidence, ClaimEvidence.claim_id == Claim.id)
        .where(ClaimEvidence.session_id == pack.id, Claim.source == "AUTO_RETURN", Claim.status == "NEW")
        .order_by(Claim.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    closed = []
    for claim in rows.all():
        claim.status, claim.closed_at = "CLOSED", clock.now()
        claim.close_reason = "Kết luận đã sửa thành Nguyên vẹn"
        claim.version += 1
        add_note(session, claim, "STATUS_CHANGE", "Mới → Đóng: Kết luận đã sửa thành Nguyên vẹn.")
        notify(session, claim)
        closed.append(claim)
    return closed


async def create_manual(session: AsyncSession, data: ClaimCreateIn, p: Principal) -> Claim:
    """API-131 (FR-08.01): kiện bắt buộc; BR-27 → `409 CLAIM_EXISTS`; bằng chứng tự chọn; cảnh báo lệch
    (nếu có, cùng kiện) → `RESOLVED` action `OPEN_CLAIM`."""
    package = await session.get(Package, data.package_id)
    if package is None:
        raise AppError("NOT_FOUND", "Không tìm thấy kiện hàng.", 404)
    case: ReturnCase | None = None
    if data.return_case_id is not None:
        case = await session.get(ReturnCase, data.return_case_id)
        linked = await session.scalar(
            select(ReturnCasePackage.package_id).where(
                ReturnCasePackage.return_case_id == data.return_case_id,
                ReturnCasePackage.package_id == package.id,
            )
        )
        if case is None or linked is None:
            raise _validation("return_case_id", "Hồ sơ hàng hoàn không chứa kiện này")
    alert = None
    if data.recon_alert_id is not None:
        alert = await recon.lock_alert(session, data.recon_alert_id)
        if alert is None or alert.package_id != package.id:
            raise _validation("recon_alert_id", "Cảnh báo không thuộc kiện này")
    note = (data.note or "").strip()

    await _lock_create(session, package.id, data.type)
    existing = await find_open(session, package.id, data.type)
    if existing is not None:
        raise _claim_exists(existing)
    cfg = await settings_service.get(session)
    deadline, deadline_source = _deadline(case, cfg.claim_deadline_days)
    source = "RECON" if alert is not None else "MANUAL"
    try:
        async with session.begin_nested():  # savepoint: đọc lại hồ sơ đã có sau lỗi unique (G3 C-04)
            claim = await _insert_claim(
                session,
                Claim(
                    package_id=package.id,
                    order_id=package.order_id,
                    return_case_id=case.id if case else None,
                    type=data.type,
                    counterparty=data.counterparty,
                    status="NEW",
                    source=source,
                    deadline_at=deadline,
                    deadline_source=deadline_source,
                    created_by=p.user_id,
                    version=1,
                ),
            )
    except IntegrityError as exc:  # lưới an toàn của partial unique BR-27 (advisory lock đã tuần tự hóa)
        found = await find_open(session, package.id, data.type)
        if found is not None:
            raise _claim_exists(found) from exc  # kèm details {claim_id, code} như nhánh thường (G3 C-04)
        raise AppError("CLAIM_EXISTS", "Kiện này đã có hồ sơ cùng loại đang mở.", 409) from exc
    await auto_evidence(
        session,
        claim,
        package.id,
        await completed_return_sessions(session, package.id, case.id if case else None),
        prior=True,
    )
    if alert is not None:
        add_note(
            session, claim, "SYSTEM", f"Tạo từ cảnh báo lệch: {RULE_LABELS.get(alert.rule, alert.rule)}."
        )
        if alert.status == "OPEN":  # đã đóng (tự hết / người khác xử lý) → giữ kết quả cũ (DEC-303)
            recon.close_alert(alert, action="OPEN_CLAIM", note=note or None, by=p.user_id, claim_id=claim.id)
            _publish_recon_after_commit(session, (await recon.summary(session)).model_dump())
    if note:
        add_note(session, claim, "NOTE", note, p.user_id)
    audit.record(
        session,
        "CLAIM_CREATE",
        user_id=p.user_id,
        object_type="CLAIM",
        object_id=claim.id,
        ip=p.ip,
        data={
            "code": claim.code,
            "source": source,
            "type": data.type,
            "package_id": str(package.id),
            "recon_alert_id": str(alert.id) if alert else None,
        },
    )
    notify(session, claim)
    return claim


def _claim_exists(existing: Claim) -> AppError:
    return AppError(
        "CLAIM_EXISTS",
        f"Kiện này đã có hồ sơ cùng loại đang mở: {existing.code}.",
        409,
        {"claim_id": str(existing.id), "code": existing.code},
    )


def _publish_recon_after_commit(session: AsyncSession, open_summary: dict[str, int]) -> None:
    from aicam.realtime import publish

    async def _send() -> None:
        await publish.to_dashboard("recon.updated", {"summary": {"open": open_summary}})

    after_commit(session, _send)


# ---------------------------------------------------------------- API-133


async def _owner(session: AsyncSession, user_id: uuid.UUID) -> User:
    user = await session.get(User, user_id)
    if user is None or not user.is_active or user.role not in OWNER_ROLES:
        raise _validation("owner_user_id", "Người phụ trách phải là Admin / Quản lý / CSKH đang hoạt động")
    return user


async def patch(session: AsyncSession, claim: Claim, data: ClaimPatchIn, p: Principal) -> bool:
    """API-133: người gọi đã khóa hồ sơ và kiểm `version`. Trả True nếu có thay đổi (đã tăng `version`)."""
    if claim.status == "CLOSED":
        raise AppError("CLAIM_CLOSED", "Hồ sơ đã đóng, chỉ thêm được ghi chú.", 409)
    reason = (data.reason or "").strip() or None
    if reason is not None and not 5 <= len(reason) <= 500:
        raise _validation("reason", "Nhập lý do 5–500 ký tự")
    ref = (data.platform_claim_ref or "").strip() or None
    if ref is not None and len(ref) > 64:
        raise _validation("platform_claim_ref", "Mã khiếu nại bên sàn tối đa 64 ký tự")
    before = _snapshot(claim)
    notes: list[str] = []

    if data.owner_user_id is not None and data.owner_user_id != claim.owner_user_id:
        owner = await _owner(session, data.owner_user_id)
        claim.owner_user_id = owner.id
        notes.append(f"Người phụ trách: {owner.display_name}.")
    if data.deadline_at is not None and data.deadline_at != claim.deadline_at:
        claim.deadline_at, claim.deadline_source = data.deadline_at, "MANUAL"
        claim.due_soon_notified_at = None  # hạn mới → J-15 nhắc lại
        notes.append(f"Hạn khiếu nại: {_vn(data.deadline_at)}.")
    if ref is not None and ref != claim.platform_claim_ref:
        claim.platform_claim_ref = ref
        notes.append(f"Mã khiếu nại bên sàn: {ref}.")
    if data.recovered_amount is not None and data.recovered_amount != claim.recovered_amount:
        claim.recovered_amount = data.recovered_amount
        notes.append(f"Số tiền thu hồi: {data.recovered_amount:,} đ.".replace(",", "."))

    target = data.status
    if target is not None and target != claim.status:
        allowed = allowed_transitions(claim.status)
        if target not in allowed:
            raise AppError(
                "INVALID_TRANSITION",
                f"Không chuyển được từ {STATUS_LABELS[claim.status]} sang {STATUS_LABELS[target]}.",
                409,
                {"allowed": allowed},
            )
        if target == "SUBMITTED" and claim.platform_claim_ref is None and reason is None:
            raise _validation("platform_claim_ref", "Nhập mã khiếu nại bên sàn hoặc lý do")
        if target == "WON" and claim.recovered_amount is None:
            raise _validation("recovered_amount", "Nhập số tiền thu hồi")
        if target == "CLOSED" and claim.status in DUE_STATUSES and reason is None:
            raise _validation("reason", "Nhập lý do đóng hồ sơ 5–500 ký tự")
        text_ = f"{STATUS_LABELS[claim.status]} → {STATUS_LABELS[target]}"
        claim.status = target
        if target == "CLOSED":
            claim.closed_at = clock.now()
            claim.close_reason = reason
        notes.insert(0, text_ + (f": {reason}" if reason else "") + ".")

    if not notes:
        return False
    for line in notes:
        add_note(session, claim, "STATUS_CHANGE", line, p.user_id)
    claim.version += 1
    audit.record(
        session,
        "CLAIM_UPDATE",
        user_id=p.user_id,
        object_type="CLAIM",
        object_id=claim.id,
        ip=p.ip,
        data={"before": before, "after": _snapshot(claim)},
    )
    notify(session, claim)
    return True


def _snapshot(claim: Claim) -> dict[str, object]:
    return {
        "status": claim.status,
        "owner_user_id": str(claim.owner_user_id) if claim.owner_user_id else None,
        "deadline_at": clock.iso_z(claim.deadline_at) if claim.deadline_at else None,
        "platform_claim_ref": claim.platform_claim_ref,
        "recovered_amount": claim.recovered_amount,
        "close_reason": claim.close_reason,
        "version": claim.version,
    }


# ---------------------------------------------------------------- API-134


async def allowed_sessions(session: AsyncSession, claim: Claim) -> set[uuid.UUID]:
    """Phiên được làm bằng chứng: phiên của kiện, của mọi kiện thuộc hồ sơ hàng hoàn của hồ sơ, và phiên
    RETURN gắn hồ sơ hàng hoàn đó (02 §6.2 API-134)."""
    package_ids = {claim.package_id}
    if claim.return_case_id is not None:
        package_ids.update(
            (
                await session.scalars(
                    select(ReturnCasePackage.package_id).where(
                        ReturnCasePackage.return_case_id == claim.return_case_id
                    )
                )
            ).all()
        )
    cond: ColumnElement[bool] = PackSession.package_id.in_(sorted(package_ids))
    if claim.return_case_id is not None:
        cond = or_(cond, PackSession.return_case_id == claim.return_case_id)
    return set((await session.scalars(select(PackSession.id).where(cond))).all())


async def set_evidence(session: AsyncSession, claim: Claim, data: EvidenceIn, p: Principal) -> bool:
    """API-134: thay tập bằng chứng. Người gọi đã khóa hồ sơ + kiểm `version`. Trả True nếu đổi."""
    if claim.status == "CLOSED":
        raise AppError("CLAIM_CLOSED", "Hồ sơ đã đóng, chỉ thêm được ghi chú.", 409)
    allowed = await allowed_sessions(session, claim)
    wanted_sessions = list(dict.fromkeys(data.session_ids))
    if any(s not in allowed for s in wanted_sessions):
        raise _validation("session_ids", "Phiên không thuộc kiện / hồ sơ hàng hoàn của hồ sơ này")
    wanted_snaps = list(dict.fromkeys(data.snapshot_ids))
    if wanted_snaps:
        snap_sessions = dict(
            (
                await session.execute(
                    select(Snapshot.id, Snapshot.session_id).where(Snapshot.id.in_(wanted_snaps))
                )
            ).all()
        )
        if any(snap_sessions.get(s) not in allowed for s in wanted_snaps):
            raise _validation("snapshot_ids", "Ảnh không thuộc kiện / hồ sơ hàng hoàn của hồ sơ này")
    current = (await session.scalars(select(ClaimEvidence).where(ClaimEvidence.claim_id == claim.id))).all()
    removed = [
        e
        for e in current
        if (e.kind == "SESSION" and e.session_id not in wanted_sessions)
        or (e.kind == "SNAPSHOT" and e.snapshot_id not in wanted_snaps)
    ]
    note = (data.note or "").strip()
    if any(e.auto for e in removed) and len(note) < 5:
        raise _validation("note", "Nhập lý do bỏ bằng chứng tự chọn (5–500 ký tự)")
    have_sessions = {e.session_id for e in current if e.kind == "SESSION"}
    have_snaps = {e.snapshot_id for e in current if e.kind == "SNAPSHOT"}
    for evidence in removed:
        await session.delete(evidence)
    await session.flush()
    added = await add_evidence(
        session,
        claim,
        [s for s in wanted_sessions if s not in have_sessions],
        [s for s in wanted_snaps if s not in have_snaps],
        auto=False,
        added_by=p.user_id,
    )
    if not removed and not added:
        return False
    parts = []
    if added:
        parts.append(f"thêm {added}")
    if removed:
        parts.append(f"bỏ {len(removed)}")
    summary = f"Cập nhật bằng chứng: {', '.join(parts)}." + (f" Lý do: {note}" if note else "")
    add_note(session, claim, "NOTE", summary, p.user_id)
    claim.version += 1
    audit.record(
        session,
        "CLAIM_EVIDENCE_UPDATE",
        user_id=p.user_id,
        object_type="CLAIM",
        object_id=claim.id,
        ip=p.ip,
        data={
            "added": added,
            "removed": [str(e.id) for e in removed],
            "note": note or None,
            "session_ids": [str(s) for s in wanted_sessions],
            "snapshot_ids": [str(s) for s in wanted_snaps],
        },
    )
    notify(session, claim)
    return True


# ---------------------------------------------------------------- gộp sang kiện thật (DEC-269, API-112)


@dataclass
class MergedClaim:
    from_code: str
    into_code: str


async def move_claims_to_package(
    session: AsyncSession,
    from_package_ids: Sequence[uuid.UUID],
    target: Package,
    *,
    return_case_id: uuid.UUID | None,
    actor: uuid.UUID | None,
) -> list[MergedClaim]:
    """Hồ sơ khiếu nại của kiện tạm sang kiện thật (02 §6.2 API-112, DEC-260): trùng BR-27 với hồ sơ đang mở
    của kiện đích → bằng chứng + ghi chú gộp vào đó, hồ sơ cũ `CLOSED` "Gộp vào KN-…"; thêm phiên PACK hiệu
    lực của kiện đích (tự chọn). Người gọi giữ khóa hồ sơ hàng hoàn + kiện."""
    ids = sorted(set(from_package_ids) - {target.id})
    if not ids:
        return []
    moving = list(
        (
            await session.scalars(
                select(Claim)
                .where(Claim.package_id.in_(ids))
                .order_by(Claim.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    merged: list[MergedClaim] = []
    for claim in moving:
        into = None
        if claim.status != "CLOSED" and claim.source != "LEGACY_HOLD":
            await _lock_create(session, target.id, claim.type)
            into = await find_open(session, target.id, claim.type, for_update=True)
        if into is not None:  # đóng trước khi đổi kiện — partial unique BR-27 không vướng
            claim.status, claim.closed_at = "CLOSED", clock.now()
            claim.close_reason = f"Gộp vào {into.code}"
        claim.package_id = target.id
        claim.order_id = target.order_id
        if return_case_id is not None:
            claim.return_case_id = return_case_id
        claim.version += 1
        if into is not None:
            evidence = (
                await session.scalars(select(ClaimEvidence).where(ClaimEvidence.claim_id == claim.id))
            ).all()
            await add_evidence(
                session,
                into,
                [e.session_id for e in evidence if e.session_id],
                [e.snapshot_id for e in evidence if e.snapshot_id],
                auto=False,
                added_by=actor,
            )
            for old in (
                await session.scalars(
                    select(ClaimNote).where(ClaimNote.claim_id == claim.id).order_by(ClaimNote.at)
                )
            ).all():
                add_note(session, into, old.kind, f"[{claim.code}] {old.text}", old.author_user_id)
            add_note(session, into, "SYSTEM", f"Gộp hồ sơ {claim.code} vào hồ sơ này.")
            into.version += 1
            add_note(session, claim, "SYSTEM", f"Gộp vào {into.code}.")
            notify(session, into)
            merged.append(MergedClaim(claim.code, into.code))
        else:
            await auto_evidence(session, claim, target.id, [])  # thêm phiên PACK hiệu lực của kiện đích
            add_note(session, claim, "SYSTEM", f"Chuyển sang kiện {target.tracking_number}.")
        notify(session, claim)
    await session.flush()
    return merged


# ---------------------------------------------------------------- J-15


async def check_deadlines(session: AsyncSession) -> int:
    """J-15 (60 phút): hồ sơ chưa gửi / đang chờ có hạn ≤ now + `claim_due_soon_hours`, chưa nhắc → ghi chú
    "Sắp hết hạn khiếu nại" (một lần), WS. Không tăng `version` (không đổi trường người dùng sửa)."""
    from aicam.core.db import commit

    cfg = await settings_service.get(session)
    now = clock.now()
    rows = (
        await session.scalars(
            select(Claim)
            .where(
                Claim.status.in_(DUE_STATUSES),
                Claim.deadline_at.is_not(None),
                Claim.deadline_at <= now + timedelta(hours=cfg.claim_due_soon_hours),
                Claim.due_soon_notified_at.is_(None),
            )
            .order_by(Claim.deadline_at)
            .with_for_update(skip_locked=True)
        )
    ).all()
    for claim in rows:
        claim.due_soon_notified_at = now
        assert claim.deadline_at is not None  # noqa: S101 — lọc ở truy vấn
        add_note(session, claim, "SYSTEM", f"Sắp hết hạn khiếu nại (hạn {_vn(claim.deadline_at)}).")
        notify(session, claim)
    await commit(session)
    if rows:
        log.info("claims_due_soon", count=len(rows))
    return len(rows)
