"""Yêu cầu duyệt: station gửi / rút (API-13, 14), dashboard xem / quyết định (API-20, 21).

FR-03.10, 03.12, UC-08, BR-03, BR-06, BR-18; 02a §4 API-13, 14, 20, 21.
Mọi thao tác trên phiên của station: khóa station (advisory, DEC-11) → đọc lại phiên / yêu cầu → ghi → commit;
WS gửi sau commit.
"""

import uuid
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.pagination import Page
from aicam.core.redis import get_redis
from aicam.core.settings import Settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.approvals.queries import pending_for_station
from aicam.modules.approvals.schemas import (
    ACTIONS_BY_TYPE,
    ApprovalCreated,
    ApprovalCreatedOut,
    ApprovalItem,
    ApprovalRequestIn,
    DecisionIn,
    DecisionOut,
    DecisionResult,
    UserBrief,
)
from aicam.modules.approvals.views import approval_item, snapshot_counts, user_brief
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Package
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import CANCEL_CAUSES, PackSession
from aicam.modules.sessions.schemas import StationStateOut
from aicam.modules.sessions.tray import read_tray
from aicam.modules.stations import service as stations
from aicam.modules.stations.models import Station
from aicam.modules.users.queries import get_user_ref

log = structlog.get_logger()

_REQUIRED_STATUS = {"MISMATCH": "MISMATCH", "ASSIST": "OPEN"}
RETURN_ACTIONS = ("CONTINUE", "CANCEL_SESSION")  # yêu cầu từ phiên RETURN (02 API-21)
RETURN_CANCEL_NOTE_MIN, RETURN_CANCEL_NOTE_MAX = 5, 500
NOTE_MAX = 500


def _not_eligible(message: str) -> AppError:
    return AppError("NOT_ELIGIBLE", message, 409)


def _invalid(field: str, message: str) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {field: message}})


async def _already_resolved(session: AsyncSession, approval: ApprovalRequest) -> AppError:
    brief = await user_brief(session, approval)
    return AppError(
        "ALREADY_RESOLVED",
        "Yêu cầu này đã được xử lý." if approval.status == "RESOLVED" else "Station đã rút yêu cầu.",
        409,
        {
            "status": approval.status,
            "decided_by": brief.model_dump(mode="json") if brief else None,
            "decided_at": clock.iso_z(approval.decided_at) if approval.decided_at else None,
        },
    )


async def _locked_approval(session: AsyncSession, approval_id: uuid.UUID) -> ApprovalRequest | None:
    result: ApprovalRequest | None = await session.scalar(
        select(ApprovalRequest)
        .where(ApprovalRequest.id == approval_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result


async def _locked_session(session: AsyncSession, session_id: uuid.UUID | None) -> PackSession | None:
    if session_id is None:
        return None
    result: PackSession | None = await session.scalar(
        select(PackSession)
        .where(PackSession.id == session_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result


def _publish(
    session: AsyncSession, station_id: uuid.UUID, state: StationStateOut, event: str, item: ApprovalItem
) -> None:
    """Sau commit: `station.state` (WS-01) + `report.updated` + `approval.*` (WS-02, ADMIN / SUPERVISOR)."""
    from aicam.realtime import publish

    sessions.notify_after_commit(session, station_id, state)
    data = item.model_dump(mode="json")

    async def _send() -> None:
        await publish.to_approvers(event, data)

    after_commit(session, _send)


# ---------------------------------------------------------------- API-13


def _context_for_session(pack: PackSession, tray_match: str) -> dict[str, Any]:
    mismatch = pack.mismatch or {}
    return {
        "expected": pack.open_code,
        "actual": mismatch.get("actual"),
        "source": mismatch.get("source"),
        "tray_match": tray_match,
    }


async def request(
    session: AsyncSession, station: Station, body: ApprovalRequestIn, settings: Settings
) -> ApprovalCreatedOut:
    """API-13: station gửi yêu cầu MISMATCH (phiên lệch mã), ASSIST (phiên mở), REPACK (kiện PACKED)."""
    if body.type == "REPACK" and not (body.tracking_number and body.tracking_number.strip()):
        raise _invalid("tracking_number", "Bắt buộc với yêu cầu đóng gói lại")
    if body.type != "REPACK" and body.session_id is None:
        raise _invalid("session_id", "Bắt buộc với yêu cầu lệch mã / gọi quản lý")

    await sessions.lock_station(session, station.id)
    if await pending_for_station(session, station.id) is not None:
        raise AppError("APPROVAL_ALREADY_PENDING", "Station đã có yêu cầu đang chờ.", 409)
    pack = await sessions.active_session(session, station.id, refresh=True)

    if body.type == "REPACK":
        tracking = (body.tracking_number or "").strip().upper()
        package = await orders.find_package(session, tracking)
        if station.work_mode == "RETURN":
            raise _not_eligible("Station đang ở chế độ nhận hàng hoàn, không đóng gói lại được.")
        if pack is not None:
            raise _not_eligible("Station đang có phiên mở. Đóng hoặc hủy phiên trước.")
        if package is None or package.warehouse_status != "PACKED":
            raise _not_eligible("Kiện không ở trạng thái Đã đóng gói, không đóng gói lại được.")
        session_id = None
        tray = await read_tray(get_redis(), station.id, tracking)
        context: dict[str, Any] = {
            "expected": tracking,
            "actual": None,
            "source": None,
            "tray_match": tray.match,
        }
    else:
        required = _REQUIRED_STATUS[body.type]
        if pack is None or pack.id != body.session_id or pack.status != required:
            raise _not_eligible(
                "Phiên không ở trạng thái Lệch mã." if body.type == "MISMATCH" else "Phiên không còn đang mở."
            )
        tracking = pack.open_code
        session_id = pack.id
        tray = await read_tray(get_redis(), station.id, pack.open_code)
        context = _context_for_session(pack, tray.match)
        pack.status_before_approval = pack.status
        pack.status = "WAITING_APPROVAL"

    approval = ApprovalRequest(
        station_id=station.id,
        session_id=session_id,
        tracking_number=tracking,
        type=body.type,
        status="PENDING",
        context=context,
        created_at=clock.now(),
    )
    session.add(approval)
    try:
        async with session.begin_nested():
            await session.flush()
    except IntegrityError as exc:  # unique PENDING / station — dự phòng ngoài khóa
        raise AppError("APPROVAL_ALREADY_PENDING", "Station đã có yêu cầu đang chờ.", 409) from exc
    if pack is not None and session_id is not None:
        sessions.record_event(
            session, pack, "APPROVAL_REQUESTED", approval_id=str(approval.id), type=body.type
        )
    await session.flush()
    state = await sessions.build_state(session, station, settings)
    _publish(session, station.id, state, "approval.created", await approval_item(session, approval))
    await commit(session)
    log.info("approval_requested", station_id=str(station.id), type=body.type, tracking_number=tracking)
    return ApprovalCreatedOut(
        approval_request=ApprovalCreated(
            id=approval.id,
            type=body.type,
            status="PENDING",
            tracking_number=tracking,
            created_at=approval.created_at,
        ),
        state=state,
    )


# ---------------------------------------------------------------- API-14


async def withdraw(
    session: AsyncSession, station: Station, approval_id: uuid.UUID, settings: Settings
) -> StationStateOut:
    """API-14: rút yêu cầu → phiên về trạng thái trước khi gửi rồi đánh giá lại khay (BR-06)."""
    await sessions.lock_station(session, station.id)
    approval = await _locked_approval(session, approval_id)
    if approval is None or approval.station_id != station.id:
        raise AppError("NOT_FOUND", "Không tìm thấy yêu cầu.", 404)
    if approval.status != "PENDING":
        raise await _already_resolved(session, approval)
    approval.status = "WITHDRAWN"
    approval.decided_at = clock.now()  # mốc kết thúc chờ duyệt: đồng hồ quá giờ tính lại từ đây (DEC-60)
    pack = await _locked_session(session, approval.session_id)
    if pack is not None and pack.status == "WAITING_APPROVAL":
        pack.status = pack.status_before_approval or "OPEN"
        pack.status_before_approval = None
        pack.warn_notified = False  # cảnh báo 15 phút tính lại từ lúc hết chờ (DEC-60)
        sessions.record_event(session, pack, "APPROVAL_WITHDRAWN", approval_id=str(approval.id))
        # Trong lúc chờ khay có thể đã đổi (bỏ / đặt phiếu sai).
        sessions.apply_tray(session, pack, await read_tray(get_redis(), station.id, pack.open_code))
    await session.flush()
    state = await sessions.build_state(session, station, settings)
    _publish(session, station.id, state, "approval.resolved", await approval_item(session, approval))
    await commit(session)
    log.info("approval_withdrawn", station_id=str(station.id), approval_id=str(approval.id))
    return state


# ---------------------------------------------------------------- API-20


async def list_requests(
    session: AsyncSession, *, status: str | None, page: int, page_size: int
) -> Page[ApprovalItem]:
    """API-20: cũ nhất trước (người chờ lâu nhất lên đầu)."""
    query = select(ApprovalRequest)
    if status is not None:
        query = query.where(ApprovalRequest.status == status)
    total = await session.scalar(select(func.count()).select_from(query.subquery())) or 0
    rows = await session.scalars(
        query.order_by(ApprovalRequest.created_at, ApprovalRequest.id)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    found = rows.all()
    counts = await snapshot_counts(session, [a.session_id for a in found if a.session_id])
    items = [await approval_item(session, a, counts) for a in found]
    return Page(items=items, page=page, page_size=page_size, total=total)


# ---------------------------------------------------------------- API-21


async def _open_repack(
    session: AsyncSession, station: Station, approval: ApprovalRequest, actor_label: str
) -> None:
    """Duyệt đóng gói lại: mở phiên mới cờ REPACK; phiên cũ `SUPERSEDED` khi phiên mới hoàn tất (BR-03)."""
    if await sessions.active_session(session, station.id, refresh=True) is not None:
        raise _not_eligible("Station đang có phiên mở.")
    package: Package | None = await orders.find_package(session, approval.tracking_number)
    if package is None or package.warehouse_status != "PACKED":
        raise _not_eligible("Kiện đã rời trạng thái Đã đóng gói, không đóng gói lại được.")
    previous = await sessions.last_completed(session, package.id)
    pack = PackSession(
        package_id=package.id,
        station_id=station.id,
        status="OPEN",
        started_at=clock.now(),
        open_code=package.tracking_number,
        package_status_before="PACKED",
        supersedes_session_id=previous.id if previous else None,
        flags=["REPACK"] if package.verified else ["REPACK", "UNVERIFIED"],
    )
    session.add(pack)
    await orders.transition(session, package, "PACKING", source="WAREHOUSE", actor_label=actor_label)
    await session.flush()
    approval.session_id = pack.id
    sessions.record_event(session, pack, "REPACK_OPEN", approval_id=str(approval.id), code=pack.open_code)
    sessions.apply_tray(session, pack, await read_tray(get_redis(), station.id, pack.open_code))


async def _decide_on_session(
    session: AsyncSession,
    station: Station,
    approval: ApprovalRequest,
    action: str,
    note: str | None,
    actor_label: str,
    reason_code: str | None = None,
) -> None:
    pack = await _locked_session(session, approval.session_id)
    if pack is None or pack.status != "WAITING_APPROVAL":
        raise _not_eligible("Phiên không còn chờ duyệt.")
    if pack.type == "RETURN" and action not in RETURN_ACTIONS:
        # Phiên hoàn đóng bằng quét + kết luận (02 API-21): chỉ "Cho tiếp tục" / "Hủy phiên".
        raise AppError("INVALID_ACTION", "Phiên mở hoàn chỉ được cho tiếp tục hoặc hủy.", 422)
    if pack.type == "RETURN" and action == "CANCEL_SESSION":
        # BR-37 / FR-04.14 (Phase 3, DEC-447): Supervisor hủy phiên mở hoàn phải ghi lý do 5–500 ký tự;
        # v0.3 (DEC-514, 521): + mã lý do — `WRONG_SCAN` / `NOT_A_RETURN` loại phiên khỏi bằng chứng (BR-39).
        fields: dict[str, str] = {}
        if reason_code not in CANCEL_CAUSES:
            fields["reason_code"] = "Chọn lý do hủy."
        if not (note and RETURN_CANCEL_NOTE_MIN <= len(note) <= RETURN_CANCEL_NOTE_MAX):
            fields["note"] = "Nhập ghi chú (5–500 ký tự)."
        if fields:
            raise AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": fields})
    elif note is not None and len(note) > NOTE_MAX:  # hủy phiên PACK: ghi chú tùy chọn, tối đa 500
        raise _invalid("note", "Nhập ghi chú (1–500 ký tự)")
    tray = await read_tray(get_redis(), station.id, pack.open_code)
    pack.status_before_approval = None
    pack.warn_notified = False  # cảnh báo 15 phút tính lại từ lúc hết chờ (DEC-60)
    if action == "CONTINUE":
        # Về OPEN rồi đánh giá lại Cam 2 ngay: khay còn phiếu sai → MISMATCH (02 API-21, TC-03.44).
        pack.status = "OPEN"
        pack.mismatch = None
        sessions.apply_tray(session, pack, tray)
    elif action == "CLOSE_WITH_NOTE":
        if tray.blocks_close:  # BR-06: không đóng khi Cam 2 còn thấy mã khác
            raise AppError(
                "TRAY_STILL_DIFFERENT", "Cam 2 vẫn thấy phiếu sai trên khay. Yêu cầu bỏ phiếu sai trước.", 409
            )
        pack.note = note
        sessions.set_flag(pack, "CLOSED_BY_SUPERVISOR")
        await sessions.complete_session(session, pack, tray=tray, close_code=None, actor_label=actor_label)
    else:  # CANCEL_SESSION — như API-12, lý do SUPERVISOR (kiện về trạng thái lúc mở phiên, BR-03)
        await sessions.end_without_packing(
            session, pack, status="CANCELLED", reason="SUPERVISOR", note=note, actor_label=actor_label
        )
        if pack.type == "RETURN":
            pack.cancel_cause = reason_code  # `cancel_reason` giữ SUPERVISOR (ai hủy) — DEC-521
    sessions.record_event(session, pack, "APPROVAL_DECIDED", approval_id=str(approval.id), action=action)


async def decide(
    session: AsyncSession, approval_id: uuid.UUID, body: DecisionIn, actor: Principal, settings: Settings
) -> DecisionOut:
    """API-21: ADMIN / SUPERVISOR duyệt. Người sau → ALREADY_RESOLVED; audit APPROVAL_DECISION (FR-03.12)."""
    note = body.note.strip() if body.note and body.note.strip() else None
    if body.action == "CLOSE_WITH_NOTE" and note is None:
        raise _invalid("note", "Nhập ghi chú (1–500 ký tự)")
    if note is not None and len(note) > NOTE_MAX and body.action != "CANCEL_SESSION":
        raise _invalid("note", "Nhập ghi chú (1–500 ký tự)")
    found = await session.get(ApprovalRequest, approval_id)
    if found is None:
        raise AppError("NOT_FOUND", "Không tìm thấy yêu cầu.", 404)
    station_id = found.station_id
    # Thứ tự khóa như API-13/14 và quét: station trước, rồi dòng yêu cầu.
    await sessions.lock_station(session, station_id)
    approval = await _locked_approval(session, approval_id)
    if approval is None:
        raise AppError("NOT_FOUND", "Không tìm thấy yêu cầu.", 404)
    if approval.status != "PENDING":
        raise await _already_resolved(session, approval)
    if body.action not in ACTIONS_BY_TYPE[approval.type]:
        raise AppError("INVALID_ACTION", "Thao tác không hợp với loại yêu cầu.", 422)
    station = await stations.get_station(session, station_id)
    if station is None:
        raise AppError("NOT_FOUND", "Không tìm thấy station.", 404)
    user = await get_user_ref(session, actor.user_id)
    actor_label = user.display_name if user else "Quản lý"

    if approval.type == "REPACK":
        if body.action == "APPROVE_REPACK":
            await _open_repack(session, station, approval, actor_label)
    else:
        await _decide_on_session(
            session, station, approval, body.action, note, actor_label, reason_code=body.reason_code
        )

    now = clock.now()
    approval.status = "RESOLVED"
    approval.decision = body.action
    approval.decided_by = actor.user_id
    approval.decided_at = now
    approval.note = note
    audit.record(
        session, "APPROVAL_DECISION", user_id=actor.user_id, object_type="approval_request",
        object_id=approval.id, ip=actor.ip,
        data={"type": approval.type, "action": body.action, "station_id": str(station_id),
              "session_id": str(approval.session_id) if approval.session_id else None,
              "tracking_number": approval.tracking_number, "note": note,
              **({"reason_code": body.reason_code} if body.action == "CANCEL_SESSION" and body.reason_code
                 else {})},
    )  # fmt: skip
    await session.flush()
    state = await sessions.build_state(session, station, settings)
    item = await approval_item(session, approval)
    _publish(session, station_id, state, "approval.resolved", item)
    if body.action == "CANCEL_SESSION":
        _alert_station(session, station_id, "SESSION_CANCELLED_BY_SUPERVISOR", approval)
    await commit(session)
    log.info("approval_decided", approval_id=str(approval.id), type=approval.type, action=body.action,
             station_id=str(station_id))  # fmt: skip
    decided_by = item.decided_by or UserBrief(id=actor.user_id, display_name=actor_label)
    return DecisionOut(
        approval_request=DecisionResult(
            id=approval.id, status="RESOLVED", decision=body.action, decided_by=decided_by, decided_at=now
        )
    )


def _alert_station(
    session: AsyncSession, station_id: uuid.UUID, code: str, approval: ApprovalRequest
) -> None:
    """WS-01 `alert` như J-07: station hiện "Quản lý đã hủy phiên." trên S1 (02b-station)."""
    from aicam.realtime import publish

    data = {"code": code, "session_id": str(approval.session_id), "tracking_number": approval.tracking_number}

    async def _send() -> None:
        await publish.to_station(station_id, "alert", data)

    after_commit(session, _send)
