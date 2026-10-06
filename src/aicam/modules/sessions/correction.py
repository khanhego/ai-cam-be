"""API-113 — sửa kết luận phiên hoàn đã đóng (FR-04.11; 02 §6.2 API-113, §6.3 #4, #8; 02a §4; DEC-261, 319).

Khóa (DEC-266): `order:{sn}` của hồ sơ → hồ sơ hàng hoàn (FOR UPDATE) → phiên (FOR UPDATE) → kiện (id tăng).
Kiểm lại phiên `COMPLETED` + hạn 7 ngày **sau** khi khóa.
"""

import uuid
from datetime import timedelta
from typing import Any

import structlog
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit
from aicam.core.errors import AppError
from aicam.modules.claims import service as claims
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions import inspection
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.schemas import Conclusion, InspectionLineIn

log = structlog.get_logger()

WINDOW = timedelta(days=7)
CORRECTED_FLAG = "INSPECTION_CORRECTED"


class CorrectIn(BaseModel):
    conclusion: Conclusion
    note: str | None = Field(default=None, max_length=2000)
    lines: list[InspectionLineIn] = Field(default_factory=list, max_length=200)
    reason: str = Field(max_length=2000)


def _invalid(field: str, message: str) -> AppError:
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {field: message}})


def _check_window(pack: PackSession | None) -> PackSession:
    if pack is None:
        raise AppError("NOT_FOUND", "Không tìm thấy phiên.", 404)
    if pack.type != "RETURN" or pack.status != "COMPLETED":
        raise AppError("NOT_RETURN_SESSION", "Chỉ sửa được kết luận của phiên mở hoàn đã hoàn tất.", 409)
    if pack.ended_at is None or pack.ended_at < clock.now() - WINDOW:
        raise AppError("CORRECTION_WINDOW_EXPIRED", "Đã quá 7 ngày, không sửa được.", 409)
    return pack


async def correct(
    session: AsyncSession,
    session_id: uuid.UUID,
    data: CorrectIn,
    *,
    actor: uuid.UUID,
    actor_name: str,
    ip: str | None,
    tz: str,
) -> PackSession:
    """Ghi đè dòng + kết luận + ghi chú (BR-22 theo `lines_mode`), nối `{at, by, reason, before}` vào
    `inspection_corrections[]`, cờ `INSPECTION_CORRECTED`; kiện `RETURN_RECEIVED_OK ⇄ _ISSUE` (MANUAL) — hồ sơ
    một phiên: mọi kiện đã nhận của hồ sơ (BR-24); `returns.recompute`; OK → vấn đề: hồ sơ khiếu nại tự tạo
    (BR-08, BR-27); vấn đề → OK: hồ sơ `AUTO_RETURN` `NEW` của phiên → `CLOSED`.
    Audit `INSPECTION_CORRECT`."""
    reason = " ".join(data.reason.split())
    if not 5 <= len(reason) <= 500:
        raise _invalid("reason", "Nhập lý do 5–500 ký tự")
    # Bước đọc không khóa: tìm đơn / hồ sơ để khóa đúng thứ tự.
    found = _check_window(await session.get(PackSession, session_id))
    case_ids = [found.return_case_id] if found.return_case_id else []
    if found.return_case_id is not None:
        case_row = await session.get(ReturnCase, found.return_case_id)
        if case_row is not None and case_row.order_id is not None:
            order = await session.get(Order, case_row.order_id)
            if order is not None:
                await orders.lock_orders(session, [order.platform_order_sn])
    locked_cases = await returns.lock_cases(session, case_ids)
    case = locked_cases[0] if locked_cases else None
    pack = _check_window(
        await session.scalar(
            select(PackSession)
            .where(PackSession.id == session_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    )
    current = await inspection.lines_of(session, pack.id)
    lines = [
        inspection.LineInput(i.order_item_id, i.quantity_received, i.condition, i.note) for i in data.lines
    ]
    note = data.note.strip() if data.note else None
    inspection.validate(
        lines_mode=pack.inspection_lines_mode or "FULL", conclusion=data.conclusion, note=note,
        current=current, lines=lines,
    )  # fmt: skip
    before_out = inspection.inspection_out(pack, current)
    before: dict[str, Any] = {
        "conclusion": before_out.conclusion,
        "note": before_out.note,
        "lines": [line.model_dump(mode="json") for line in before_out.lines],
    }
    old = pack.inspection_conclusion
    inspection.replace_lines(current, lines)
    pack.inspection_conclusion = data.conclusion
    pack.inspection_note = note or None
    now = clock.now()
    pack.inspection_corrections = [
        *(pack.inspection_corrections or []),
        {
            "at": clock.iso_z(now),
            "by": {"id": str(actor), "display_name": actor_name},
            "reason": reason,
            "before": before,
        },
    ]
    if CORRECTED_FLAG not in pack.flags:
        pack.flags = [*pack.flags, CORRECTED_FLAG]
    await session.flush()

    was_ok, now_ok = old == "OK", data.conclusion == "OK"
    moved: list[uuid.UUID] = []
    if was_ok != now_ok:
        target = returns.received_status(data.conclusion)
        package_ids = [pack.package_id]
        if case is not None and await returns.is_single_session(session, case):
            package_ids = [p.id for p in await returns.packages_of_case(session, case.id)]
        for package in await returns.lock_packages(session, package_ids):
            if package.warehouse_status in returns.RECEIVED_STATUSES and await orders.transition(
                session, package, target, source="MANUAL", actor_user_id=actor
            ):
                moved.append(package.id)
    if case is not None:
        await returns.recompute(session, case)
        returns.notify_updated(session, case)
    created_claim: str | None = None
    closed_claims: list[str] = []
    if was_ok and not now_ok:
        result = await claims.create_from_return(session, pack, case)
        created_claim = result.claim.code if result else None
    elif not was_ok and now_ok:
        closed_claims = [c.code for c in await claims.close_auto_on_correct_ok(session, pack)]
    audit.record(
        session, "INSPECTION_CORRECT", user_id=actor, object_type="SESSION", object_id=pack.id, ip=ip,
        data={
            "reason": reason, "before": before,
            "after": {"conclusion": data.conclusion, "note": pack.inspection_note or "",
                      "lines": [line.model_dump(mode="json")
                                for line in inspection.inspection_out(pack, current).lines]},
            "packages": [str(p) for p in moved], "claim_created": created_claim,
            "claims_closed": closed_claims,
        },
    )  # fmt: skip
    await session.flush()
    _report_updated(session, tz)
    log.info(
        "inspection_corrected", session_id=str(pack.id), before=old, after=data.conclusion,
        return_case_id=str(case.id) if case else None,
    )  # fmt: skip
    return pack


def _report_updated(session: AsyncSession, tz: str) -> None:
    from zoneinfo import ZoneInfo

    from aicam.realtime import publish

    day = clock.now().astimezone(ZoneInfo(tz)).date().isoformat()

    async def _send() -> None:
        await publish.to_dashboard("report.updated", {"date": day})

    after_commit(session, _send)
