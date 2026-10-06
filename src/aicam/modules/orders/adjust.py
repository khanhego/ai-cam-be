"""API-122 — điều chỉnh trạng thái kho thủ công (FR-06.05, L6; 02a §4, DEC-266, DEC-303).

Thứ tự khóa (DEC-266): `order:{sn}` → hồ sơ hàng hoàn (FOR UPDATE, id tăng) → kiện (FOR UPDATE) → cảnh báo.
Kiểm lại trạng thái và phiên hoạt động **sau** khi khóa kiện.
"""

import uuid
from typing import Literal

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit
from aicam.core.errors import AppError
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import RETURN_STATUSES, WAREHOUSE_STATUSES, Order, Package
from aicam.modules.reconciliation import service as recon
from aicam.modules.reconciliation.schemas import ReconAlertOut
from aicam.modules.returns import service as returns
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession

WarehouseStatus = Literal[WAREHOUSE_STATUSES]  # type: ignore[valid-type]


class AdjustIn(BaseModel):
    to_status: WarehouseStatus
    reason: str  # 5–500 ký tự kiểm ở service → `details.fields.reason` tiếng Việt (G3 C-05)
    recon_alert_id: uuid.UUID | None = None


class AdjustedPackage(BaseModel):
    id: uuid.UUID
    tracking_number: str
    warehouse_status: str


class AdjustOut(BaseModel):
    package: AdjustedPackage
    recon_alert: ReconAlertOut | None


def _not_found() -> AppError:
    return AppError("NOT_FOUND", "Không tìm thấy kiện.", 404)


async def adjust_status(
    session: AsyncSession,
    package_id: uuid.UUID,
    data: AdjustIn,
    *,
    actor: uuid.UUID,
    ip: str | None,
    tz: str,
) -> AdjustOut:
    reason = data.reason.strip()
    if not 5 <= len(reason) <= 500:
        raise AppError(
            "VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"reason": "Nhập lý do 5–500 ký tự"}}
        )
    # Bước đọc không khóa: tìm đơn và hồ sơ hàng hoàn để khóa đúng thứ tự.
    package = await session.get(Package, package_id)
    if package is None:
        raise _not_found()
    if package.order_id is not None:
        order = await session.get(Order, package.order_id)
        if order is not None:
            await orders.lock_orders(session, [order.platform_order_sn])
    cases = await returns.lock_cases(session, await returns.open_case_ids_of_package(session, package_id))
    package = await session.scalar(
        select(Package)
        .where(Package.id == package_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if package is None:
        raise _not_found()
    # G3 BB-19: hồ sơ của kiện đọc lại dưới khóa kiện — có hồ sơ mới gắn giữa bước đọc và bước khóa thì chưa
    # khóa được (thứ tự hồ sơ → kiện) → báo người dùng thử lại, không sửa nửa vời.
    if set(await returns.open_case_ids_of_package(session, package_id)) - {c.id for c in cases}:
        raise AppError("VERSION_CONFLICT", "Dữ liệu kiện vừa thay đổi, tải lại rồi thử lại.", 409)

    active = await session.scalar(
        select(PackSession.id).where(
            PackSession.package_id == package.id, PackSession.status.in_(ACTIVE_STATUSES)
        )
    )
    if active is not None:
        raise AppError("SESSION_ACTIVE", "Kiện đang có phiên mở ở station.", 409)
    from_status = package.warehouse_status
    allowed = orders.MANUAL_TRANSITIONS.get(from_status, ())
    if data.to_status not in allowed:
        raise AppError(
            "TRANSITION_NOT_ALLOWED",
            "Không được chuyển trạng thái kho này bằng tay.",
            409,
            {"from": from_status, "allowed": list(allowed)},
        )

    alert = None
    if data.recon_alert_id is not None:
        alert = await recon.lock_alert(session, data.recon_alert_id)
        if alert is None or alert.package_id != package.id:
            raise AppError(
                "VALIDATION_ERROR",
                "Dữ liệu không hợp lệ.",
                422,
                {"fields": {"recon_alert_id": "Cảnh báo không thuộc kiện này"}},
            )

    await orders.transition(session, package, data.to_status, source="MANUAL", actor_user_id=actor)
    cancelled: list[uuid.UUID] = []
    await session.flush()
    for case in cases:
        # G3 SM-F3 / R12: kiện rời luồng hoàn (→ DELIVERED) thì rời hồ sơ (như giao lại — `apply_redelivery`);
        # mọi hồ sơ đã khóa tính lại (BR-24) — không kẹt `MISSING` khi kiện gia hạn về `RETURN_EXPECTED`,
        # không
        # kẹt `PARTIALLY_RECEIVED` khi kiện còn lại được xác nhận đã giao.
        if from_status in RETURN_STATUSES and data.to_status == "DELIVERED":
            await returns.unlink_package(session, case.id, package.id)
            await session.flush()
            if await returns.cancel_if_no_active_package(session, case):
                cancelled.append(case.id)
                continue
        if await returns.recompute(session, case):
            returns.notify_updated(session, case)
    alert_changed = False
    if alert is not None and alert.status == "OPEN":
        # Cảnh báo đã đóng (tự hết / người khác xử lý) → giữ nguyên, vẫn điều chỉnh kiện (DEC-303).
        recon.close_alert(alert, action="ADJUST_STATUS", note=reason, by=actor, to_status=data.to_status)
        alert_changed = True
    audit.record(
        session,
        "WAREHOUSE_STATUS_ADJUST",
        user_id=actor,
        object_type="PACKAGE",
        object_id=package.id,
        ip=ip,
        data={
            "from": from_status,
            "to": data.to_status,
            "reason": reason,
            "recon_alert_id": str(alert.id) if alert else None,
            "cancelled_return_cases": [str(c) for c in cancelled],
        },
    )
    await session.flush()
    out = AdjustOut(
        package=AdjustedPackage(
            id=package.id, tracking_number=package.tracking_number, warehouse_status=package.warehouse_status
        ),
        recon_alert=await recon.alert_out(session, alert, orders.MANUAL_TRANSITIONS) if alert else None,
    )
    open_summary = (await recon.summary(session)).model_dump() if alert_changed else None
    _publish_after_commit(session, tz, open_summary, cancelled)
    await commit(session)
    return out


def _publish_after_commit(
    session: AsyncSession, tz: str, open_summary: dict[str, int] | None, cancelled: list[uuid.UUID]
) -> None:
    from zoneinfo import ZoneInfo

    from aicam.realtime import publish

    day = clock.now().astimezone(ZoneInfo(tz)).date().isoformat()

    async def _send() -> None:
        if open_summary is not None:
            await publish.to_dashboard("recon.updated", {"summary": {"open": open_summary}})
        for case_id in cancelled:
            await publish.to_dashboard(
                "return.updated", {"return_case_id": str(case_id), "status": "CANCELLED"}
            )
        await publish.to_dashboard("report.updated", {"date": day})

    after_commit(session, _send)
