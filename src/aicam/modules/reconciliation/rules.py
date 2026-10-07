"""7 quy tắc đối soát set-based (02a §5 BR-10..14, 19, 20; DEC-226, 228, 254, 255, 262).

Mỗi quy tắc một truy vấn → tập `Hit(package_id, context, context_key)`. `context_key` là "đợt" vi phạm: cảnh
báo đã xử lý tay (`RESOLVED`) cùng (kiện, quy tắc, key) không tạo lại (BR-26). Chỉ đọc, không khóa.
"""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, not_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.orders.models import Order, Package, StatusHistory
from aicam.modules.platforms.base import CANCEL_GROUPS
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage

RULE_SEVERITY = {
    "SHIPPED_NOT_PACKED": "HIGH",
    "CANCELLED_AFTER_PACK": "MEDIUM",
    "RETURN_OVERDUE": "HIGH",
    "RETURN_UNANNOUNCED": "LOW",
    "PACKED_NOT_HANDED_OVER": "MEDIUM",
    "RETURN_DONE_NOT_RECEIVED": "HIGH",
    "UNVERIFIED_STALE": "LOW",
}
# BR-10: đơn đã giao đi trên sàn (nhóm — BR-30; lõi không đọc chữ trạng thái sàn, NFR-28).
SHIPPED_ORDER_GROUPS = ("SHIPPED", "DELIVERED")
# BR-19: nhóm yêu cầu trả `DONE` = đã hoàn tiền (DEC-262); `CLOSED` không cảnh báo.
DONE_RETURN_GROUP = "DONE"
UNANNOUNCED_AFTER = timedelta(hours=24)  # BR-13
UNVERIFIED_AFTER = timedelta(hours=24)  # BR-20


@dataclass(frozen=True)
class Hit:
    package_id: uuid.UUID
    rule: str
    context: dict[str, Any]
    context_key: str


@dataclass(frozen=True)
class Params:
    now: datetime
    recon_start_at: datetime
    return_missing_days: int
    handover_warn_hours: int


def _iso(at: datetime | None) -> str | None:
    return clock.iso_z(at) if at else None


def _hours(now: datetime, since: datetime) -> int:
    return int((now - since).total_seconds() // 3600)


async def shipped_not_packed(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-10 (HIGH): kiện `NEW` / `PACKING`, đơn sàn đã giao đi, đơn tạo sau `recon_start_at` (DEC-254),
    không phải
    kiện tạm. Key = trạng thái sàn."""
    created = func.coalesce(Order.created_at_platform, Package.created_at)
    rows = (
        await session.execute(
            select(Package.id, Package.warehouse_status, Order.platform_status, created)
            .join(Order, Order.id == Package.order_id)
            .where(
                Package.warehouse_status.in_(("NEW", "PACKING")),
                Order.platform_status_group.in_(SHIPPED_ORDER_GROUPS),
                created >= p.recon_start_at,
                Package.is_placeholder.is_(False),
            )
        )
    ).all()
    return [
        Hit(
            pid,
            "SHIPPED_NOT_PACKED",
            {"warehouse_status": ws, "platform_status": ps, "since": _iso(at)},
            # G3 C4: key theo nhóm trạng thái sàn (đã giao đi) — SHIPPED → TO_CONFIRM_RECEIVE → COMPLETED tiến
            # bình thường không làm cảnh báo đã xử lý tay bắn lại.
            "SHIPPED",
        )
        for pid, ws, ps, at in rows
    ]


async def cancelled_after_pack(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-11 (MEDIUM): `CANCELLED_AFTER_PACK`, hoặc `PACKED` mà đơn **đã hủy** trên sàn (nhóm `CANCELLED` —
    đang yêu cầu hủy không tính, sàn có thể từ chối: BR-21 v0.4, DEC-519). Key = trạng thái sàn."""
    rows = (
        await session.execute(
            select(Package.id, Package.warehouse_status, Order.platform_status, Package.status_changed_at)
            .outerjoin(Order, Order.id == Package.order_id)
            .where(
                or_(
                    Package.warehouse_status == "CANCELLED_AFTER_PACK",
                    and_(
                        Package.warehouse_status == "PACKED",
                        Order.platform_status_group == "CANCELLED",
                    ),
                )
            )
        )
    ).all()
    return [
        Hit(
            pid,
            "CANCELLED_AFTER_PACK",
            {"warehouse_status": ws, "platform_status": ps, "since": _iso(at)},
            "CANCELLED",  # G3 C4: IN_CANCEL → CANCELLED, PACKED → CANCELLED_AFTER_PACK cùng một đợt
        )
        for pid, ws, ps, at in rows
    ]


async def return_overdue(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-12 (HIGH): kiện `RETURN_MISSING` (J-14 bước 1 đã chuyển theo `status_changed_at` — DEC-255). Key =
    mốc vào `RETURN_MISSING` (mỗi lần gia hạn qua API-122 rồi quá hạn lại là một đợt mới). `since` = lần vào
    `RETURN_EXPECTED` gần nhất."""
    expected_since = (
        select(func.max(StatusHistory.at))
        .where(StatusHistory.package_id == Package.id, StatusHistory.to_status == "RETURN_EXPECTED")
        .correlate(Package)
        .scalar_subquery()
    )
    rows = (
        await session.execute(
            select(
                Package.id,
                Package.warehouse_status,
                Order.platform_status,
                Package.status_changed_at,
                expected_since,
            )
            .outerjoin(Order, Order.id == Package.order_id)
            .where(Package.warehouse_status == "RETURN_MISSING")
        )
    ).all()
    out: list[Hit] = []
    for pid, ws, ps, changed_at, since in rows:
        start = since or changed_at
        out.append(
            Hit(
                pid,
                "RETURN_OVERDUE",
                {
                    "warehouse_status": ws,
                    "platform_status": ps,
                    "since": _iso(start),
                    "days": int((p.now - start).total_seconds() // 86400),
                },
                _iso(changed_at) or "",
            )
        )
    return out


async def return_unannounced(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-13 (LOW): hồ sơ `UNANNOUNCED` chưa có mã sàn, không có tín hiệu giao thất bại, tạo quá 24 giờ,
    chưa hủy.
    Cảnh báo cho mọi kiện thật của hồ sơ. Key = id hồ sơ."""
    rows = (
        await session.execute(
            select(
                Package.id, Package.warehouse_status, ReturnCase.id, ReturnCase.code, ReturnCase.created_at
            )
            .join(ReturnCasePackage, ReturnCasePackage.package_id == Package.id)
            .join(ReturnCase, ReturnCase.id == ReturnCasePackage.return_case_id)
            .where(
                ReturnCase.kind == "UNANNOUNCED",
                ReturnCase.platform_return_sn.is_(None),
                ReturnCase.status.notin_(("CANCELLED",)),
                ReturnCase.created_at < p.now - UNANNOUNCED_AFTER,
                not_(func.array_to_string(ReturnCase.signal_keys, ",").like("%FAILED:%")),
                Package.is_placeholder.is_(False),
            )
            .order_by(
                ReturnCase.created_at, ReturnCase.id
            )  # G3 R5: hồ sơ cũ nhất thắng khi kiện thuộc nhiều hồ sơ
        )
    ).all()
    return [
        Hit(
            pid,
            "RETURN_UNANNOUNCED",
            {"warehouse_status": ws, "return_case": code, "since": _iso(at), "hours": _hours(p.now, at)},
            str(case_id),
        )
        for pid, ws, case_id, code, at in rows
    ]


async def packed_not_handed_over(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-14 (MEDIUM): `PACKED` quá `handover_warn_hours`, kiện tạo sau `recon_start_at`. Key = mốc vào
    `PACKED`."""
    rows = (
        await session.execute(
            select(Package.id, Package.warehouse_status, Order.platform_status, Package.status_changed_at)
            .outerjoin(Order, Order.id == Package.order_id)
            .where(
                Package.warehouse_status == "PACKED",
                Package.status_changed_at < p.now - timedelta(hours=p.handover_warn_hours),
                Package.created_at >= p.recon_start_at,
                # G3 R8 / BB-11: sàn chưa lấy hàng (đã giao đi → J-06 chuyển; đã hủy → BR-11 lo; đang yêu cầu
                # hủy → để riêng chờ sàn quyết, không cảnh báo — như Phase 2 với `IN_CANCEL`, DEC-519).
                or_(
                    Order.platform_status_group.is_(None),  # kiện chưa gắn đơn (outer join)
                    Order.platform_status_group.notin_((*SHIPPED_ORDER_GROUPS, *CANCEL_GROUPS)),
                ),
            )
        )
    ).all()
    return [
        Hit(
            pid,
            "PACKED_NOT_HANDED_OVER",
            {"warehouse_status": ws, "platform_status": ps, "since": _iso(at), "hours": _hours(p.now, at)},
            _iso(at) or "",
        )
        for pid, ws, ps, at in rows
    ]


async def return_done_not_received(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-19 (HIGH): yêu cầu trả nhóm sàn `DONE` (`REFUND_PAID`) mà kiện còn `RETURN_EXPECTED` / `MISSING`.
    Key = trạng thái sàn của hồ sơ."""
    rows = (
        await session.execute(
            select(
                Package.id,
                Package.warehouse_status,
                ReturnCase.platform_status,
                ReturnCase.code,
                ReturnCase.updated_at,
            )
            .join(ReturnCasePackage, ReturnCasePackage.package_id == Package.id)
            .join(ReturnCase, ReturnCase.id == ReturnCasePackage.return_case_id)
            .where(
                ReturnCase.platform_status_group == DONE_RETURN_GROUP,
                ReturnCase.status.notin_(("CANCELLED",)),
                ReturnCase.needs_parcel.is_not(False),  # G3 C1: chỉ hoàn tiền (không cần kiện về) không xét
                ReturnCase.kind != "REFUND_ONLY",
                ReturnCase.created_at >= p.recon_start_at,  # G3 C2: hồ sơ sau nâng cấp
                Package.warehouse_status.in_(("RETURN_EXPECTED", "RETURN_MISSING")),
            )
            .order_by(ReturnCase.created_at, ReturnCase.id)  # G3 R5
        )
    ).all()
    return [
        Hit(
            pid,
            "RETURN_DONE_NOT_RECEIVED",
            {"warehouse_status": ws, "platform_status": ps, "return_case": code, "since": _iso(at)},
            ps or "",
        )
        for pid, ws, ps, code, at in rows
    ]


async def unverified_stale(session: AsyncSession, p: Params) -> list[Hit]:
    """BR-20 (LOW): kiện chưa xác minh quá 24 giờ (không tính kiện tạm), tạo sau `recon_start_at`. Key = lúc
    tạo (một lần / kiện)."""
    rows = (
        await session.execute(
            select(Package.id, Package.warehouse_status, Package.created_at).where(
                Package.verified.is_(False),
                Package.is_placeholder.is_(False),
                # G3 R8: kiện đã ở trạng thái cuối (hủy / đã nhận hoàn) không còn gì để xác minh → tự đóng.
                Package.warehouse_status.notin_(("CANCELLED", "RETURN_RECEIVED_OK", "RETURN_RECEIVED_ISSUE")),
                Package.created_at < p.now - UNVERIFIED_AFTER,
                Package.created_at >= p.recon_start_at,
            )
        )
    ).all()
    return [
        Hit(
            pid,
            "UNVERIFIED_STALE",
            {"warehouse_status": ws, "since": _iso(at), "hours": _hours(p.now, at)},
            _iso(at) or "",
        )
        for pid, ws, at in rows
    ]


RULES: tuple[Callable[[AsyncSession, Params], Awaitable[list[Hit]]], ...] = (
    shipped_not_packed,
    cancelled_after_pack,
    return_overdue,
    return_unannounced,
    packed_not_handed_over,
    return_done_not_received,
    unverified_stale,
)
