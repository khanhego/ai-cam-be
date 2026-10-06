"""Hồ sơ hàng hoàn lõi (02a §2 `returns`, §4.1, §5 BR-23, BR-24, "Gắn tín hiệu hoàn"; DEC-248, 265..271).

- `attach_or_create(order, signal)`: một cửa cho mọi tín hiệu hoàn (J-04 / J-06 / J-13 / quét ở bàn hoàn).
- `resolve_code(code)`: tra mã quét ở bàn hoàn theo 3 nguồn, thứ tự cứng (DEC-202, DEC-229).
- `create_unidentified(code)`: hồ sơ "Chưa xác định" + kiện tạm `TAM-…` (02 §6.4 #2, §6.5 #9).
- `merge_unidentified_by_code(order)`: gộp hồ sơ chưa xác định khi đơn của mã quét xuất hiện (DEC-269).
- `recompute(case)`: trạng thái hồ sơ theo phiên / kiện (BR-24).

Thứ tự khóa DEC-266: `order:{sn}` → station → `return_case` (FOR UPDATE, id tăng) → `package` → clip.
Hàm ở đây không lấy khóa station; người gọi giữ station thì phải lấy `order:{sn}` **trước** đó.
"""

import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit
from aicam.modules.claims import service as claims
from aicam.modules.claims.models import Claim
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, OrderItem, Package, StatusHistory
from aicam.modules.platforms.base import PlatformReturn
from aicam.modules.returns.models import (
    OPEN_CASE_STATUSES,
    PLACEHOLDER_CODE_SEQ,
    ReturnCase,
    ReturnCasePackage,
)
from aicam.modules.sessions.models import ACTIVE_STATUSES, PackSession

log = structlog.get_logger()

# Tín hiệu gắn hồ sơ (02a §5 "Gắn tín hiệu hoàn").
SIGNAL_PLATFORM_RETURN = "PLATFORM_RETURN"
SIGNAL_FAILED_DELIVERY = "FAILED_DELIVERY"
SIGNAL_WAREHOUSE_SCAN = "WAREHOUSE_SCAN"

# Ưu tiên `kind` khi gắn tín hiệu vào hồ sơ đang mở (02 §6.3 #6).
_KIND_RANK = {"UNIDENTIFIED": 0, "UNANNOUNCED": 1, "FAILED_DELIVERY": 2, "BUYER_RETURN": 3}
RECEIVED_STATUSES = ("RETURN_RECEIVED_OK", "RETURN_RECEIVED_ISSUE")
RECEIVED_CASE_STATUSES = ("RECEIVED_OK", "RECEIVED_ISSUE")
# Kiện mở được phiên hoàn (02 §5.3) — `NEW` chỉ khi đơn sàn đã giao / đang hoàn (EX-R3).
OPENABLE_STATUSES = ("RETURN_EXPECTED", "RETURN_MISSING", "HANDED_OVER", "DELIVERED")
SHIPPED_PLATFORM_STATUSES = frozenset({"SHIPPED", "TO_CONFIRM_RECEIVE", "COMPLETED", "TO_RETURN"})
# Kiện được gắn vào hồ sơ khi có tín hiệu sàn (đã rời kho hoặc đang trong luồng hoàn).
_LINKABLE_STATUSES = (
    "NEW",
    "HANDED_OVER",
    "DELIVERED",
    "RETURN_EXPECTED",
    "RETURN_MISSING",
    "RETURN_INSPECTING",
)
# Kiện chuyển `→ RETURN_EXPECTED` khi sàn báo có kiện về (02 §5.3, DEC-254).
_TO_EXPECTED_FROM = ("NEW", "HANDED_OVER", "DELIVERED")
_AWAY_STATUSES = ("HANDED_OVER", "DELIVERED", "RETURN_EXPECTED", "RETURN_MISSING")
RECENT_RECEIVED_WINDOW = timedelta(days=30)  # DEC-267 (b)
PARTIAL_RETURN_LABEL = "Khách trả một phần"

# Kiện còn "thuộc" hồ sơ đang chờ / đang kiểm / đã nhận — hồ sơ không được hủy khi còn kiện như vậy.
_ACTIVE_PACKAGE_STATUSES = (
    "RETURN_EXPECTED",
    "RETURN_MISSING",
    "RETURN_INSPECTING",
    "RETURN_RECEIVED_OK",
    "RETURN_RECEIVED_ISSUE",
)


# ---------------------------------------------------------------- đọc / khóa


async def open_case_ids_of_package(session: AsyncSession, package_id: uuid.UUID) -> list[uuid.UUID]:
    """Đọc không khóa (bước trước khi khóa theo thứ tự DEC-266)."""
    rows = await session.scalars(
        select(ReturnCase.id)
        .join(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
        .where(ReturnCasePackage.package_id == package_id, ReturnCase.status.in_(OPEN_CASE_STATUSES))
        .order_by(ReturnCase.id)
    )
    return list(rows.all())


async def lock_cases(session: AsyncSession, case_ids: Sequence[uuid.UUID]) -> list[ReturnCase]:
    if not case_ids:
        return []
    rows = await session.scalars(
        select(ReturnCase)
        .where(ReturnCase.id.in_(list(case_ids)))
        .order_by(ReturnCase.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


async def lock_case(session: AsyncSession, case_id: uuid.UUID) -> ReturnCase | None:
    cases = await lock_cases(session, [case_id])
    return cases[0] if cases else None


async def lock_packages(session: AsyncSession, package_ids: Iterable[uuid.UUID]) -> list[Package]:
    """Khóa kiện theo id tăng dần rồi đọc lại (DEC-266)."""
    ids = sorted(set(package_ids))
    if not ids:
        return []
    rows = await session.scalars(
        select(Package)
        .where(Package.id.in_(ids))
        .order_by(Package.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


async def packages_of_case(session: AsyncSession, case_id: uuid.UUID) -> list[Package]:
    rows = await session.scalars(
        select(Package)
        .join(ReturnCasePackage, ReturnCasePackage.package_id == Package.id)
        .where(ReturnCasePackage.return_case_id == case_id)
        .order_by(Package.tracking_number, Package.id)
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


async def packages_of_order(session: AsyncSession, order_id: uuid.UUID) -> list[Package]:
    rows = await session.scalars(
        select(Package).where(Package.order_id == order_id).order_by(Package.tracking_number, Package.id)
    )
    return list(rows.all())


async def open_case_of_order(
    session: AsyncSession, order_id: uuid.UUID, *, for_update: bool = False
) -> ReturnCase | None:
    query = select(ReturnCase).where(
        ReturnCase.order_id == order_id, ReturnCase.status.in_(OPEN_CASE_STATUSES)
    )
    if for_update:
        query = query.with_for_update().execution_options(populate_existing=True)
    result: ReturnCase | None = await session.scalar(query)
    return result


async def return_sessions_of_case(session: AsyncSession, case_id: uuid.UUID) -> list[PackSession]:
    rows = await session.scalars(
        select(PackSession)
        .where(PackSession.return_case_id == case_id, PackSession.type == "RETURN")
        .order_by(PackSession.started_at, PackSession.id)
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


async def link_package(session: AsyncSession, case: ReturnCase, package_id: uuid.UUID) -> bool:
    exists = await session.scalar(
        select(ReturnCasePackage.package_id).where(
            ReturnCasePackage.return_case_id == case.id, ReturnCasePackage.package_id == package_id
        )
    )
    if exists is not None:
        return False
    session.add(ReturnCasePackage(return_case_id=case.id, package_id=package_id))
    await session.flush()
    return True


async def unlink_package(session: AsyncSession, case_id: uuid.UUID, package_id: uuid.UUID) -> None:
    await session.execute(
        delete(ReturnCasePackage).where(
            ReturnCasePackage.return_case_id == case_id, ReturnCasePackage.package_id == package_id
        )
    )


async def cancel_if_no_active_package(session: AsyncSession, case: ReturnCase) -> bool:
    """API-122 `RETURN_* → DELIVERED` (02a §4): hồ sơ mở → `CANCELLED` khi không còn kiện nào của hồ sơ
    đang chờ / đang kiểm / đã nhận (DEC-303). Còn kiện như vậy → giữ, `recompute` xử lý."""
    if case.status not in OPEN_CASE_STATUSES:
        return False
    remaining = await session.scalar(
        select(Package.id)
        .join(ReturnCasePackage, ReturnCasePackage.package_id == Package.id)
        .where(
            ReturnCasePackage.return_case_id == case.id,
            Package.warehouse_status.in_(_ACTIVE_PACKAGE_STATUSES),
        )
        .limit(1)
    )
    if remaining is not None:
        return False
    case.status = "CANCELLED"
    return True


# ---------------------------------------------------------------- hồ sơ một phiên (BR-24, DEC-265)


async def order_package_count(session: AsyncSession, order_id: uuid.UUID | None) -> int:
    if order_id is None:
        return 1
    return int(
        await session.scalar(
            select(func.count()).where(Package.order_id == order_id, Package.is_placeholder.is_(False))
        )
        or 0
    )


async def is_single_session(session: AsyncSession, case: ReturnCase) -> bool:
    """`BUYER_RETURN` hoặc đơn có đúng 1 kiện (DEC-265). Hồ sơ chưa xác định: một kiện tạm → một phiên."""
    if case.single_session is not None:
        return case.single_session
    return case.kind == "BUYER_RETURN" or await order_package_count(session, case.order_id) <= 1


async def covers_whole_order(session: AsyncSession, case: ReturnCase) -> bool:
    """Yêu cầu trả bao trọn mọi dòng × số lượng của đơn (DEC-271). Không có dòng yêu cầu → không bao trọn,
    trừ đơn một kiện (khi đó mọi kiện của đơn chính là kiện của phiên)."""
    if case.order_id is None:
        return False
    items = (await session.scalars(select(OrderItem).where(OrderItem.order_id == case.order_id))).all()
    if not items:
        return False
    requested: dict[str, int] = {}
    for line in case.requested_items or []:
        key = line.get("order_item_id")
        if key:
            requested[str(key)] = requested.get(str(key), 0) + int(line.get("quantity") or 0)
    return all(requested.get(str(i.id), 0) >= i.quantity for i in items)


# ---------------------------------------------------------------- ghép dòng yêu cầu trả


def match_requested_items(ret: PlatformReturn, items: Sequence[OrderItem]) -> list[dict[str, Any]]:
    """Dòng yêu cầu trả → `order_item` theo `sku`, rồi tên + phân loại (02a §7 "Ghép dòng").

    Không khớp → `order_item_id = null` (dòng "Không ghép được", không chặn — RB-22).
    """
    used: set[uuid.UUID] = set()
    out: list[dict[str, Any]] = []
    for line in ret.items:
        match = next((i for i in items if line.sku and i.sku == line.sku and i.id not in used), None)
        if match is None:
            match = next(
                (
                    i
                    for i in items
                    if i.id not in used
                    and line.product_name
                    and i.product_name.strip().lower() == line.product_name.strip().lower()
                    and (i.variation or "").strip().lower() == (line.variation or "").strip().lower()
                ),
                None,
            )
        if match is not None:
            used.add(match.id)
        out.append(
            {
                "order_item_id": str(match.id) if match else None,
                "product_name": match.product_name if match else (line.product_name or "Không ghép được"),
                "variation": match.variation if match else line.variation,
                "quantity": line.quantity,
            }
        )
    return out


async def _apply_platform_fields(session: AsyncSession, case: ReturnCase, ret: PlatformReturn) -> None:
    case.platform_return_sn = ret.return_sn
    case.platform_status = ret.status
    case.needs_parcel = ret.needs_parcel
    case.return_tracking_number = ret.return_tracking_number or case.return_tracking_number
    case.reason = ret.reason
    case.reason_text = ret.reason_text
    case.seller_due_at = ret.seller_due_at
    case.reported_at = case.reported_at or ret.created_at or clock.now()
    case.raw_payload = ret.raw
    if case.order_id is not None and ret.items:
        items = (
            await session.scalars(
                select(OrderItem).where(OrderItem.order_id == case.order_id).order_by(OrderItem.id)
            )
        ).all()
        case.requested_items = match_requested_items(ret, items)


def _upgrade_kind(case: ReturnCase, kind: str) -> None:
    if _KIND_RANK.get(kind, -1) > _KIND_RANK.get(case.kind, -1):
        case.kind = kind


def _add_key(case: ReturnCase, key: str | None) -> None:
    if key and key not in (case.signal_keys or []):
        case.signal_keys = [*(case.signal_keys or []), key]


# ---------------------------------------------------------------- attach_or_create (DEC-248, DEC-267)


@dataclass
class Signal:
    kind: str  # SIGNAL_*
    key: str | None = None  # `RETURN:{return_sn}` | `FAILED:{order_sn}:{logistics_update_time}`
    ret: PlatformReturn | None = None
    # Quét ở bàn hoàn: chỉ gắn kiện đang cầm; tín hiệu sàn: None = mọi kiện đủ điều kiện của đơn.
    package_ids: tuple[uuid.UUID, ...] | None = None


@dataclass
class AttachResult:
    case: ReturnCase | None
    created: bool = False
    changed: bool = False
    moved_packages: list[uuid.UUID] = field(default_factory=list)


def _new_kind(signal: Signal) -> str:
    if signal.kind == SIGNAL_PLATFORM_RETURN:
        return "BUYER_RETURN" if signal.ret is None or signal.ret.needs_parcel else "REFUND_ONLY"
    if signal.kind == SIGNAL_FAILED_DELIVERY:
        return "FAILED_DELIVERY"
    return "UNANNOUNCED"


async def _case_with_key(session: AsyncSession, order_id: uuid.UUID, key: str) -> ReturnCase | None:
    result: ReturnCase | None = await session.scalar(
        select(ReturnCase)
        .where(
            ReturnCase.order_id == order_id,
            ReturnCase.status != "CANCELLED",
            ReturnCase.signal_keys.contains([key]),
        )
        .limit(1)
    )
    return result


async def _recent_received_case(session: AsyncSession, order_id: uuid.UUID) -> ReturnCase | None:
    """(b) DEC-267 / R3-6: hồ sơ do kho tạo đã nhận, chưa có mã sàn, trong 30 ngày (không `CANCELLED`)."""
    found: ReturnCase | None = await session.scalar(
        select(ReturnCase)
        .where(
            ReturnCase.order_id == order_id,
            ReturnCase.kind.in_(("UNANNOUNCED", "FAILED_DELIVERY")),
            ReturnCase.platform_return_sn.is_(None),
            ReturnCase.created_at > clock.now() - RECENT_RECEIVED_WINDOW,
            ReturnCase.status.in_(RECEIVED_CASE_STATUSES),
        )
        .order_by(ReturnCase.created_at.desc())
        .limit(1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return found


async def _link_and_move(
    session: AsyncSession, case: ReturnCase, order: Order, signal: Signal, actor_label: str
) -> list[uuid.UUID]:
    """Gắn kiện vào hồ sơ; tín hiệu sàn có kiện về → kiện `→ RETURN_EXPECTED` (khóa + kiểm lại — R3-4)."""
    if signal.package_ids is not None:
        candidates = list(signal.package_ids)
    else:
        candidates = [
            p.id
            for p in await packages_of_order(session, order.id)
            if p.warehouse_status in _LINKABLE_STATUSES
        ]
    moved: list[uuid.UUID] = []
    move = signal.kind != SIGNAL_WAREHOUSE_SCAN and case.status in OPEN_CASE_STATUSES
    for package in await lock_packages(session, candidates):
        await link_package(session, case, package.id)
        if move and package.warehouse_status in _TO_EXPECTED_FROM:
            await orders.transition(
                session, package, "RETURN_EXPECTED", source="PLATFORM", actor_label=actor_label
            )
            moved.append(package.id)
    return moved


async def attach_or_create(
    session: AsyncSession, order: Order, signal: Signal, *, actor_label: str = "Sàn"
) -> AttachResult:
    """Gắn tín hiệu hoàn vào hồ sơ của đơn hoặc tạo hồ sơ mới (02a §5, DEC-248, DEC-267, R3-6, R3-7).

    Khóa `order:{sn}` (đầu tiên — người gọi đang giữ station phải lấy khóa này trước station).
    """
    await orders.lock_orders(session, [order.platform_order_sn])
    if signal.ret is not None:
        # Mã yêu cầu sàn là định danh (unique): đã có hồ sơ (kể cả đã hủy) → không gắn lại; cập nhật
        # trạng thái sàn của hồ sơ đó là việc của J-13 `upsert_from_platform` (T-105).
        known: ReturnCase | None = await session.scalar(
            select(ReturnCase).where(ReturnCase.platform_return_sn == signal.ret.return_sn)
        )
        if known is not None:
            return AttachResult(known)
    if signal.key:
        existing = await _case_with_key(session, order.id, signal.key)
        if existing is not None:
            return AttachResult(existing)
    if signal.kind == SIGNAL_FAILED_DELIVERY:
        packages = await packages_of_order(session, order.id)
        if packages and all(p.warehouse_status in RECEIVED_STATUSES for p in packages):
            return AttachResult(None)  # tín hiệu giao thất bại muộn của đơn đã nhận đủ (DEC-267)

    if signal.kind == SIGNAL_PLATFORM_RETURN and signal.ret is not None and not signal.ret.needs_parcel:
        return await _create(session, order, signal, actor_label)  # chỉ hoàn tiền: hồ sơ riêng `NO_PARCEL`

    case = await open_case_of_order(session, order.id, for_update=True)
    if case is not None:  # (a)
        _upgrade_kind(case, _new_kind(signal))
        _add_key(case, signal.key)
        if signal.ret is not None:
            await _apply_platform_fields(session, case, signal.ret)
        moved = await _link_and_move(session, case, order, signal, actor_label)
        if moved and case.expected_since is None:
            case.expected_since = clock.now()
        await session.flush()
        return AttachResult(case, changed=True, moved_packages=moved)

    if signal.kind != SIGNAL_WAREHOUSE_SCAN:  # (b)
        received = await _recent_received_case(session, order.id)
        if received is not None and received.merged_into_id is not None:
            received = await lock_case(session, received.merged_into_id)
        if received is not None and received.status != "CANCELLED":
            _upgrade_kind(received, _new_kind(signal))
            _add_key(received, signal.key)
            if signal.ret is not None:
                await _apply_platform_fields(session, received, signal.ret)
            await session.flush()
            return AttachResult(received, changed=True)
    return await _create(session, order, signal, actor_label)  # (c)


async def _create(session: AsyncSession, order: Order, signal: Signal, actor_label: str) -> AttachResult:
    kind = _new_kind(signal)
    now = clock.now()
    case = ReturnCase(
        order_id=order.id,
        kind=kind,
        status="NO_PARCEL" if kind == "REFUND_ONLY" else "EXPECTED",
        source="WAREHOUSE" if signal.kind == SIGNAL_WAREHOUSE_SCAN else "PLATFORM",
        signal_keys=[signal.key] if signal.key else [],
        requested_items=[],
        reported_at=None if signal.kind == SIGNAL_WAREHOUSE_SCAN else now,
        expected_since=None if signal.kind == SIGNAL_WAREHOUSE_SCAN or kind == "REFUND_ONLY" else now,
    )
    if signal.ret is not None:
        await _apply_platform_fields(session, case, signal.ret)
    session.add(case)
    await session.flush()
    moved = await _link_and_move(session, case, order, signal, actor_label)
    if signal.kind == SIGNAL_WAREHOUSE_SCAN:
        # Về trước khi sàn báo, đơn > 1 kiện: kiện khác đã rời kho cũng thuộc lần hoàn này (hồ sơ "nhận
        # một phần" tới khi đủ — TC-04.50); không đổi trạng thái kiện (DEC-308).
        for package in await packages_of_order(session, order.id):
            if package.warehouse_status in _AWAY_STATUSES:
                await link_package(session, case, package.id)
    await session.flush()
    log.info("return_case_created", return_case_id=str(case.id), kind=kind, source=case.source)
    return AttachResult(case, created=True, changed=True, moved_packages=moved)


# ---------------------------------------------------------------- tra mã ở bàn hoàn (DEC-202, DEC-229)


@dataclass
class Resolution:
    """`FOUND` (có `package`), `MULTIPLE` (đơn > 1 kiện, chưa có hồ sơ chỉ ra kiện), `NOT_FOUND`."""

    status: str
    package: Package | None = None
    case: ReturnCase | None = None
    order: Order | None = None


async def _first_unreceived(session: AsyncSession, case: ReturnCase) -> Package | None:
    """Kiện đầu tiên của hồ sơ chưa có phiên RETURN `COMPLETED` (không có → kiện đầu tiên)."""
    packages = await packages_of_case(session, case.id)
    if not packages:
        return None
    done = set(
        (
            await session.scalars(
                select(PackSession.package_id).where(
                    PackSession.return_case_id == case.id,
                    PackSession.type == "RETURN",
                    PackSession.status == "COMPLETED",
                )
            )
        ).all()
    )
    return next((p for p in packages if p.id not in done), packages[0])


async def _order_of(session: AsyncSession, package: Package | None) -> Order | None:
    if package is None or package.order_id is None:
        return None
    return await session.get(Order, package.order_id)


def _openable_by_status(package: Package, order: Order | None, has_open_case: bool) -> bool:
    if package.warehouse_status in OPENABLE_STATUSES:
        return True
    return package.warehouse_status == "NEW" and (
        has_open_case or (order is not None and (order.platform_status or "") in SHIPPED_PLATFORM_STATUSES)
    )


async def resolve_code(session: AsyncSession, code: str) -> Resolution:
    """Tra mã quét ở bàn hoàn (02a §4.1) — đọc, không khóa; người gọi kiểm lại dưới khóa."""
    code = code.strip().upper()
    # 1. Mã vận đơn chiều về (ưu tiên hồ sơ chưa kết thúc).
    case = await session.scalar(
        select(ReturnCase)
        .where(func.upper(ReturnCase.return_tracking_number) == code)
        .order_by(ReturnCase.status.in_(OPEN_CASE_STATUSES).desc(), ReturnCase.created_at.desc())
        .limit(1)
    )
    if case is not None and case.merged_into_id is not None:
        case = await session.get(ReturnCase, case.merged_into_id) or case
    if case is not None:
        package = await _first_unreceived(session, case)
        if package is not None:
            return Resolution("FOUND", package, case, await _order_of(session, package))

    # 2. Mã vận đơn gốc.
    package = await orders.find_package(session, code)
    if package is not None:
        case_ids = await open_case_ids_of_package(session, package.id)
        found_case = await session.get(ReturnCase, case_ids[0]) if case_ids else None
        return Resolution("FOUND", package, found_case, await _order_of(session, package))

    # 3. Mã đơn sàn / mã yêu cầu trả của sàn.
    order = await session.scalar(select(Order).where(Order.platform_order_sn == code))
    if order is None:
        by_return_sn = await session.scalar(select(ReturnCase).where(ReturnCase.platform_return_sn == code))
        if by_return_sn is not None and by_return_sn.order_id is not None:
            order = await session.get(Order, by_return_sn.order_id)
    if order is None:
        return Resolution("NOT_FOUND")
    open_case = await open_case_of_order(session, order.id)
    if open_case is not None:
        package = await _first_unreceived(session, open_case)
        if package is not None:
            return Resolution("FOUND", package, open_case, order)
    packages = [p for p in await packages_of_order(session, order.id) if not p.is_placeholder]
    eligible = [p for p in packages if _openable_by_status(p, order, False)]
    if len(eligible) == 1:
        return Resolution("FOUND", eligible[0], None, order)
    if len(eligible) > 1:
        return Resolution("MULTIPLE", None, None, order)
    if packages:  # không kiện nào mở được: trả kiện đầu để báo đúng lý do (đã nhận / chưa gửi đi)
        return Resolution("FOUND", packages[0], None, order)
    return Resolution("NOT_FOUND", order=order)


# ---------------------------------------------------------------- hồ sơ chưa xác định (EX-R12)


async def create_placeholder_package(session: AsyncSession) -> Package:
    """Kiện tạm `TAM-` + 6 số (02 §6.5 #9) — không chiếm mã thật; mã quét lưu ở `session.open_code`."""
    seq = await session.scalar(select(PLACEHOLDER_CODE_SEQ.next_value()))
    package = Package(tracking_number=f"TAM-{int(seq or 0):06d}", verified=False, is_placeholder=True)
    session.add(package)
    await session.flush()
    return package


async def create_unidentified(
    session: AsyncSession, *, force_note: str | None = None, manual_link_only: bool = False
) -> tuple[ReturnCase, Package]:
    package = await create_placeholder_package(session)
    case = ReturnCase(
        order_id=None,
        kind="UNIDENTIFIED",
        status="EXPECTED",
        source="WAREHOUSE",
        signal_keys=[],
        requested_items=[],
        single_session=True,
        manual_link_only=manual_link_only,
        force_note=force_note,
    )
    session.add(case)
    await session.flush()
    await link_package(session, case, package.id)
    return case, package


# ---------------------------------------------------------------- tính lại hồ sơ (BR-24)


@dataclass
class _SessionView:
    status: str
    conclusion: str | None
    package_id: uuid.UUID


def _summary_conclusion(conclusions: Sequence[str | None]) -> str | None:
    done = [c for c in conclusions if c]
    if not done:
        return None
    return next((c for c in done if c != "OK"), "OK")


async def recompute(session: AsyncSession, case: ReturnCase) -> bool:
    """BR-24: trạng thái hồ sơ theo phiên RETURN + kiện. Người gọi đã khóa hồ sơ. Trả True nếu đổi.

    - Có phiên đang hoạt động → `INSPECTING`.
    - Một phiên (`single_session`): có phiên `COMPLETED` → `RECEIVED_*` theo kết luận phiên đó.
    - Nhiều phiên: mọi kiện đã nhận → `RECEIVED_*` (ISSUE nếu có kiện ISSUE); một phần → `PARTIALLY_RECEIVED`.
    - Chưa nhận gì: `MISSING` nếu mọi kiện chưa nhận đều quá hạn, ngược lại `EXPECTED`.
    `CANCELLED` / `NO_PARCEL` giữ nguyên.
    """
    if case.status in ("CANCELLED", "NO_PARCEL"):
        return False
    before = (case.status, case.conclusion, case.received_at)
    views = [
        _SessionView(s.status, s.inspection_conclusion, s.package_id)
        for s in await return_sessions_of_case(session, case.id)
    ]
    packages = await packages_of_case(session, case.id)
    completed = [v for v in views if v.status == "COMPLETED"]
    if any(v.status in ACTIVE_STATUSES for v in views):
        case.status = "INSPECTING"
    elif await is_single_session(session, case) and completed:
        case.status = "RECEIVED_OK" if completed[-1].conclusion == "OK" else "RECEIVED_ISSUE"
        case.conclusion = completed[-1].conclusion
    else:
        received = [p for p in packages if p.warehouse_status in RECEIVED_STATUSES]
        pending = [p for p in packages if p.warehouse_status not in RECEIVED_STATUSES]
        if packages and not pending:
            issue = any(p.warehouse_status == "RETURN_RECEIVED_ISSUE" for p in received)
            case.status = "RECEIVED_ISSUE" if issue else "RECEIVED_OK"
            case.conclusion = _summary_conclusion([v.conclusion for v in completed]) or (
                "OK" if not issue else None
            )
        elif received:
            case.status = "PARTIALLY_RECEIVED"
        elif pending and all(p.warehouse_status == "RETURN_MISSING" for p in pending):
            case.status = "MISSING"
        else:
            case.status = "EXPECTED"
    if case.status in RECEIVED_CASE_STATUSES and case.received_at is None:
        case.received_at = clock.now()
    if case.status not in RECEIVED_CASE_STATUSES:
        case.received_at = None
        case.conclusion = None
    return before != (case.status, case.conclusion, case.received_at)


# ---------------------------------------------------------------- đóng phiên: chuyển kiện của hồ sơ (BR-24)


def received_status(conclusion: str) -> str:
    return "RETURN_RECEIVED_OK" if conclusion == "OK" else "RETURN_RECEIVED_ISSUE"


async def _previous_status(session: AsyncSession, package: Package) -> str:
    """Trạng thái trước khi vào luồng hoàn (`DELIVERED` / `HANDED_OVER`) — cho kiện tách khỏi hồ sơ (R3-3)."""
    last = await session.scalar(
        select(StatusHistory.from_status)
        .where(StatusHistory.package_id == package.id, StatusHistory.to_status == "RETURN_EXPECTED")
        .order_by(StatusHistory.at.desc())
        .limit(1)
    )
    return "HANDED_OVER" if last == "HANDED_OVER" else "DELIVERED"


async def apply_close_to_packages(
    session: AsyncSession,
    case: ReturnCase,
    session_package_id: uuid.UUID,
    conclusion: str,
    *,
    source: str,
    actor_label: str,
    actor_user_id: uuid.UUID | None = None,
) -> list[uuid.UUID]:
    """Đóng phiên của hồ sơ **một phiên** (BR-24, DEC-249, DEC-271, R3-3). Người gọi giữ khóa hồ sơ.

    Yêu cầu trả bao trọn đơn → mọi kiện của hồ sơ `RETURN_RECEIVED_*`; ngược lại chỉ kiện của phiên, kiện khác
    rời hồ sơ và về trạng thái trước khi vào hồ sơ (nguồn WAREHOUSE, "Khách trả một phần").
    Trả id kiện đã đổi trạng thái (không gồm kiện của phiên — người gọi tự chuyển).
    """
    if not await is_single_session(session, case):
        return []
    whole = await covers_whole_order(session, case)
    others = [p for p in await packages_of_case(session, case.id) if p.id != session_package_id]
    changed: list[uuid.UUID] = []
    target = received_status(conclusion)
    for package in await lock_packages(session, [p.id for p in others]):
        if package.warehouse_status not in (*OPENABLE_STATUSES, *RECEIVED_STATUSES):
            continue  # kiện đang kiểm ở nơi khác / trạng thái lạ: không đụng
        if whole:
            if await orders.transition(
                session, package, target, source=source, actor_label=actor_label, actor_user_id=actor_user_id
            ):
                changed.append(package.id)
        elif package.warehouse_status in ("RETURN_EXPECTED", "RETURN_MISSING"):
            await unlink_package(session, case.id, package.id)
            await orders.transition(
                session, package, await _previous_status(session, package), source="WAREHOUSE",
                actor_label=PARTIAL_RETURN_LABEL,
            )  # fmt: skip
            changed.append(package.id)
    return changed


# ---------------------------------------------------------------- gộp hồ sơ chưa xác định (DEC-269, R3-2)


async def _unidentified_candidates(
    session: AsyncSession, codes: Sequence[str]
) -> list[tuple[ReturnCase, str]]:
    upper = sorted({c.strip().upper() for c in codes if c})
    if not upper:
        return []
    rows = (
        await session.execute(
            select(ReturnCase, PackSession.open_code)
            .join(PackSession, PackSession.return_case_id == ReturnCase.id)
            .where(
                ReturnCase.kind == "UNIDENTIFIED",
                ReturnCase.manual_link_only.is_(False),
                ReturnCase.status != "CANCELLED",
                ReturnCase.order_id.is_(None),
                func.upper(PackSession.open_code).in_(upper),
            )
            .order_by(ReturnCase.id)
        )
    ).all()
    seen: dict[uuid.UUID, tuple[ReturnCase, str]] = {}
    for case, code in rows:
        seen.setdefault(case.id, (case, code.upper()))
    return list(seen.values())


async def merge_unidentified_by_code(session: AsyncSession, order: Order) -> list[uuid.UUID]:
    """Sau mỗi upsert đơn (J-04 / J-05 / tra sàn khi quét / API-112): hồ sơ `UNIDENTIFIED` có `open_code` là
    mã vận đơn của đơn → gộp (DEC-269). Còn phiên hoạt động → `pending_merge_order_id`, gộp lúc đóng (R3-2).

    Người gọi đã giữ (hoặc được phép lấy) `order:{sn}`; không giữ station. Trả id hồ sơ đã gộp ngay.
    """
    codes = [p.tracking_number for p in await packages_of_order(session, order.id)]
    merged: list[uuid.UUID] = []
    for candidate, code in await _unidentified_candidates(session, codes):
        await orders.lock_orders(session, [order.platform_order_sn])
        case = await lock_case(session, candidate.id)
        if case is None or case.status == "CANCELLED" or case.order_id is not None:
            continue
        sessions = await return_sessions_of_case(session, case.id)
        if any(s.status in ACTIVE_STATUSES for s in sessions):
            case.pending_merge_order_id = order.id
            continue
        if await merge_unidentified(session, case, order, code):
            merged.append(case.id)
    return merged


async def merge_unidentified(
    session: AsyncSession,
    case: ReturnCase,
    order: Order,
    code: str,
    *,
    actor_label: str = "Hệ thống",
    actor_user_id: uuid.UUID | None = None,
    merged_claims: list[claims.MergedClaim] | None = None,
) -> bool:
    """Gộp hồ sơ chưa xác định (đã khóa, mọi phiên đã kết thúc) vào đơn: phiên sang kiện thật, kiện tạm xóa.

    Đơn có hồ sơ mở → gộp vào đó (`merged_into_id`, hồ sơ cũ `CANCELLED`); không → hồ sơ gắn đơn,
    `kind = UNANNOUNCED`. Kiện thật `→ RETURN_INSPECTING → RETURN_RECEIVED_*` theo kết luận (2 bước).
    Hồ sơ khiếu nại của kiện tạm chuyển sang kiện thật (trùng BR-27 → gộp — DEC-311); `merged_claims` nhận
    các cặp đã gộp (API-112).
    """
    target = next(
        (p for p in await packages_of_order(session, order.id) if p.tracking_number.upper() == code.upper()),
        None,
    )
    if target is None:
        return False
    sessions = await return_sessions_of_case(session, case.id)
    completed = [s for s in sessions if s.status == "COMPLETED"]
    open_case = await open_case_of_order(session, order.id, for_update=True)
    placeholders = [p for p in await packages_of_case(session, case.id) if p.is_placeholder]
    locked = await lock_packages(session, [target.id, *(p.id for p in placeholders)])
    target = next(p for p in locked if p.id == target.id)
    already = await session.scalar(
        select(PackSession.id).where(
            PackSession.package_id == target.id,
            PackSession.type == "RETURN",
            PackSession.status == "COMPLETED",
        )
    )
    if already is not None or (
        completed and not (_openable_by_status(target, order, open_case is not None)
                           or target.warehouse_status in RECEIVED_STATUSES)
    ):  # fmt: skip
        log.info("unidentified_merge_skipped", return_case_id=str(case.id), package_id=str(target.id))
        return False

    destination = open_case or case
    session_ids = [s.id for s in sessions]
    if session_ids:
        await session.execute(
            update(PackSession)
            .where(PackSession.id.in_(session_ids))
            .values(package_id=target.id, return_case_id=destination.id)
            .execution_options(synchronize_session=False)
        )
    for placeholder in placeholders:
        await unlink_package(session, case.id, placeholder.id)
    if open_case is not None:
        case.status = "CANCELLED"
        case.merged_into_id = open_case.id
    else:
        case.order_id = order.id
        case.kind = "UNANNOUNCED"
        case.single_session = await order_package_count(session, order.id) <= 1
    case.pending_merge_order_id = None
    await link_package(session, destination, target.id)
    if completed:
        conclusion = completed[-1].inspection_conclusion or "OTHER"
        if target.warehouse_status not in RECEIVED_STATUSES:
            await orders.transition(
                session, target, "RETURN_INSPECTING", source="WAREHOUSE", actor_label=actor_label
            )
            await orders.transition(
                session, target, received_status(conclusion), source="WAREHOUSE", actor_label=actor_label
            )
        if open_case is not None:
            await apply_close_to_packages(
                session, open_case, target.id, conclusion, source="WAREHOUSE", actor_label=actor_label
            )
    await session.flush()
    moved = await claims.move_claims_to_package(
        session, [p.id for p in placeholders], target, return_case_id=destination.id, actor=actor_user_id
    )
    if destination is not case:
        await session.execute(
            update(Claim).where(Claim.return_case_id == case.id).values(return_case_id=destination.id)
        )
    if merged_claims is not None:
        merged_claims.extend(moved)
    for placeholder in placeholders:
        still_used = await session.scalar(
            select(PackSession.id).where(PackSession.package_id == placeholder.id)
        )
        if still_used is None:
            await session.delete(placeholder)
    await recompute(session, destination)
    audit.record(
        session, "RETURN_CASE_MERGED", user_id=None, object_type="RETURN_CASE", object_id=case.id,
        data={"into": str(destination.id), "order_sn": order.platform_order_sn, "code": code,
              "package_id": str(target.id)},
    )  # fmt: skip
    await session.flush()
    notify_updated(session, case)
    if destination is not case:
        notify_updated(session, destination)
    log.info("unidentified_merged", return_case_id=str(case.id), into=str(destination.id))
    return True


# ---------------------------------------------------------------- mã đóng phiên (BR-23)


async def accepted_codes(session: AsyncSession, case: ReturnCase, open_code: str) -> list[str]:
    """Mã đóng được phiên của hồ sơ: mã gốc mọi kiện + mã chiều về + mã đơn + mã đã mở phiên.

    Hồ sơ chưa xác định: chỉ mã đã mở phiên (kiện tạm `TAM-…` không in trên kiện thật)."""
    codes: list[str] = [open_code.upper()]
    if case.kind == "UNIDENTIFIED" and case.order_id is None:
        return codes
    for package in await packages_of_case(session, case.id):
        if not package.is_placeholder:
            codes.append(package.tracking_number.upper())
    if case.return_tracking_number:
        codes.append(case.return_tracking_number.upper())
    if case.order_id is not None:
        order = await session.get(Order, case.order_id)
        if order is not None:
            codes.append(order.platform_order_sn.upper())
    return list(dict.fromkeys(codes))


# ---------------------------------------------------------------- realtime


def notify_updated(session: AsyncSession, case: ReturnCase) -> None:
    """WS-02 `return.updated` sau commit (D14, D4, D2)."""
    from aicam.realtime import publish

    data = {"return_case_id": str(case.id), "status": case.status}

    async def _send() -> None:
        await publish.to_dashboard("return.updated", data)

    after_commit(session, _send)
