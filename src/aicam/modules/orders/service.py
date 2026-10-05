"""Đơn, kiện, trạng thái kho (02a §5 "Chuyển warehouse_status", BR-01, BR-04, BR-17)."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.modules.orders.models import Order, OrderItem, Package, StatusHistory
from aicam.modules.platforms.base import CANCELLED_STATUSES, PlatformItem, PlatformOrder

# 01 §7 v0.3 (DEC-24). Khóa: (từ, tới).
ALLOWED_TRANSITIONS: frozenset[tuple[str, str]] = frozenset(
    {
        ("NEW", "PACKING"),  # quét mở phiên
        ("PACKING", "PACKED"),  # đóng phiên hợp lệ / hủy phiên đóng gói lại
        ("PACKING", "NEW"),  # hủy / bỏ dở phiên lần đầu
        ("PACKED", "PACKING"),  # Supervisor duyệt đóng gói lại (BR-03)
        ("PACKED", "HANDED_OVER"),  # sàn xác nhận đã lấy hàng
        ("PACKED", "CANCELLED_AFTER_PACK"),  # sàn hủy sau khi đóng
        ("NEW", "CANCELLED"),  # sàn hủy trước khi đóng
        ("HANDED_OVER", "DELIVERED"),  # sàn báo giao thành công
    }
)


class InvalidTransition(Exception):
    def __init__(self, from_status: str, to_status: str) -> None:
        super().__init__(f"Không chuyển được kiện {from_status} → {to_status}")
        self.from_status = from_status
        self.to_status = to_status


async def transition(
    session: AsyncSession,
    package: Package,
    to_status: str,
    *,
    source: str,
    actor_user_id: uuid.UUID | None = None,
    actor_label: str | None = None,
) -> bool:
    """Điểm duy nhất đổi `warehouse_status` (02a §5). Trả False nếu đã ở trạng thái đích."""
    if package.warehouse_status == to_status:
        return False
    if (package.warehouse_status, to_status) not in ALLOWED_TRANSITIONS:
        raise InvalidTransition(package.warehouse_status, to_status)
    session.add(
        StatusHistory(
            package_id=package.id,
            source=source,
            from_status=package.warehouse_status,
            to_status=to_status,
            at=clock.now(),
            actor_user_id=actor_user_id,
            actor_label=actor_label,
        )
    )
    package.warehouse_status = to_status
    return True


async def find_package(session: AsyncSession, code: str) -> Package | None:
    result: Package | None = await session.scalar(
        select(Package).where(func.upper(Package.tracking_number) == code.strip().upper())
    )
    return result


async def get_order(session: AsyncSession, order_id: uuid.UUID) -> Order | None:
    return await session.get(Order, order_id)


async def items_of(session: AsyncSession, order_id: uuid.UUID) -> Sequence[OrderItem]:
    return (
        await session.scalars(select(OrderItem).where(OrderItem.order_id == order_id).order_by(OrderItem.id))
    ).all()


async def create_unverified_package(session: AsyncSession, code: str) -> Package:
    """BR-04: mã không có trong hệ thống và tra sàn thất bại → kiện chưa xác minh, không gắn đơn."""
    package = Package(tracking_number=code.strip().upper(), verified=False)
    session.add(package)
    await session.flush()
    return package


async def apply_platform_cancel(session: AsyncSession, package: Package) -> bool:
    """Sàn hủy đơn: kiện NEW → CANCELLED, PACKED → CANCELLED_AFTER_PACK (EX-P10); trạng thái khác giữ."""
    target = {"NEW": "CANCELLED", "PACKED": "CANCELLED_AFTER_PACK"}.get(package.warehouse_status)
    if target is None:
        return False
    return await transition(session, package, target, source="PLATFORM", actor_label="Sàn")


@dataclass
class UpsertResult:
    order: Order
    created: bool
    packages: list[Package]


def _csv_snapshot(order: Order, items: Sequence[OrderItem]) -> dict[str, object]:
    return {
        "platform_order_sn": order.platform_order_sn,
        "buyer_note": order.buyer_note,
        "items": [
            {"sku": i.sku, "product_name": i.product_name, "variation": i.variation, "quantity": i.quantity}
            for i in items
        ],
    }


async def upsert_platform_order(
    session: AsyncSession, data: PlatformOrder, *, shop_id: uuid.UUID | None = None
) -> UpsertResult:
    """Ghi đơn từ API sàn (source=API). Đơn nguồn CSV bị ghi đè, giữ bản cũ trong audit (BR-17, FR-05.10).

    Đơn hủy trên sàn: kiện NEW → CANCELLED, kiện PACKED → CANCELLED_AFTER_PACK (EX-P10).
    """
    order = await session.scalar(select(Order).where(Order.platform_order_sn == data.platform_order_sn))
    created = order is None
    if order is None:
        order = Order(platform_order_sn=data.platform_order_sn, source="API")
        session.add(order)
    elif order.source == "CSV":
        audit.record(
            session,
            "ORDER_OVERWRITTEN_BY_API",
            user_id=None,
            object_type="ORDER",
            object_id=order.id,
            data=_csv_snapshot(order, await items_of(session, order.id)),
        )
        order.source = "API"
    order.shop_id = shop_id or order.shop_id
    order.platform_status = data.status
    order.buyer_note = data.buyer_note
    order.created_at_platform = data.created_at
    order.raw_payload = data.raw
    await session.flush()

    await session.execute(delete(OrderItem).where(OrderItem.order_id == order.id))
    for item in data.items:
        session.add(
            OrderItem(
                order_id=order.id,
                sku=item.sku,
                product_name=item.product_name,
                variation=item.variation,
                quantity=item.quantity,
                image_url=item.image_url,
            )
        )

    packages: list[Package] = []
    for code in data.tracking_numbers:
        package = await find_package(session, code)
        if package is None:
            package = Package(tracking_number=code.upper(), order_id=order.id)
            session.add(package)
            await session.flush()
        else:
            package.order_id = order.id
            package.verified = True
        packages.append(package)
    if data.is_cancelled:
        # Đơn hủy trên sàn thường không còn mã vận đơn trong dữ liệu sàn → xét mọi kiện đã gắn đơn (EX-P10).
        linked = (await session.scalars(select(Package).where(Package.order_id == order.id))).all()
        for package in {p.id: p for p in [*packages, *linked]}.values():
            await apply_platform_cancel(session, package)
    await session.flush()
    return UpsertResult(order=order, created=created, packages=packages)


async def orders_by_sn(session: AsyncSession, sns: Sequence[str]) -> dict[str, Order]:
    if not sns:
        return {}
    rows = (await session.scalars(select(Order).where(Order.platform_order_sn.in_(list(sns))))).all()
    return {o.platform_order_sn: o for o in rows}


async def packages_by_code(
    session: AsyncSession, codes: Sequence[str]
) -> dict[str, tuple[Package, str | None]]:
    """Mã vận đơn (upper) → (kiện, mã đơn sàn đang gắn hoặc None)."""
    if not codes:
        return {}
    rows = (
        await session.execute(
            select(Package, Order.platform_order_sn)
            .outerjoin(Order, Order.id == Package.order_id)
            .where(func.upper(Package.tracking_number).in_([c.upper() for c in codes]))
        )
    ).all()
    return {p.tracking_number.upper(): (p, sn) for p, sn in rows}


@dataclass(frozen=True)
class CsvOrder:
    platform_order_sn: str
    buyer_note: str | None
    items: tuple[PlatformItem, ...]
    tracking_numbers: tuple[str, ...]


async def apply_csv_order(
    session: AsyncSession,
    data: CsvOrder,
    *,
    import_id: uuid.UUID,
    shop_id: uuid.UUID | None,
    actor_user_id: uuid.UUID,
) -> bool | None:
    """Ghi một đơn từ file nhập (API-51). Trả True = tạo mới, False = cập nhật đơn CSV, None = bỏ qua.

    BR-17: đơn nguồn API không bị file ghi đè. Kiện đã có giữ nguyên `warehouse_status`; kiện chưa xác minh
    (BR-04) được gắn vào đơn.
    """
    order = await session.scalar(
        select(Order).where(Order.platform_order_sn == data.platform_order_sn).with_for_update()
    )
    if order is not None and order.source == "API":
        return None
    created = order is None
    if order is None:
        order = Order(platform_order_sn=data.platform_order_sn, source="CSV", shop_id=shop_id)
        session.add(order)
    order.buyer_note = data.buyer_note
    order.csv_import_id = import_id
    await session.flush()
    await session.execute(delete(OrderItem).where(OrderItem.order_id == order.id))
    for item in data.items:
        session.add(
            OrderItem(
                order_id=order.id,
                sku=item.sku,
                product_name=item.product_name,
                variation=item.variation,
                quantity=item.quantity,
            )
        )
    for code in data.tracking_numbers:
        package = await find_package(session, code)
        if package is None:
            package = Package(tracking_number=code.upper(), order_id=order.id)
            session.add(package)
            await session.flush()
            session.add(
                StatusHistory(
                    package_id=package.id,
                    source="MANUAL",
                    from_status=None,
                    to_status="NEW",
                    at=clock.now(),
                    actor_user_id=actor_user_id,
                    actor_label="Nhập đơn từ file",
                )
            )
        else:
            package.order_id = order.id
            package.verified = True
    await session.flush()
    return created


async def is_cancelled(session: AsyncSession, package: Package) -> bool:
    """BR-01: đơn hủy / đang hủy trên sàn, hoặc kiện đã ở trạng thái hủy."""
    if package.warehouse_status in ("CANCELLED", "CANCELLED_AFTER_PACK"):
        return True
    if package.order_id is None:
        return False
    order = await session.get(Order, package.order_id)
    return order is not None and order.platform_status in CANCELLED_STATUSES
