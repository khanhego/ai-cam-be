"""Đơn, kiện, trạng thái kho (02a §5 "Chuyển warehouse_status", BR-01, BR-04, BR-17)."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.modules.orders.models import Order, OrderItem, Package, StatusHistory
from aicam.modules.platforms.base import CANCELLED_STATUSES, PlatformOrder

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
        if data.is_cancelled:
            if package.warehouse_status == "NEW":
                await transition(session, package, "CANCELLED", source="PLATFORM", actor_label="Sàn")
            elif package.warehouse_status == "PACKED":
                await transition(
                    session, package, "CANCELLED_AFTER_PACK", source="PLATFORM", actor_label="Sàn"
                )
        packages.append(package)
    await session.flush()
    return UpsertResult(order=order, created=created, packages=packages)


async def is_cancelled(session: AsyncSession, package: Package) -> bool:
    """BR-01: đơn hủy / đang hủy trên sàn, hoặc kiện đã ở trạng thái hủy."""
    if package.warehouse_status in ("CANCELLED", "CANCELLED_AFTER_PACK"):
        return True
    if package.order_id is None:
        return False
    order = await session.get(Order, package.order_id)
    return order is not None and order.platform_status in CANCELLED_STATUSES
