"""Đơn, kiện, trạng thái kho (02a §5 "Chuyển warehouse_status", BR-01, BR-04, BR-17)."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.modules.media import jobs
from aicam.modules.orders.models import Order, OrderItem, Package, StatusHistory
from aicam.modules.platforms.base import CANCELLED_STATUSES, PlatformItem, PlatformOrder

_RETURN_OPENABLE = ("RETURN_EXPECTED", "RETURN_MISSING", "HANDED_OVER", "DELIVERED", "NEW")
_RETURN_RECEIVED = ("RETURN_RECEIVED_OK", "RETURN_RECEIVED_ISSUE")

# 01 §7 v0.3 (DEC-24) + Phase 2. Khóa: (từ, tới).
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
        # ---- Phase 2 (02 §5.3 v0.4, 01 §7.1)
        ("NEW", "HANDED_OVER"),  # điều chỉnh tay (L6)
        ("CANCELLED_AFTER_PACK", "HANDED_OVER"),  # điều chỉnh tay (DEC-258)
        ("PACKING", "CANCELLED_AFTER_PACK"),  # đóng phiên khi đơn đã hủy (BR-21)
        ("HANDED_OVER", "RETURN_EXPECTED"),  # giao thất bại / sàn hoàn về
        ("DELIVERED", "RETURN_EXPECTED"),  # yêu cầu trả có kiện về
        ("NEW", "RETURN_EXPECTED"),  # đơn trước khi dùng hệ thống (DEC-254)
        ("RETURN_EXPECTED", "DELIVERED"),  # sàn hủy yêu cầu / chỉnh tay / khách trả một phần
        ("RETURN_EXPECTED", "HANDED_OVER"),  # giao lại sau thất bại / khách trả một phần
        ("RETURN_MISSING", "DELIVERED"),  # chỉnh tay / khách trả một phần
        ("RETURN_MISSING", "HANDED_OVER"),  # khách trả một phần (DEC-271, R3-3)
        ("RETURN_EXPECTED", "RETURN_MISSING"),  # BR-12
        ("RETURN_MISSING", "RETURN_EXPECTED"),  # chỉnh tay gia hạn (DEC-255)
        *((src, "RETURN_INSPECTING") for src in _RETURN_OPENABLE),  # mở phiên hoàn
        *(
            (src, dst) for src in _RETURN_OPENABLE for dst in _RETURN_RECEIVED if src != "NEW"
        ),  # DEC-249, R2-9
        ("RETURN_INSPECTING", "RETURN_RECEIVED_OK"),
        ("RETURN_INSPECTING", "RETURN_RECEIVED_ISSUE"),
        *(("RETURN_INSPECTING", src) for src in _RETURN_OPENABLE),  # hủy / bỏ dở → trạng thái trước
        ("RETURN_RECEIVED_OK", "RETURN_RECEIVED_ISSUE"),  # sửa kết luận (API-113)
        ("RETURN_RECEIVED_ISSUE", "RETURN_RECEIVED_OK"),
    }
)

# Điều chỉnh tay API-122 (01 §7.1 "Điều chỉnh tay", FR-06.05): từ → các đích được phép.
MANUAL_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "NEW": ("HANDED_OVER",),
    "PACKED": ("HANDED_OVER",),
    "CANCELLED_AFTER_PACK": ("HANDED_OVER",),
    "HANDED_OVER": ("DELIVERED",),
    "RETURN_MISSING": ("RETURN_EXPECTED", "DELIVERED"),
    "RETURN_EXPECTED": ("DELIVERED",),
}


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
    now = clock.now()
    session.add(
        StatusHistory(
            package_id=package.id,
            source=source,
            from_status=package.warehouse_status,
            to_status=to_status,
            at=now,
            actor_user_id=actor_user_id,
            actor_label=actor_label,
        )
    )
    package.warehouse_status = to_status
    package.status_changed_at = now  # DEC-225: mốc cho BR-12, BR-14
    return True


async def find_package(session: AsyncSession, code: str, *, for_update: bool = False) -> Package | None:
    query = select(Package).where(func.upper(Package.tracking_number) == code.strip().upper())
    if for_update:
        query = query.with_for_update().execution_options(populate_existing=True)
    result: Package | None = await session.scalar(query)
    return result


async def lock_orders(session: AsyncSession, platform_order_sns: Sequence[str]) -> None:
    """Khóa advisory theo mã đơn sàn tới hết transaction (G3-F4).

    Mọi đường ghi đơn (J-04 / tra sàn khi quét / J-05 / nhập file) tuần tự hóa theo đơn: không nhân đôi
    `order_item`, không đụng unique khi cùng tạo đơn. Nhiều mã → khóa theo thứ tự sắp xếp để hai transaction
    không khóa chéo nhau."""
    for sn in sorted(set(platform_order_sns)):
        await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": f"order:{sn}"})


async def try_lock_order(session: AsyncSession, platform_order_sn: str) -> bool:
    """Khóa `order:{sn}` không chờ (G3 SM-F5 / SM-F6): dùng khi đã giữ khóa station / hồ sơ / kiện — đã
    giữ (cùng transaction, khóa advisory tái nhập) hoặc chưa ai giữ → True; transaction khác đang giữ → False
    (không chờ → không khóa chéo với đường lấy đơn trước)."""
    got = await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtext(:k))"), {"k": f"order:{platform_order_sn}"}
    )
    return bool(got)


class CsvWriteConflict(Exception):
    """Lúc ghi (sau khi khóa) dữ liệu đã khác bước phân loại: kiện đã thuộc đơn khác (G3-F5), hoặc đơn
    phân loại NEW vừa được nơi khác tạo."""


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
    """Sàn hủy đơn: kiện NEW → CANCELLED, PACKED → CANCELLED_AFTER_PACK (EX-P10); trạng thái khác giữ.

    Kiện `PACKING` (BR-21): không khóa kiện / phiên trong transaction đồng bộ — sau commit đẩy task
    `sessions.flag_order_cancelled` (transaction riêng: station → kiện, DEC-266)."""
    if package.warehouse_status == "PACKING":
        jobs.enqueue_flag_order_cancelled(session, package.id)
        return False
    target = {"NEW": "CANCELLED", "PACKED": "CANCELLED_AFTER_PACK"}.get(package.warehouse_status)
    if target is None:
        return False
    return await transition(session, package, target, source="PLATFORM", actor_label="Sàn")


def _item_key(sku: str | None, name: str, variation: str | None) -> tuple[str, str, str]:
    return ((sku or "").strip().upper(), name.strip().lower(), (variation or "").strip().lower())


async def sync_items(
    session: AsyncSession, order_id: uuid.UUID, items: Sequence[PlatformItem], *, with_image: bool
) -> None:
    """Ghi dòng đơn giữ nguyên `order_item.id` khi dòng không đổi (sku + tên + phân loại).

    Phase 2: dòng kiểm phiên hoàn (`inspection_line`) và yêu cầu trả (`requested_items`) trỏ `order_item_id`
    — xóa / tạo lại mọi dòng ở mỗi lần đồng bộ sẽ làm mất liên kết giữa phiên đang mở (DEC-307).
    """
    existing = list(
        (
            await session.scalars(
                select(OrderItem).where(OrderItem.order_id == order_id).order_by(OrderItem.id)
            )
        ).all()
    )
    unused = list(existing)
    for item in items:
        key = _item_key(item.sku, item.product_name, item.variation)
        row = next((r for r in unused if _item_key(r.sku, r.product_name, r.variation) == key), None)
        if row is None:
            session.add(
                OrderItem(
                    order_id=order_id, sku=item.sku, product_name=item.product_name, variation=item.variation,
                    quantity=item.quantity, image_url=item.image_url if with_image else None,
                )
            )  # fmt: skip
            continue
        unused.remove(row)
        row.quantity = item.quantity
        row.sku, row.product_name, row.variation = item.sku, item.product_name, item.variation
        if with_image:
            row.image_url = item.image_url
    if unused:
        await session.execute(delete(OrderItem).where(OrderItem.id.in_([r.id for r in unused])))


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
    Khóa theo mã đơn trước khi đọc (G3-F4).
    """
    await lock_orders(session, [data.platform_order_sn])
    order = await session.scalar(
        select(Order)
        .where(Order.platform_order_sn == data.platform_order_sn)
        .execution_options(populate_existing=True)
    )
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

    await sync_items(session, order.id, data.items, with_image=True)

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
        # G3 SM-F4: khóa kiện (theo id) + đọc lại trước khi quyết — quét PACK vừa chuyển NEW → PACKING thì
        # nhánh PACKING (đánh cờ phiên) chạy, không ghi đè PACKING bằng CANCELLED.
        ids = sorted({p.id for p in packages} | set(
            (await session.scalars(select(Package.id).where(Package.order_id == order.id))).all()
        ))  # fmt: skip
        locked = (
            await session.scalars(
                select(Package)
                .where(Package.id.in_(ids))
                .order_by(Package.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
        for package in locked:
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
    expect_new: bool = False,
) -> bool | None:
    """Ghi một đơn từ file nhập (API-51). Trả True = tạo mới, False = cập nhật đơn CSV, None = bỏ qua.

    BR-17: đơn nguồn API không bị file ghi đè. Kiện đã có giữ nguyên `warehouse_status`; kiện chưa xác minh
    (BR-04) được gắn vào đơn. Kiện đã thuộc đơn khác (đọc lại `FOR UPDATE` lúc ghi — G3-F5) hoặc đơn
    `expect_new` đã có → `CsvWriteConflict`: người gọi rollback cả lần nhập (409 `IMPORT_CONFLICT`).
    """
    await lock_orders(session, [data.platform_order_sn])
    order = await session.scalar(
        select(Order)
        .where(Order.platform_order_sn == data.platform_order_sn)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if order is not None and expect_new:
        raise CsvWriteConflict(f"Đơn {data.platform_order_sn} vừa được tạo trong lúc nhập")
    if order is not None and order.source == "API":
        return None
    created = order is None
    if order is None:
        order = Order(platform_order_sn=data.platform_order_sn, source="CSV", shop_id=shop_id)
        session.add(order)
    order.buyer_note = data.buyer_note
    order.csv_import_id = import_id
    await session.flush()
    await sync_items(session, order.id, data.items, with_image=False)
    for code in data.tracking_numbers:
        package = await find_package(session, code, for_update=True)
        if package is not None and package.order_id is not None and package.order_id != order.id:
            raise CsvWriteConflict(f"Mã vận đơn {code} đã thuộc đơn khác")
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
