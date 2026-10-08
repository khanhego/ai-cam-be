"""Đơn, kiện, trạng thái kho (02a §5 "Chuyển warehouse_status", BR-01, BR-04, BR-17)."""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

import structlog
from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.modules.media import jobs
from aicam.modules.orders.models import (
    Order,
    OrderItem,
    Package,
    PackageOrder,
    Shop,
    StatusHistory,
)
from aicam.modules.platforms.base import CANCEL_GROUPS, PlatformItem, PlatformOrder

log = structlog.get_logger()

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

# BR-21 v0.4 (DEC-519): chuyển ngược của kiện hủy oan — **chỉ** qua `cancel_revert.revert_cancel` (cờ
# `revert_cancel=True` của `transition`), không thuộc `ALLOWED_TRANSITIONS` chung.
REVERT_TRANSITIONS: frozenset[tuple[str, str]] = frozenset(
    {("CANCELLED", "NEW"), ("CANCELLED_AFTER_PACK", "PACKED")}
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
    revert_cancel: bool = False,
) -> bool:
    """Điểm duy nhất đổi `warehouse_status` (02a §5). Trả False nếu đã ở trạng thái đích.

    `revert_cancel` chỉ `cancel_revert.revert_cancel` truyền (guard 2 chuyển ngược — DEC-519)."""
    if package.warehouse_status == to_status:
        return False
    pair = (package.warehouse_status, to_status)
    if pair not in ALLOWED_TRANSITIONS and not (revert_cancel and pair in REVERT_TRANSITIONS):
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


async def shop_of(session: AsyncSession, order: Order) -> Shop | None:
    """Shop của đơn; None = đơn chưa gắn shop (đơn file — "Chưa rõ sàn", T-212)."""
    return await session.get(Shop, order.shop_id) if order.shop_id else None


async def platform_of(session: AsyncSession, order: Order) -> str | None:
    """Sàn của đơn theo shop (NFR-28: không gán cứng một sàn trong lõi); đơn file → None."""
    shop = await shop_of(session, order)
    return shop.platform if shop else None


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


def set_platform_status(order: Order, raw: str | None, group: str | None) -> tuple[str, str]:
    """**Nơi duy nhất** ghi `order.platform_status` + `platform_status_group` (BR-30, DEC-508 — test AST).
    Trả (nhóm cũ, nhóm mới); người gọi áp hệ quả qua `apply_status_effects` (BR-21)."""
    old = order.platform_status_group or "UNKNOWN"
    order.platform_status = raw
    order.platform_status_group = group or "UNKNOWN"
    return old, order.platform_status_group


async def apply_status_effects(
    session: AsyncSession,
    order: Order,
    old_group: str,
    extra_package_ids: Sequence[uuid.UUID] = (),
    *,
    only_package_ids: Sequence[uuid.UUID] | None = None,
) -> bool:
    """BR-21 làm rõ (DEC-494) theo nhóm **mới** của đơn (người gọi giữ khóa `order:{sn}`):

    - `CANCELLED` → luật hủy kiện như Phase 2 (`apply_platform_cancel`: NEW → CANCELLED, PACKED →
      CANCELLED_AFTER_PACK, PACKING → cờ phiên `ORDER_CANCELLED`). Đơn hủy thường không còn mã vận đơn trong
      dữ liệu sàn → xét mọi kiện đã gắn đơn (EX-P10); khóa kiện (id tăng) + đọc lại (G3 SM-F4).
    - `CANCEL_REQUESTED` (người mua xin hủy, Shopee `IN_CANCEL`) → **không** hủy kiện; kiện `PACKING` → cờ
      phiên `ORDER_CANCEL_REQUESTED` (task riêng, DEC-266); kiện NEW / PACKED giữ nguyên (quét mới bị BR-01
      chặn).
    - Rời `CANCEL_REQUESTED` sang nhóm ∉ {`CANCELLED`, `UNKNOWN`} → trả lại kiện hủy oan của đơn (lưới an toàn
      khi ops chưa chạy `aicam fix-cancel-requests` — T-285, DEC-519).

    Kiện gộp (đơn chính khác — `package_order`) không đổi theo đơn phụ. `only_package_ids` (J-06): chỉ xét
    các kiện này. Trả True nếu có kiện đổi trạng thái."""
    group = order.platform_status_group
    if old_group == "CANCEL_REQUESTED" and group not in ("CANCELLED", "UNKNOWN", "CANCEL_REQUESTED"):
        from aicam.modules.orders import cancel_revert  # cancel_revert → service: import muộn

        return await cancel_revert.revert_for_order(session, order)
    if group not in ("CANCELLED", "CANCEL_REQUESTED"):
        return False
    if only_package_ids is not None:
        ids = sorted(set(only_package_ids))
    else:
        ids = sorted(set(extra_package_ids) | set(
            (await session.scalars(select(Package.id).where(Package.order_id == order.id))).all()
        ))  # fmt: skip
    if not ids:
        return False
    locked = (
        await session.scalars(
            select(Package)
            .where(Package.id.in_(ids))
            .order_by(Package.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    changed = False
    for package in locked:
        if group == "CANCELLED":
            changed = await apply_platform_cancel(session, package) or changed
        elif package.warehouse_status == "PACKING":
            jobs.enqueue_flag_order_cancelled(session, package.id, kind="CANCEL_REQUESTED")
    return changed


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


PLATFORM_LABELS = {"SHOPEE": "Shopee", "TIKTOK": "TikTok Shop"}
SYNC_WARNINGS_MAX = 20  # 02a §3 `shop.sync_warnings` ≤ 20 phần tử (mới nhất trước)


async def add_sync_warning(session: AsyncSession, shop_id: uuid.UUID, entry: dict[str, object]) -> None:
    """Cảnh báo đồng bộ của shop (DEC-432 — không đặt `last_error`): mới nhất trước, ≤ 20, cùng (mã, mã vận
    đơn) chỉ giữ bản mới nhất (J-04 chạy 5 phút / lần không nhân bản)."""
    shop = await session.scalar(
        select(Shop).where(Shop.id == shop_id).with_for_update().execution_options(populate_existing=True)
    )
    if shop is None:
        return
    key = (entry.get("code"), entry.get("tracking_number"))
    rest = [w for w in shop.sync_warnings or [] if (w.get("code"), w.get("tracking_number")) != key]
    shop.sync_warnings = [entry, *rest][:SYNC_WARNINGS_MAX]


async def order_for_upsert(
    session: AsyncSession, platform_order_sn: str, shop_id: uuid.UUID | None
) -> Order | None:
    """BR-29: đơn mà dữ liệu sàn (shop, mã) sẽ ghi vào — (shop, mã), không có → đơn chưa gắn shop cùng mã
    (đơn file — sẽ được nhận). Người gọi giữ khóa `order:{sn}` khi định ghi."""
    if shop_id is not None:
        order: Order | None = await session.scalar(
            select(Order)
            .where(Order.shop_id == shop_id, Order.platform_order_sn == platform_order_sn)
            .execution_options(populate_existing=True)
        )
        if order is not None:
            return order
    found: Order | None = await session.scalar(
        select(Order)
        .where(Order.shop_id.is_(None), Order.platform_order_sn == platform_order_sn)
        .execution_options(populate_existing=True)
    )
    return found


async def _owner_conflict(
    session: AsyncSession, order: Order, package: Package, data: PlatformOrder, code: str
) -> str | None:
    """Kiện (đã khóa) đang thuộc đơn khác: `OTHER_SHOP` (EX-T2 — bỏ qua kiện), `MERGED` (kiện gộp cùng shop —
    FR-05.22), None = như Phase 1 (gắn kiện sang đơn này)."""
    owner = await session.get(Order, package.order_id) if package.order_id else None
    if owner is None:
        return None
    if owner.shop_id is not None and order.shop_id is not None and owner.shop_id != order.shop_id:
        other = await session.get(Shop, owner.shop_id)
        name = (other.name if other else None) or "khác"
        label = PLATFORM_LABELS.get(other.platform, other.platform) if other else "?"
        await add_sync_warning(
            session,
            order.shop_id,
            {
                "code": "TRACKING_OWNED_BY_OTHER_SHOP",
                "tracking_number": code.upper(),
                "message": f"Mã vận đơn {code.upper()} đã thuộc đơn của shop {name} ({label}).",
                "at": clock.iso_z(clock.now()),
            },
        )
        log.warning(
            "tracking_owned_by_other_shop", tracking_number=code.upper(), shop_id=str(order.shop_id),
            owner_shop_id=str(owner.shop_id), order_sn=data.platform_order_sn,
        )  # fmt: skip
        return "OTHER_SHOP"
    if owner.shop_id == order.shop_id and owner.platform_order_sn in data.merged_order_sns:
        return "MERGED"
    return None


async def upsert_platform_order(
    session: AsyncSession, data: PlatformOrder, *, shop_id: uuid.UUID | None = None
) -> UpsertResult:
    """Ghi đơn từ API sàn (source=API) của shop `shop_id` (BR-29). Khóa `order:{sn}` trước khi đọc (G3-F4,
    DEC-493 — hai shop cùng mã chia một khóa).

    - Tìm (shop, mã) → không có: đơn chưa gắn shop cùng mã (đơn file) → **nhận** (đặt shop, `source = API`,
      bản CSV cũ vào audit `ORDER_OVERWRITTEN_BY_API` — BR-17) → không có: tạo.
    - Mã vận đơn đã thuộc đơn của **shop khác** → bỏ qua kiện, cảnh báo `TRACKING_OWNED_BY_OTHER_SHOP` vào
      `shop.sync_warnings` (EX-T2); đơn cùng shop mà adapter đánh dấu gộp (`merged_order_sns`) →
      `package_order` (FR-05.22), kiện vẫn thuộc đơn chính; còn lại như Phase 1 (gắn kiện sang đơn này).
    - Đơn hủy trên sàn: kiện NEW → CANCELLED, kiện PACKED → CANCELLED_AFTER_PACK (EX-P10).
    """
    await lock_orders(session, [data.platform_order_sn])
    order = await order_for_upsert(session, data.platform_order_sn, shop_id)
    created = order is None
    if order is None:
        order = Order(platform_order_sn=data.platform_order_sn, source="API", shop_id=shop_id)
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
    old_group = order.platform_status_group if not created else "UNKNOWN"
    set_platform_status(order, data.status, data.status_group)
    order.buyer_note = data.buyer_note
    order.created_at_platform = data.created_at
    order.raw_payload = data.raw
    await session.flush()

    await sync_items(session, order.id, data.items, with_image=True)

    packages: list[Package] = []
    if data.tracking_numbers:
        # EX-T2 (02a §6): khóa kiện (id tăng) trước khi quyết gắn / bỏ qua.
        await session.execute(
            select(Package.id)
            .where(func.upper(Package.tracking_number).in_([c.upper() for c in data.tracking_numbers]))
            .order_by(Package.id)
            .with_for_update()
        )
    for code in data.tracking_numbers:
        package = await find_package(session, code, for_update=True)
        if package is None:
            package = Package(tracking_number=code.upper(), order_id=order.id)
            session.add(package)
            await session.flush()
        elif package.order_id is not None and package.order_id != order.id:
            conflict = await _owner_conflict(session, order, package, data, code)
            if conflict == "OTHER_SHOP":
                continue
            if conflict == "MERGED":
                await session.execute(
                    pg_insert(PackageOrder)
                    .values(package_id=package.id, order_id=order.id)
                    .on_conflict_do_nothing(index_elements=["package_id", "order_id"])
                )
            else:
                package.order_id = order.id
            package.verified = True
        else:
            package.order_id = order.id
            package.verified = True
        packages.append(package)
    await apply_status_effects(session, order, old_group, [p.id for p in packages if p.order_id == order.id])
    await session.flush()
    return UpsertResult(order=order, created=created, packages=packages)


async def merged_orders(session: AsyncSession, package_id: uuid.UUID) -> Sequence[Order]:
    """Đơn **thêm** của kiện gộp (FR-05.22), theo mã đơn."""
    return (
        await session.scalars(
            select(Order)
            .join(PackageOrder, PackageOrder.order_id == Order.id)
            .where(PackageOrder.package_id == package_id)
            .order_by(Order.platform_order_sn)
        )
    ).all()


async def orders_by_sn(session: AsyncSession, sns: Sequence[str]) -> dict[str, Order]:
    """Đơn **chưa gắn shop** (đơn file, đơn API cũ không shop) theo mã — §5.1 #3 (BR-29: mã chỉ unique trong
    `shop_id IS NULL`). Đơn của shop xem `api_orders_by_sn`."""
    if not sns:
        return {}
    rows = (
        await session.scalars(
            select(Order).where(Order.platform_order_sn.in_(list(sns)), Order.shop_id.is_(None))
        )
    ).all()
    return {o.platform_order_sn: o for o in rows}


async def api_orders_by_sn(session: AsyncSession, sns: Sequence[str]) -> dict[str, list[Order]]:
    """Đơn **của shop** theo mã (có thể nhiều shop cùng mã — BR-29)."""
    if not sns:
        return {}
    rows = (
        await session.scalars(
            select(Order)
            .where(Order.platform_order_sn.in_(list(sns)), Order.shop_id.is_not(None))
            .order_by(Order.platform_order_sn, Order.id)
        )
    ).all()
    out: dict[str, list[Order]] = {}
    for o in rows:
        out.setdefault(o.platform_order_sn, []).append(o)
    return out


async def find_orders_by_sn(session: AsyncSession, code: str) -> list[Order]:
    """Mọi đơn mang mã (mọi shop + đơn file) — nơi không biết shop (bàn hoàn §5.1 #10)."""
    rows = (
        await session.scalars(select(Order).where(Order.platform_order_sn == code).order_by(Order.id))
    ).all()
    return list(rows)


@dataclass(frozen=True)
class PackageOwner:
    """Kiện theo mã vận đơn + đơn đang gắn (§5.1 #5: câu lỗi nêu đúng shop)."""

    package: Package
    order_sn: str | None
    order_id: uuid.UUID | None
    shop_name: str | None
    platform: str | None


async def packages_by_code(session: AsyncSession, codes: Sequence[str]) -> dict[str, PackageOwner]:
    """Mã vận đơn (upper) → kiện + đơn đang gắn (mã, id, tên shop / sàn) hoặc None."""
    if not codes:
        return {}
    rows = (
        await session.execute(
            select(Package, Order.platform_order_sn, Order.id, Shop.name, Shop.platform)
            .outerjoin(Order, Order.id == Package.order_id)
            .outerjoin(Shop, Shop.id == Order.shop_id)
            .where(func.upper(Package.tracking_number).in_([c.upper() for c in codes]))
        )
    ).all()
    return {
        p.tracking_number.upper(): PackageOwner(p, sn, oid, name, plat) for p, sn, oid, name, plat in rows
    }


async def api_order_owning_all(session: AsyncSession, sn: str, codes: Sequence[str]) -> Order | None:
    """§5.1 #3 / #4 (BR-17): đơn của một shop mang mã `sn` mà **mọi** mã vận đơn của nhóm dòng file đã thuộc
    đơn đó → nhập file bỏ qua (`SKIP`); None nếu không có."""
    wanted = {c.upper() for c in codes}
    for order in (await api_orders_by_sn(session, [sn])).get(sn, []):
        owned = set(
            (
                await session.scalars(
                    select(func.upper(Package.tracking_number)).where(Package.order_id == order.id)
                )
            ).all()
        )
        if wanted and wanted <= owned:
            return order
    return None


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
    expect_new: bool | None = None,
) -> bool | None:
    """Ghi một đơn từ file nhập (API-51). Trả True = tạo mới, False = cập nhật đơn CSV, None = bỏ qua.

    `expect_new`: True = bản xem trước phân loại NEW, False = UPDATE (đơn file biến mất → shop vừa nhận →
    bỏ qua), None = không ràng buộc.

    BR-17: đơn nguồn API không bị file ghi đè. Kiện đã có giữ nguyên `warehouse_status`; kiện chưa xác minh
    (BR-04) được gắn vào đơn. Kiện đã thuộc đơn khác (đọc lại `FOR UPDATE` lúc ghi — G3-F5) hoặc đơn
    `expect_new` đã có → `CsvWriteConflict`: người gọi rollback cả lần nhập (409 `IMPORT_CONFLICT`).
    """
    await lock_orders(session, [data.platform_order_sn])
    # §5.1 #4 (BR-29): file chỉ ghi đơn **chưa gắn shop**; kiểm lại điều kiện SKIP của #3 dưới khóa.
    if await api_order_owning_all(session, data.platform_order_sn, data.tracking_numbers) is not None:
        return None
    order = await session.scalar(
        select(Order)
        .where(Order.platform_order_sn == data.platform_order_sn, Order.shop_id.is_(None))
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if order is not None and expect_new:
        raise CsvWriteConflict(f"Đơn {data.platform_order_sn} vừa được tạo trong lúc nhập")
    if order is None and expect_new is False:
        return None  # đơn file vừa được shop "nhận" (J-04 — BR-29) giữa xem trước và nhập: không ghi đè
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
    return order is not None and order.platform_status_group in CANCEL_GROUPS
