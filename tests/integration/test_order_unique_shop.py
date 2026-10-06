"""T-204 — đa shop: unique (shop, mã đơn), nhận đơn file, EX-T2, kiện gộp, J-04 / J-05 / J-06 theo shop.

BR-29, FR-05.14, FR-05.22, 02a §5.1 #1, #2, #6. Adapter mock / adapter giả ghi lời gọi — **chưa test với sàn
thật (thiếu tài khoản đối tác)**.
"""

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.imports.models import CsvImport
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, PackageOrder, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import (
    PlatformItem,
    PlatformOrder,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group

from .factories import make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)
SN = "2410DUP0001"


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True


def _order(
    sn: str = SN, *codes: str, status: str = "READY_TO_SHIP", merged: tuple[str, ...] = ()
) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=sn,
        status=status,
        tracking_numbers=codes,
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L"),),
        created_at=NOW,
        updated_at=NOW,
        status_group=shopee_order_group(status),
        merged_order_sns=merged,
    )


async def _shop(
    db: AsyncSession, settings: Settings, ext_id: str, name: str, platform: str = "SHOPEE"
) -> Shop:
    shop = Shop(platform=platform, platform_shop_id=ext_id, name=name)
    platforms.store_credentials(
        shop, ShopCredentials(ext_id, f"acc-{ext_id}", f"ref-{ext_id}", NOW + timedelta(hours=4)),
        Cipher(settings.fernet_key),
    )  # fmt: skip
    db.add(shop)
    await db.flush()
    return shop


async def _orders(db: AsyncSession, sn: str = SN) -> list[Order]:
    return list((await db.scalars(select(Order).where(Order.platform_order_sn == sn))).all())


async def _package(db: AsyncSession, code: str) -> Package:
    package = await orders.find_package(db, code)
    assert package is not None
    await db.refresh(package)
    return package


# ---------------------------------------------------------------- §5.1 #2 upsert


async def test_two_shops_same_order_sn_two_orders(db: AsyncSession, test_settings: Settings) -> None:
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh", "TIKTOK")

    ra = await orders.upsert_platform_order(db, _order(SN, "SPXDUPA1"), shop_id=a.id)
    rb = await orders.upsert_platform_order(db, _order(SN, "SPXDUPB1"), shop_id=b.id)
    again = await orders.upsert_platform_order(db, _order(SN, "SPXDUPA1"), shop_id=a.id)

    assert (ra.created, rb.created, again.created) == (True, True, False)
    assert ra.order.id != rb.order.id
    assert again.order.id == ra.order.id
    assert {(o.shop_id, o.platform_order_sn) for o in await _orders(db)} == {(a.id, SN), (b.id, SN)}
    assert (await _package(db, "SPXDUPA1")).order_id == ra.order.id
    assert (await _package(db, "SPXDUPB1")).order_id == rb.order.id


async def test_file_order_claimed_by_first_shop_then_other_shop_creates(
    db: AsyncSession, test_settings: Settings
) -> None:
    """BR-29 + BR-17: đơn file được shop đầu tiên đồng bộ thấy nhận; shop thứ hai tạo đơn mới."""
    user = await make_user(db, "imp204", "ADMIN")
    imp = CsvImport(
        file_name="a.csv", created_by=user.id, expires_at=NOW + timedelta(minutes=30), status="COMMITTED"
    )
    db.add(imp)
    await db.flush()
    await orders.apply_csv_order(
        db, orders.CsvOrder(SN, "ghi chú file", _order().items, ("SPXDUPF1",)),
        import_id=imp.id, shop_id=None, actor_user_id=user.id,
    )  # fmt: skip
    file_order = (await _orders(db))[0]
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh")

    ra = await orders.upsert_platform_order(db, _order(SN, "SPXDUPF1"), shop_id=a.id)
    rb = await orders.upsert_platform_order(db, _order(SN, "SPXDUPB1"), shop_id=b.id)

    assert (ra.order.id, ra.created) == (file_order.id, False)
    assert (ra.order.shop_id, ra.order.source) == (a.id, "API")
    assert (rb.created, rb.order.shop_id) == (True, b.id)
    audits = (await db.scalars(select(AuditLog).where(AuditLog.action == "ORDER_OVERWRITTEN_BY_API"))).all()
    assert [x.object_id for x in audits] == [str(file_order.id)]


async def test_tracking_owned_by_other_shop_is_skipped_with_warning(
    db: AsyncSession, test_settings: Settings
) -> None:
    """EX-T2: mã vận đơn đã thuộc đơn shop A, shop B trả cùng mã → không ghi đè, cảnh báo của shop B."""
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh", "TIKTOK")
    ra = await orders.upsert_platform_order(db, _order("2410A", "812345678901"), shop_id=a.id)

    for _ in range(2):  # J-04 chạy lại không nhân bản cảnh báo
        rb = await orders.upsert_platform_order(db, _order("5761B", "812345678901"), shop_id=b.id)
        assert rb.packages == []

    assert (await _package(db, "812345678901")).order_id == ra.order.id
    await db.refresh(b)
    await db.refresh(a)
    assert a.sync_warnings == []
    assert b.last_error is None  # DEC-432: không làm shop "lỗi"
    assert b.sync_warnings == [
        {
            "code": "TRACKING_OWNED_BY_OTHER_SHOP",
            "tracking_number": "812345678901",
            "message": "Mã vận đơn 812345678901 đã thuộc đơn của shop Áo Đẹp (Shopee).",
            "at": clock.iso_z(NOW),
        }
    ]


async def test_sync_warnings_capped_at_20(db: AsyncSession, test_settings: Settings) -> None:
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh")
    for i in range(23):
        await orders.upsert_platform_order(db, _order(f"A{i}", f"SPXCAP{i:03d}"), shop_id=a.id)
        await orders.upsert_platform_order(db, _order(f"B{i}", f"SPXCAP{i:03d}"), shop_id=b.id)
    await db.refresh(b)
    assert len(b.sync_warnings) == 20
    assert b.sync_warnings[0]["tracking_number"] == "SPXCAP022"  # mới nhất trước


async def test_merged_package_same_shop(db: AsyncSession, test_settings: Settings) -> None:
    """FR-05.22: đánh dấu gộp → `package_order`, kiện vẫn thuộc đơn chính; đơn phụ hủy không hủy kiện."""
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    main = await orders.upsert_platform_order(db, _order("X1", "SPXMRG1"), shop_id=a.id)
    extra = await orders.upsert_platform_order(db, _order("X2", "SPXMRG1", merged=("X1",)), shop_id=a.id)
    await orders.upsert_platform_order(db, _order("X2", "SPXMRG1", merged=("X1",)), shop_id=a.id)  # lặp lại

    package = await _package(db, "SPXMRG1")
    assert package.order_id == main.order.id
    assert [o.id for o in await orders.merged_orders(db, package.id)] == [extra.order.id]
    assert [p.id for p in extra.packages] == [package.id]

    await orders.upsert_platform_order(
        db, _order("X2", "SPXMRG1", status="CANCELLED", merged=("X1",)), shop_id=a.id
    )
    assert (await _package(db, "SPXMRG1")).warehouse_status == "NEW"


async def test_same_shop_without_merge_flag_moves_package_like_phase1(
    db: AsyncSession, test_settings: Settings
) -> None:
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    await orders.upsert_platform_order(db, _order("Y1", "SPXMOV1"), shop_id=a.id)
    moved = await orders.upsert_platform_order(db, _order("Y2", "SPXMOV1"), shop_id=a.id)
    assert (await _package(db, "SPXMOV1")).order_id == moved.order.id
    assert await db.scalar(select(func.count()).select_from(PackageOrder)) == 0


# ---------------------------------------------------------------- §5.1 #6 J-04 theo đơn của shop


async def test_j04_cancel_of_shop_b_does_not_touch_shop_a_packages(
    db: AsyncSession, test_settings: Settings
) -> None:
    """Shop B hủy đơn trùng mã với A: chỉ kiện của đơn B đổi; kiện A `PACKED` giữ nguyên."""
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh")
    ra = await orders.upsert_platform_order(db, _order(SN, "SPXJ4A"), shop_id=a.id)
    pa = ra.packages[0]
    await orders.transition(db, pa, "PACKING", source="WAREHOUSE")
    await orders.transition(db, pa, "PACKED", source="WAREHOUSE")
    await orders.upsert_platform_order(db, _order(SN, "SPXJ4B"), shop_id=b.id)
    await db.flush()

    changed = await sync._upsert(db, _order(SN, "SPXJ4B", status="CANCELLED"), b.id)

    assert changed is True  # kiện B NEW → CANCELLED
    assert (await _package(db, "SPXJ4A")).warehouse_status == "PACKED"
    assert (await _package(db, "SPXJ4B")).warehouse_status == "CANCELLED"
    assert await sync._upsert(db, _order(SN, "SPXJ4A"), a.id) is False


class _PerShopAdapter(MockAdapter):
    """Mock trả dữ liệu theo token của shop; ghi lại token của mọi lời gọi."""

    def __init__(self, by_token: dict[str, dict[str, PlatformOrder]]) -> None:
        super().__init__()
        self.by_token = by_token
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    async def find_by_tracking(self, creds: Any, tracking_number: str) -> PlatformOrder | None:
        token = creds.access_token if creds else "-"
        self.calls.append((token, (tracking_number,)))
        return self.by_token.get(token, {}).get(tracking_number.upper())

    async def get_shipping_statuses(self, creds: Any, refs: Sequence[ShipmentRef]) -> list[ShippingStatus]:
        token = creds.access_token if creds else "-"
        self.calls.append((token, tuple(r.tracking_number for r in refs)))
        return [ShippingStatus(r.tracking_number, "PICKED_UP", "HANDED_OVER", "SHIPPED", NOW, "SHIPPED")
                for r in refs]  # fmt: skip


async def test_j05_verifies_into_shop_that_found_it(db: AsyncSession, test_settings: Settings) -> None:
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh")
    for code in ("SPXV1", "SPXV2"):
        await orders.create_unverified_package(db, code)
    await db.flush()
    adapter = _PerShopAdapter(
        {"acc-1002": {"SPXV1": _order(SN, "SPXV1")}, "acc-1001": {"SPXV2": _order("OTHER", "SPXV2")}}
    )

    out = await sync.verify_unverified(db, adapter, test_settings)

    assert out == {"checked": 2, "verified": 2}
    p1, p2 = await _package(db, "SPXV1"), await _package(db, "SPXV2")
    assert (await db.get(Order, p1.order_id)).shop_id == b.id  # type: ignore[union-attr]
    assert (await db.get(Order, p2.order_id)).shop_id == a.id  # type: ignore[union-attr]


async def test_j05_ambiguous_two_shops_keeps_unverified(db: AsyncSession, test_settings: Settings) -> None:
    await _shop(db, test_settings, "1001", "Áo Đẹp")
    await _shop(db, test_settings, "1002", "Quần Xinh")
    await orders.create_unverified_package(db, "SPXAMB")
    await db.flush()
    found = _order(SN, "SPXAMB")
    adapter = _PerShopAdapter({"acc-1001": {"SPXAMB": found}, "acc-1002": {"SPXAMB": found}})

    assert await sync.verify_unverified(db, adapter, test_settings) == {"checked": 1, "verified": 0}
    assert (await _package(db, "SPXAMB")).order_id is None


async def test_j06_queries_each_package_with_its_own_shop_token(
    db: AsyncSession, test_settings: Settings
) -> None:
    """DEC-509: không gọi sàn bằng token shop khác; shop đã ngắt → không tra kiện của nó."""
    a = await _shop(db, test_settings, "1001", "Áo Đẹp")
    b = await _shop(db, test_settings, "1002", "Quần Xinh")
    c = await _shop(db, test_settings, "1003", "Đã ngắt")
    for shop, code in ((a, "SPXJ6A"), (b, "SPXJ6B"), (c, "SPXJ6C")):
        package = (await orders.upsert_platform_order(db, _order(SN, code), shop_id=shop.id)).packages[0]
        await orders.transition(db, package, "PACKING", source="WAREHOUSE")
        await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    c.auth_status = "DISCONNECTED"
    await db.flush()
    adapter = _PerShopAdapter({})

    out = await sync.sync_shipping_status(db, adapter, test_settings)

    assert sorted(adapter.calls) == [("acc-1001", ("SPXJ6A",)), ("acc-1002", ("SPXJ6B",))]
    assert out == {"checked": 2, "changed": 2}
    assert (await _package(db, "SPXJ6C")).warehouse_status == "PACKED"


# ---------------------------------------------------------------- §5.1 #1 hai shop ghi cùng mã song song


@pytest.fixture
async def committed(migrated_database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text('TRUNCATE status_history, order_item, package, "order", shop CASCADE'))
    await dispose_engine()


async def test_two_shops_write_same_sn_concurrently(committed: AsyncEngine, test_settings: Settings) -> None:
    """Khóa `order:{sn}` chung (DEC-493): B chờ A commit → 2 đơn, không deadlock, không IntegrityError."""
    async with sessionmaker()() as db:
        a = await _shop(db, test_settings, "2001", "A")
        b = await _shop(db, test_settings, "2002", "B")
        await db.commit()
    wrote, release = asyncio.Event(), asyncio.Event()

    async def first() -> None:
        async with sessionmaker()() as db:
            await orders.upsert_platform_order(db, _order(SN, "SPXCCA"), shop_id=a.id)
            wrote.set()
            await release.wait()
            await db.commit()

    async def second() -> None:
        await wrote.wait()
        async with sessionmaker()() as db:
            await orders.upsert_platform_order(db, _order(SN, "SPXCCB"), shop_id=b.id)
            await db.commit()

    ta, tb = asyncio.create_task(first()), asyncio.create_task(second())
    await asyncio.wait_for(wrote.wait(), timeout=10)
    await asyncio.sleep(0.3)
    assert not tb.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(ta, tb), timeout=10)

    async with sessionmaker()() as db:
        rows = (await db.scalars(select(Order).where(Order.platform_order_sn == SN))).all()
        assert sorted(str(o.shop_id) for o in rows) == sorted([str(a.id), str(b.id)])
