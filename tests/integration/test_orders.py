"""Upsert đơn từ sàn, BR-17, EX-P10, BR-04 (T-9)."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, OrderItem, StatusHistory
from aicam.modules.platforms.mock.adapter import MockAdapter

pytestmark = pytest.mark.integration


async def test_upsert_creates_order_items_and_package(db: AsyncSession) -> None:
    data = await MockAdapter().find_by_tracking(None, "spxtst0000012")
    assert data is not None

    result = await orders.upsert_platform_order(db, data)

    assert result.created is True
    assert result.order.source == "API"
    assert [p.tracking_number for p in result.packages] == ["SPXTST0000012"]
    items = await orders.items_of(db, result.order.id)
    assert [(i.product_name, i.quantity) for i in items] == [
        ("Áo thun basic", 2),
        ("Tất cổ ngắn", 1),
        ("Túi vải", 1),
    ]


async def test_upsert_is_idempotent(db: AsyncSession) -> None:
    data = await MockAdapter().find_by_tracking(None, "SPXTST0000001")
    assert data is not None
    first = await orders.upsert_platform_order(db, data)

    second = await orders.upsert_platform_order(db, data)

    assert second.created is False
    assert second.order.id == first.order.id
    assert len(await orders.items_of(db, first.order.id)) == 1


async def test_multi_package_order(db: AsyncSession) -> None:
    """EX-P8: đơn nhiều kiện → mỗi mã vận đơn một kiện."""
    base = await MockAdapter().find_by_tracking(None, "SPXTST0000002")
    assert base is not None

    result = await orders.upsert_platform_order(
        db, replace(base, tracking_numbers=("SPXTST0000002", "SPXTST0000102"))
    )

    assert {p.tracking_number for p in result.packages} == {"SPXTST0000002", "SPXTST0000102"}
    assert {p.order_id for p in result.packages} == {result.order.id}


async def test_cancelled_on_platform_before_pack(db: AsyncSession) -> None:
    """BR-01 dữ liệu: đơn …09 hủy trên sàn → kiện CANCELLED."""
    data = await MockAdapter().find_by_tracking(None, "SPXTST0000009")
    assert data is not None

    result = await orders.upsert_platform_order(db, data)

    package = result.packages[0]
    assert package.warehouse_status == "CANCELLED"
    assert await orders.is_cancelled(db, package)


async def test_cancelled_after_pack(db: AsyncSession) -> None:
    """EX-P10: kiện đã PACKED, sàn hủy → CANCELLED_AFTER_PACK, có lịch sử nguồn PLATFORM."""
    adapter = MockAdapter()
    data = await adapter.find_by_tracking(None, "SPXTST0000005")
    assert data is not None
    package = (await orders.upsert_platform_order(db, data)).packages[0]
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")

    await orders.upsert_platform_order(db, replace(data, status="CANCELLED"))

    assert package.warehouse_status == "CANCELLED_AFTER_PACK"
    history = (await db.scalars(select(StatusHistory).where(StatusHistory.package_id == package.id))).all()
    assert [(h.to_status, h.source) for h in history][-1] == ("CANCELLED_AFTER_PACK", "PLATFORM")


async def test_api_overwrites_csv_order_and_keeps_snapshot(db: AsyncSession) -> None:
    """BR-17, FR-05.10."""
    csv_order = Order(platform_order_sn="2410TST00003", source="CSV", buyer_note="ghi chú CSV")
    db.add(csv_order)
    await db.flush()
    db.add(OrderItem(order_id=csv_order.id, product_name="Tên từ file", quantity=3))
    await db.flush()
    data = await MockAdapter().find_by_tracking(None, "SPXTST0000003")
    assert data is not None

    result = await orders.upsert_platform_order(db, data)

    assert result.order.id == csv_order.id
    assert result.order.source == "API"
    log = await db.scalar(
        select(AuditLog).where(
            AuditLog.action == "ORDER_OVERWRITTEN_BY_API", AuditLog.object_id == str(csv_order.id)
        )
    )
    assert log is not None
    assert log.data is not None
    assert log.data["items"][0]["product_name"] == "Tên từ file"


async def test_unverified_package_and_later_verification(db: AsyncSession) -> None:
    """BR-04: kiện chưa xác minh, sau đó sàn trả đơn → gắn đơn, verified."""
    package = await orders.create_unverified_package(db, "spxtst9990001")
    assert package.verified is False
    assert package.order_id is None
    adapter = MockAdapter()
    base = await adapter.find_by_tracking(None, "SPXTST0000004")
    assert base is not None

    result = await orders.upsert_platform_order(
        db, replace(base, platform_order_sn="2410TST99901", tracking_numbers=("SPXTST9990001",))
    )

    assert result.packages[0].id == package.id
    assert package.verified is True
    assert package.order_id == result.order.id


async def test_mock_list_updated_since() -> None:
    adapter = MockAdapter()
    later = datetime(2026, 10, 3, tzinfo=UTC)
    adapter.set_status("2410TST00007", "SHIPPED", later)

    changed = [o.platform_order_sn async for o in adapter.list_updated_orders(None, later)]

    assert changed == ["2410TST00007"]
