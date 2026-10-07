"""T-209: J-04 / J-06 với adapter TikTok thật trên HTTP giả (respx, định dạng giả định 02a §7.1) — đơn gắn
đúng shop TikTok, nhóm trạng thái (AC-41), EX-T5 bỏ qua đơn kho TikTok, kiện gộp `package_order` (FR-05.22),
yêu cầu hủy chặn quét không hủy kiện (BR-21). **Chưa test với TikTok thật — thiếu tài khoản đối tác.**"""

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
import respx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, PackageOrder, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import ShopCredentials
from aicam.modules.platforms.tiktok.adapter import TikTokAdapter
from aicam.modules.platforms.tiktok.client import TikTokClient

pytestmark = pytest.mark.integration

API = "https://open-api.tiktok.test"
NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.tiktok_enabled = True
    test_settings.tiktok_adapter = "tiktok"
    test_settings.tiktok_app_key, test_settings.tiktok_app_secret, test_settings.tiktok_service_id = (
        "k",
        "s",
        "1",
    )


@pytest.fixture
def adapter() -> TikTokAdapter:
    async def _no_sleep(_: float) -> None:
        return None

    return TikTokAdapter(
        TikTokClient("k", "s", API, "https://auth.test", max_attempts=2, sleep=_no_sleep),
        authorize_url="a", service_id="1",
    )  # fmt: skip


async def _shop(db: AsyncSession, settings: Settings) -> Shop:
    shop = Shop(platform="TIKTOK", platform_shop_id="7001", name="TST TikTok A (mock)", grant_ref="OPEN-1")
    platforms.store_credentials(
        shop,
        ShopCredentials("7001", "tt-acc", "tt-ref", NOW + timedelta(days=3), shop_cipher="C1"),
        Cipher(settings.fernet_key),
    )
    db.add(shop)
    await db.flush()
    return shop


def ok(data: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "message": "Success", "request_id": "r", "data": data})


def _line(tracking: str, sku: str = "AO") -> dict[str, Any]:
    return {"product_name": "Áo thun", "sku_name": "Đen", "seller_sku": sku, "tracking_number": tracking}


def _detail(oid: str, status: str, tracking: str, **kw: Any) -> dict[str, Any]:
    return {"id": oid, "status": status, "line_items": [_line(tracking)], "update_time": 1790000000, **kw}


def _mock_api(details: list[dict[str, Any]], cancels: list[dict[str, Any]] | None = None) -> None:
    by_id = {d["id"]: d for d in details}
    respx.post(f"{API}/order/202309/orders/search").mock(
        return_value=ok({"orders": [{"id": i} for i in by_id]})
    )
    respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok({"cancellations": cancels or []})
    )

    def _side(request: httpx.Request) -> httpx.Response:
        ids = parse_qs(request.url.query.decode())["ids"][0].split(",")
        return ok({"orders": [by_id[i] for i in ids if i in by_id]})

    respx.get(f"{API}/order/202309/orders").mock(side_effect=_side)


@respx.mock
async def test_j04_tiktok_orders_groups_fbt_and_merged(
    db: AsyncSession, adapter: TikTokAdapter, test_settings: Settings
) -> None:
    shop = await _shop(db, test_settings)
    _mock_api(
        [
            _detail("5761001", "AWAITING_SHIPMENT", "TTTST0000000001"),
            _detail("5761002", "IN_TRANSIT", "TTTST0000000002"),
            _detail("5761003", "XYZ", "TTTST0000000003"),
            _detail(
                "5761004", "AWAITING_SHIPMENT", "TTTST0000000004", fulfillment_type="FULFILLMENT_BY_TIKTOK"
            ),
            _detail("5761077", "AWAITING_SHIPMENT", "TTTST0000000077"),
            _detail("5761078", "AWAITING_SHIPMENT", "TTTST0000000077"),
            _detail("5761050", "AWAITING_SHIPMENT", "TTTST0000000050"),
        ],
        cancels=[{"order_id": "5761050", "cancel_status": "PENDING", "update_time": 1790000000}],
    )
    out = await sync.sync_orders(db, adapter, test_settings, shop.id)
    res = out[str(shop.id)]
    assert res["status"] == "OK"
    assert (res["orders"], res["skipped"]) == (7, 1)  # EX-T5: đơn kho TikTok chỉ đếm
    rows = {
        o.platform_order_sn: o
        for o in (await db.scalars(select(Order).where(Order.shop_id == shop.id))).all()
    }
    assert {sn: o.platform_status_group for sn, o in rows.items()} == {
        "5761001": "AWAITING_SHIPMENT",
        "5761002": "SHIPPED",
        "5761003": "UNKNOWN",
        "5761077": "AWAITING_SHIPMENT",
        "5761078": "AWAITING_SHIPMENT",
        "5761050": "CANCEL_REQUESTED",
    }
    assert rows["5761003"].platform_status == "XYZ"
    merged = await orders.find_package(db, "TTTST0000000077")
    assert merged is not None
    assert merged.order_id == rows["5761077"].id  # kiện vẫn thuộc đơn chính
    link = await db.scalar(select(PackageOrder).where(PackageOrder.package_id == merged.id))
    assert link is not None
    assert link.order_id == rows["5761078"].id
    assert await orders.find_package(db, "TTTST0000000004") is None
    cancel_req = await orders.find_package(db, "TTTST0000000050")
    assert cancel_req is not None
    assert cancel_req.warehouse_status == "NEW"  # BR-21: yêu cầu hủy không hủy kiện
    assert await orders.is_cancelled(db, cancel_req)  # BR-01: chặn mở phiên


@respx.mock
async def test_j06_tiktok_shipping_hands_over_and_failed_delivery(
    db: AsyncSession, adapter: TikTokAdapter, test_settings: Settings
) -> None:
    shop = await _shop(db, test_settings)
    _mock_api(
        [
            _detail("5761101", "AWAITING_SHIPMENT", "TTSHIP0000001"),
            _detail("5761102", "AWAITING_SHIPMENT", "TTSHIP0000002"),
        ]
    )
    await sync.sync_orders(db, adapter, test_settings, shop.id)
    for code in ("TTSHIP0000001", "TTSHIP0000002"):
        package = await orders.find_package(db, code)
        assert package is not None
        await orders.transition(db, package, "PACKING", source="WAREHOUSE")
        await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    respx.reset()
    _mock_api(
        [
            _detail("5761101", "IN_TRANSIT", "TTSHIP0000001"),
            {
                **_detail("5761102", "IN_TRANSIT", "TTSHIP0000002"),
                "line_items": [{**_line("TTSHIP0000002"), "package_status": "DELIVERY_FAILED"}],
            },
        ]
    )
    out = await sync.sync_shipping_status(db, adapter, test_settings, shop.id)
    assert out["checked"] == 2
    first = await db.scalar(select(Package).where(Package.tracking_number == "TTSHIP0000001"))
    second = await db.scalar(select(Package).where(Package.tracking_number == "TTSHIP0000002"))
    assert first is not None
    assert second is not None
    assert first.warehouse_status == "HANDED_OVER"
    assert second.warehouse_status == "RETURN_EXPECTED"
