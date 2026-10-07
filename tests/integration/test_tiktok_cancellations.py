"""T-277: yêu cầu hủy TikTok — `cancellations/search` → **luôn** đọc chi tiết `orders?ids=`; nhóm từ (đơn, yêu
cầu hủy mới nhất); fixture 4 kịch bản `pending` / `rejected` / `withdrawn` / `approved` (02a §7.1, DEC-468,
502; FR-05.17; BR-21 kịch bản (3) với TikTok `REJECTED`, (4) được chấp nhận khi `PACKED`; AC-41).

Mock TikTok = adapter thật trên transport giả; mỗi lượt J-04 áp một bước kịch bản. Chưa test với TikTok thật.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import ShopCredentials
from aicam.modules.platforms.mock.tiktok import MOCK_OPEN_ID, MockTikTokAdapter, cipher_of
from aicam.modules.platforms.router import get_platform_adapter

from .factories import PASSWORD, make_station_account

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 4, 0, tzinfo=UTC)
ORDERS = {n: f"5761TT00000000{n}" for n in (50, 51, 52, 53)}


async def _no_sleep(_: float) -> None:
    return None


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.tiktok_enabled = True
    test_settings.tiktok_adapter = "mock"


@pytest.fixture
def tiktok() -> MockTikTokAdapter:
    return MockTikTokAdapter(sleep=_no_sleep)


async def _shop(db: AsyncSession, settings: Settings) -> Shop:
    shop = Shop(
        platform="TIKTOK", platform_shop_id="TTMOCKA", name="TST TikTok A (mock)", grant_ref=MOCK_OPEN_ID
    )
    platforms.store_credentials(
        shop,
        ShopCredentials("TTMOCKA", "a", "r", NOW + timedelta(days=7), shop_cipher=cipher_of("TTMOCKA")),
        Cipher(settings.fernet_key),
    )
    db.add(shop)
    await db.flush()
    return shop


async def _j04(db: AsyncSession, tiktok: MockTikTokAdapter, settings: Settings, shop: Shop) -> dict[int, str]:
    clock.advance(timedelta(minutes=5))
    out = await sync.sync_orders(db, tiktok, settings, shop.id)
    assert out[str(shop.id)]["status"] == "OK"
    rows = (await db.scalars(select(Order).where(Order.platform_order_sn.in_(list(ORDERS.values()))))).all()
    for o in rows:
        await db.refresh(o)
    by_sn = {o.platform_order_sn: o.platform_status_group for o in rows}
    return {n: by_sn[sn] for n, sn in ORDERS.items()}


async def test_four_cancel_scenarios_follow_latest_request(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """050 `PENDING`; 051 chưa có → `PENDING` → `REJECTED`; 052 `PENDING` → người mua rút; 053 `PENDING` →
    `APPROVED` (đơn `CANCELLED`). Lượt J-04 mà chỉ yêu cầu hủy đổi (đơn không đổi) vẫn đọc chi tiết đơn."""
    shop = await _shop(db, test_settings)
    assert await _j04(db, tiktok, test_settings, shop) == {
        50: "CANCEL_REQUESTED", 51: "AWAITING_SHIPMENT", 52: "CANCEL_REQUESTED", 53: "CANCEL_REQUESTED"
    }  # fmt: skip
    tiktok.data.calls.clear()
    assert await _j04(db, tiktok, test_settings, shop) == {
        50: "CANCEL_REQUESTED", 51: "CANCEL_REQUESTED", 52: "AWAITING_SHIPMENT", 53: "CANCELLED"
    }  # fmt: skip
    # Đơn 051 / 052 không đổi `update_time` (chỉ yêu cầu hủy đổi) nhưng vẫn có trong lô chi tiết đơn.
    assert ("/order/202309/orders", "TTMOCKA") in tiktok.data.calls
    assert await _j04(db, tiktok, test_settings, shop) == {
        50: "CANCEL_REQUESTED", 51: "AWAITING_SHIPMENT", 52: "AWAITING_SHIPMENT", 53: "CANCELLED"
    }  # fmt: skip
    p53 = await orders.find_package(db, "TTTST0000000053")
    assert p53 is not None
    assert p53.warehouse_status == "CANCELLED"  # nhóm CANCELLED: luật hủy (kiện NEW → CANCELLED)


async def test_br21_tiktok_rejected_then_pack_and_hand_over(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """BR-21 (3) với TikTok `REJECTED`: đang xin hủy → chặn mở phiên, kiện vẫn `NEW`; bị từ chối → mở phiên
    được, đóng gói, J-06 (`IN_TRANSIT`) → `HANDED_OVER`; kiện không bao giờ `CANCELLED`."""
    shop = await _shop(db, test_settings)
    await _j04(db, tiktok, test_settings, shop)
    await _j04(db, tiktok, test_settings, shop)  # 051 PENDING
    package = await orders.find_package(db, "TTTST0000000051")
    assert package is not None
    assert package.warehouse_status == "NEW"
    assert await orders.is_cancelled(db, package)
    await _j04(db, tiktok, test_settings, shop)  # 051 REJECTED
    await db.refresh(package)
    assert not await orders.is_cancelled(db, package)
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    tiktok.data.set_status("TTMOCKA", ORDERS[51], "IN_TRANSIT")
    await sync.sync_shipping_status(db, tiktok, test_settings, shop.id)
    await db.refresh(package)
    assert package.warehouse_status == "HANDED_OVER"


async def test_br21_tiktok_approved_when_packed_cancels_after_pack(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """BR-21 (4): kiện đã `PACKED` (đóng trước khi người mua xin hủy) → yêu cầu được chấp nhận (đơn
    `CANCELLED`) → `CANCELLED_AFTER_PACK` (EX-P10)."""
    shop = await _shop(db, test_settings)
    await _j04(db, tiktok, test_settings, shop)  # 053 PENDING
    package = await orders.find_package(db, "TTTST0000000053")
    assert package is not None
    assert package.warehouse_status == "NEW"
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    await _j04(db, tiktok, test_settings, shop)  # 053 APPROVED → CANCELLED
    await db.refresh(package)
    assert package.warehouse_status == "CANCELLED_AFTER_PACK"


async def test_station_scan_cancel_requested_alert_tiktok(
    api: AsyncClient, db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """FR-05.17 / AC-41: quét kiện TikTok đơn đang xin hủy → ALERT `ORDER_CANCEL_REQUESTED` `data.platform =
    TIKTOK`; đơn đã hủy → `ORDER_CANCELLED`."""
    shop = await _shop(db, test_settings)
    await _j04(db, tiktok, test_settings, shop)
    await _j04(db, tiktok, test_settings, shop)
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: tiktok  # type: ignore[attr-defined]
    user, _ = await make_station_account(db, "tst_st277", "TST Station 277")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    headers = {"Authorization": f"Bearer {res.json()['access_token']}"}

    async def scan(code: str) -> dict[str, Any]:
        r = await api.post(
            "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
        )
        assert r.status_code == 200, r.text
        return r.json()  # type: ignore[no-any-return]

    body = await scan("TTTST0000000050")
    assert (body["outcome"], body["alert"]["code"], body["alert"]["data"]) == (
        "ALERT", "ORDER_CANCEL_REQUESTED", {"platform": "TIKTOK"},
    )  # fmt: skip
    body = await scan("TTTST0000000053")
    assert (body["alert"]["code"], body["alert"]["data"]["platform"]) == ("ORDER_CANCELLED", "TIKTOK")
