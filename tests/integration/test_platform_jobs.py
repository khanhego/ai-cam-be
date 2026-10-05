"""Job sàn J-04, J-05, J-06, J-12 (T-22) — FR-05.02..04, BR-04, BR-17, EX-P10.

TC-05.04, 05.05, 05.06, 05.07, 05.08, 05.09, 05.10, 05.16, 05.19. Adapter mock (PRE-6); TC-05.08 thêm biến thể
adapter Shopee trên HTTP giả (respx). **Chưa test với Shopee thật — thiếu tài khoản partner (T-3).**
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.imports.models import CsvImport
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, Shop, StatusHistory
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MOCK_SHOP_ID, MockAdapter
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient

from .factories import PASSWORD, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)
BASE = "https://partner.test-stable.shopeemobile.com"


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True


@pytest.fixture
def mock() -> MockAdapter:
    return MockAdapter()


async def _shop(db: AsyncSession, settings: Settings, expires_in: timedelta = timedelta(hours=4)) -> Shop:
    shop = Shop(platform="SHOPEE", platform_shop_id=MOCK_SHOP_ID, name="TST Shop")
    platforms.store_credentials(
        shop, ShopCredentials(MOCK_SHOP_ID, "acc-1", "ref-1", NOW + expires_in), Cipher(settings.fernet_key)
    )
    db.add(shop)
    await db.flush()
    return shop


def _order(n: int, *codes: str, status: str = "READY_TO_SHIP") -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=f"2410TST{n:05d}",
        status=status,
        tracking_numbers=codes or (f"SPXTST{n:07d}",),
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L"),),
        created_at=NOW,
        updated_at=NOW,
    )


async def _packed(db: AsyncSession, mock: MockAdapter, n: int) -> Package:
    result = await orders.upsert_platform_order(db, mock.orders[f"2410TST{n:05d}"])
    package = result.packages[0]
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    return package


async def test_j04_new_orders(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """TC-05.04: 5 đơn mới, đơn 35 có 2 mã vận đơn → 5 đơn, 6 kiện, nguồn API; cursor + last_synced_at."""
    shop = await _shop(db, test_settings)
    for n in range(31, 35):
        mock.put(_order(n))
    mock.put(_order(35, "SPXTST0000035", "SPXTST0000036"))
    out = await sync.sync_orders(db, mock, test_settings)
    assert out[str(shop.id)]["status"] == "OK"
    assert out[str(shop.id)]["orders"] == 5
    sns = [f"2410TST{n:05d}" for n in range(31, 36)]
    rows = (await db.scalars(select(Order).where(Order.platform_order_sn.in_(sns)))).all()
    assert len(rows) == 5
    assert {o.source for o in rows} == {"API"}
    assert {o.shop_id for o in rows} == {shop.id}
    packages = (await db.scalars(select(Package).where(Package.order_id.in_([o.id for o in rows])))).all()
    assert len(packages) == 6
    await db.refresh(shop)
    assert (shop.last_sync_cursor, shop.last_synced_at, shop.last_error) == (NOW, NOW, None)
    assert await platforms.acquire_sync_lock(shop.id, "test")  # lock đã nhả


async def test_j04_since_cursor_minus_10_minutes(db: AsyncSession, test_settings: Settings) -> None:
    """02a J-04: since = cursor − 10 phút; lần đầu lùi SHOPEE_INITIAL_SYNC_DAYS ngày."""
    seen: list[datetime] = []

    class Spy(MockAdapter):
        async def list_updated_orders(self, creds: Any, since: datetime) -> Any:  # type: ignore[override]
            seen.append(since)
            return
            yield

    shop = await _shop(db, test_settings)
    await sync.sync_orders(db, Spy(), test_settings)
    clock.advance(timedelta(minutes=5))
    await sync.sync_orders(db, Spy(), test_settings)
    assert seen == [NOW - timedelta(days=3, minutes=10), NOW - timedelta(minutes=10)]
    await db.refresh(shop)
    assert shop.last_sync_cursor == NOW + timedelta(minutes=5)


async def test_j04_cancel_after_pack(
    api: AsyncClient, db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.05 / EX-P10: đơn hủy khi kho PACKED → CANCELLED_AFTER_PACK; API-32 đếm + attention."""
    await _shop(db, test_settings)
    package = await _packed(db, mock, 10)
    mock.set_status("2410TST00010", "CANCELLED", NOW)
    await sync.sync_orders(db, mock, test_settings)
    await db.refresh(package)
    assert package.warehouse_status == "CANCELLED_AFTER_PACK"

    await make_user(db, "tst_sup", "SUPERVISOR")
    token = (
        await api.post(
            "/api/v1/auth/login", json={"username": "tst_sup", "password": PASSWORD, "client": "DASHBOARD"}
        )
    ).json()["access_token"]
    report = (await api.get("/api/v1/reports/daily", headers={"Authorization": f"Bearer {token}"})).json()
    assert report["counts"]["cancelled_after_pack"] == 1
    assert {"kind": "CANCELLED_AFTER_PACK", "count": 1} in report["attention"]


async def test_j04_overwrites_csv_order(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """TC-05.16 / BR-17, FR-05.10: đơn CSV bị J-04 ghi đè → nguồn API, audit giữ bản CSV."""
    await _shop(db, test_settings)
    user = await make_user(db, "tst_sup", "SUPERVISOR")
    imp = CsvImport(
        file_name="x.csv", created_by=user.id, expires_at=NOW + timedelta(minutes=30), status="COMMITTED"
    )
    db.add(imp)
    await db.flush()
    await orders.apply_csv_order(
        db,
        orders.CsvOrder("2410TST00040", "ghi chú CSV", (PlatformItem("Hàng CSV", 3),), ("SPXTST0000040",)),
        import_id=imp.id, shop_id=None, actor_user_id=user.id,
    )  # fmt: skip
    mock.put(_order(40))
    await sync.sync_orders(db, mock, test_settings)
    order = await db.scalar(select(Order).where(Order.platform_order_sn == "2410TST00040"))
    assert order is not None
    assert order.source == "API"
    log = await db.scalar(
        select(AuditLog).where(
            AuditLog.action == "ORDER_OVERWRITTEN_BY_API", AuditLog.object_id == str(order.id)
        )
    )
    assert log is not None
    assert log.data is not None
    assert log.data["items"][0]["product_name"] == "Hàng CSV"


async def test_j04_platform_error_sets_last_error(
    api: AsyncClient, db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.09: sàn lỗi tới hết lượt thử → `shop.last_error`; API-32 attention SYNC_ERROR; giữ cursor."""
    shop = await _shop(db, test_settings)
    mock.fail_list_times = 99
    out = await sync.sync_orders(db, mock, test_settings)
    assert out[str(shop.id)]["status"] == "FAILED"
    await db.refresh(shop)
    assert shop.last_error is not None
    assert shop.last_error["code"] == "SYNC_FAILED"
    assert shop.last_sync_cursor is None
    assert shop.auth_status == "CONNECTED"

    await make_user(db, "tst_sup", "SUPERVISOR")
    token = (
        await api.post(
            "/api/v1/auth/login", json={"username": "tst_sup", "password": PASSWORD, "client": "DASHBOARD"}
        )
    ).json()["access_token"]
    report = (await api.get("/api/v1/reports/daily", headers={"Authorization": f"Bearer {token}"})).json()
    assert {"kind": "SYNC_ERROR", "shop_id": str(shop.id), "at": clock.iso_z(NOW)} in report["attention"]

    # Lượt sau thành công → xóa lỗi.
    mock.fail_list_times = 0
    await sync.sync_orders(db, mock, test_settings)
    await db.refresh(shop)
    assert shop.last_error is None


@respx.mock
async def test_j04_shopee_503_twice_then_ok(db: AsyncSession, test_settings: Settings) -> None:
    """TC-05.08 (adapter Shopee, HTTP giả): 503 hai lần rồi OK → thành công; chạy lại không trùng đơn."""

    async def _no_sleep(_: float) -> None:
        return None

    adapter = ShopeeAdapter(ShopeeClient(2001234, "k", BASE, sleep=_no_sleep))
    ok_list = httpx.Response(
        200,
        json={
            "error": "",
            "response": {
                "more": False,
                "order_list": [{"order_sn": "2410SPE00001", "order_status": "PROCESSED"}],
            },
        },
    )
    respx.get(f"{BASE}/api/v2/order/get_order_list").mock(
        side_effect=[httpx.Response(503), httpx.Response(503), ok_list, ok_list]
    )
    respx.get(f"{BASE}/api/v2/order/get_order_detail").mock(
        return_value=httpx.Response(200, json={"error": "", "response": {"order_list": [
            {"order_sn": "2410SPE00001", "order_status": "PROCESSED", "update_time": 1791160000,
             "item_list": [{"item_name": "Áo", "model_quantity_purchased": 1}], "package_list": []}]}})
    )  # fmt: skip
    respx.get(f"{BASE}/api/v2/logistics/get_tracking_number").mock(
        return_value=httpx.Response(200, json={"error": "", "response": {"tracking_number": "SPXVN0000501"}})
    )
    shop = await _shop(db, test_settings)
    out = await sync.sync_orders(db, adapter, test_settings)
    assert out[str(shop.id)]["status"] == "OK"
    await sync.sync_orders(db, adapter, test_settings)
    dup = (
        await db.execute(
            select(Order.platform_order_sn).group_by(Order.platform_order_sn).having(func.count() > 1)
        )
    ).all()
    assert dup == []
    assert await orders.find_package(db, "SPXVN0000501") is not None


async def test_j04_lock_and_disabled(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """Đồng bộ chạy chồng → bỏ qua (lock `sync:{shop}`); `SHOPEE_ENABLED=false` → không làm gì."""
    shop = await _shop(db, test_settings)
    assert await platforms.acquire_sync_lock(shop.id, "api")
    out = await sync.sync_orders(db, mock, test_settings)
    assert out[str(shop.id)] == {"status": "SKIPPED", "reason": "locked"}
    # API-73 đã giữ lock → job chạy và nhả lock.
    out = await sync.sync_orders(db, mock, test_settings, shop.id, lock_held=True)
    assert out[str(shop.id)]["status"] == "OK"
    assert await platforms.acquire_sync_lock(shop.id, "x")
    await platforms.release_sync_lock(shop.id)

    test_settings.shopee_enabled = False
    assert await sync.sync_orders(db, mock, test_settings) == {"skipped": "not_configured"}
    assert await sync.verify_unverified(db, mock, test_settings) == {"checked": 0, "verified": 0}
    assert await sync.sync_shipping_status(db, mock, test_settings) == {"checked": 0, "changed": 0}


async def test_j04_auth_error_refreshes_once_then_expired(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """Token bị thu hồi giữa chừng: refresh một lần rồi thử lại; vẫn hỏng → shop EXPIRED."""
    shop = await _shop(db, test_settings)
    mock.fail_list_auth = True
    out = await sync.sync_orders(db, mock, test_settings)
    assert out[str(shop.id)]["status"] == "EXPIRED"
    assert mock.calls.count("refresh") == 1
    await db.refresh(shop)
    assert shop.auth_status == "EXPIRED"


async def test_j05_verify_unverified(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """TC-05.07 / BR-04: kiện chưa xác minh, sàn đã có đơn → J-05 gắn đơn, verified = true."""
    await _shop(db, test_settings)
    package = await orders.create_unverified_package(db, "SPXTST9990001")
    other = await orders.create_unverified_package(db, "SPXTST9990002")  # sàn vẫn không có
    mock.put(_order(901, "SPXTST9990001"))
    out = await sync.verify_unverified(db, mock, test_settings)
    assert out == {"checked": 2, "verified": 1}
    await db.refresh(package)
    await db.refresh(other)
    assert package.verified is True
    assert package.order_id is not None
    assert other.verified is False


async def test_j06_shipping(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """TC-05.06 → TC-05.19: PICKED_UP → HANDED_OVER; DELIVERED → DELIVERED; status_history nguồn PLATFORM."""
    await _shop(db, test_settings)
    package = await _packed(db, mock, 10)
    mock.shipping["SPXTST0000010"] = "PICKED_UP"
    out = await sync.sync_shipping_status(db, mock, test_settings)
    assert out["changed"] == 1
    await db.refresh(package)
    assert (package.warehouse_status, package.platform_logistics_status) == ("HANDED_OVER", "PICKED_UP")
    mock.shipping["SPXTST0000010"] = "DELIVERED"
    await sync.sync_shipping_status(db, mock, test_settings)
    await db.refresh(package)
    assert package.warehouse_status == "DELIVERED"
    history = (
        await db.scalars(
            select(StatusHistory)
            .where(StatusHistory.package_id == package.id)
            .order_by(StatusHistory.at, StatusHistory.id)
        )
    ).all()
    assert [(h.to_status, h.source) for h in history][-2:] == [
        ("HANDED_OVER", "PLATFORM"),
        ("DELIVERED", "PLATFORM"),
    ]


async def test_j06_packed_straight_to_delivered_and_cancel(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """Bỏ lỡ mốc lấy hàng: PACKED → HANDED_OVER → DELIVERED một lượt; hủy sau đóng → CANCELLED_AFTER_PACK."""
    await _shop(db, test_settings)
    delivered = await _packed(db, mock, 13)
    cancelled = await _packed(db, mock, 14)
    mock.shipping["SPXTST0000013"] = "DELIVERED"
    mock.orders["2410TST00014"] = replace(mock.orders["2410TST00014"], status="CANCELLED")
    await sync.sync_shipping_status(db, mock, test_settings)
    await db.refresh(delivered)
    await db.refresh(cancelled)
    assert delivered.warehouse_status == "DELIVERED"
    assert cancelled.warehouse_status == "CANCELLED_AFTER_PACK"


async def test_j12_refresh(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """J-12: token còn < 1 giờ → làm mới (mã hóa lại); TC-05.10: refresh bị từ chối → EXPIRED."""
    shop = await _shop(db, test_settings, expires_in=timedelta(minutes=30))
    fresh = await _shop_copy(db, test_settings)
    out = await sync.refresh_tokens(db, mock, test_settings)
    assert out == {"refreshed": 1, "expired": 0, "failed": 0}
    await db.refresh(shop)
    assert shop.auth_expires_at == NOW + timedelta(hours=4)
    creds = platforms.credentials(shop, Cipher(test_settings.fernet_key))
    assert creds is not None
    assert creds.access_token.startswith("mock-access-")
    await db.refresh(fresh)
    assert fresh.auth_expires_at == NOW + timedelta(hours=4)  # còn hạn: không đụng

    clock.advance(timedelta(hours=3, minutes=30))
    mock.fail_refresh = True
    out = await sync.refresh_tokens(db, mock, test_settings)
    assert out["expired"] == 2
    await db.refresh(shop)
    assert shop.auth_status == "EXPIRED"
    assert shop.last_error is not None
    assert shop.last_error["code"] == "AUTH_EXPIRED"


async def _shop_copy(db: AsyncSession, settings: Settings) -> Shop:
    shop = Shop(platform="SHOPEE", platform_shop_id="990002", name="Shop 2")
    platforms.store_credentials(
        shop, ShopCredentials("990002", "a", "r", NOW + timedelta(hours=4)), Cipher(settings.fernet_key)
    )
    db.add(shop)
    await db.flush()
    return shop
