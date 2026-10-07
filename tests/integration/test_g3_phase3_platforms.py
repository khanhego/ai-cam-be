"""Review G3 Phase 3 — đồng bộ sàn (G3-MS-1..4, 6). Mock TikTok = adapter thật trên transport giả.

Chưa test với TikTok thật (thiếu tài khoản đối tác).
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import grants, sync
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import ShopCredentials
from aicam.modules.platforms.mock.tiktok import MOCK_OPEN_ID, MockTikTokAdapter, cipher_of

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 4, 0, tzinfo=UTC)


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


async def _tt_shop(
    db: AsyncSession, settings: Settings, code: str = "TTMOCKA", expires_in: timedelta = timedelta(days=7)
) -> Shop:
    shop = Shop(
        platform="TIKTOK", platform_shop_id=code, name=f"TST {code}", grant_ref=MOCK_OPEN_ID, region="VN"
    )
    platforms.store_credentials(
        shop,
        ShopCredentials(code, "a", "r", NOW + expires_in, shop_cipher=cipher_of(code)),
        Cipher(settings.fernet_key),
    )
    db.add(shop)
    await db.flush()
    return shop


# ---------------------------------------------------------------- G3-MS-1


async def test_ms1_j04_tiktok_refresh_keeps_shop_cipher(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """Token còn 30 phút → J-04 làm mới trước khi gọi; token mới phải đi kèm `shop_cipher` của shop (không thì
    TikTok từ chối mọi lời gọi cấp shop → EXPIRED mỗi chu kỳ token, đốt refresh token)."""
    shop = await _tt_shop(db, test_settings, expires_in=timedelta(minutes=30))
    out = await sync.sync_orders(db, tiktok, test_settings, shop.id)
    assert out[str(shop.id)]["status"] == "OK", out
    await db.refresh(shop)
    assert shop.auth_status == "CONNECTED"
    assert shop.shop_cipher == cipher_of("TTMOCKA")
    assert tiktok.data.tokens_issued == 1
    assert ("/order/202309/orders/search", "TTMOCKA") in tiktok.data.calls


async def test_ms1_auth_error_force_refresh_keeps_shop_cipher(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nhánh `PlatformAuthError` giữa lượt → làm mới `force` rồi thử lại: lần thử lại phải có
    `shop_cipher`."""
    shop = await _tt_shop(db, test_settings)
    tiktok.data.fail_auth.add("TTMOCKA")
    original = grants.acquire

    async def heal_then_acquire(platform: str, ref: str, *, wait_s: float = 10.0) -> str | None:
        tiktok.data.fail_auth.discard("TTMOCKA")  # sàn chấp nhận token mới
        return await original(platform, ref, wait_s=wait_s)

    monkeypatch.setattr(grants, "acquire", heal_then_acquire)
    out = await sync.sync_orders(db, tiktok, test_settings, shop.id)
    assert out[str(shop.id)]["status"] == "OK", out
    assert tiktok.data.tokens_issued == 1
    await db.refresh(shop)
    assert shop.auth_status == "CONNECTED"


async def test_ms1_refresh_grant_returns_full_credentials(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    shop = await _tt_shop(db, test_settings, expires_in=timedelta(minutes=10))
    creds = await grants.ensure_fresh(db, shop, tiktok, Cipher(test_settings.fernet_key))
    assert creds is not None
    assert (creds.shop_cipher, creds.grant_ref, creds.region) == (cipher_of("TTMOCKA"), MOCK_OPEN_ID, "VN")
    assert creds.access_token.startswith("mock-tt-access-")


# ---------------------------------------------------------------- G3-MS-2


def _tt_detail(sn: str, tracking: str, status: str = "AWAITING_COLLECTION") -> dict[str, object]:
    return {
        "id": sn,
        "status": status,
        "fulfillment_type": "FULFILLMENT_BY_SELLER",
        "buyer_message": "",
        "line_items": [
            {
                "id": f"LI{sn}",
                "product_name": "Áo thun TikTok",
                "sku_name": "Trắng / M",
                "seller_sku": "TT-AO-TRANG-M",
                "package_id": f"PK{sn}",
                "tracking_number": tracking,
            }
        ],
        "packages": [{"id": f"PK{sn}"}],
    }


async def test_ms2_j06_keeps_cancel_requested_without_buyer_flag(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """Đơn đã đóng gói (`PACKED`), người mua xin hủy (`PENDING`) nhưng chi tiết đơn **không** có cờ
    `is_buyer_request_cancel`: J-04 → `CANCEL_REQUESTED`; J-06 (chữ trạng thái đơn không đổi) không được hạ
    nhóm."""
    shop = await _tt_shop(db, test_settings)
    sn, code = "5761TT0000000900", "TTTST0000000900"
    tiktok.data.put_order("TTMOCKA", _tt_detail(sn, code))
    out = await sync.sync_orders(db, tiktok, test_settings, shop.id)
    assert out[str(shop.id)]["status"] == "OK"
    package = await orders.find_package(db, code)
    assert package is not None
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    stamp = int(clock.now().timestamp()) - 1
    tiktok.data.cancellations["TTMOCKA"].append(
        {"cancel_id": "CC900", "order_id": sn, "cancel_status": "PENDING", "create_time": stamp,
         "update_time": stamp}
    )  # fmt: skip
    clock.advance(timedelta(minutes=5))
    await sync.sync_orders(db, tiktok, test_settings, shop.id)
    order = await db.scalar(select(Order).where(Order.platform_order_sn == sn))
    assert order is not None
    await db.refresh(order)
    assert order.platform_status_group == "CANCEL_REQUESTED"

    clock.advance(timedelta(minutes=5))
    await sync.sync_shipping_status(db, tiktok, test_settings, shop.id)
    await db.refresh(order)
    await db.refresh(package)
    assert order.platform_status_group == "CANCEL_REQUESTED"
    assert package.warehouse_status == "PACKED"


async def test_ms2_j06_applies_cancelled_from_cancel_requested(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """Chữ trạng thái đơn đổi sang `CANCELLED` → J-06 vẫn áp (kiện `PACKED` → `CANCELLED_AFTER_PACK`)."""
    shop = await _tt_shop(db, test_settings)
    sn, code = "5761TT0000000901", "TTTST0000000901"
    tiktok.data.put_order("TTMOCKA", _tt_detail(sn, code))
    await sync.sync_orders(db, tiktok, test_settings, shop.id)
    package = await orders.find_package(db, code)
    assert package is not None
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    stamp = int(clock.now().timestamp()) - 1
    tiktok.data.cancellations["TTMOCKA"].append(
        {"cancel_id": "CC901", "order_id": sn, "cancel_status": "PENDING", "create_time": stamp,
         "update_time": stamp}
    )  # fmt: skip
    clock.advance(timedelta(minutes=5))
    await sync.sync_orders(db, tiktok, test_settings, shop.id)
    tiktok.data.set_status("TTMOCKA", sn, "CANCELLED")
    clock.advance(timedelta(minutes=5))
    await sync.sync_shipping_status(db, tiktok, test_settings, shop.id)
    await db.refresh(package)
    assert package.warehouse_status == "CANCELLED_AFTER_PACK"


# ---------------------------------------------------------------- G3-MS-3


async def test_ms3_j06_shop_crash_does_not_break_next_shop(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nhóm shop thứ nhất lỗi bất ngờ (rollback hết hạn mọi ORM) → nhóm thứ hai vẫn chạy (không đọc ORM hết
    hạn — MissingGreenlet)."""
    shops = [await _tt_shop(db, test_settings, code) for code in ("TTMOCKA", "TTMOCKB")]
    packages = []
    for i, shop in enumerate(shops):
        sn, code = f"5761TT000000091{i}", f"TTTST000000091{i}"
        tiktok.data.put_order(shop.platform_shop_id, _tt_detail(sn, code))
        await sync.sync_orders(db, tiktok, test_settings, shop.id)
        package = await orders.find_package(db, code)
        assert package is not None
        await orders.transition(db, package, "PACKING", source="WAREHOUSE")
        await orders.transition(db, package, "PACKED", source="WAREHOUSE")
        tiktok.data.set_status(shop.platform_shop_id, sn, "IN_TRANSIT")
        packages.append(package)
    await db.flush()
    original = sync._shipping_for_target
    calls: list[object] = []

    async def crash_first(*args: object, **kwargs: object) -> None:
        calls.append(args[3])
        if len(calls) == 1:
            session = args[0]
            assert isinstance(session, AsyncSession)
            await session.execute(select(Shop.id).limit(1))  # đang trong transaction → rollback hết hạn ORM
            raise RuntimeError("boom")
        await original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(sync, "_shipping_for_target", crash_first)
    out = await sync.sync_shipping_status(db, tiktok, test_settings)
    assert len(calls) == 2
    assert out["changed"] == 1
    statuses = []
    for package in packages:
        await db.refresh(package)
        statuses.append(package.warehouse_status)
    assert sorted(statuses) == ["HANDED_OVER", "PACKED"]
