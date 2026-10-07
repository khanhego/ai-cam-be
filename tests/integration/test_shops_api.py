"""API-70..73 kết nối Shopee + tra sàn 2 giây khi quét bằng adapter Shopee thật trên HTTP giả (T-16).

TC-05.03, TC-05.01 / 05.02 (luồng API với adapter mock), TC-03.12 / 03.13 với adapter Shopee + respx.
**Chưa test với Shopee thật — thiếu tài khoản partner (T-3).**
"""

import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAdapter, ShopCredentials
from aicam.modules.platforms.mock.adapter import MOCK_SHOP_ID, MockAdapter
from aicam.modules.platforms.router import get_platform_adapter
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)
BASE = "https://partner.test-stable.shopeemobile.com"


def _use(api: AsyncClient, adapter: PlatformAdapter) -> None:
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: adapter  # type: ignore[attr-defined]


async def _login(
    api: AsyncClient, db: AsyncSession, username: str, role: str, client: str = "DASHBOARD"
) -> dict[str, str]:
    if role != "STATION":
        await make_user(db, username, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture
def mock(api: AsyncClient, test_settings: Settings) -> MockAdapter:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True
    adapter = MockAdapter()
    _use(api, adapter)
    return adapter


@pytest.fixture
async def admin(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    return await _login(api, db, "tst_admin", "ADMIN")


async def _connect(api: AsyncClient, admin: dict[str, str]) -> httpx.Response:
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    assert res.status_code == 200, res.text
    url = urlparse(res.json()["url"])
    return await api.get(f"{url.path}?{url.query}")


async def test_not_configured(api: AsyncClient, admin: dict[str, str], test_settings: Settings) -> None:
    """TC-05.03: `SHOPEE_ENABLED=false` → API-71 / API-73 503 PLATFORM_NOT_CONFIGURED; API-70 vẫn chạy."""
    test_settings.shopee_enabled = False
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (503, "PLATFORM_NOT_CONFIGURED")
    assert res.json()["error"]["message"] == "Chưa cấu hình Shopee Open Platform. Dùng Nhập đơn từ file."
    res = await api.post(f"/api/v1/shops/{uuid.uuid4()}/sync", headers=admin)
    assert res.status_code == 503
    assert (await api.get("/api/v1/shops", headers=admin)).json()["items"] == []
    # Adapter shopee chưa có partner key: quét vẫn chạy (chưa xác minh), không 503.
    test_settings.platform_adapter = "shopee"
    test_settings.shopee_enabled = True
    assert not platforms.is_configured(test_settings)
    assert isinstance(platforms.get_adapter(test_settings), platforms.UnconfiguredAdapter)


async def test_connect_flow(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], mock: MockAdapter,
    sent_jobs: list[tuple[str, list[Any], str, float]], test_settings: Settings,
) -> None:  # fmt: skip
    """TC-05.01 (luồng API, adapter mock): auth-url có state → callback lưu shop CONNECTED, token mã hóa,
    audit SHOP_CONNECT, J-04 ngay; API-70 hiện trạng thái."""
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    cookie = api.cookies.get("aicam_shopee_state")
    url = urlparse(res.json()["url"])
    q = parse_qs(url.query)
    assert url.path == "/api/v1/shops/shopee/callback"
    assert len(q["state"][0]) >= 24
    res = await api.get(f"{url.path}?{url.query}")
    assert res.status_code == 302
    assert res.headers["location"] == "/admin/settings/platforms?platform=shopee&result=connected&count=1"

    shop = await db.scalar(select(Shop).where(Shop.platform_shop_id == MOCK_SHOP_ID))
    assert shop is not None
    assert (shop.auth_status, shop.name) == ("CONNECTED", "TST Shop (mock)")
    assert shop.access_token_enc is not None
    assert b"mock-access" not in shop.access_token_enc
    creds = platforms.credentials(shop, Cipher(test_settings.fernet_key))
    assert creds is not None
    assert creds.access_token.startswith("mock-access-")
    assert shop.auth_expires_at == NOW + timedelta(hours=4)
    audit = await db.scalar(select(AuditLog).where(AuditLog.action == "SHOP_CONNECT"))
    assert audit is not None
    assert audit.object_id == str(shop.id)
    assert sent_jobs[-2:] == [
        ("platforms.sync_shop_orders", [str(shop.id), False], "sync", 0.0),
        ("platforms.sync_shop_returns", [str(shop.id)], "sync", 0.0),  # J-13 ngay sau kết nối (T-105)
    ]

    # state dùng một lần: gọi lại cùng URL → expired (Phase 3 — 02 §6.2 API-72)
    res = await api.get(f"{url.path}?{url.query}", headers={"cookie": f"aicam_shopee_state={cookie}"})
    assert res.headers["location"] == "/admin/settings/platforms?platform=shopee&result=expired"

    body = (await api.get("/api/v1/shops", headers=admin)).json()
    item = body["items"][0]
    assert item["auth_status"] == "CONNECTED"
    assert item["name"] == "TST Shop (mock)"
    assert item["today_synced_orders"] == 0
    assert item["last_error"] is None
    await orders.upsert_platform_order(db, mock.orders["2410TST00001"], shop_id=shop.id)
    body = (await api.get("/api/v1/shops", headers=admin)).json()
    assert body["items"][0]["today_synced_orders"] == 1


async def test_callback_denied_and_bad_state(
    api: AsyncClient, admin: dict[str, str], mock: MockAdapter
) -> None:
    """TC-05.02: người bán từ chối (không có code) → `denied`; state / code sai → `error`."""
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    state = parse_qs(urlparse(res.json()["url"]).query)["state"][0]
    res = await api.get("/api/v1/shops/shopee/callback", params={"state": state})
    assert res.headers["location"].endswith("result=denied")
    res = await api.get(
        "/api/v1/shops/shopee/callback", params={"state": "khong-co", "code": "MOCK-CODE", "shop_id": "1"}
    )
    assert res.headers["location"].endswith("result=error")  # cookie không khớp `state` (G3-N7)
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    state = parse_qs(urlparse(res.json()["url"]).query)["state"][0]
    res = await api.get(
        "/api/v1/shops/shopee/callback", params={"state": state, "code": "SAI", "shop_id": "1"}
    )
    assert res.headers["location"].endswith("result=error")


async def test_connect_other_shop_keeps_old_connected(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], mock: MockAdapter
) -> None:
    """Phase 3 (FR-05.14, AC-40 — T-204): kết nối shop khác **không** ngắt shop cũ (bỏ DEC-12 một shop)."""
    old = Shop(
        platform="SHOPEE",
        platform_shop_id="123",
        auth_status="CONNECTED",
        access_token_enc=b"x",
        refresh_token_enc=b"y",
    )
    db.add(old)
    await db.flush()
    await _connect(api, admin)
    await db.refresh(old)
    assert (old.auth_status, old.access_token_enc) == ("CONNECTED", b"x")


async def test_sync_now(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], mock: MockAdapter,
    sent_jobs: list[tuple[str, list[Any], str, float]],
) -> None:  # fmt: skip
    """API-73: 202 + J-04 queue `sync` (giữ lock); lần 2 khi đang chạy → 409 SYNC_IN_PROGRESS; quyền ADMIN."""
    await _connect(api, admin)
    shop = await db.scalar(select(Shop))
    assert shop is not None
    res = await api.post(f"/api/v1/shops/{shop.id}/sync", headers=admin)
    assert (res.status_code, res.json()) == (202, {"queued": True})
    task, (sent_shop, token), queue, _ = sent_jobs[-1]
    assert (task, sent_shop, queue) == ("platforms.sync_shop_orders", str(shop.id), "sync")
    assert isinstance(token, str)
    assert token.startswith("api:")  # token chủ lock: job nhả bằng compare-and-delete (G3-N4)
    res = await api.post(f"/api/v1/shops/{shop.id}/sync", headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (409, "SYNC_IN_PROGRESS")
    await platforms.release_sync_lock(shop.id)

    shop.auth_status = "EXPIRED"
    await db.flush()
    res = await api.post(f"/api/v1/shops/{shop.id}/sync", headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (409, "SHOP_NOT_CONNECTED")
    assert (await api.post(f"/api/v1/shops/{uuid.uuid4()}/sync", headers=admin)).status_code == 404

    sup = await _login(api, db, "tst_sup", "SUPERVISOR")
    assert (await api.get("/api/v1/shops", headers=sup)).status_code == 403
    assert (await api.post("/api/v1/shops/shopee/auth-url", headers=sup)).status_code == 403


# ---------------------------------------------------------------- tra sàn khi quét (adapter Shopee, BR-04)


def _shopee_adapter() -> ShopeeAdapter:
    async def _no_sleep(_: float) -> None:
        return None

    return ShopeeAdapter(ShopeeClient(2001234, "k", BASE, max_attempts=2, sleep=_no_sleep))


async def _connected_shop(db: AsyncSession, settings: Settings) -> Shop:
    shop = Shop(platform="SHOPEE", platform_shop_id="990001", name="Shop ABC")
    platforms.store_credentials(
        shop, ShopCredentials("990001", "acc", "ref", NOW + timedelta(hours=4)), Cipher(settings.fernet_key)
    )
    db.add(shop)
    await db.flush()
    return shop


def _ok(response: Any) -> httpx.Response:
    return httpx.Response(200, json={"error": "", "message": "", "request_id": "r", "response": response})


async def _scan(api: AsyncClient, h: dict[str, str], code: str) -> httpx.Response:
    return await api.post(
        "/api/v1/station/scan", headers=h, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )


@respx.mock
async def test_scan_lookup_with_shopee_adapter(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """TC-03.12 với adapter Shopee (HTTP giả): mã chưa có → tra sàn → mở phiên có sản phẩm, đơn gắn shop."""
    clock.freeze(NOW)
    _use(api, _shopee_adapter())
    shop = await _connected_shop(db, test_settings)
    await make_station_account(db)
    station = await _login(api, db, "tst_station01", "STATION", "STATION")
    respx.get(f"{BASE}/api/v2/order/get_order_list").mock(
        return_value=_ok(
            {"more": False, "order_list": [{"order_sn": "2410SPE001", "order_status": "PROCESSED"}]}
        )
    )
    respx.get(f"{BASE}/api/v2/logistics/get_tracking_number").mock(
        return_value=_ok({"tracking_number": "SPXVN0000777"})
    )
    respx.get(f"{BASE}/api/v2/order/get_order_detail").mock(
        return_value=_ok(
            {
                "order_list": [
                    {
                        "order_sn": "2410SPE001",
                        "order_status": "PROCESSED",
                        "package_list": [],
                        "item_list": [{"item_name": "Áo thun", "model_quantity_purchased": 1}],
                    }
                ]
            }
        )
    )
    res = await _scan(api, station, "spxvn0000777")
    body = res.json()
    assert body["outcome"] == "SESSION_OPENED", body
    assert "UNVERIFIED" not in body["state"]["session"]["flags"]
    order = await db.scalar(select(Order).where(Order.platform_order_sn == "2410SPE001"))
    assert order is not None
    assert (order.shop_id, order.source) == (shop.id, "API")


@respx.mock
async def test_scan_lookup_shopee_slow_cut_at_2s(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """TC-03.13 với adapter Shopee: sàn trả lời sau 3 giây → cắt ở 2 giây, phiên UNVERIFIED."""
    _use(api, _shopee_adapter())
    await _connected_shop(db, test_settings)
    await make_station_account(db)
    station = await _login(api, db, "tst_station01", "STATION", "STATION")

    async def _slow(_: httpx.Request) -> httpx.Response:
        import asyncio

        await asyncio.sleep(3)
        return _ok({"more": False, "order_list": []})

    respx.get(f"{BASE}/api/v2/order/get_order_list").mock(side_effect=_slow)
    started = time.monotonic()
    body = (await _scan(api, station, "SPXVN0000888")).json()
    assert time.monotonic() - started < 2.8
    assert body["outcome"] == "SESSION_OPENED"
    assert body["state"]["session"]["flags"] == ["UNVERIFIED"]


@respx.mock
async def test_scan_without_connected_shop_does_not_call_shopee(api: AsyncClient, db: AsyncSession) -> None:
    """Chưa kết nối shop: không gọi Shopee, mở phiên chưa xác minh ngay (BR-04)."""
    _use(api, _shopee_adapter())
    route = respx.get(url__startswith=BASE).mock(return_value=_ok({}))
    await make_station_account(db)
    station = await _login(api, db, "tst_station01", "STATION", "STATION")
    body = (await _scan(api, station, "SPXVN0000999")).json()
    assert body["state"]["session"]["flags"] == ["UNVERIFIED"]
    assert route.call_count == 0


async def test_callback_requires_state_cookie_of_same_browser(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], mock: MockAdapter
) -> None:
    """G3-N7: link callback hợp lệ mở ở trình duyệt khác (không có / sai cookie băm `state`) → `error`, không
    gắn shop; `state` không bị đốt nên trình duyệt đúng vẫn hoàn tất được."""
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    cookie = res.headers["set-cookie"].lower()
    assert "httponly" in cookie
    assert "samesite=lax" in cookie
    url = urlparse(res.json()["url"])
    good = api.cookies.get("aicam_shopee_state")
    assert good

    api.cookies.clear()
    other = await api.get(f"{url.path}?{url.query}")
    assert other.headers["location"].endswith("result=error")
    forged = await api.get(f"{url.path}?{url.query}", headers={"cookie": "aicam_shopee_state=" + "0" * 64})
    assert forged.headers["location"].endswith("result=error")
    assert await db.scalar(select(Shop).where(Shop.platform_shop_id == MOCK_SHOP_ID)) is None

    ok = await api.get(f"{url.path}?{url.query}", headers={"cookie": f"aicam_shopee_state={good}"})
    assert ok.headers["location"].endswith("result=connected&count=1")
