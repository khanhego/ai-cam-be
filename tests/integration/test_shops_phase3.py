"""T-207: API-70 mở rộng, API-71 theo sàn, API-72 / API-155 (không ngắt shop khác, `expired`, `count`),
API-154, API-156, `RESULT_PATH`, audit `SHOP_CONNECT` / `SHOP_DISCONNECT`, WS `shop.updated` kênh `ws:admin`
(FR-05.13, 05.14, 05.20; AC-40, AC-44). Adapter TikTok giả trong test (thật / mock 2 shop: T-208..T-211).
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.redis import get_redis
from aicam.core.security import AccessClaims, Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAuthError, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.router import get_platform_adapter, get_tiktok_adapter
from aicam.realtime.hub import channels_for

from .factories import PASSWORD, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 2, 0, tzinfo=UTC)


class FakeTikTok(MockAdapter):
    """TikTok giả: một lần ủy quyền (`open_id` OPEN-1) trả nhiều shop."""

    code = "TIKTOK"

    def __init__(self, shops: tuple[str, ...] = ("TTA", "TTB")) -> None:
        super().__init__()
        self.shops = shops

    def build_auth_url(self, redirect_url: str, state: str) -> str:
        return f"https://services.tiktokshop.test/open/authorize?service_id=1&state={state}&r={redirect_url}"

    async def exchange_code(self, code: str, shop_id: str | None) -> list[ShopCredentials]:
        if code != "TT-CODE":
            raise PlatformAuthError("36004004: code")
        exp = clock.now() + timedelta(days=7)
        return [
            ShopCredentials(
                s, f"tt-acc-{s}", "tt-ref", exp, shop_cipher=f"CIPHER-{s}", grant_ref="OPEN-1",
                shop_name=f"TST TikTok {s[-1]} (mock)", region="VN",
            )
            for s in self.shops
        ]  # fmt: skip


@pytest.fixture(autouse=True)
def _env(api: AsyncClient, test_settings: Settings) -> MockAdapter:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = True
    test_settings.tiktok_returns_enabled = False
    shopee = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: shopee  # type: ignore[attr-defined]
    return shopee


@pytest.fixture
def tiktok(api: AsyncClient) -> FakeTikTok:
    adapter = FakeTikTok()
    api._transport.app.dependency_overrides[get_tiktok_adapter] = lambda: adapter  # type: ignore[attr-defined]
    return adapter


async def _login(api: AsyncClient, db: AsyncSession, username: str, role: str) -> dict[str, str]:
    await make_user(db, username, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture
async def admin(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    return await _login(api, db, "tst_admin7", "ADMIN")


async def _tiktok_connect(api: AsyncClient, admin: dict[str, str], code: str = "TT-CODE") -> Any:
    res = await api.post("/api/v1/shops/tiktok/auth-url", headers=admin)
    assert res.status_code == 200, res.text
    state = parse_qs(urlparse(res.json()["url"]).query)["state"][0]
    assert "httponly" in res.headers["set-cookie"].lower()
    assert "path=/api/v1/shops/tiktok/callback" in res.headers["set-cookie"].lower()
    return await api.get(
        "/api/v1/shops/tiktok/callback", params={"state": state, "code": code, "app_key": "x", "locale": "vi"}
    )


async def _shop(db: AsyncSession, settings: Settings, psid: str, platform: str = "SHOPEE", **kw: Any) -> Shop:
    status = kw.pop("auth_status", "CONNECTED")
    shop = Shop(platform=platform, platform_shop_id=psid, name=kw.pop("name", f"TST {psid}"), **kw)
    platforms.store_credentials(
        shop, ShopCredentials(psid, "a", "r", NOW + timedelta(hours=4)), Cipher(settings.fernet_key)
    )
    shop.auth_status = status
    db.add(shop)
    await db.flush()
    return shop


# ---------------------------------------------------------------- API-70 / API-156


async def test_api70_platforms_and_all_shops(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], test_settings: Settings
) -> None:
    """API-70: `platforms[]` theo cờ (TikTok trả hàng tắt), mọi shop kể cả đã ngắt, sắp theo sàn rồi tạo
    trước; `region`, `sync_warnings`, `disconnected_at`, `sync_in_progress`."""
    test_settings.tiktok_enabled = False
    tt = await _shop(db, test_settings, "TTA", "TIKTOK", region="VN")
    a = await _shop(db, test_settings, "990001")
    a.sync_warnings = [
        {"code": "TRACKING_OWNED_BY_OTHER_SHOP", "tracking_number": "SPX1", "message": "Mã vận đơn SPX1…",
         "at": "2026-10-07T01:00:00Z"},
    ]  # fmt: skip
    b = await _shop(db, test_settings, "990002")
    b.auth_status, b.disconnected_at = "DISCONNECTED", NOW
    await db.flush()
    token = await platforms.acquire_sync_lock(a.id, "test")

    body = (await api.get("/api/v1/shops", headers=admin)).json()

    assert body["platforms"] == [
        {"platform": "SHOPEE", "enabled": True, "returns_enabled": True, "configured": True},
        {"platform": "TIKTOK", "enabled": False, "returns_enabled": False, "configured": False},
    ]
    assert [i["id"] for i in body["items"]] == [str(a.id), str(b.id), str(tt.id)]
    by_id = {i["id"]: i for i in body["items"]}
    assert by_id[str(a.id)]["sync_in_progress"] is True
    assert by_id[str(a.id)]["sync_warnings"][0]["code"] == "TRACKING_OWNED_BY_OTHER_SHOP"
    assert by_id[str(b.id)]["auth_status"] == "DISCONNECTED"
    assert by_id[str(b.id)]["disconnected_at"] == "2026-10-07T02:00:00Z"
    assert by_id[str(tt.id)]["region"] == "VN"
    assert "shop_cipher" not in by_id[str(tt.id)]
    await platforms.release_sync_lock(a.id, token)


async def test_api156_brief_roles_and_order(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    await _shop(db, test_settings, "TTB", "TIKTOK", name="B TikTok")
    await _shop(db, test_settings, "990002", name="Z Shopee")
    gone = await _shop(db, test_settings, "990001", name="A Shopee", auth_status="DISCONNECTED")
    for role in ("ADMIN", "SUPERVISOR", "CSKH"):
        headers = await _login(api, db, f"tst_brief_{role.lower()}", role)
        res = await api.get("/api/v1/shops/brief", headers=headers)
        assert res.status_code == 200
        items = res.json()["items"]
        assert [(i["platform"], i["name"]) for i in items] == [
            ("SHOPEE", "A Shopee"),
            ("SHOPEE", "Z Shopee"),
            ("TIKTOK", "B TikTok"),
        ]
        assert items[0] == {
            "id": str(gone.id), "platform": "SHOPEE", "name": "A Shopee", "auth_status": "DISCONNECTED"
        }  # fmt: skip


# ---------------------------------------------------------------- API-71 / API-155


async def test_api71_platform_errors(
    api: AsyncClient, admin: dict[str, str], test_settings: Settings, tiktok: FakeTikTok
) -> None:
    """Sàn lạ → 404; TikTok tắt → 503 câu riêng TikTok (EX-T1, AC-44)."""
    res = await api.post("/api/v1/shops/lazada/auth-url", headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (404, "NOT_FOUND")
    test_settings.tiktok_enabled = False
    res = await api.post("/api/v1/shops/tiktok/auth-url", headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (503, "PLATFORM_NOT_CONFIGURED")
    assert res.json()["error"]["message"] == (
        "Chưa cấu hình TikTok Shop. Liên hệ IT để bật (cần tài khoản đối tác TikTok Shop)."
    )


async def test_tiktok_callback_adds_all_shops_of_grant(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], tiktok: FakeTikTok,
    sent_jobs: list[tuple[str, list[Any], str, float]], test_settings: Settings,
) -> None:  # fmt: skip
    """API-155 (FR-05.13): một lần ủy quyền → 2 shop TikTok, `count=2`, cùng `grant_ref`, `shop_cipher` lưu
    (không trả API); shop Shopee đang kết nối không bị ngắt (FR-05.14); audit + J-04 mỗi shop; J-13 chỉ khi
    bật cờ trả hàng TikTok."""
    shopee = await _shop(db, test_settings, "990001")
    res = await _tiktok_connect(api, admin)
    assert res.status_code == 302
    assert res.headers["location"] == "/admin/settings/platforms?platform=tiktok&result=connected&count=2"
    shops = (
        await db.scalars(select(Shop).where(Shop.platform == "TIKTOK").order_by(Shop.platform_shop_id))
    ).all()
    assert [(s.platform_shop_id, s.auth_status, s.grant_ref, s.shop_cipher, s.region) for s in shops] == [
        ("TTA", "CONNECTED", "OPEN-1", "CIPHER-TTA", "VN"),
        ("TTB", "CONNECTED", "OPEN-1", "CIPHER-TTB", "VN"),
    ]
    assert shops[0].name == "TST TikTok A (mock)"
    await db.refresh(shopee)
    assert shopee.auth_status == "CONNECTED"
    audits = (await db.scalars(select(AuditLog).where(AuditLog.action == "SHOP_CONNECT"))).all()
    assert sorted(a.object_id for a in audits) == sorted(str(s.id) for s in shops)
    sent = [(t, args[0]) for t, args, *_ in sent_jobs]
    assert sorted(sent) == sorted(("platforms.sync_shop_orders", str(s.id)) for s in shops)


async def test_tiktok_reauth_without_shop_marks_not_authorized(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], tiktok: FakeTikTok
) -> None:
    """Ủy quyền lại cùng tài khoản TikTok nhưng bỏ chọn shop B → B `EXPIRED` + `SHOP_NOT_AUTHORIZED`; A vẫn
    kết nối; shop đã ngắt không đổi."""
    await _tiktok_connect(api, admin)
    tiktok.shops = ("TTA",)
    res = await _tiktok_connect(api, admin)
    assert res.headers["location"].endswith("result=connected&count=1")
    rows = {
        s.platform_shop_id: s for s in (await db.scalars(select(Shop).where(Shop.platform == "TIKTOK"))).all()
    }
    for s in rows.values():
        await db.refresh(s)
    assert rows["TTA"].auth_status == "CONNECTED"
    assert rows["TTB"].auth_status == "EXPIRED"
    assert rows["TTB"].last_error is not None
    assert rows["TTB"].last_error["code"] == "SHOP_NOT_AUTHORIZED"


async def test_callback_results_denied_expired_error(
    api: AsyncClient, admin: dict[str, str], tiktok: FakeTikTok
) -> None:
    """`denied` (không có code), `expired` (state hết hạn / đã dùng), `error` (đổi code lỗi)."""
    res = await api.post("/api/v1/shops/tiktok/auth-url", headers=admin)
    state = parse_qs(urlparse(res.json()["url"]).query)["state"][0]
    cookie = api.cookies.get("aicam_tiktok_state")
    res = await api.get("/api/v1/shops/tiktok/callback", params={"state": state})
    assert res.headers["location"] == "/admin/settings/platforms?platform=tiktok&result=denied"
    res = await api.get(
        "/api/v1/shops/tiktok/callback", params={"state": state, "code": "TT-CODE"},
        headers={"cookie": f"aicam_tiktok_state={cookie}"},
    )  # fmt: skip
    assert res.headers["location"] == "/admin/settings/platforms?platform=tiktok&result=expired"
    res = await _tiktok_connect(api, admin, code="SAI")
    assert res.headers["location"] == "/admin/settings/platforms?platform=tiktok&result=error"


async def test_state_expires_after_10_minutes(
    api: AsyncClient, admin: dict[str, str], tiktok: FakeTikTok
) -> None:
    res = await api.post("/api/v1/shops/tiktok/auth-url", headers=admin)
    state = parse_qs(urlparse(res.json()["url"]).query)["state"][0]
    ttl = await get_redis().ttl(f"oauth:TIKTOK:{state}")
    assert 590 <= ttl <= 600
    await get_redis().delete(f"oauth:TIKTOK:{state}")  # hết hạn
    res = await api.get("/api/v1/shops/tiktok/callback", params={"state": state, "code": "TT-CODE"})
    assert res.headers["location"].endswith("result=expired")


# ---------------------------------------------------------------- API-154 / API-73


async def test_disconnect_one_shop(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], test_settings: Settings
) -> None:
    """API-154 (EX-T7, AC-40): ngắt A → token xóa, `DISCONNECTED`, audit; B không đổi; gọi lại → 200 không
    audit thêm; shop lạ → 404; SUPERVISOR → 403."""
    a = await _shop(db, test_settings, "990001")
    b = await _shop(db, test_settings, "990002")
    res = await api.post(f"/api/v1/shops/{a.id}/disconnect", headers=admin)
    assert res.status_code == 200
    body = res.json()
    assert (body["auth_status"], body["disconnected_at"]) == ("DISCONNECTED", "2026-10-07T02:00:00Z")
    await db.refresh(a)
    await db.refresh(b)
    assert (a.access_token_enc, a.refresh_token_enc, a.auth_expires_at) == (None, None, None)
    assert a.disconnected_by is not None
    assert b.auth_status == "CONNECTED"
    res = await api.post(f"/api/v1/shops/{a.id}/disconnect", headers=admin)
    assert res.status_code == 200
    count = await db.scalar(
        select(func.count()).select_from(AuditLog).where(AuditLog.action == "SHOP_DISCONNECT")
    )
    assert count == 1
    assert (await api.post(f"/api/v1/shops/{uuid.uuid4()}/disconnect", headers=admin)).status_code == 404
    sup = await _login(api, db, "tst_sup7", "SUPERVISOR")
    assert (await api.post(f"/api/v1/shops/{b.id}/disconnect", headers=sup)).status_code == 403
    # Kết nối lại shop đã ngắt: cùng dòng, xóa `disconnected_*`.
    res = await api.post("/api/v1/shops/shopee/auth-url", headers=admin)
    url = urlparse(res.json()["url"])
    await api.get(f"{url.path}?{url.query}")
    await db.refresh(a)
    assert (a.auth_status, a.disconnected_at) == ("CONNECTED", None)


async def test_api73_tiktok_shop_when_tiktok_disabled(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], test_settings: Settings
) -> None:
    tt = await _shop(db, test_settings, "TTA", "TIKTOK")
    test_settings.tiktok_enabled = False
    res = await api.post(f"/api/v1/shops/{tt.id}/sync", headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (503, "PLATFORM_NOT_CONFIGURED")
    assert "TikTok" in res.json()["error"]["message"]


# ---------------------------------------------------------------- WS `shop.updated` (ws:admin)


def test_ws_admin_channel_only_for_admin() -> None:
    exp = datetime.now(UTC) + timedelta(minutes=5)
    admin = AccessClaims(user_id=uuid.uuid4(), role="ADMIN", station_id=None, expires_at=exp)
    sup = AccessClaims(user_id=uuid.uuid4(), role="SUPERVISOR", station_id=None, expires_at=exp)
    assert "ws:admin" in (channels_for("dashboard", admin) or [])
    assert "ws:admin" not in (channels_for("dashboard", sup) or [])


async def test_connect_and_disconnect_publish_shop_updated(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str], tiktok: FakeTikTok, redis_client: Any
) -> None:
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ws:admin")
    await _tiktok_connect(api, admin)
    shop = await db.scalar(select(Shop).where(Shop.platform_shop_id == "TTA"))
    assert shop is not None
    await api.post(f"/api/v1/shops/{shop.id}/disconnect", headers=admin)
    messages: list[dict[str, Any]] = []
    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if msg:
            messages.append(json.loads(msg["data"]))
        elif len(messages) >= 3:
            break
        await asyncio.sleep(0)
    await pubsub.aclose()
    events = [(m["type"], m["data"]["shop_id"], m["data"]["auth_status"]) for m in messages]
    assert ("shop.updated", str(shop.id), "CONNECTED") in events
    assert ("shop.updated", str(shop.id), "DISCONNECTED") in events
