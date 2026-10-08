"""Adapter Shopee v2 với HTTP giả (respx) theo định dạng tài liệu công khai Shopee Open Platform v2.

**Chưa test với Shopee thật — thiếu tài khoản partner (T-3).** TC-05.08 / 05.09 (mức HTTP), FR-05.03, 05.04,
05.06, 05.08.
"""

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from aicam.core import clock
from aicam.modules.platforms.base import PlatformAuthError, PlatformError, ShipmentRef, ShopCredentials
from aicam.modules.platforms.shopee import mapping
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient, ShopeeRequestError

BASE = "https://partner.test-stable.shopeemobile.com"
PARTNER_ID = 2001234
KEY = "test-partner-key"
NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)
CREDS = ShopCredentials("990001", "acc-tok-123", "ref-tok-456", NOW + timedelta(hours=4))


@pytest.fixture(autouse=True)
def _clock() -> None:
    clock.freeze(NOW)


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def adapter(sleeps: list[float]) -> ShopeeAdapter:
    async def _sleep(s: float) -> None:
        sleeps.append(s)

    client = ShopeeClient(PARTNER_ID, KEY, BASE, max_attempts=5, backoff_s=0.5, sleep=_sleep)
    return ShopeeAdapter(client, lookup_lookback=timedelta(minutes=60))


def ok(response: Any = None, **top: Any) -> httpx.Response:
    body: dict[str, Any] = {"error": "", "message": "", "request_id": "req-1", **top}
    if response is not None:
        body["response"] = response
    return httpx.Response(200, json=body)


def _expected_sign(path: str, ts: int, token: str = "", shop: str = "") -> str:
    base = f"{PARTNER_ID}{path}{ts}{token}{shop}"
    return hmac.new(KEY.encode(), base.encode(), hashlib.sha256).hexdigest()


def _detail(
    sn: str, status: str = "PROCESSED", packages: int = 1, logistics: str = "LOGISTICS_READY"
) -> dict[str, Any]:
    return {
        "order_sn": sn,
        "order_status": status,
        "create_time": 1791100000,
        "update_time": 1791160000,
        "message_to_seller": "Gói kỹ",
        "item_list": [
            {
                "item_name": "Áo thun",
                "item_sku": "AT",
                "model_name": "Đen,L",
                "model_sku": "AT-DEN-L",
                "model_quantity_purchased": 2,
                "image_info": {"image_url": "https://cf.shopee/img"},
            },
            {"item_name": "Quà tặng", "model_quantity_purchased": 0},
        ],
        "package_list": [
            {"package_number": f"PK{sn}-{i}", "logistics_status": logistics} for i in range(1, packages + 1)
        ],
    }


# ---------------------------------------------------------------- ký + ủy quyền


def test_auth_partner_url_signed_with_public_base(adapter: ShopeeAdapter) -> None:
    """FR-05.01: auth_partner ký `partner_id + path + timestamp`, giữ nguyên `redirect` (có state)."""
    url = adapter.build_auth_url("https://kho.local/api/v1/shops/shopee/callback?state=abc", "abc")
    parsed = urlparse(url)
    q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
    ts = int(NOW.timestamp())
    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == f"{BASE}/api/v2/shop/auth_partner"
    assert q["partner_id"] == str(PARTNER_ID)
    assert q["timestamp"] == str(ts)
    assert q["sign"] == _expected_sign("/api/v2/shop/auth_partner", ts)
    assert q["redirect"] == "https://kho.local/api/v1/shops/shopee/callback?state=abc"


@respx.mock
async def test_exchange_code_and_shop_call_signature(adapter: ShopeeAdapter) -> None:
    """token/get ký mức public, body có code / shop_id / partner_id; API cấp shop ký thêm token + shop_id."""
    token_route = respx.post(f"{BASE}/api/v2/auth/token/get").mock(
        return_value=ok(access_token="acc-new", refresh_token="ref-new", expire_in=14400)
    )
    info_route = respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(return_value=ok(shop_name="Shop ABC"))
    (creds,) = await adapter.exchange_code("CODE-1", "990001")
    assert creds == ShopCredentials(
        "990001", "acc-new", "ref-new", NOW + timedelta(hours=4), grant_ref="990001"
    )  # Phase 3: list một phần tử, `grant_ref` = mã shop (02a §2 base.py)
    req = token_route.calls.last.request
    assert json.loads(req.content) == {"code": "CODE-1", "shop_id": 990001, "partner_id": PARTNER_ID}
    q = {k: v[0] for k, v in parse_qs(req.url.query.decode()).items()}
    assert q["sign"] == _expected_sign("/api/v2/auth/token/get", int(NOW.timestamp()))
    assert "access_token" not in q

    assert await adapter.shop_name(creds) == "Shop ABC"
    q = {k: v[0] for k, v in parse_qs(info_route.calls.last.request.url.query.decode()).items()}
    ts = int(NOW.timestamp())
    assert q["sign"] == _expected_sign("/api/v2/shop/get_shop_info", ts, "acc-new", "990001")
    assert (q["access_token"], q["shop_id"]) == ("acc-new", "990001")


@respx.mock
async def test_refresh_rejected_is_auth_error(adapter: ShopeeAdapter, sleeps: list[float]) -> None:
    """TC-05.10 (mức HTTP): refresh token hỏng → PlatformAuthError, không thử lại."""
    route = respx.post(f"{BASE}/api/v2/auth/access_token/get").mock(
        return_value=httpx.Response(401, json={"error": "error_auth", "message": "Invalid refresh_token"})
    )
    with pytest.raises(PlatformAuthError):
        await adapter.refresh(CREDS)
    assert route.call_count == 1
    assert sleeps == []
    body = json.loads(route.calls.last.request.content)
    assert body == {"refresh_token": "ref-tok-456", "shop_id": 990001, "partner_id": PARTNER_ID}


# ---------------------------------------------------------------- thử lại / rate limit (FR-05.08)


@respx.mock
async def test_retry_503_twice_then_ok(adapter: ShopeeAdapter, sleeps: list[float]) -> None:
    """TC-05.08 (mức HTTP): 503 hai lần rồi OK → thành công, giãn cách 0,5 → 1 giây."""
    route = respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(
        side_effect=[httpx.Response(503), httpx.Response(503), ok(shop_name="Shop ABC")]
    )
    assert await adapter.shop_name(CREDS) == "Shop ABC"
    assert route.call_count == 3
    assert sleeps == [0.5, 1.0]


@respx.mock
async def test_rate_limit_respects_retry_after_and_error_code(
    adapter: ShopeeAdapter, sleeps: list[float]
) -> None:
    """429 + Retry-After → chờ đúng số giây; mã lỗi tạm trong body (HTTP 200) cũng thử lại."""
    respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "2"}, json={"error": "error_too_many_request"}),
            httpx.Response(200, json={"error": "error_server", "message": "busy"}),
            ok(shop_name="Shop ABC"),
        ]
    )
    assert await adapter.shop_name(CREDS) == "Shop ABC"
    assert sleeps == [2.0, 1.0]


@respx.mock
async def test_gives_up_after_max_attempts(adapter: ShopeeAdapter, sleeps: list[float]) -> None:
    """TC-05.09 (mức HTTP): 503 liên tục → PlatformError sau 5 lần; lỗi mạng cũng thử lại."""
    route = respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(
        side_effect=[httpx.ConnectError("down"), *[httpx.Response(503)] * 4]
    )
    with pytest.raises(PlatformError, match="5 lần"):
        await adapter.shop_name(CREDS)
    assert route.call_count == 5
    assert sleeps == [0.5, 1.0, 2.0, 4.0]


class _Recorder:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def info(self, event: str, **kw: Any) -> None:
        self.events.append({"event": event, **kw})

    warning = info


@respx.mock
async def test_param_error_not_retried_and_logs_hide_token(
    adapter: ShopeeAdapter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lỗi tham số → ShopeeRequestError ngay; log ghi mỗi lần gọi nhưng không có token / sign."""
    from aicam.modules.platforms.shopee import client as client_module

    logs = _Recorder()
    monkeypatch.setattr(client_module, "log", logs)
    respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(
        return_value=httpx.Response(200, json={"error": "error_param", "message": "bad", "request_id": "r9"})
    )
    with pytest.raises(ShopeeRequestError):
        await adapter.shop_name(CREDS)
    assert [e["event"] for e in logs.events] == ["shopee_call"]
    assert logs.events[0]["request_id"] == "r9"
    assert logs.events[0]["error"] == "error_param"
    assert "acc-tok-123" not in repr(logs.events)
    assert "sign" not in logs.events[0]


# ---------------------------------------------------------------- đơn (FR-05.02, 05.03)


@respx.mock
async def test_list_updated_orders_pages_details_tracking(adapter: ShopeeAdapter) -> None:
    """get_order_list phân trang theo cursor → get_order_detail → get_tracking_number từng kiện; đơn hủy
    không hỏi mã vận đơn."""
    lists = respx.get(f"{BASE}/api/v2/order/get_order_list").mock(
        side_effect=[
            ok(
                {
                    "more": True,
                    "next_cursor": "c2",
                    "order_list": [{"order_sn": "SN1", "order_status": "PROCESSED"}],
                }
            ),
            ok(
                {
                    "more": False,
                    "next_cursor": "",
                    "order_list": [{"order_sn": "SN2", "order_status": "CANCELLED"}],
                }
            ),
        ]
    )
    respx.get(f"{BASE}/api/v2/order/get_order_detail").mock(
        return_value=ok({"order_list": [_detail("SN1", packages=2), _detail("SN2", status="CANCELLED")]})
    )

    def _tracking(request: httpx.Request) -> httpx.Response:
        package = request.url.params.get("package_number")
        return ok({"tracking_number": {"PKSN1-1": "spxvn001", "PKSN1-2": "SPXVN002"}[package]})

    tracking = respx.get(f"{BASE}/api/v2/logistics/get_tracking_number").mock(side_effect=_tracking)
    orders = [o async for o in adapter.list_updated_orders(CREDS, NOW - timedelta(minutes=10))]

    assert lists.calls[1].request.url.params["cursor"] == "c2"
    first = lists.calls[0].request.url.params
    assert first["time_range_field"] == "update_time"
    assert int(first["time_to"]) - int(first["time_from"]) == 600
    assert tracking.call_count == 2  # SN2 hủy: không gọi
    sn1, sn2 = orders
    assert sn1.tracking_numbers == ("SPXVN001", "SPXVN002")
    assert sn1.status == "PROCESSED"
    assert sn1.buyer_note == "Gói kỹ"
    assert [(i.product_name, i.quantity, i.sku, i.variation) for i in sn1.items] == [
        ("Áo thun", 2, "AT-DEN-L", "Đen,L")
    ]
    assert sn1.raw["tracking_by_package"] == {"PKSN1-1": "SPXVN001", "PKSN1-2": "SPXVN002"}
    assert sn2.is_cancelled
    assert sn2.tracking_numbers == ()


@respx.mock
async def test_order_list_windows_max_15_days(adapter: ShopeeAdapter) -> None:
    """Khoảng `update_time` > 15 ngày → tách nhiều lần gọi (giới hạn get_order_list)."""
    route = respx.get(f"{BASE}/api/v2/order/get_order_list").mock(
        return_value=ok({"more": False, "order_list": []})
    )
    assert [o async for o in adapter.list_updated_orders(CREDS, NOW - timedelta(days=20))] == []
    assert route.call_count == 2


@respx.mock
async def test_find_by_tracking_scans_recent_then_caches(adapter: ShopeeAdapter) -> None:
    """FR-05.06: dò đơn cập nhật 60 phút gần nhất, khớp mã → chi tiết; lần sau dùng bộ nhớ, không dò lại."""
    lists = respx.get(f"{BASE}/api/v2/order/get_order_list").mock(
        return_value=ok(
            {
                "more": False,
                "order_list": [
                    {"order_sn": "SNA", "order_status": "PROCESSED"},
                    {"order_sn": "SNB", "order_status": "UNPAID"},
                    {"order_sn": "SNC", "order_status": "READY_TO_SHIP"},
                ],
            }
        )
    )

    def _tracking(request: httpx.Request) -> httpx.Response:
        return ok(
            {"tracking_number": {"SNA": "SPXVNAAA1", "SNC": "SPXVNCCC1"}[request.url.params["order_sn"]]}
        )

    respx.get(f"{BASE}/api/v2/logistics/get_tracking_number").mock(side_effect=_tracking)
    respx.get(f"{BASE}/api/v2/order/get_order_detail").mock(
        return_value=ok({"order_list": [_detail("SNC", packages=0)]})
    )
    found = await adapter.find_by_tracking(CREDS, "spxvnccc1")
    assert found is not None
    assert found.platform_order_sn == "SNC"
    params = lists.calls.last.request.url.params
    assert int(params["time_to"]) - int(params["time_from"]) == 3600

    again = await adapter.find_by_tracking(CREDS, "SPXVNCCC1")
    assert again is not None
    assert lists.call_count == 1
    assert await adapter.find_by_tracking(None, "SPXVNCCC1") is None


# ---------------------------------------------------------------- vận chuyển (FR-05.04)


@respx.mock
async def test_shipping_statuses(adapter: ShopeeAdapter) -> None:
    """Một kiện: theo logistics_status; đơn COMPLETED → DELIVERED; đơn hủy trả `order_status`."""
    respx.get(f"{BASE}/api/v2/order/get_order_detail").mock(
        return_value=ok(
            {
                "order_list": [
                    _detail("S1", "SHIPPED", logistics="LOGISTICS_PICKUP_DONE"),
                    _detail("S2", "COMPLETED", logistics="LOGISTICS_DELIVERY_DONE"),
                    _detail("S3", "CANCELLED", logistics="LOGISTICS_REQUEST_CANCELED"),
                ]
            }
        )
    )
    out = await adapter.get_shipping_statuses(
        CREDS, [ShipmentRef("S1", "SPXA0001"), ShipmentRef("S2", "SPXB0001"), ShipmentRef("S3", "SPXC0001")]
    )
    assert [(s.tracking_number, s.warehouse_hint, s.order_status) for s in out] == [
        ("SPXA0001", "HANDED_OVER", "SHIPPED"),
        ("SPXB0001", "DELIVERED", "COMPLETED"),
        ("SPXC0001", None, "CANCELLED"),
    ]


def test_warehouse_hint_mapping() -> None:
    assert mapping.warehouse_hint("PROCESSED", "LOGISTICS_PICKUP_DONE") == "HANDED_OVER"
    assert mapping.warehouse_hint("SHIPPED", "LOGISTICS_DELIVERY_DONE") == "DELIVERED"
    assert mapping.warehouse_hint("TO_CONFIRM_RECEIVE", "") == "DELIVERED"
    assert mapping.warehouse_hint("READY_TO_SHIP", "LOGISTICS_READY") is None


# ---------------------------------------------------------------- G3-N6 / N8


@respx.mock
async def test_auth_calls_are_not_retried(adapter: ShopeeAdapter, sleeps: list[float]) -> None:
    """G3-N8: refresh token dùng một lần — `/api/v2/auth/*` lỗi tạm (503, mạng) không gửi lại."""
    route = respx.post(f"{BASE}/api/v2/auth/access_token/get").mock(return_value=httpx.Response(503))
    with pytest.raises(PlatformError) as err:
        await adapter.refresh(CREDS)
    assert not isinstance(err.value, PlatformAuthError)
    assert route.call_count == 1
    assert sleeps == []
    token_route = respx.post(f"{BASE}/api/v2/auth/token/get").mock(
        side_effect=httpx.ConnectTimeout("timeout")
    )
    with pytest.raises(PlatformError):
        await adapter.client.call("POST", "/api/v2/auth/token/get", body={})
    assert token_route.call_count == 1
    assert sleeps == []


@pytest.mark.parametrize("code", ["error_sign", "error_permission", "error_param"])
@respx.mock
async def test_config_errors_are_not_auth_errors(adapter: ShopeeAdapter, code: str) -> None:
    """G3-N6: 403 `error_sign` / `error_permission` là lỗi cấu hình — không đánh shop EXPIRED."""
    respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(
        return_value=httpx.Response(403, json={"error": code, "message": "x", "request_id": "r"})
    )
    with pytest.raises(ShopeeRequestError) as err:
        await adapter.shop_name(CREDS)
    assert not isinstance(err.value, PlatformAuthError)
    assert err.value.error == code


@pytest.mark.parametrize(("status", "code"), [(403, "invalid_access_token"), (401, "")])
@respx.mock
async def test_token_errors_are_auth_errors(adapter: ShopeeAdapter, status: int, code: str) -> None:
    respx.get(f"{BASE}/api/v2/shop/get_shop_info").mock(
        return_value=httpx.Response(status, json={"error": code, "message": "x"})
    )
    with pytest.raises(PlatformAuthError):
        await adapter.shop_name(CREDS)


@respx.mock
async def test_find_by_tracking_bounded_per_scan(adapter: ShopeeAdapter) -> None:
    """G3-P2-12: tra khi quét đọc 1 trang danh sách, hỏi tối đa 10 mã vận đơn; lần sau bỏ qua đơn đã biết."""
    lists = respx.get(f"{BASE}/api/v2/order/get_order_list").mock(
        return_value=ok(
            {
                "more": True,
                "next_cursor": "p2",
                "order_list": [{"order_sn": f"SN{i:02d}", "order_status": "PROCESSED"} for i in range(30)],
            }
        )
    )
    tracking = respx.get(f"{BASE}/api/v2/logistics/get_tracking_number").mock(
        side_effect=lambda r: ok({"tracking_number": f"SPXVN{r.url.params['order_sn']}"})
    )

    assert await adapter.find_by_tracking(CREDS, "SPXVNKHONGCO") is None
    assert lists.call_count == 1
    assert tracking.call_count == 10
    assert await adapter.find_by_tracking(CREDS, "SPXVNKHONGCO") is None
    asked = [c.request.url.params["order_sn"] for c in tracking.calls]
    assert asked[10:] == [f"SN{i:02d}" for i in range(10, 20)]  # 10 đơn đầu đã biết → hỏi 10 đơn kế
