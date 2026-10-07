"""T-208: TikTok client (ký HMAC — vector cố định, thử lại, `Retry-After`, ngân sách, log, che log) + ủy quyền
(token get / refresh, danh sách shop) trên HTTP giả (respx) theo **định dạng giả định** 02a §7.1.

**Chưa test với TikTok thật — thiếu tài khoản đối tác (Q18, Q19).**
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx
from structlog.testing import capture_logs

from aicam.core import clock
from aicam.core.logging import install_stdlib_redaction, redact_query
from aicam.modules.platforms import budget
from aicam.modules.platforms.base import PlatformAuthError, PlatformError, ShopCredentials
from aicam.modules.platforms.tiktok.adapter import TikTokAdapter
from aicam.modules.platforms.tiktok.client import TikTokClient, TikTokRequestError, expires_at, sign

API = "https://open-api.tiktok.test"
AUTH = "https://auth.tiktok.test"
NOW = datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC)  # epoch 1700000000
SECRET = "tst_secret_123"


@pytest.fixture(autouse=True)
def _clock() -> Any:
    clock.freeze(NOW)
    yield
    clock.reset()


def _client(attempts: int = 3, slept: list[float] | None = None) -> TikTokClient:
    async def _sleep(s: float) -> None:
        if slept is not None:
            slept.append(s)

    return TikTokClient("tst_key", SECRET, API, AUTH, max_attempts=attempts, backoff_s=0.5, sleep=_sleep)


def ok(data: dict[str, Any] | None = None, **extra: Any) -> httpx.Response:
    return httpx.Response(
        200, json={"code": 0, "message": "Success", "request_id": "rq-1", "data": data or {}}
    )


# ---------------------------------------------------------------- ký


def test_sign_fixed_vector() -> None:
    """Vector cố định: chuỗi gốc = secret + path + Σ(k+v, k tăng; bỏ `sign`, `access_token`) + body +
    secret."""
    params = {
        "timestamp": 1700000000,
        "app_key": "tst_key",
        "shop_cipher": "GCP_XF90igAAAABh00qsWgtvOiGFNqyubMt3",
        "page_size": 50,
        "access_token": "KHÔNG-KÝ",
        "sign": "cũ",
    }
    body = '{"update_time_ge":1699990000,"update_time_lt":1700000000}'
    assert sign(SECRET, "/order/202309/orders/search", params, body) == (
        "5e3a785c500ab0a709f6edbe75d62955c299d2bc64eb2b15032ca8d73ff4d010"
    )


@respx.mock
async def test_call_signs_query_and_sends_token_header() -> None:
    route = respx.post(f"{API}/order/202309/orders/search").mock(return_value=ok({"orders": []}))
    data = await _client().call(
        "POST", "/order/202309/orders/search", params={"page_size": 50},
        body={"update_time_ge": 1699990000, "update_time_lt": 1700000000},
        access_token="tt-acc", shop_cipher="CIPHER-1",
    )  # fmt: skip
    assert data == {"orders": []}
    req = route.calls.last.request
    q = {k: v[0] for k, v in parse_qs(req.url.query.decode()).items()}
    assert q["app_key"] == "tst_key"
    assert q["timestamp"] == "1700000000"
    assert q["shop_cipher"] == "CIPHER-1"
    assert "access_token" not in q
    assert req.headers["x-tts-access-token"] == "tt-acc"
    body = req.content.decode()
    assert json.loads(body) == {"update_time_ge": 1699990000, "update_time_lt": 1700000000}
    expected = sign(SECRET, "/order/202309/orders/search", {k: v for k, v in q.items() if k != "sign"}, body)
    assert q["sign"] == expected


# ---------------------------------------------------------------- lỗi + thử lại


@respx.mock
async def test_retry_on_429_uses_retry_after_then_succeeds() -> None:
    """AC-43: quá tần suất → thử lại giãn cách theo `Retry-After`; mỗi lần gọi có log `tiktok_call`."""
    slept: list[float] = []
    respx.get(f"{API}/x").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "3"}, json={"code": 36009004, "message": "limit"}),
            httpx.Response(200, json={"code": 36009003, "message": "busy"}),
            ok({"v": 1}),
        ]
    )
    with capture_logs() as logs:
        assert await _client(slept=slept).call("GET", "/x") == {"v": 1}
    assert slept == [3.0, 1.0]  # Retry-After, rồi giãn cách mũ (0,5 × 2¹)
    calls = [e for e in logs if e["event"] == "tiktok_call"]
    assert [(e["attempt"], e.get("http_status"), e.get("code")) for e in calls] == [
        (1, 429, 36009004), (2, 200, 36009003), (3, 200, 0),
    ]  # fmt: skip
    assert all("request_id" in e and "duration_ms" in e for e in calls)


@respx.mock
async def test_5xx_exhausted_raises_platform_error() -> None:
    respx.get(f"{API}/x").mock(return_value=httpx.Response(503, json={}))
    with pytest.raises(PlatformError, match="sau 3 lần thử"):
        await _client(attempts=3).call("GET", "/x")


@respx.mock
async def test_auth_code_raises_auth_error_without_retry() -> None:
    route = respx.get(f"{API}/x").mock(
        return_value=httpx.Response(200, json={"code": 105002, "message": "Expired credentials"})
    )
    with pytest.raises(PlatformAuthError):
        await _client().call("GET", "/x")
    assert route.call_count == 1


@respx.mock
async def test_business_error_no_retry() -> None:
    route = respx.get(f"{API}/x").mock(
        return_value=httpx.Response(200, json={"code": 12345, "message": "bad"})
    )
    with pytest.raises(TikTokRequestError) as err:
        await _client().call("GET", "/x")
    assert err.value.code == 12345
    assert route.call_count == 1


@respx.mock
async def test_budget_stops_long_retry_after() -> None:
    """Ngân sách lượt (`budget.time_budget`): `Retry-After` dài hơn thời gian còn lại → lỗi ngay, không
    ngủ."""
    slept: list[float] = []
    respx.get(f"{API}/x").mock(return_value=httpx.Response(429, headers={"Retry-After": "30"}, json={}))
    with budget.time_budget(5.0), pytest.raises(PlatformError, match="hết thời gian"):
        await _client(slept=slept).call("GET", "/x")
    assert slept == []


@respx.mock
async def test_network_error_retried() -> None:
    slept: list[float] = []
    respx.get(f"{API}/x").mock(side_effect=[httpx.ConnectError("x"), ok({"v": 2})])
    assert await _client(slept=slept).call("GET", "/x") == {"v": 2}
    assert slept == [0.5]


# ---------------------------------------------------------------- ủy quyền


def _token_payload(**over: Any) -> dict[str, Any]:
    data = {
        "access_token": "TTP_acc", "access_token_expire_in": 1700604800, "refresh_token": "TTP_ref",
        "refresh_token_expire_in": 1731536000, "open_id": "OPEN-1", "seller_name": "Áo Đẹp",
    }  # fmt: skip
    data.update(over)
    return data


@respx.mock
async def test_exchange_code_lists_all_authorized_shops() -> None:
    """FR-05.13: đổi code → token chung (grant `open_id`) + mọi shop (`id`, `name`, `region`, `cipher`)."""
    token_route = respx.get(f"{AUTH}/api/v2/token/get").mock(return_value=ok(_token_payload()))
    shops_route = respx.get(f"{API}/authorization/202309/shops").mock(
        return_value=ok(
            {
                "shops": [
                    {"id": "7001", "name": "Áo Đẹp Official", "region": "VN", "cipher": "C1", "code": "VNA"},
                    {"id": "7002", "name": "Áo Đẹp Kids", "region": "VN", "cipher": "C2", "code": "VNB"},
                ]
            }
        )
    )
    adapter = TikTokAdapter(
        _client(), authorize_url="https://services.tiktok.test/open/authorize", service_id="9"
    )
    creds = await adapter.exchange_code("AUTH-CODE-1", None)
    assert [(c.shop_id, c.shop_cipher, c.grant_ref, c.shop_name, c.region) for c in creds] == [
        ("7001", "C1", "OPEN-1", "Áo Đẹp Official", "VN"),
        ("7002", "C2", "OPEN-1", "Áo Đẹp Kids", "VN"),
    ]
    assert creds[0].expires_at == datetime(2023, 11, 21, 22, 13, 20, tzinfo=UTC)  # epoch (tài liệu)
    q = {k: v[0] for k, v in parse_qs(token_route.calls.last.request.url.query.decode()).items()}
    assert q == {
        "app_key": "tst_key", "app_secret": SECRET, "auth_code": "AUTH-CODE-1",
        "grant_type": "authorized_code",
    }  # fmt: skip
    assert shops_route.calls.last.request.headers["x-tts-access-token"] == "TTP_acc"
    url = adapter.build_auth_url("https://x.local/api/v1/shops/tiktok/callback", "st-1")
    assert parse_qs(urlparse(url).query) == {"service_id": ["9"], "state": ["st-1"]}


@respx.mock
async def test_refresh_once_no_retry_keeps_shop_fields() -> None:
    """Làm mới **một lần** (không thử lại — refresh token có thể chỉ dùng một lần); 5xx → `PlatformError` tạm,
    mã token → `PlatformAuthError`."""
    adapter = TikTokAdapter(_client(), authorize_url="a", service_id="9")
    old = ShopCredentials(
        "7001", "a", "r-old", NOW, shop_cipher="C1", grant_ref="OPEN-1", shop_name="S", region="VN"
    )
    route = respx.get(f"{AUTH}/api/v2/token/refresh").mock(
        return_value=ok(_token_payload(access_token="TTP_new", access_token_expire_in=14400))
    )
    fresh = await adapter.refresh(old)
    assert (fresh.access_token, fresh.shop_cipher, fresh.grant_ref) == ("TTP_new", "C1", "OPEN-1")
    assert fresh.expires_at == NOW + timedelta(seconds=14400)  # số nhỏ = giây còn lại (phòng sai — Q19)
    q = {k: v[0] for k, v in parse_qs(route.calls.last.request.url.query.decode()).items()}
    assert (q["refresh_token"], q["grant_type"]) == ("r-old", "refresh_token")

    route.mock(return_value=httpx.Response(502, json={}))
    with pytest.raises(PlatformError) as err:
        await adapter.refresh(old)
    assert not isinstance(err.value, PlatformAuthError)
    assert route.call_count == 2  # không thử lại
    route.mock(return_value=httpx.Response(200, json={"code": 105003, "message": "invalid refresh token"}))
    with pytest.raises(PlatformAuthError):
        await adapter.refresh(old)


def test_expires_at_epoch_or_seconds() -> None:
    assert expires_at(1700003600, NOW) == NOW + timedelta(hours=1)
    assert expires_at(3600, NOW) == NOW + timedelta(hours=1)
    assert expires_at(None, NOW) == NOW + timedelta(hours=4)


# ---------------------------------------------------------------- che log (02a §2 core/logging.py)


@respx.mock
async def test_logs_hide_token_sign_and_app_secret(caplog: pytest.LogCaptureFixture) -> None:
    """Log client không có query / header; kể cả khi bật INFO cho httpx, `sign`, `app_secret`, `auth_code`,
    `refresh_token` trong URL bị che."""
    install_stdlib_redaction()
    caplog.set_level(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.INFO)
    respx.get(f"{AUTH}/api/v2/token/get").mock(return_value=ok(_token_payload()))
    respx.get(f"{API}/authorization/202309/shops").mock(return_value=ok({"shops": [{"id": "1"}]}))
    adapter = TikTokAdapter(_client(), authorize_url="a", service_id="9")
    try:
        with capture_logs() as logs:
            await adapter.exchange_code("AUTH-CODE-SECRET", None)
    finally:
        logging.getLogger("httpx").setLevel(logging.WARNING)
    text = caplog.text + json.dumps(logs, default=str)
    for secret in (SECRET, "AUTH-CODE-SECRET", "TTP_acc", "sign="):
        assert secret not in text, secret
    assert "HTTP Request" in caplog.text
    assert "[app_secret đã che]" in caplog.text
    assert redact_query("/api/v2/token/refresh?app_key=k&refresh_token=r1&app_secret=s") == (
        "/api/v2/token/refresh?app_key=k&[refresh_token đã che]&[app_secret đã che]"
    )
