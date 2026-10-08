"""HTTP client Shopee Open Platform v2: ký request, thử lại giãn cách, xử lý rate limit, log mọi lần gọi.

Theo tài liệu công khai Shopee Open Platform v2 (open.shopee.com → Developer Guide →
"Authorization and Authentication", "API Call"):

- Tham số chung: `partner_id`, `timestamp` (giây, lệch ≤ 5 phút), `sign`; API cấp shop thêm
  `access_token`, `shop_id`.
- `sign` = HMAC-SHA256(partner_key, chuỗi gốc) dạng hex chữ thường; chuỗi gốc API public (auth) =
  `partner_id + path + timestamp`, API cấp shop = `partner_id + path + timestamp + access_token + shop_id`.
- Response JSON luôn có `error` (rỗng khi thành công), `message`, `request_id`; dữ liệu nằm trong `response`
  (riêng nhóm `/api/v2/auth/*` và `shop/get_shop_info` trả trường ở mức gốc).

**Chưa test với Shopee thật — thiếu tài khoản partner (T-3).** Test dùng HTTP giả (respx) theo định dạng trên.
Tên mã lỗi auth / rate limit cụ thể: cần xác nhận ở T-3 (DEC-123).
"""

import asyncio
import hashlib
import hmac
import time
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

from aicam.core import clock
from aicam.modules.platforms import budget
from aicam.modules.platforms.base import PlatformAuthError, PlatformError

log = structlog.get_logger()

# Mã `error` coi là lỗi token (gồm chữ sai chính tả "acceess" trong tài liệu Shopee) — cần xác nhận T-3.
AUTH_ERRORS = frozenset(
    {
        "error_auth",
        "invalid_access_token",
        "invalid_acceess_token",
        "error_invalid_token",
        "invalid_refresh_token",
    }
)
# Mã `error` coi là bị giới hạn tần suất / lỗi tạm phía Shopee → thử lại giãn cách.
RETRY_ERRORS = frozenset(
    {"error_server", "error_inner", "error_busy", "error_too_many_request", "error_rate_limit"}
)

Sleep = Callable[[float], Awaitable[None]]

# G3 F-14: hạn chót (monotonic) của job đang gọi — chờ Retry-After / giãn cách không vượt thời gian còn lại
# của task Celery. Phase 3: chuyển sang `platforms/budget.py` dùng chung Shopee + TikTok (giữ tên cũ).
time_budget = budget.time_budget


class ShopeeRequestError(PlatformError):
    """Shopee trả lỗi nghiệp vụ / tham số (không thử lại), vd đơn chưa có mã vận đơn."""

    def __init__(self, error: str, message: str) -> None:
        super().__init__(f"{error}: {message}".strip())
        self.error = error


def sign(partner_key: str, base: str) -> str:
    return hmac.new(partner_key.encode(), base.encode(), hashlib.sha256).hexdigest()


class ShopeeClient:
    def __init__(
        self,
        partner_id: int,
        partner_key: str,
        base_url: str,
        *,
        timeout_s: float = 10.0,
        max_attempts: int = 5,
        backoff_s: float = 0.5,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.partner_id = partner_id
        self._key = partner_key
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self._sleep = sleep

    # ------------------------------------------------------------ ký
    def signed_params(
        self, path: str, *, access_token: str | None = None, shop_id: str | None = None
    ) -> dict[str, Any]:
        timestamp = int(clock.now().timestamp())
        base = f"{self.partner_id}{path}{timestamp}{access_token or ''}{shop_id or ''}"
        params: dict[str, Any] = {
            "partner_id": self.partner_id,
            "timestamp": timestamp,
            "sign": sign(self._key, base),
        }
        if access_token is not None:
            params["access_token"] = access_token
        if shop_id is not None:
            params["shop_id"] = int(shop_id)
        return params

    def auth_partner_url(self, redirect_url: str) -> str:
        path = "/api/v2/shop/auth_partner"
        params = self.signed_params(path)
        params["redirect"] = redirect_url
        return f"{self.base_url}{path}?{urlencode(params)}"

    # ------------------------------------------------------------ gọi
    def _delay(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), 60.0)
            except ValueError:
                pass
        return float(self.backoff_s * (2 ** (attempt - 1)))

    async def call(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        access_token: str | None = None,
        shop_id: str | None = None,
    ) -> dict[str, Any]:
        """Gọi một API; thử lại tối đa `max_attempts` lần với lỗi mạng / 5xx / 429 / mã lỗi tạm (FR-05.08).

        Lỗi token → `PlatformAuthError` (không thử lại); lỗi khác của Shopee → `PlatformError`.
        Log mỗi lần gọi: path, HTTP, mã lỗi, request_id, thời gian — không log token / sign (query string).
        """
        last = "unknown"
        # `/api/v2/auth/*` (đổi code, làm mới token) KHÔNG thử lại (G3-N8): refresh token dùng một lần — gửi
        # lại sau khi Shopee đã nhận (timeout phía mình) sẽ bị từ chối và đánh shop EXPIRED oan.
        attempts = 1 if path.startswith("/api/v2/auth/") else self.max_attempts
        for attempt in range(1, attempts + 1):
            query = {**(params or {}), **self.signed_params(path, access_token=access_token, shop_id=shop_id)}
            started = time.monotonic()
            retry_after: str | None = None
            try:
                async with httpx.AsyncClient(timeout=self.timeout_s) as http:
                    res = await http.request(method, f"{self.base_url}{path}", params=query, json=body)
            except httpx.HTTPError as exc:
                last = f"network: {type(exc).__name__}"
                log.warning("shopee_call", path=path, attempt=attempt, outcome="network_error", error=last,
                            duration_ms=round((time.monotonic() - started) * 1000))  # fmt: skip
            else:
                data: dict[str, Any] = {}
                try:
                    parsed = res.json()
                    data = parsed if isinstance(parsed, dict) else {}
                except ValueError:
                    data = {}
                error = str(data.get("error") or "")
                log.info(
                    "shopee_call",
                    path=path,
                    attempt=attempt,
                    http_status=res.status_code,
                    error=error or None,
                    request_id=data.get("request_id"),
                    duration_ms=round((time.monotonic() - started) * 1000),
                )
                # Chỉ mã lỗi token cụ thể (hoặc 401/403 không kèm mã) mới là hết ủy quyền (G3-N6).
                # `error_sign`, `error_permission`, `error_param*`… là lỗi cấu hình / tham số →
                # ShopeeRequestError, shop giữ CONNECTED, `last_error` báo để admin sửa.
                if error in AUTH_ERRORS or (res.status_code in (401, 403) and not error):
                    raise PlatformAuthError(
                        f"{error or res.status_code}: {data.get('message') or ''}".strip()
                    )
                if res.status_code == 429 or res.status_code >= 500 or error in RETRY_ERRORS:
                    last = f"HTTP {res.status_code} {error}".strip()
                    retry_after = res.headers.get("Retry-After")
                elif error:
                    raise ShopeeRequestError(error, str(data.get("message") or ""))
                elif res.status_code >= 400:
                    raise ShopeeRequestError(f"HTTP {res.status_code}", "")
                else:
                    return data
            if attempt < attempts:
                delay = self._delay(attempt, retry_after)
                deadline = budget.deadline()
                if deadline is not None and time.monotonic() + delay + self.timeout_s > deadline:
                    log.warning(
                        "shopee_call", path=path, attempt=attempt, outcome="no_time_left", wait_s=delay
                    )
                    raise PlatformError(
                        f"Shopee lỗi, hết thời gian của lượt đồng bộ (chờ {delay:.0f} giây): {last}"
                    )
                await self._sleep(delay)
        raise PlatformError(f"Shopee lỗi sau {attempts} lần thử: {last}")
