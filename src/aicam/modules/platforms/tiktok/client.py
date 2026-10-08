"""HTTP client TikTok Shop Partner API (02a §7.1): ký HMAC, thử lại giãn cách, `Retry-After`, ngân sách
lượt, log mỗi lần gọi (không log query / header — token, `sign`, `app_secret`).

**Giả định theo tài liệu công khai TikTok Shop Partner Center (version `202309`) — chưa test với TikTok thật,
thiếu tài khoản đối tác (Q18, Q19).** Điểm phải xác minh ở T-3 TikTok:

- Tham số chung API cấp shop: `app_key`, `timestamp` (giây), `shop_cipher` (API cấp shop), `sign`; header
  `x-tts-access-token`. `sign` = HMAC-SHA256(`app_secret`, `app_secret + path + Σ(k + v theo k tăng, trừ sign,
  access_token) + body JSON + app_secret`) dạng hex.
- Response `{code, message, request_id, data}`; `code = 0` là thành công.
- Mã lỗi token (giả định) `105002`, `105003`, `36004004` → `PlatformAuthError`; giới hạn tần suất (giả định)
  `36009003`, `36009004` / HTTP 429 → thử lại; 5xx → thử lại.
- Ủy quyền: `GET {AUTH_BASE}/api/v2/token/get?app_key&app_secret&auth_code&grant_type=authorized_code`,
  `GET {AUTH_BASE}/api/v2/token/refresh?app_key&app_secret&refresh_token&grant_type=refresh_token` — **không
  thử lại** (refresh token có thể chỉ dùng một lần, như Shopee G3-N8). `access_token_expire_in` /
  `refresh_token_expire_in`: tài liệu ghi là mốc epoch (giây) — số < 10⁹ coi là số giây còn lại (phòng sai).
"""

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

from aicam.core import clock
from aicam.modules.platforms import budget
from aicam.modules.platforms.base import PlatformAuthError, PlatformError

log = structlog.get_logger()

VERSION = "202309"
# Giả định (Q19): mã `code` coi là lỗi token / bị thu hồi.
AUTH_CODES = frozenset({105001, 105002, 105003, 36004004})
# Giả định (Q19): mã giới hạn tần suất / lỗi tạm phía TikTok → thử lại giãn cách.
RETRY_CODES = frozenset({36009003, 36009004, 36009001})
EXCLUDED_SIGN_PARAMS = frozenset({"sign", "access_token"})

Sleep = Callable[[float], Awaitable[None]]


class TikTokRequestError(PlatformError):
    """TikTok trả lỗi nghiệp vụ / tham số (`code ≠ 0`, không thử lại)."""

    def __init__(self, code: int | str, message: str) -> None:
        super().__init__(f"{code}: {message}".strip())
        self.code = code


def body_json(body: dict[str, Any] | None) -> str:
    """Chuỗi body **đúng như gửi đi** (ký trên chính chuỗi này)."""
    return "" if body is None else json.dumps(body, separators=(",", ":"), ensure_ascii=False)


def sign(app_secret: str, path: str, params: dict[str, Any], body: str = "") -> str:
    """02a §7.1: HMAC-SHA256(secret, secret + path + Σ(k+v, k tăng, trừ sign / access_token) + body +
    secret)."""
    joined = "".join(f"{k}{params[k]}" for k in sorted(params) if k not in EXCLUDED_SIGN_PARAMS)
    base = f"{app_secret}{path}{joined}{body}{app_secret}"
    return hmac.new(app_secret.encode(), base.encode(), hashlib.sha256).hexdigest()


def expires_at(value: Any, now: datetime) -> datetime:
    """`*_expire_in`: epoch giây (tài liệu) hoặc số giây còn lại (phòng sai — Q19)."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return now + timedelta(hours=4)
    if n > 1_000_000_000:
        return datetime.fromtimestamp(n, tz=UTC)
    return now + timedelta(seconds=max(n, 0))


@dataclass(frozen=True)
class Token:
    access_token: str
    refresh_token: str
    expires_at: datetime
    open_id: str | None
    seller_name: str | None


class TikTokClient:
    def __init__(
        self,
        app_key: str,
        app_secret: str,
        api_base: str,
        auth_base: str,
        *,
        timeout_s: float = 10.0,
        max_attempts: int = 5,
        backoff_s: float = 0.5,
        sleep: Sleep = asyncio.sleep,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.app_key = app_key
        self._secret = app_secret
        self.api_base = api_base.rstrip("/")
        self.auth_base = auth_base.rstrip("/")
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.backoff_s = backoff_s
        self._sleep = sleep
        self._transport = (
            transport  # adapter mock (02a §7.2): phục vụ fixture trong tiến trình, cùng đường ký / thử lại
        )

    # ------------------------------------------------------------ ký
    def signed_query(
        self, path: str, params: dict[str, Any] | None = None, body: str = "", shop_cipher: str | None = None
    ) -> dict[str, Any]:
        query: dict[str, Any] = {**(params or {}), "app_key": self.app_key}
        query["timestamp"] = int(clock.now().timestamp())
        if shop_cipher:
            query["shop_cipher"] = shop_cipher
        query["sign"] = sign(self._secret, path, query, body)
        return query

    def _delay(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), 60.0)
            except ValueError:
                pass
        return float(self.backoff_s * (2 ** (attempt - 1)))

    def _log(self, path: str, attempt: int, started: float, **fields: Any) -> None:
        log.info(
            "tiktok_call", path=path, attempt=attempt,
            duration_ms=round((time.monotonic() - started) * 1000), **fields,
        )  # fmt: skip

    # ------------------------------------------------------------ gọi API cấp shop / ứng dụng
    async def call(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        access_token: str | None = None,
        shop_cipher: str | None = None,
    ) -> dict[str, Any]:
        """Gọi một API; thử lại tối đa `max_attempts` lần với lỗi mạng / 5xx / 429 / mã giới hạn tần suất
        (FR-05.08), chờ `Retry-After` nếu có, không vượt `budget.deadline()`. Trả `data` (dict)."""
        payload = body_json(body)
        headers = {"content-type": "application/json"}
        if access_token:
            headers["x-tts-access-token"] = access_token
        last = "unknown"
        for attempt in range(1, self.max_attempts + 1):
            query = self.signed_query(path, params, payload, shop_cipher)
            started = time.monotonic()
            retry_after: str | None = None
            try:
                async with httpx.AsyncClient(timeout=self.timeout_s, transport=self._transport) as http:
                    res = await http.request(
                        method,
                        f"{self.api_base}{path}",
                        params=query,
                        content=payload or None,
                        headers=headers,
                    )
            except httpx.HTTPError as exc:
                last = f"network: {type(exc).__name__}"
                self._log(path, attempt, started, outcome="network_error", error=last)
            else:
                data = _json(res)
                code = data.get("code")
                self._log(path, attempt, started, http_status=res.status_code, code=code,
                          request_id=data.get("request_id"))  # fmt: skip
                if _int(code) in AUTH_CODES or (res.status_code == 401 and code in (None, "")):
                    raise PlatformAuthError(f"{code or res.status_code}: {data.get('message') or ''}".strip())
                if res.status_code == 429 or res.status_code >= 500 or _int(code) in RETRY_CODES:
                    last = f"HTTP {res.status_code} {code if code is not None else ''}".strip()
                    retry_after = res.headers.get("Retry-After")
                elif code not in (0, "0", None):
                    raise TikTokRequestError(code, str(data.get("message") or ""))
                elif res.status_code >= 400:
                    raise TikTokRequestError(f"HTTP {res.status_code}", "")
                else:
                    inner = data.get("data")
                    return inner if isinstance(inner, dict) else {}
            if attempt < self.max_attempts:
                delay = self._delay(attempt, retry_after)
                deadline = budget.deadline()
                if deadline is not None and time.monotonic() + delay + self.timeout_s > deadline:
                    log.warning(
                        "tiktok_call", path=path, attempt=attempt, outcome="no_time_left", wait_s=delay
                    )
                    raise PlatformError(
                        f"TikTok lỗi, hết thời gian của lượt đồng bộ (chờ {delay:.0f} giây): {last}"
                    )
                await self._sleep(delay)
        raise PlatformError(f"TikTok lỗi sau {self.max_attempts} lần thử: {last}")

    # ------------------------------------------------------------ ủy quyền (02a §7.1)
    async def _token(self, path: str, params: dict[str, str]) -> Token:
        """`/api/v2/token/*` — **một lần** (không thử lại: refresh token có thể chỉ dùng một lần)."""
        query = {"app_key": self.app_key, "app_secret": self._secret, **params}
        started = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s, transport=self._transport) as http:
                res = await http.get(f"{self.auth_base}{path}", params=query)
        except httpx.HTTPError as exc:
            self._log(path, 1, started, outcome="network_error", error=type(exc).__name__)
            raise PlatformError(f"TikTok ủy quyền lỗi mạng: {type(exc).__name__}") from exc
        data = _json(res)
        code = data.get("code")
        self._log(path, 1, started, http_status=res.status_code, code=code, request_id=data.get("request_id"))
        raw = data.get("data")
        inner: dict[str, Any] = raw if isinstance(raw, dict) else {}
        if _int(code) in AUTH_CODES or res.status_code in (401, 403):
            raise PlatformAuthError(f"{code or res.status_code}: {data.get('message') or ''}".strip())
        if code not in (0, "0") or res.status_code >= 400:
            # Lỗi khác (tham số / cấu hình ứng dụng / 5xx): không đánh shop EXPIRED — `REFRESH_FAILED`, Admin
            # xem (như Shopee G3-N6). Mã token thật cần xác minh (Q19).
            raise TikTokRequestError(
                code if code is not None else f"HTTP {res.status_code}", str(data.get("message") or "")
            )
        access, refresh = inner.get("access_token"), inner.get("refresh_token")
        if not access or not refresh:
            raise PlatformError("TikTok không trả access_token / refresh_token")
        return Token(
            access_token=str(access),
            refresh_token=str(refresh),
            expires_at=expires_at(inner.get("access_token_expire_in"), clock.now()),
            open_id=str(inner["open_id"]) if inner.get("open_id") else None,
            seller_name=str(inner["seller_name"]) if inner.get("seller_name") else None,
        )

    async def token_get(self, auth_code: str) -> Token:
        return await self._token(
            "/api/v2/token/get", {"auth_code": auth_code, "grant_type": "authorized_code"}
        )

    async def token_refresh(self, refresh_token: str) -> Token:
        return await self._token(
            "/api/v2/token/refresh", {"refresh_token": refresh_token, "grant_type": "refresh_token"}
        )

    async def authorized_shops(self, access_token: str) -> list[dict[str, Any]]:
        """`GET /authorization/202309/shops` → `[{id, name, region, cipher, code}]` (API cấp ứng dụng)."""
        data = await self.call("GET", f"/authorization/{VERSION}/shops", access_token=access_token)
        shops = data.get("shops") or []
        return [s for s in shops if isinstance(s, dict) and s.get("id")]

    def authorize_url(self, authorize_base: str, service_id: str, state: str) -> str:
        return f"{authorize_base}?{urlencode({'service_id': service_id, 'state': state})}"


def _json(res: httpx.Response) -> dict[str, Any]:
    try:
        parsed = res.json()
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
