"""Zalo OA (02a §7.5, DEC-445) — **giả định theo tài liệu công khai, chưa test với OA thật (Q21)**.

- Gửi tin tư vấn: `POST {ZALO_API_BASE}/v3.0/oa/message/cs`, header `access_token`, body
  `{"recipient": {"user_id": …}, "message": {"text": …}}` → `{"error": 0, "message": "Success", …}`.
- Làm mới token: `POST {ZALO_OAUTH_BASE}/v4/oa/access_token`, header `secret_key`, form `app_id`,
  `refresh_token`, `grant_type=refresh_token` → `{access_token, refresh_token, expires_in}`. Refresh token
  Zalo **xoay vòng, dùng một lần** → làm mới dưới khóa Redis `zalo:token` (SET NX, chờ ≤ 10 giây), đọc lại
  sau khi có khóa, lưu cặp mới vào `notify_provider_token` (Fernet) và **commit trước khi nhả khóa**
  (như DEC-433).
  Lần đầu lấy refresh token từ `ZALO_OA_REFRESH_TOKEN`; sau đó luôn dùng bản trong DB. Cặp (làm mới → lưu →
  nhả khóa) che khỏi hủy bằng `asyncio.shield` (G3-NT-1); mất chuỗi token → IT cấp lại refresh token, đặt
  `ZALO_OA_REFRESH_TOKEN`, chạy `aicam notify-reset-zalo-token` (ops §10).
- Mã lỗi (giả định): `-216` access token sai / hết hạn → làm mới (ép) rồi gửi lại một lần; `-213` người
  nhận chưa quan tâm OA; `-230` người nhận không tương tác với OA trong 7 ngày (hết hạn tương tác — EX-N2).
"""

import asyncio
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

import httpx
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.modules.notify.models import NotifyProviderToken
from aicam.modules.notify.providers.base import HTTP_TIMEOUT_S, SendError

log = structlog.get_logger()

PROVIDER = "ZALO_OA"
TEXT_MAX = 2000  # giới hạn tin văn bản OA (giả định)
REFRESH_MARGIN = timedelta(minutes=5)
LOCK_KEY = "zalo:token"
LOCK_TTL_S = 60
# G3-NT-1: chờ khóa ngắn hơn ngân sách gửi (J-27 `HTTP_TIMEOUT_S + 2` = 10 giây, API-174 10 giây) — chờ hết
# ngân sách thì bị hủy trước khi kịp gửi; bận → `TOKEN_LOCK_BUSY`, lượt sau.
LOCK_WAIT_S = 3.0
LOCK_POLL_S = 0.2

# Lượt làm mới đang chạy (đã che khỏi hủy — G3-NT-1). Worker Celery chạy `asyncio.run` mỗi task: J-27 chờ các
# lượt này xong (`drain_pending`) trước khi vòng lặp đóng (đóng vòng lặp sẽ hủy task còn lại).
_PENDING: set[asyncio.Task[Any]] = set()

ERR_TOKEN_INVALID = -216
ERR_NOT_FOLLOWER = -213
ERR_INTERACTION_EXPIRED = -230

MSG_NOT_FOLLOWER = "Người nhận chưa quan tâm OA của shop."
MSG_INTERACTION = "Người nhận chưa tương tác với OA trong 7 ngày gần đây (hết hạn tương tác)."
MSG_TOKEN = "Không làm mới được token Zalo OA trên máy chủ. Liên hệ IT."  # noqa: S105 — thông điệp, không phải bí mật
MSG_UNREACHABLE = "Không kết nối được Zalo từ máy chủ (mạng chặn?)."
MSG_BUSY = "Đang làm mới token Zalo OA — sẽ thử lại."
MSG_OTHER = "Zalo OA từ chối tin."

_RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"""


@dataclass
class ZaloToken:
    access_token: str | None
    refresh_token: str | None
    expires_at: datetime | None


class TokenStore(Protocol):
    async def load(self) -> ZaloToken: ...

    async def save(self, token: ZaloToken) -> None: ...


class DbTokenStore:
    """Bảng `notify_provider_token` (Fernet). Mỗi lần đọc / ghi dùng **session riêng** trên `bind` và commit
    ngay — cặp token mới phải bền trước khi nhả khóa `zalo:token` (tiến trình khác đọc lại ngay sau đó)."""

    def __init__(self, bind: AsyncEngine | AsyncConnection, cipher: Cipher) -> None:
        self._bind = bind
        self._cipher = cipher

    def _session(self) -> AsyncSession:
        return AsyncSession(bind=self._bind, expire_on_commit=False, join_transaction_mode="create_savepoint")

    async def load(self) -> ZaloToken:
        async with self._session() as s:
            row = await s.get(NotifyProviderToken, PROVIDER, populate_existing=True)
            if row is None:
                return ZaloToken(None, None, None)
            return ZaloToken(
                self._cipher.decrypt(row.access_token_enc) if row.access_token_enc else None,
                self._cipher.decrypt(row.refresh_token_enc) if row.refresh_token_enc else None,
                row.expires_at,
            )

    async def save(self, token: ZaloToken) -> None:
        async with self._session() as s:
            row = (
                await s.execute(
                    select(NotifyProviderToken)
                    .where(NotifyProviderToken.provider == PROVIDER)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                row = NotifyProviderToken(provider=PROVIDER)
                s.add(row)
            row.access_token_enc = self._cipher.encrypt(token.access_token) if token.access_token else None
            row.refresh_token_enc = self._cipher.encrypt(token.refresh_token) if token.refresh_token else None
            row.expires_at = token.expires_at
            row.updated_at = clock.now()
            await s.commit()


class ZaloProvider:
    type = PROVIDER

    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        initial_refresh_token: str,
        api_base: str,
        oauth_base: str,
        store: TokenStore,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._initial_refresh = initial_refresh_token
        self._api = api_base.rstrip("/")
        self._oauth = oauth_base.rstrip("/")
        self._store = store
        self._transport = transport

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=HTTP_TIMEOUT_S, transport=self._transport)

    # ------------------------------------------------------------ token

    @staticmethod
    def _fresh(token: ZaloToken) -> bool:
        return (
            bool(token.access_token)
            and token.expires_at is not None
            and (token.expires_at - REFRESH_MARGIN > clock.now())
        )

    async def access_token(self, *, stale: str | None = None) -> str:
        """Token còn hạn; `stale` = token vừa bị Zalo từ chối → làm mới (trừ khi người khác đã làm mới)."""
        current = await self._store.load()
        if stale is None and self._fresh(current) and current.access_token:
            return current.access_token
        owner = secrets.token_hex(8)
        end = time.monotonic() + LOCK_WAIT_S
        redis = get_redis()
        while not await redis.set(LOCK_KEY, owner, nx=True, ex=LOCK_TTL_S):
            if time.monotonic() >= end:
                raise SendError(MSG_BUSY, provider_code="TOKEN_LOCK_BUSY")
            await asyncio.sleep(LOCK_POLL_S)
        handed_off = False
        try:
            current = await self._store.load()  # đọc lại dưới khóa
            if self._fresh(current) and current.access_token and current.access_token != stale:
                return current.access_token
            refresh = current.refresh_token or self._initial_refresh
            if not refresh:
                raise SendError(MSG_TOKEN, provider_code="NO_REFRESH_TOKEN")
            # G3-NT-1: refresh token Zalo dùng **một lần** — cặp (gọi làm mới → lưu DB → nhả khóa) chạy trong
            # task riêng che bằng `asyncio.shield`: lời gọi ngoài bị hủy (timeout gửi / API-174) không bỏ dở
            # giữa lúc Zalo đã xoay token mà DB chưa lưu.
            task = asyncio.create_task(self._refresh_save_release(refresh, owner))
            _PENDING.add(task)
            task.add_done_callback(_forget)
            handed_off = True
            new = await asyncio.shield(task)
            return new.access_token or ""
        finally:
            if not handed_off:
                await _release(owner)

    async def _refresh_save_release(self, refresh: str, owner: str) -> ZaloToken:
        try:
            new = await self._refresh(refresh)
            await self._store.save(new)  # commit trước khi nhả khóa
            return new
        finally:
            await _release(owner)

    async def _refresh(self, refresh_token: str) -> ZaloToken:
        form = {"app_id": self._app_id, "refresh_token": refresh_token, "grant_type": "refresh_token"}
        try:
            async with self._client() as client:
                resp = await client.post(
                    f"{self._oauth}/v4/oa/access_token", data=form, headers={"secret_key": self._app_secret}
                )
        except httpx.TransportError as exc:
            log.warning("zalo_unreachable", step="refresh", error=type(exc).__name__)
            raise SendError(MSG_UNREACHABLE, provider_code=type(exc).__name__, timeout=True) from None
        data = _json(resp)
        access = data.get("access_token")
        if resp.status_code != 200 or not access:
            code = data.get("error") or resp.status_code
            log.warning("zalo_token_refresh_failed", http_status=resp.status_code, provider_code=code)
            raise SendError(MSG_TOKEN, provider_code=str(code))
        try:
            expires_in = int(data.get("expires_in") or 3600)
        except (TypeError, ValueError):
            expires_in = 3600
        log.info("zalo_token_refreshed", expires_in=expires_in)
        return ZaloToken(
            str(access),
            str(data.get("refresh_token") or refresh_token),
            clock.now() + timedelta(seconds=expires_in),
        )

    # ------------------------------------------------------------ gửi

    async def send(self, target: str, text: str) -> None:
        token = await self.access_token()
        code = await self._post(token, target, text)
        if code == ERR_TOKEN_INVALID:  # token bị thu hồi / hết hạn sớm → làm mới ép, gửi lại một lần
            token = await self.access_token(stale=token)
            code = await self._post(token, target, text)
        if code == 0:
            return
        if code == ERR_NOT_FOLLOWER:
            raise SendError(MSG_NOT_FOLLOWER, provider_code=str(code))
        if code == ERR_INTERACTION_EXPIRED:
            raise SendError(MSG_INTERACTION, provider_code=str(code))
        if code == ERR_TOKEN_INVALID:
            raise SendError(MSG_TOKEN, provider_code=str(code))
        raise SendError(MSG_OTHER, provider_code=str(code))

    async def _post(self, token: str, target: str, text: str) -> int:
        body = {"recipient": {"user_id": target}, "message": {"text": text[:TEXT_MAX]}}
        try:
            async with self._client() as client:
                resp = await client.post(
                    f"{self._api}/v3.0/oa/message/cs", json=body, headers={"access_token": token}
                )
        except httpx.TransportError as exc:
            log.warning("zalo_unreachable", step="send", error=type(exc).__name__)
            raise SendError(MSG_UNREACHABLE, provider_code=type(exc).__name__, timeout=True) from None
        data = _json(resp)
        if resp.status_code >= 500 or "error" not in data:
            log.warning("zalo_rejected", http_status=resp.status_code)
            raise SendError(MSG_OTHER, provider_code=str(resp.status_code))
        try:
            code = int(data["error"])
        except (TypeError, ValueError):
            code = -1
        if code != 0:
            log.warning("zalo_rejected", http_status=resp.status_code, provider_code=code)
        return code


async def reset_stored_token(db: AsyncSession) -> bool:
    """`aicam notify-reset-zalo-token` (G3-NT-1 — đường khôi phục): xóa cặp token đã lưu → lần gửi kế lấy lại
    refresh token từ `ZALO_OA_REFRESH_TOKEN` (IT vừa cấp lại trên Zalo Developers khi chuỗi token trong DB bị
    mất / hỏng). True = đã có dòng để xóa. Người gọi commit."""
    from sqlalchemy import delete

    res = await db.execute(delete(NotifyProviderToken).where(NotifyProviderToken.provider == PROVIDER))
    return bool(res.rowcount)  # type: ignore[attr-defined]


def _forget(task: asyncio.Task[Any]) -> None:
    _PENDING.discard(task)
    if not task.cancelled():
        task.exception()  # lỗi đã log ở `_refresh` / đã ném cho người chờ — tránh cảnh báo "never retrieved"


async def _release(owner: str) -> None:
    await get_redis().eval(_RELEASE_IF_OWNER, 1, LOCK_KEY, owner)  # type: ignore[misc]


async def drain_pending(timeout_s: float = 15.0) -> int:
    """Chờ các lượt làm mới token đang chạy (đã che khỏi hủy) xong — gọi trước khi vòng lặp sự kiện đóng
    (cuối J-27). Trả số lượt còn dở sau `timeout_s` (log)."""
    pending = [t for t in _PENDING if not t.done()]
    if not pending:
        return 0
    _done, left = await asyncio.wait(pending, timeout=timeout_s)
    if left:
        log.error("zalo_token_refresh_unfinished", pending=len(left))
    return len(left)


def _json(resp: httpx.Response) -> dict[str, Any]:
    try:
        data = resp.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}
