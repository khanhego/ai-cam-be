"""Làm mới token theo **grant** (02a §6, DEC-433, DEC-507): nhiều shop cùng một lần ủy quyền (TikTok
`open_id`; Shopee mỗi shop một grant = `platform_shop_id`) dùng chung refresh token **một lần**.

Khóa Redis `grant:{platform}:{ref}` (SET NX TTL 60 giây, chờ tối đa 10 giây — poll 200 ms) đứng **sau**
`sync:{shop}` / `sync_returns:{shop}` và **trước** mọi khóa DB (02a §6). Có khóa → đọc lại shop
(`populate_existing`): người khác vừa làm mới → dùng luôn; còn lại → gọi sàn, ghi token cho **mọi** shop
`CONNECTED` cùng grant (`FOR UPDATE` id tăng), **commit**, rồi nhả khóa compare-and-delete (G3-N4).
J-12 chỉ lấy khóa này (không chờ — bận thì bỏ lượt), không bao giờ lấy `sync:`.
"""

import asyncio
import secrets
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import commit
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAdapter, PlatformAuthError, PlatformError, ShopCredentials

log = structlog.get_logger()

REFRESH_MARGIN = timedelta(hours=1)  # 02a J-12: còn < 1 giờ thì làm mới
LOCK_TTL_S = 60
LOCK_WAIT_S = 10.0
LOCK_POLL_S = 0.2

_RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"""


class GrantBusy(PlatformError):
    """Khóa grant đang bị giữ quá thời gian chờ (người khác đang làm mới) — lượt sau."""


def grant_ref(shop: Shop) -> str:
    """Shopee: `grant_ref` = `platform_shop_id` (backfill 0006); shop cũ chưa có → `platform_shop_id`."""
    return shop.grant_ref or shop.platform_shop_id


def lock_key(platform: str, ref: str) -> str:
    return f"grant:{platform}:{ref}"


def _error(code: str, exc: Exception) -> dict[str, Any]:
    return {"code": code, "message": str(exc)[:500], "at": clock.iso_z(clock.now())}


async def acquire(platform: str, ref: str, *, wait_s: float = LOCK_WAIT_S) -> str | None:
    """Token chủ khóa, hoặc None nếu sau `wait_s` vẫn bận."""
    token = secrets.token_hex(8)
    end = time.monotonic() + wait_s
    while True:
        if await get_redis().set(lock_key(platform, ref), token, nx=True, ex=LOCK_TTL_S):
            return token
        if time.monotonic() >= end:
            return None
        await asyncio.sleep(LOCK_POLL_S)


async def release(platform: str, ref: str, token: str) -> None:
    await get_redis().eval(_RELEASE_IF_OWNER, 1, lock_key(platform, ref), token)  # type: ignore[misc]


def _grant_filter(platform: str, ref: str) -> Any:
    return (Shop.platform == platform) & (func.coalesce(Shop.grant_ref, Shop.platform_shop_id) == ref)


async def _reload(session: AsyncSession, shop_id: Any) -> Shop | None:
    shop: Shop | None = await session.scalar(
        select(Shop).where(Shop.id == shop_id).execution_options(populate_existing=True)
    )
    return shop


@dataclass
class Outcome:
    """Kết quả làm mới một grant (J-12 đếm theo shop)."""

    creds: ShopCredentials | None
    refreshed: bool = False  # lần này gọi sàn làm mới thành công
    shops: int = 0  # số shop được ghi token / đánh EXPIRED


async def _refresh_locked(
    session: AsyncSession, shop: Shop, adapter: PlatformAdapter, cipher: Cipher
) -> Outcome:
    """Đang giữ khóa grant: gọi sàn rồi ghi cho mọi shop `CONNECTED` cùng grant. Lỗi token → `EXPIRED` mọi
    shop chưa ngắt của grant (+ `AUTH_EXPIRED`). Lỗi tạm → ném `PlatformError` (người gọi ghi
    `last_error`)."""
    creds = platforms.credentials(shop, cipher)
    if creds is None:
        await commit(session)  # `credentials` có thể vừa đánh EXPIRED (CREDENTIALS_UNREADABLE)
        return Outcome(None)
    ref = grant_ref(shop)
    try:
        fresh = await adapter.refresh(creds)
    except PlatformAuthError as exc:
        rows = (
            await session.scalars(
                select(Shop)
                .where(_grant_filter(shop.platform, ref), Shop.auth_status != "DISCONNECTED")
                .order_by(Shop.id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).all()
        for row in rows:
            row.auth_status = "EXPIRED"
            row.last_error = _error("AUTH_EXPIRED", exc)
        await commit(session)
        log.warning(
            "platform_token_expired", platform=shop.platform, grant=ref, shops=len(rows), error=str(exc)
        )
        return Outcome(None, shops=len(rows))
    rows = (
        await session.scalars(
            select(Shop)
            .where(_grant_filter(shop.platform, ref), Shop.auth_status == "CONNECTED")
            .order_by(Shop.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    targets = list(rows) if any(r.id == shop.id for r in rows) else [*rows, shop]
    for row in targets:
        platforms.store_credentials(
            row,
            ShopCredentials(row.platform_shop_id, fresh.access_token, fresh.refresh_token, fresh.expires_at),
            cipher,
        )
    await commit(session)
    log.info(
        "platform_token_refreshed", platform=shop.platform, grant=ref, shops=len(targets),
        expires_at=fresh.expires_at.isoformat(),
    )  # fmt: skip
    # G3-MS-1: trả token đầy đủ của shop (`shop_cipher`, `grant_ref`, `region` — làm mới không trả các trường
    # này); thiếu `shop_cipher` thì mọi lời gọi cấp shop TikTok bị từ chối → shop EXPIRED mỗi chu kỳ token.
    return Outcome(platforms.credentials(shop, cipher), refreshed=True, shops=len(targets))


async def ensure_fresh(
    session: AsyncSession,
    shop: Shop,
    adapter: PlatformAdapter,
    cipher: Cipher,
    *,
    force: bool = False,
    wait_s: float = LOCK_WAIT_S,
) -> ShopCredentials | None:
    """Token dùng được của `shop`; còn < 1 giờ (hoặc `force` — sàn vừa từ chối token) → làm mới dưới khóa
    grant.

    None = không dùng được (chưa có token / `EXPIRED`). Khóa bận quá `wait_s` → `PlatformError` (lượt sau).
    **Commit** session khi làm mới (token mới phải thấy được trước khi nhả khóa)."""
    return (await refresh_grant(session, shop, adapter, cipher, force=force, wait_s=wait_s)).creds


async def refresh_grant(
    session: AsyncSession,
    shop: Shop,
    adapter: PlatformAdapter,
    cipher: Cipher,
    *,
    force: bool = False,
    wait_s: float = LOCK_WAIT_S,
) -> Outcome:
    creds = platforms.credentials(shop, cipher)
    if creds is None:
        return Outcome(None)
    if not force and creds.expires_at - clock.now() > REFRESH_MARGIN:
        return Outcome(creds)
    stale_access = creds.access_token
    platform, ref, shop_id = shop.platform, grant_ref(shop), shop.id
    await commit(session)  # không giữ transaction / khóa DB khi chờ khóa Redis
    token = await acquire(platform, ref, wait_s=wait_s)
    if token is None:
        current = await _reload(session, shop_id)
        again = platforms.credentials(current, cipher) if current is not None else None
        if again is not None and again.access_token != stale_access:
            return Outcome(again)  # người giữ khóa vừa làm mới xong
        raise GrantBusy(f"Đang làm mới token của grant {platform}:{ref} — thử lại lượt sau")
    try:
        current = await _reload(session, shop_id)
        if current is None or current.auth_status == "DISCONNECTED":
            return Outcome(None)
        again = platforms.credentials(current, cipher)
        if again is None:
            await commit(session)
            return Outcome(None)
        # Đọc lại sau khóa: người khác (J-04 shop cùng grant / J-12) vừa làm mới → dùng luôn, không đốt
        # refresh token một lần lần nữa.
        if again.access_token != stale_access or (
            not force and again.expires_at - clock.now() > REFRESH_MARGIN
        ):
            return Outcome(again)
        return await _refresh_locked(session, current, adapter, cipher)
    finally:
        await release(platform, ref, token)
