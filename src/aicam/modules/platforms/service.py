"""Chọn adapter Shopee, token mã hóa Fernet, khóa `sync:{shop}`, đẩy job sàn (02a §4, §7, §9).

Kết nối / ngắt / danh sách shop (API-70..73, 154..156): `platforms/connect.py`."""

import secrets
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any

import structlog
from cryptography.fernet import InvalidToken
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms.base import (
    PlatformAdapter,
    PlatformError,
    PlatformOrder,
    PlatformReturn,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient

log = structlog.get_logger()

PLATFORM = "SHOPEE"
STATE_TTL_S = 600  # API-71: state chống CSRF hạn 10 phút
SYNC_LOCK_TTL_S = 600  # 02a §6: lock Redis `sync:{shop}` TTL 10 phút
SYNC_TASK = "platforms.sync_shop_orders"  # J-04 một shop (fan-out — T-205; `dispatch.SHOP_TASKS`)
SYNC_RETURNS_TASK = "platforms.sync_shop_returns"  # J-13 một shop


class UnconfiguredAdapter:
    """Sàn chưa bật / chưa có khóa ứng dụng: quét → kiện chưa xác minh (BR-04), job bỏ qua sàn."""

    def __init__(self, code: str = PLATFORM) -> None:
        self.code = code

    def build_auth_url(self, redirect_url: str, state: str) -> str:
        raise not_configured(self.code)

    async def exchange_code(self, code: str, shop_id: str | None) -> list[ShopCredentials]:
        raise PlatformError("Chưa cấu hình sàn")

    async def refresh(self, creds: ShopCredentials) -> ShopCredentials:
        raise PlatformError("Chưa cấu hình Shopee")

    async def shop_name(self, creds: ShopCredentials) -> str | None:
        return None

    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None:
        return None

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None:
        return None

    async def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]:
        raise PlatformError("Chưa cấu hình Shopee")
        yield  # pragma: no cover — biến hàm thành async generator

    async def get_shipping_statuses(self, creds: ShopCredentials | None, refs: Any) -> list[ShippingStatus]:
        return []

    async def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]:
        raise PlatformError("Chưa cấu hình Shopee")
        yield  # pragma: no cover — biến hàm thành async generator

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None:
        return None


@lru_cache
def _mock(shop_ids: tuple[str, ...] = ()) -> MockAdapter:
    return MockAdapter.multi_shop(shop_ids)


@lru_cache
def _shopee(
    partner_id: int,
    partner_key: str,
    base_url: str,
    timeout_s: float,
    attempts: int,
    backoff_s: float,
    lookback_min: int,
    returns_page_size: int = 50,
    returns_window_days: int = 15,
) -> ShopeeAdapter:
    client = ShopeeClient(
        partner_id, partner_key, base_url, timeout_s=timeout_s, max_attempts=attempts, backoff_s=backoff_s
    )
    return ShopeeAdapter(
        client,
        lookup_lookback=timedelta(minutes=lookback_min),
        returns_page_size=returns_page_size,
        returns_window=timedelta(days=returns_window_days),
    )


def is_configured(settings: Settings) -> bool:
    """Flag `SHOPEE_ENABLED` + (adapter mock, hoặc đủ partner id / key / redirect URL)."""
    if not settings.shopee_enabled:
        return False
    if settings.platform_adapter == "mock":
        return True
    return bool(
        settings.shopee_partner_id.strip().isdigit()
        and settings.shopee_partner_key
        and settings.shopee_redirect_url
    )


NOT_CONFIGURED_MESSAGES = {
    "SHOPEE": "Chưa cấu hình Shopee Open Platform. Dùng Nhập đơn từ file.",
    "TIKTOK": "Chưa cấu hình TikTok Shop. Liên hệ IT để bật (cần tài khoản đối tác TikTok Shop).",
}


def not_configured(platform: str = PLATFORM) -> AppError:
    """503 `PLATFORM_NOT_CONFIGURED`, `message` theo sàn (02 §6.2 API-71)."""
    message = NOT_CONFIGURED_MESSAGES.get(platform, "Chưa cấu hình sàn.")
    return AppError("PLATFORM_NOT_CONFIGURED", message, 503)


def require_configured(settings: Settings) -> None:
    if not is_configured(settings):
        raise not_configured()


def get_adapter(settings: Settings) -> PlatformAdapter:
    if settings.platform_adapter == "mock":
        return _mock(tuple(x.strip() for x in settings.mock_shopee_shop_ids.split(",") if x.strip()))
    if not is_configured(settings):
        return UnconfiguredAdapter()
    return _shopee(
        int(settings.shopee_partner_id), settings.shopee_partner_key, settings.shopee_base_url,
        settings.shopee_timeout_s, settings.shopee_max_attempts, settings.shopee_backoff_s,
        settings.shopee_lookup_lookback_min, settings.shopee_returns_page_size,
        settings.shopee_returns_window_days,
    )  # fmt: skip


# ---------------------------------------------------------------- token (Fernet, 02a §3)


def credentials(shop: Shop, cipher: Cipher) -> ShopCredentials | None:
    """Token đã giải mã; None nếu chưa có. Không giải mã được (FERNET_KEY đổi / khôi phục sai `.env` —
    G3-P2-6) → shop `EXPIRED` + `last_error.code = CREDENTIALS_UNREADABLE` (D7 hiện "Kết nối lại"),
    không ném lỗi."""
    if not shop.access_token_enc or not shop.refresh_token_enc or shop.auth_expires_at is None:
        return None
    try:
        access, refresh = cipher.decrypt(shop.access_token_enc), cipher.decrypt(shop.refresh_token_enc)
    except InvalidToken:
        log.error("shop_credentials_unreadable", shop_id=str(shop.id))
        shop.auth_status = "EXPIRED"
        shop.last_error = {
            "code": "CREDENTIALS_UNREADABLE",
            "message": "Không giải mã được token shop (FERNET_KEY khác lúc kết nối). Bấm Kết nối lại.",
            "at": clock.iso_z(clock.now()),
        }
        return None
    return ShopCredentials(
        shop_id=shop.platform_shop_id,
        access_token=access,
        refresh_token=refresh,
        expires_at=shop.auth_expires_at,
        shop_cipher=shop.shop_cipher,
        grant_ref=shop.grant_ref,
        shop_name=shop.name,
        region=shop.region,
    )


def store_credentials(shop: Shop, creds: ShopCredentials, cipher: Cipher) -> None:
    shop.access_token_enc = cipher.encrypt(creds.access_token)
    shop.refresh_token_enc = cipher.encrypt(creds.refresh_token)
    shop.auth_expires_at = creds.expires_at
    shop.auth_status = "CONNECTED"
    # Phase 3: thông tin ủy quyền adapter trả khi kết nối (làm mới token không trả → giữ giá trị cũ).
    if creds.grant_ref:
        shop.grant_ref = creds.grant_ref
    if creds.shop_cipher:
        shop.shop_cipher = creds.shop_cipher
    if creds.region:
        shop.region = creds.region


async def connected_shop(session: AsyncSession) -> Shop | None:
    shop: Shop | None = await session.scalar(
        select(Shop)
        .where(Shop.platform == PLATFORM, Shop.auth_status == "CONNECTED")
        .order_by(Shop.created_at.desc())
        .limit(1)
    )
    return shop


@dataclass(frozen=True)
class LookupTarget:
    creds: ShopCredentials | None
    shop_id: uuid.UUID | None


async def lookup_target(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> LookupTarget | None:
    """FR-05.06: token của shop đang kết nối để tra khi quét; None = không tra được (chưa kết nối).

    Đọc DB ở đây, **trước** khi bấm giờ 2 giây: timeout chỉ cắt lời gọi sàn, không cắt giữa câu SQL.
    Adapter mock không cần token (dev / test).
    """
    shop = await connected_shop(session)
    creds = credentials(shop, Cipher(settings.fernet_key)) if shop else None
    if creds is None and not isinstance(adapter, MockAdapter):
        return None
    return LookupTarget(creds, shop.id if shop else None)


async def lookup_targets(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> list[LookupTarget]:
    """Mọi shop `CONNECTED` có token đọc được (J-05, J-06 — T-204 đa shop). Adapter mock chưa có shop → một
    đích không shop (dev / test như Phase 2)."""
    cipher = Cipher(settings.fernet_key)
    shops = (
        await session.scalars(
            select(Shop)
            .where(Shop.platform == adapter.code, Shop.auth_status == "CONNECTED")
            .order_by(Shop.created_at)
        )
    ).all()
    out = [LookupTarget(c, s.id) for s in shops if (c := credentials(s, cipher)) is not None]
    if not out and isinstance(adapter, MockAdapter):
        return [LookupTarget(None, None)]
    if isinstance(adapter, MockAdapter):
        out += [LookupTarget(None, s.id) for s in shops if s.id not in {t.shop_id for t in out}]
    return out


# ---------------------------------------------------------------- API-73 (FR-05.02)


def sync_lock_key(shop_id: uuid.UUID) -> str:
    return f"sync:{shop_id}"


_RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"""


async def acquire_sync_lock(shop_id: uuid.UUID, owner: str) -> str | None:
    """Lock Redis `sync:{shop}` (J-04, J-12, API-73). Trả token chủ (để nhả) hoặc None nếu đang bị giữ."""
    token = f"{owner}:{secrets.token_hex(8)}"
    ok = await get_redis().set(sync_lock_key(shop_id), token, nx=True, ex=SYNC_LOCK_TTL_S)
    return token if ok else None


async def release_sync_lock(shop_id: uuid.UUID, token: str | None = None) -> None:
    """Nhả lock. Có `token` → chỉ xóa nếu vẫn là chủ (G3-N4: lock đã hết TTL và người khác giữ thì không xóa
    nhầm). `token=None` chỉ cho message cũ `lock_held=True` / dọn tay."""
    if token is None:
        await get_redis().delete(sync_lock_key(shop_id))
        return
    await get_redis().eval(_RELEASE_IF_OWNER, 1, sync_lock_key(shop_id), token)  # type: ignore[misc]


async def enqueue_sync(shop_id: uuid.UUID, *, lock_held: bool | str) -> None:
    """`lock_held`: token lock API-73 đã giữ (job nhả bằng token), hoặc False."""
    from aicam.modules.media import jobs  # gửi Celery theo tên task (test thay sender)

    await jobs.send(SYNC_TASK, [str(shop_id), lock_held], "sync")


async def enqueue_sync_returns(shop_id: uuid.UUID) -> None:
    """J-13 cho một shop (queue `sync`)."""
    from aicam.modules.media import jobs

    await jobs.send(SYNC_RETURNS_TASK, [str(shop_id)], "sync")
