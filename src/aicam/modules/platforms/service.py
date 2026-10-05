"""Chọn adapter, kết nối shop Shopee (API-70..73), token mã hóa Fernet, tra sàn khi quét (02a §4, §7, §9)."""

import secrets
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms.base import (
    PlatformAdapter,
    PlatformError,
    PlatformOrder,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.schemas import ShopOut
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient

log = structlog.get_logger()

PLATFORM = "SHOPEE"
STATE_TTL_S = 600  # API-71: state chống CSRF hạn 10 phút
SYNC_LOCK_TTL_S = 600  # 02a §6: lock Redis `sync:{shop}` TTL 10 phút
SYNC_TASK = "platforms.sync_orders"
CALLBACK_PATH = "/api/v1/shops/shopee/callback"
RESULT_PATH = "/admin/settings/shopee"


class UnconfiguredAdapter:
    """`PLATFORM_ADAPTER=shopee` nhưng chưa bật / chưa có partner key: quét → kiện chưa xác minh (BR-04)."""

    code = PLATFORM

    def build_auth_url(self, redirect_url: str) -> str:
        raise not_configured()

    async def exchange_code(self, code: str, shop_id: str) -> ShopCredentials:
        raise PlatformError("Chưa cấu hình Shopee")

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


@lru_cache
def _mock() -> MockAdapter:
    return MockAdapter()


@lru_cache
def _shopee(
    partner_id: int,
    partner_key: str,
    base_url: str,
    timeout_s: float,
    attempts: int,
    backoff_s: float,
    lookback_min: int,
) -> ShopeeAdapter:
    client = ShopeeClient(
        partner_id, partner_key, base_url, timeout_s=timeout_s, max_attempts=attempts, backoff_s=backoff_s
    )
    return ShopeeAdapter(client, lookup_lookback=timedelta(minutes=lookback_min))


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


def not_configured() -> AppError:
    return AppError(
        "PLATFORM_NOT_CONFIGURED", "Chưa cấu hình Shopee Open Platform. Dùng Nhập đơn từ file.", 503
    )


def require_configured(settings: Settings) -> None:
    if not is_configured(settings):
        raise not_configured()


def get_adapter(settings: Settings) -> PlatformAdapter:
    if settings.platform_adapter == "mock":
        return _mock()
    if not is_configured(settings):
        return UnconfiguredAdapter()
    return _shopee(
        int(settings.shopee_partner_id), settings.shopee_partner_key, settings.shopee_base_url,
        settings.shopee_timeout_s, settings.shopee_max_attempts, settings.shopee_backoff_s,
        settings.shopee_lookup_lookback_min,
    )  # fmt: skip


# ---------------------------------------------------------------- token (Fernet, 02a §3)


def credentials(shop: Shop, cipher: Cipher) -> ShopCredentials | None:
    if not shop.access_token_enc or not shop.refresh_token_enc or shop.auth_expires_at is None:
        return None
    return ShopCredentials(
        shop_id=shop.platform_shop_id,
        access_token=cipher.decrypt(shop.access_token_enc),
        refresh_token=cipher.decrypt(shop.refresh_token_enc),
        expires_at=shop.auth_expires_at,
    )


def store_credentials(shop: Shop, creds: ShopCredentials, cipher: Cipher) -> None:
    shop.access_token_enc = cipher.encrypt(creds.access_token)
    shop.refresh_token_enc = cipher.encrypt(creds.refresh_token)
    shop.auth_expires_at = creds.expires_at
    shop.auth_status = "CONNECTED"


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


# ---------------------------------------------------------------- API-70


def _day_start(day: date, tz: str) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))


async def list_shops(session: AsyncSession, settings: Settings) -> list[ShopOut]:
    """API-70. `today_synced_orders` = số đơn nguồn API của shop ghi / cập nhật từ 00:00 giờ VN hôm nay."""
    start = _day_start(clock.now().astimezone(ZoneInfo(settings.tz_display)).date(), settings.tz_display)
    shops = (
        await session.scalars(select(Shop).where(Shop.platform == PLATFORM).order_by(Shop.created_at))
    ).all()
    out = []
    for s in shops:
        count = await session.scalar(
            select(func.count())
            .select_from(Order)
            .where(Order.shop_id == s.id, Order.source == "API", Order.updated_at >= start)
        )
        out.append(
            ShopOut(
                id=s.id,
                platform=s.platform,
                name=s.name,
                auth_status=s.auth_status,
                auth_expires_at=s.auth_expires_at,
                last_synced_at=s.last_synced_at,
                today_synced_orders=int(count or 0),
                last_error=s.last_error,
            )
        )
    return out


# ---------------------------------------------------------------- API-71, API-72 (FR-05.01, UC-10)


def _state_key(state: str) -> str:
    return f"shopee:oauth:{state}"


def callback_url(settings: Settings, state: str) -> str:
    """Shopee giữ nguyên query của `redirect` và nối thêm `code`, `shop_id` (DEC-123: cần xác nhận ở T-3)."""
    base = settings.shopee_redirect_url or CALLBACK_PATH
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}state={state}"


async def auth_url(adapter: PlatformAdapter, settings: Settings, user_id: uuid.UUID) -> str:
    require_configured(settings)
    state = secrets.token_urlsafe(24)
    await get_redis().set(_state_key(state), str(user_id), ex=STATE_TTL_S)
    return adapter.build_auth_url(callback_url(settings, state))


async def handle_callback(
    session: AsyncSession,
    adapter: PlatformAdapter,
    settings: Settings,
    *,
    state: str | None,
    code: str | None,
    shop_id: str | None,
    ip: str | None,
) -> str:
    """API-72: trả `result` cho redirect: `connected` | `denied` | `error`. Không ném lỗi ra trình duyệt."""
    user_raw = await get_redis().getdel(_state_key(state)) if state else None
    if not user_raw:
        log.warning("shopee_callback_bad_state")
        return "error"
    if not code or not shop_id:
        return "denied"  # người bán bấm từ chối: Shopee không trả code (cần xác nhận T-3)
    if not is_configured(settings) or not shop_id.strip().isdigit():
        return "error"
    try:
        creds = await adapter.exchange_code(code, shop_id.strip())
        try:
            name = await adapter.shop_name(creds)
        except PlatformError as exc:
            log.warning("shopee_shop_name_failed", error=str(exc))
            name = None
    except PlatformError as exc:
        log.warning("shopee_connect_failed", error=str(exc))
        return "error"

    cipher = Cipher(settings.fernet_key)
    shop = await session.scalar(
        select(Shop)
        .where(Shop.platform == PLATFORM, Shop.platform_shop_id == creds.shop_id)
        .with_for_update()
    )
    if shop is None:
        shop = Shop(platform=PLATFORM, platform_shop_id=creds.shop_id)
        session.add(shop)
    store_credentials(shop, creds, cipher)
    shop.name = name or shop.name
    shop.last_error = None
    await session.flush()
    # MVP một shop (DEC-12): kết nối shop khác → ngắt shop cũ.
    others = (
        await session.scalars(
            select(Shop).where(
                Shop.platform == PLATFORM, Shop.id != shop.id, Shop.auth_status != "DISCONNECTED"
            )
        )
    ).all()
    for other in others:
        other.auth_status = "DISCONNECTED"
        other.access_token_enc = other.refresh_token_enc = None
    user_id = uuid.UUID(user_raw)
    audit.record(
        session, "SHOP_CONNECT", user_id=user_id, object_type="SHOP", object_id=shop.id, ip=ip,
        data={"platform": PLATFORM, "platform_shop_id": creds.shop_id, "name": shop.name},
    )  # fmt: skip
    shop_uuid = shop.id

    async def _sync_now() -> None:
        await enqueue_sync(shop_uuid, lock_held=False)

    after_commit(session, _sync_now)  # J-04 ngay sau khi kết nối (02a API-72)
    await commit(session)
    return "connected"


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


async def request_sync(session: AsyncSession, shop_id: uuid.UUID, settings: Settings) -> None:
    require_configured(settings)
    shop = await session.get(Shop, shop_id)
    if shop is None:
        raise AppError("NOT_FOUND", "Không tìm thấy shop.", 404)
    if shop.auth_status != "CONNECTED":
        raise AppError(
            "SHOP_NOT_CONNECTED", "Shop chưa kết nối hoặc ủy quyền đã hết hạn. Bấm Kết nối lại.", 409
        )
    token = await acquire_sync_lock(shop.id, "api")
    if token is None:
        raise AppError("SYNC_IN_PROGRESS", "Đang đồng bộ, thử lại sau.", 409)
    try:
        await enqueue_sync(shop.id, lock_held=token)
    except Exception:
        await release_sync_lock(shop.id, token)
        raise
