"""Kết nối nhiều shop nhiều sàn (02a §4 API-70..73, 154..156; FR-05.13, 05.14, 05.20; DEC-457).

- API-71 `POST /shops/{platform}/auth-url`: `state` Redis `oauth:{platform}:{state}` 10 phút + cookie băm gắn
  trình duyệt (G3-N7).
- API-72 / API-155 callback: `GETDEL` state (không còn → `expired`); `adapter.exchange_code` → **list** shop
  của lần ủy quyền; mỗi shop upsert `FOR UPDATE` theo id tăng; **không** ngắt shop khác; audit `SHOP_CONNECT`
  mỗi shop; sau commit J-04 + J-13 từng shop, WS `shop.updated` (kênh `ws:admin`).
- API-154 ngắt một shop (idempotent), API-156 danh sách rút gọn cho bộ lọc.
"""

import hashlib
import hmac
import secrets
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.errors import AppError
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import registry
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAdapter, PlatformError, ShopCredentials
from aicam.modules.platforms.schemas import (
    PlatformInfo,
    ShopBrief,
    ShopBriefList,
    ShopList,
    ShopOut,
    SyncWarning,
)
from aicam.realtime import publish

log = structlog.get_logger()

RESULT_PATH = "/admin/settings/platforms"  # D7 "Kết nối sàn" (DEC-457)
STATE_TTL_S = 600  # API-71: state chống CSRF hạn 10 phút
STATE_COOKIES = {registry.SHOPEE: "aicam_shopee_state", registry.TIKTOK: "aicam_tiktok_state"}


def parse_platform(raw: str) -> str:
    """`shopee` | `tiktok` (path API-71) → mã sàn; lạ → 404 `NOT_FOUND` (02 §6.2)."""
    code = raw.strip().upper()
    if code not in registry.PLATFORM_CODES:
        raise AppError("NOT_FOUND", "Không tìm thấy sàn.", 404)
    return code


def callback_path(platform: str) -> str:
    return f"/api/v1/shops/{platform.lower()}/callback"


def state_cookie(platform: str) -> str:
    return STATE_COOKIES[platform]


def state_key(platform: str, state: str) -> str:
    return f"oauth:{platform}:{state}"


def state_fingerprint(state: str) -> str:
    """Giá trị cookie gắn `state` với trình duyệt đã bấm Kết nối (G3-N7): chỉ lưu băm, không lưu `state`."""
    return hashlib.sha256(state.encode()).hexdigest()


def redirect_url(settings: Settings, platform: str, state: str) -> str:
    """Shopee giữ nguyên query của `redirect` và nối `code`, `shop_id` (DEC-123) → `state` nằm trong URL về.
    TikTok: URL về đăng ký sẵn ở Partner Center (RK-26), `state` là tham số riêng của trang ủy quyền."""
    if platform == registry.SHOPEE:
        base = settings.shopee_redirect_url or callback_path(platform)
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}state={state}"
    return settings.tiktok_redirect_url or callback_path(platform)


# ---------------------------------------------------------------- API-70


def _day_start(day: date, tz: str) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(tz))


def platform_infos(settings: Settings) -> list[PlatformInfo]:
    return [
        PlatformInfo(
            platform=p,
            enabled=registry.is_enabled(p, settings),
            returns_enabled=registry.returns_enabled(p, settings),
            configured=registry.is_configured(p, settings),
        )
        for p in registry.PLATFORM_CODES
    ]


async def shop_out(session: AsyncSession, shop: Shop, settings: Settings) -> ShopOut:
    """Một item API-70. `today_synced_orders` = đơn nguồn API của shop ghi / cập nhật từ 00:00 giờ VN."""
    start = _day_start(clock.now().astimezone(ZoneInfo(settings.tz_display)).date(), settings.tz_display)
    count = await session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.shop_id == shop.id, Order.source == "API", Order.updated_at >= start)
    )
    warnings = []
    for w in shop.sync_warnings or []:
        if isinstance(w, dict) and w.get("code") and w.get("message"):
            warnings.append(SyncWarning.model_validate(w))
    return ShopOut(
        id=shop.id,
        platform=shop.platform,
        name=shop.name,
        auth_status=shop.auth_status,
        auth_expires_at=shop.auth_expires_at,
        last_synced_at=shop.last_synced_at,
        today_synced_orders=int(count or 0),
        last_error=shop.last_error,
        region=shop.region,
        sync_warnings=warnings,
        disconnected_at=shop.disconnected_at,
        sync_in_progress=bool(await get_redis().exists(platforms.sync_lock_key(shop.id))),
    )


async def list_shops(session: AsyncSession, settings: Settings) -> ShopList:
    """API-70: cấu hình từng sàn + mọi shop (kể cả `DISCONNECTED`), sắp theo sàn rồi `created_at`."""
    shops = (await session.scalars(select(Shop).order_by(Shop.platform, Shop.created_at, Shop.id))).all()
    return ShopList(
        platforms=platform_infos(settings), items=[await shop_out(session, s, settings) for s in shops]
    )


async def brief(session: AsyncSession) -> ShopBriefList:
    """API-156: mọi shop (kể cả đã ngắt) cho bộ lọc sàn / shop, sắp theo sàn rồi tên."""
    shops = (
        await session.scalars(select(Shop).order_by(Shop.platform, Shop.name.nulls_last(), Shop.id))
    ).all()
    return ShopBriefList(
        items=[ShopBrief(id=s.id, platform=s.platform, name=s.name, auth_status=s.auth_status) for s in shops]
    )


# ---------------------------------------------------------------- WS-02 `shop.updated`


def shop_event(shop: Shop) -> dict[str, object]:
    return {
        "shop_id": str(shop.id),
        "auth_status": shop.auth_status,
        "last_synced_at": clock.iso_z(shop.last_synced_at) if shop.last_synced_at else None,
    }


def notify_shop_updated(session: AsyncSession, shop: Shop) -> None:
    """WS-02 `shop.updated` (kênh `ws:admin`) sau commit — D7 làm mới."""
    payload = shop_event(shop)

    async def _send() -> None:
        await publish.to_admin("shop.updated", payload)

    after_commit(session, _send)


# ---------------------------------------------------------------- API-71


async def auth_url(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings, platform: str, user_id: uuid.UUID
) -> tuple[str, str]:
    """Trả (URL ủy quyền, `state`) — router đặt cookie `state_fingerprint(state)`. Sàn chưa cấu hình → 503."""
    if not registry.is_configured(platform, settings):
        raise platforms.not_configured(platform)
    prepare = getattr(adapter, "prepare_auth", None)
    if callable(prepare):  # adapter mock (02a §7.2): lần lượt trả shop chưa kết nối
        connected = await session.scalars(
            select(Shop.platform_shop_id).where(Shop.platform == platform, Shop.auth_status == "CONNECTED")
        )
        prepare(set(connected.all()))
    state = secrets.token_urlsafe(24)
    await get_redis().set(state_key(platform, state), str(user_id), ex=STATE_TTL_S)
    return adapter.build_auth_url(redirect_url(settings, platform, state), state), state


# ---------------------------------------------------------------- API-72 / API-155


@dataclass(frozen=True)
class CallbackResult:
    result: str  # connected | denied | expired | error
    count: int = 0


async def _names(adapter: PlatformAdapter, creds: list[ShopCredentials]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for c in creds:
        if c.shop_name:
            out[c.shop_id] = c.shop_name
            continue
        try:
            out[c.shop_id] = await adapter.shop_name(c)
        except PlatformError as exc:
            log.warning("platform_shop_name_failed", platform=adapter.code, error=str(exc))
            out[c.shop_id] = None
    return out


async def handle_callback(
    session: AsyncSession,
    platform: str,
    adapter: PlatformAdapter,
    settings: Settings,
    *,
    state: str | None,
    code: str | None,
    shop_id: str | None,
    ip: str | None,
    state_cookie: str | None = None,
) -> CallbackResult:
    """API-72 / API-155: kết quả cho redirect. Không ném lỗi ra trình duyệt.

    `state` phải khớp cookie của trình duyệt đã gọi API-71 (G3-N7) — lệch → `error` và **không** đốt `state`
    (trình duyệt đúng vẫn hoàn tất được). `state` hết hạn / đã dùng → `expired`."""
    if not state or not state_cookie or not hmac.compare_digest(state_fingerprint(state), state_cookie):
        log.warning("platform_callback_state_cookie_mismatch", platform=platform)
        return CallbackResult("error")
    user_raw = await get_redis().getdel(state_key(platform, state))
    if not user_raw:
        log.warning("platform_callback_state_expired", platform=platform)
        return CallbackResult("expired")
    if not code:
        return CallbackResult("denied")  # người bán bấm từ chối: sàn không trả code (cần xác nhận T-3)
    if not registry.is_configured(platform, settings):
        return CallbackResult("error")
    try:
        creds = await adapter.exchange_code(code, shop_id.strip() if shop_id else None)
        names = await _names(adapter, creds)
    except PlatformError as exc:
        log.warning("platform_connect_failed", platform=platform, error=str(exc))
        return CallbackResult("error")
    if not creds:
        log.warning("platform_connect_no_shop", platform=platform)
        return CallbackResult("error")
    try:
        shops = await _store_shops(session, platform, creds, names, uuid.UUID(user_raw), ip, settings)
    except IntegrityError:
        await rollback(session)  # callback trùng chạy song song: lần kia đã tạo shop
        log.warning("platform_connect_conflict", platform=platform)
        return CallbackResult("error")
    await commit(session)
    log.info("platform_connected", platform=platform, shops=len(shops))
    return CallbackResult("connected", len(shops))


async def _store_shops(
    session: AsyncSession,
    platform: str,
    creds: list[ShopCredentials],
    names: dict[str, str | None],
    user_id: uuid.UUID,
    ip: str | None,
    settings: Settings,
) -> list[Shop]:
    cipher = Cipher(settings.fernet_key)
    by_id = {c.shop_id: c for c in creds}
    existing = {
        s.platform_shop_id: s
        for s in (
            await session.scalars(
                select(Shop)
                .where(Shop.platform == platform, Shop.platform_shop_id.in_(list(by_id)))
                .order_by(Shop.id)
                .with_for_update()
            )
        ).all()
    }
    shops: list[Shop] = []
    for psid in sorted(by_id):
        c = by_id[psid]
        shop = existing.get(psid)
        if shop is None:
            shop = Shop(platform=platform, platform_shop_id=psid)
            session.add(shop)
        platforms.store_credentials(shop, c, cipher)
        shop.grant_ref = c.grant_ref or shop.grant_ref or psid
        shop.name = names.get(psid) or shop.name
        shop.last_error = None
        shop.error_since = None
        shop.disconnected_at = None
        shop.disconnected_by = None
        shops.append(shop)
    await session.flush()
    # TikTok (FR-05.13): shop cùng grant không còn trong danh sách ủy quyền mới → không gọi API thay được nữa.
    grants = {s.grant_ref for s in shops if s.grant_ref}
    if platform == registry.TIKTOK and grants:
        dropped = (
            await session.scalars(
                select(Shop)
                .where(
                    Shop.platform == platform,
                    Shop.grant_ref.in_(grants),
                    Shop.platform_shop_id.notin_(list(by_id)),
                    Shop.auth_status != "DISCONNECTED",
                )
                .order_by(Shop.id)
                .with_for_update()
            )
        ).all()
        for shop in dropped:
            shop.auth_status = "EXPIRED"
            shop.last_error = {
                "code": "SHOP_NOT_AUTHORIZED",
                "message": (
                    "Shop không còn trong lần ủy quyền TikTok Shop mới nhất. Kết nối lại để chọn shop."
                ),
                "at": clock.iso_z(clock.now()),
            }
            shop.error_since = shop.error_since or clock.now()
            notify_shop_updated(session, shop)
    returns_on = registry.returns_enabled(platform, settings)
    for shop in shops:
        audit.record(
            session, "SHOP_CONNECT", user_id=user_id, object_type="SHOP", object_id=shop.id, ip=ip,
            data={"platform": platform, "platform_shop_id": shop.platform_shop_id, "name": shop.name},
        )  # fmt: skip
        notify_shop_updated(session, shop)
        _enqueue_after_connect(session, shop.id, returns_on)
    return shops


def _enqueue_after_connect(session: AsyncSession, shop_id: uuid.UUID, returns_on: bool) -> None:
    async def _sync_now() -> None:
        await platforms.enqueue_sync(shop_id, lock_held=False)  # J-04 ngay (02a API-72)
        if returns_on:
            await platforms.enqueue_sync_returns(shop_id)  # J-13 ngay (02a §7)

    after_commit(session, _sync_now)


# ---------------------------------------------------------------- API-154


async def disconnect(
    session: AsyncSession, shop_id: uuid.UUID, user_id: uuid.UUID, ip: str | None, settings: Settings
) -> ShopOut:
    """API-154 (FR-05.13, EX-T7): ngắt một shop — xóa token đã mã hóa, `DISCONNECTED`, giữ đơn / kiện / hồ sơ;
    job đang chạy của shop dừng ở lần kiểm kế. Đã ngắt → trả luôn (idempotent, không audit lại)."""
    shop: Shop | None = await session.scalar(select(Shop).where(Shop.id == shop_id).with_for_update())
    if shop is None:
        raise AppError("NOT_FOUND", "Không tìm thấy shop.", 404)
    if shop.auth_status != "DISCONNECTED":
        shop.auth_status = "DISCONNECTED"
        shop.access_token_enc = None
        shop.refresh_token_enc = None
        shop.auth_expires_at = None
        shop.disconnected_at = clock.now()
        shop.disconnected_by = user_id
        audit.record(
            session, "SHOP_DISCONNECT", user_id=user_id, object_type="SHOP", object_id=shop.id, ip=ip,
            data={"platform": shop.platform, "platform_shop_id": shop.platform_shop_id, "name": shop.name},
        )  # fmt: skip
        notify_shop_updated(session, shop)
        await session.flush()
    out = await shop_out(session, shop, settings)
    await commit(session)
    return out


# ---------------------------------------------------------------- API-73 (FR-05.02)


async def request_sync(session: AsyncSession, shop_id: uuid.UUID, settings: Settings) -> None:
    """API-73: J-04 ngay cho một shop (giữ lock `sync:{shop}`). Sàn của shop tắt → 503."""
    if not any(registry.is_configured(p, settings) for p in registry.PLATFORM_CODES):
        raise platforms.not_configured()
    shop = await session.get(Shop, shop_id)
    if shop is None:
        raise AppError("NOT_FOUND", "Không tìm thấy shop.", 404)
    if not registry.is_configured(shop.platform, settings):
        raise platforms.not_configured(shop.platform)
    if shop.auth_status != "CONNECTED":
        raise AppError(
            "SHOP_NOT_CONNECTED", "Shop chưa kết nối hoặc ủy quyền đã hết hạn. Bấm Kết nối lại.", 409
        )
    token = await platforms.acquire_sync_lock(shop.id, "api")
    if token is None:
        raise AppError("SYNC_IN_PROGRESS", "Đang đồng bộ, thử lại sau.", 409)
    try:
        await platforms.enqueue_sync(shop.id, lock_held=token)
    except Exception:
        await platforms.release_sync_lock(shop.id, token)
        raise
