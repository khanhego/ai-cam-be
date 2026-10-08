"""Sổ đăng ký sàn (02a §2, ADR-011): sàn nào bật, đã cấu hình, có đồng bộ yêu cầu trả, adapter của sàn.

Lõi hỏi registry theo `shop.platform`, không gán cứng một sàn (NFR-28). Shopee giữ logic Phase 2
(`service.get_adapter` / `service.is_configured` — tên cũ là alias Shopee). TikTok: cờ + khóa ở đây;
adapter thật ở T-208 / T-209, mock 2 shop ở T-211 — trước đó `adapter_for("TIKTOK")` trả adapter
"chưa cấu hình" (quét → kiện chưa xác minh, job bỏ qua sàn).
"""

from datetime import timedelta
from functools import lru_cache

from aicam.core.settings import Settings
from aicam.modules.platforms import service
from aicam.modules.platforms.base import PlatformAdapter

SHOPEE = "SHOPEE"
TIKTOK = "TIKTOK"
PLATFORM_CODES = (SHOPEE, TIKTOK)


def _check(platform: str) -> str:
    code = platform.upper()
    if code not in PLATFORM_CODES:
        raise ValueError(f"Sàn không hỗ trợ: {platform}")
    return code


def is_enabled(platform: str, settings: Settings) -> bool:
    """Cờ bật sàn (FR-05.20): `SHOPEE_ENABLED`, `TIKTOK_ENABLED`."""
    return settings.shopee_enabled if _check(platform) == SHOPEE else settings.tiktok_enabled


def enabled_platforms(settings: Settings) -> list[str]:
    return [p for p in PLATFORM_CODES if is_enabled(p, settings)]


def is_configured(platform: str, settings: Settings) -> bool:
    """Bật + đủ khóa (hoặc adapter mock); chưa cấu hình → API-71 / 73 trả 503 `PLATFORM_NOT_CONFIGURED`."""
    if _check(platform) == SHOPEE:
        return service.is_configured(settings)
    if not settings.tiktok_enabled:
        return False
    if settings.tiktok_adapter == "mock":
        return True
    return bool(settings.tiktok_app_key and settings.tiktok_app_secret and settings.tiktok_service_id)


def returns_enabled(platform: str, settings: Settings) -> bool:
    """J-13 cho sàn (EX-T1): Shopee như Phase 2 (G3 F-11 — adapter mock luôn chạy); TikTok cờ riêng."""
    if not is_configured(platform, settings):
        return False
    if _check(platform) == SHOPEE:
        return settings.platform_adapter == "mock" or settings.shopee_returns_enabled
    return settings.tiktok_returns_enabled


@lru_cache
def _tiktok(
    app_key: str, app_secret: str, api_base: str, auth_base: str, authorize_url: str, service_id: str,
    timeout_s: float, attempts: int, backoff_s: float, lookback_min: int,
) -> PlatformAdapter:  # fmt: skip
    from aicam.modules.platforms.tiktok.adapter import TikTokAdapter
    from aicam.modules.platforms.tiktok.client import TikTokClient

    client = TikTokClient(
        app_key,
        app_secret,
        api_base,
        auth_base,
        timeout_s=timeout_s,
        max_attempts=attempts,
        backoff_s=backoff_s,
    )
    return TikTokAdapter(
        client, authorize_url=authorize_url, service_id=service_id,
        lookup_lookback=timedelta(minutes=lookback_min),
    )  # fmt: skip


@lru_cache
def _tiktok_mock() -> PlatformAdapter:
    from aicam.modules.platforms.mock.tiktok import MockTikTokAdapter

    return MockTikTokAdapter()


def adapter_for(platform: str, settings: Settings) -> PlatformAdapter:
    if _check(platform) == SHOPEE:
        return service.get_adapter(settings)
    if not is_configured(TIKTOK, settings):
        return service.UnconfiguredAdapter(TIKTOK)
    if settings.tiktok_adapter == "mock":
        return _tiktok_mock()  # 2 shop giả, adapter thật trên transport giả (02a §7.2)
    return _tiktok(
        settings.tiktok_app_key, settings.tiktok_app_secret, settings.tiktok_api_base,
        settings.tiktok_auth_base, settings.tiktok_authorize_url, settings.tiktok_service_id,
        settings.tiktok_timeout_s, settings.tiktok_max_attempts, settings.tiktok_backoff_s,
        settings.tiktok_lookup_lookback_min,
    )  # fmt: skip
