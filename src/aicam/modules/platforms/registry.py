"""Sổ đăng ký sàn (02a §2, ADR-011): sàn nào bật, đã cấu hình, có đồng bộ yêu cầu trả, adapter của sàn.

Lõi hỏi registry theo `shop.platform`, không gán cứng một sàn (NFR-28). Shopee giữ logic Phase 2
(`service.get_adapter` / `service.is_configured` — tên cũ là alias Shopee). TikTok: cờ + khóa ở đây;
adapter thật ở T-208 / T-209, mock 2 shop ở T-211 — trước đó `adapter_for("TIKTOK")` trả adapter
"chưa cấu hình" (quét → kiện chưa xác minh, job bỏ qua sàn).
"""

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


def adapter_for(platform: str, settings: Settings) -> PlatformAdapter:
    if _check(platform) == SHOPEE:
        return service.get_adapter(settings)
    return service.UnconfiguredAdapter(TIKTOK)  # TikTok: adapter thật / mock ở T-208, T-209, T-211
