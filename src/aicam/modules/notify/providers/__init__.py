"""Chọn nhà cung cấp theo loại kênh + `NOTIFY_TRANSPORT` (02a §7.2, §7.5, §9)."""

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.notify.providers.base import Provider, SendError
from aicam.modules.notify.providers.mock import MockProvider
from aicam.modules.notify.providers.telegram import TelegramProvider
from aicam.modules.notify.providers.zalo import DbTokenStore, ZaloProvider

__all__ = ["Provider", "SendError", "configured", "get_provider"]


def configured(channel_type: str, settings: Settings) -> bool:
    """EX-N1: loại kênh dùng được trên máy chủ. `mock` → mọi loại (dev / test thêm được kênh mock)."""
    if settings.notify_transport == "mock":
        return True
    if channel_type == "TELEGRAM":
        return bool(settings.telegram_bot_token.strip())
    if channel_type == "ZALO_OA":
        return all(
            v.strip()
            for v in (settings.zalo_app_id, settings.zalo_app_secret, settings.zalo_oa_refresh_token)
        )
    return False


def _mock_fails(channel_type: str, settings: Settings) -> bool:
    return channel_type in {t.strip().upper() for t in settings.notify_mock_fail.split(",") if t.strip()}


def get_provider(channel_type: str, settings: Settings, db: AsyncSession) -> Provider:
    """`db` chỉ để lấy `bind` cho kho token Zalo (session riêng, commit ngay — DEC-445)."""
    if settings.notify_transport == "mock":
        return MockProvider(channel_type, fail=_mock_fails(channel_type, settings))
    if channel_type == "TELEGRAM":
        return TelegramProvider(settings.telegram_bot_token, settings.telegram_api_base)
    if channel_type == "ZALO_OA":
        return ZaloProvider(
            app_id=settings.zalo_app_id,
            app_secret=settings.zalo_app_secret,
            initial_refresh_token=settings.zalo_oa_refresh_token,
            api_base=settings.zalo_api_base,
            oauth_base=settings.zalo_oauth_base,
            store=DbTokenStore(db.bind, Cipher(settings.fernet_key)),  # type: ignore[arg-type]
        )
    raise SendError("Loại kênh không hỗ trợ.", provider_code="UNKNOWN_TYPE")
