"""Chọn adapter theo cấu hình (02a §9 `PLATFORM_ADAPTER`)."""

from functools import lru_cache

from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.platforms.base import PlatformAdapter
from aicam.modules.platforms.mock.adapter import MockAdapter


@lru_cache
def _mock() -> MockAdapter:
    return MockAdapter()


def get_adapter(settings: Settings) -> PlatformAdapter:
    if settings.platform_adapter == "mock":
        return _mock()
    # Adapter Shopee thật thêm ở T-16 sau spike S1 (T-3).
    raise AppError(
        "PLATFORM_NOT_CONFIGURED",
        "Chưa cấu hình Shopee Open Platform. Dùng Nhập đơn từ file.",
        503,
    )
