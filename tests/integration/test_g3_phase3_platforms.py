"""Review G3 Phase 3 — đồng bộ sàn (G3-MS-1..4, 6). Mock TikTok = adapter thật trên transport giả.

Chưa test với TikTok thật (thiếu tài khoản đối tác).
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import grants, sync
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import ShopCredentials
from aicam.modules.platforms.mock.tiktok import MOCK_OPEN_ID, MockTikTokAdapter, cipher_of

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 4, 0, tzinfo=UTC)


async def _no_sleep(_: float) -> None:
    return None


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.tiktok_enabled = True
    test_settings.tiktok_adapter = "mock"


@pytest.fixture
def tiktok() -> MockTikTokAdapter:
    return MockTikTokAdapter(sleep=_no_sleep)


async def _tt_shop(
    db: AsyncSession, settings: Settings, code: str = "TTMOCKA", expires_in: timedelta = timedelta(days=7)
) -> Shop:
    shop = Shop(
        platform="TIKTOK", platform_shop_id=code, name=f"TST {code}", grant_ref=MOCK_OPEN_ID, region="VN"
    )
    platforms.store_credentials(
        shop,
        ShopCredentials(code, "a", "r", NOW + expires_in, shop_cipher=cipher_of(code)),
        Cipher(settings.fernet_key),
    )
    db.add(shop)
    await db.flush()
    return shop


# ---------------------------------------------------------------- G3-MS-1


async def test_ms1_j04_tiktok_refresh_keeps_shop_cipher(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """Token còn 30 phút → J-04 làm mới trước khi gọi; token mới phải đi kèm `shop_cipher` của shop (không thì
    TikTok từ chối mọi lời gọi cấp shop → EXPIRED mỗi chu kỳ token, đốt refresh token)."""
    shop = await _tt_shop(db, test_settings, expires_in=timedelta(minutes=30))
    out = await sync.sync_orders(db, tiktok, test_settings, shop.id)
    assert out[str(shop.id)]["status"] == "OK", out
    await db.refresh(shop)
    assert shop.auth_status == "CONNECTED"
    assert shop.shop_cipher == cipher_of("TTMOCKA")
    assert tiktok.data.tokens_issued == 1
    assert ("/order/202309/orders/search", "TTMOCKA") in tiktok.data.calls


async def test_ms1_auth_error_force_refresh_keeps_shop_cipher(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nhánh `PlatformAuthError` giữa lượt → làm mới `force` rồi thử lại: lần thử lại phải có
    `shop_cipher`."""
    shop = await _tt_shop(db, test_settings)
    tiktok.data.fail_auth.add("TTMOCKA")
    original = grants.acquire

    async def heal_then_acquire(platform: str, ref: str, *, wait_s: float = 10.0) -> str | None:
        tiktok.data.fail_auth.discard("TTMOCKA")  # sàn chấp nhận token mới
        return await original(platform, ref, wait_s=wait_s)

    monkeypatch.setattr(grants, "acquire", heal_then_acquire)
    out = await sync.sync_orders(db, tiktok, test_settings, shop.id)
    assert out[str(shop.id)]["status"] == "OK", out
    assert tiktok.data.tokens_issued == 1
    await db.refresh(shop)
    assert shop.auth_status == "CONNECTED"


async def test_ms1_refresh_grant_returns_full_credentials(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    shop = await _tt_shop(db, test_settings, expires_in=timedelta(minutes=10))
    creds = await grants.ensure_fresh(db, shop, tiktok, Cipher(test_settings.fernet_key))
    assert creds is not None
    assert (creds.shop_cipher, creds.grant_ref, creds.region) == (cipher_of("TTMOCKA"), MOCK_OPEN_ID, "VN")
    assert creds.access_token.startswith("mock-tt-access-")
