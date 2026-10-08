"""Dữ liệu mẫu Phase 3 cho `aicam seed-demo` (T-211; 02a §1, §7.2; 04-test-cases §1 "Dữ liệu test").

4 shop mock `CONNECTED` (token giả mã hóa Fernet) rồi chạy J-04 + J-13 **thật** của từng shop qua adapter
mock:

- Shopee `990001` "TST Shop (mock)" — nhận mọi đơn Phase 1–2 đã seed (đơn chưa gắn shop → BR-29 "nhận").
- Shopee `990002` "TST B" — `SPXTSTB000000001..20`, đơn trùng mã `2410DUP00001`, `SPXTSTX0000001`, đơn
  `2410TSTB0015` yêu cầu hủy rồi bị từ chối, yêu cầu trả `RSDUP0000001` mã chiều về `RTTST-DUP-1`.
- TikTok `TTMOCKA`, `TTMOCKB` (một grant `MOCK-OPEN-1`) — fixture định dạng TikTok qua adapter + mapping thật:
  9 trạng thái + `XYZ`, kiện gộp `TTTST0000000077`, đơn kho TikTok (bỏ qua), 6 kịch bản trả, EX-T2, mã lạ.

Idempotent: shop theo (`platform`, `platform_shop_id`); đơn / yêu cầu trả upsert. Link chia sẻ mẫu và kênh
thông báo mock thuộc M16 / M17 (module chưa có) — thêm ở T-224 / T-227.
"""

from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformAdapter, ShopCredentials
from aicam.modules.platforms.mock.adapter import (
    MOCK_DATA_SINCE,
    MOCK_SHOP_B_NAME,
    MOCK_SHOP_NAME,
    MockAdapter,
)
from aicam.modules.platforms.mock.tiktok import MOCK_OPEN_ID, MOCK_TT_SHOPS, MockTikTokAdapter, cipher_of

SHOPEE_SHOPS = (("990001", MOCK_SHOP_NAME), ("990002", MOCK_SHOP_B_NAME))


async def _shop(
    session: AsyncSession,
    platform: str,
    psid: str,
    name: str,
    settings: Settings,
    *,
    grant: str,
    cipher: str | None = None,
) -> tuple[Shop, bool]:
    shop = await session.scalar(select(Shop).where(Shop.platform == platform, Shop.platform_shop_id == psid))
    created = shop is None
    if shop is None:
        shop = Shop(platform=platform, platform_shop_id=psid)
        session.add(shop)
    if created or shop.auth_status != "CONNECTED":
        creds = ShopCredentials(
            psid, f"mock-seed-access-{psid}", f"mock-seed-refresh-{psid}", clock.now() + timedelta(hours=4),
            shop_cipher=cipher, grant_ref=grant, region="VN" if platform == "TIKTOK" else None,
        )  # fmt: skip
        platforms.store_credentials(shop, creds, Cipher(settings.fernet_key))
        shop.disconnected_at = None
    shop.name = shop.name or name
    shop.grant_ref = shop.grant_ref or grant
    await session.flush()
    return shop, created


async def seed_phase3(session: AsyncSession, settings: Settings) -> list[str]:
    """Tạo 4 shop + đồng bộ đơn / yêu cầu trả bằng adapter mock. Trả các dòng log cho CLI."""
    lines: list[str] = []
    shopee = MockAdapter.multi_shop([s for s, _ in SHOPEE_SHOPS])
    tiktok = MockTikTokAdapter()
    targets: list[tuple[Shop, PlatformAdapter]] = []
    for psid, name in SHOPEE_SHOPS:
        shop, created = await _shop(session, "SHOPEE", psid, name, settings, grant=psid)
        targets.append((shop, shopee))
        lines.append(f"{'+' if created else '='} shop Shopee {psid} {shop.name}")
    for psid, name in MOCK_TT_SHOPS.items():
        shop, created = await _shop(
            session, "TIKTOK", psid, name, settings, grant=MOCK_OPEN_ID, cipher=cipher_of(psid)
        )
        targets.append((shop, tiktok))
        lines.append(f"{'+' if created else '='} shop TikTok {psid} {shop.name}")
    await session.commit()
    # J-04 lần đầu của shop Shopee chỉ lùi `shopee_initial_sync_days` (3 ngày): dữ liệu mock Phase 1–2 có
    # `updated_at` cố định 01/10/2026 → seed chạy sau ngày đó thì `990001` không nhận đơn nào (đơn Phase 1
    # không shop, nhóm `UNKNOWN` → BR-01 / BR-21 không áp, vd. SPXTST0000009 hủy mà quét vẫn mở phiên —
    # T-229, DEC-821). Seed lùi tới mốc dữ liệu mock.
    lookback = max(settings.shopee_initial_sync_days, (clock.now() - MOCK_DATA_SINCE).days + 1)
    seed_settings = settings.model_copy(
        update={
            "shopee_initial_sync_days": lookback,
            "shopee_enabled": True,
            "platform_adapter": "mock",
            "tiktok_enabled": True,
            "tiktok_adapter": "mock",
            "tiktok_returns_enabled": True,
        }
    )
    for shop, adapter in targets:
        shop_id, label = shop.id, f"{shop.platform} {shop.platform_shop_id}"
        out = await sync.sync_orders(session, adapter, seed_settings, shop_id)
        res = out.get(str(shop_id), {})
        lines.append(f"= J-04 {label}: {res.get('status')} {res.get('orders', 0)} đơn")
        out = await sync.sync_returns(session, adapter, seed_settings, shop_id)
        res = out.get(str(shop_id), {})
        lines.append(f"= J-13 {label}: {res.get('status', res)} {res.get('orders', 0)} yêu cầu trả")
    return lines
