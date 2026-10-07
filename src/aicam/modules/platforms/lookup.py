"""Tra mã lạ ở **mọi** shop đang kết nối, song song (BR-32, FR-05.19, DEC-435; 02a §5 BR-32, §8).

Đọc shop + token (DB) **trước** khi bấm giờ — timeout chỉ cắt lời gọi sàn, không cắt giữa câu SQL. Mỗi shop
`asyncio.wait_for(…, PLATFORM_LOOKUP_TIMEOUT_S)`, `gather(return_exceptions=True)`, tổng bọc
`asyncio.timeout(PLATFORM_LOOKUP_TIMEOUT_S + 0.2)`. Kết quả: 0 → chưa xác minh (như Phase 1); 1 → người gọi
ghi đơn vào đúng shop; ≥ 2 → mơ hồ (`AMBIGUOUS_SHOP` — không đoán shop).

Không ghi DB ở đây (trừ `credentials()` đánh `EXPIRED` khi không giải mã được token — như Phase 2).
"""

import asyncio
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import registry
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformAdapter, PlatformOrder, ShopCredentials

log = structlog.get_logger()

TOTAL_SLACK_S = 0.2  # 02a BR-32: tổng = timeout mỗi shop + 0,2 giây


@dataclass(frozen=True)
class Target:
    adapter: PlatformAdapter
    platform: str
    creds: ShopCredentials | None
    shop_id: uuid.UUID | None
    shop_name: str | None


@dataclass(frozen=True)
class Hit:
    order: PlatformOrder
    platform: str
    shop_id: uuid.UUID | None
    shop_name: str | None


@dataclass
class LookupResult:
    hits: list[Hit] = field(default_factory=list)
    shops: int = 0  # số shop đã hỏi
    failed: int = 0  # lỗi sàn / quá hạn
    duration_ms: int = 0

    @property
    def ambiguous(self) -> bool:
        return len(self.hits) >= 2

    @property
    def single(self) -> Hit | None:
        return self.hits[0] if len(self.hits) == 1 else None

    def shops_brief(self) -> list[dict[str, str | None]]:
        """`[{platform, name}]` cho sự kiện phiên `AMBIGUOUS_SHOP` / dòng thời gian API-31 (02 §6.2)."""
        return [{"platform": h.platform, "name": h.shop_name} for h in self.hits]


def adapters_for(
    settings: Settings,
    override: PlatformAdapter | Mapping[str, PlatformAdapter] | None = None,
    *,
    scan: bool = False,
) -> dict[str, PlatformAdapter]:
    """Adapter của mọi sàn **bật + đã cấu hình** (registry); `override` (dependency API / test) thay sàn của
    nó. `scan` (tra khi quét — API-11 / 104): giữ hành vi Phase 1–2 — adapter Shopee của dependency (mock
    luôn tra kể cả `SHOPEE_ENABLED=false` — dev / test, production cấm mock); TikTok chỉ khi bật (AC-44) —
    DEC-561."""

    def wanted(platform: str, adapter: PlatformAdapter) -> bool:
        if registry.is_configured(platform, settings):
            return True
        # Phase 1–2: tra khi quét dùng adapter Shopee của dependency (`get_adapter`: mock luôn có, thật
        # chỉ khi đã cấu hình — chưa thì `UnconfiguredAdapter`).
        return scan and platform == registry.SHOPEE and not isinstance(adapter, platforms.UnconfiguredAdapter)

    candidates = {p: registry.adapter_for(p, settings) for p in registry.PLATFORM_CODES}
    if override is not None:
        candidates.update(override if isinstance(override, Mapping) else {override.code: override})
    return {p: a for p, a in candidates.items() if wanted(p, a)}


def _tokenless(adapter: PlatformAdapter) -> bool:
    """Adapter mock tra không cần token (dev / test như Phase 2)."""
    return bool(getattr(adapter, "is_mock", False))


async def targets(
    session: AsyncSession, settings: Settings, adapters: Mapping[str, PlatformAdapter]
) -> list[Target]:
    """Mọi shop `CONNECTED` của các sàn trong `adapters`, có token đọc được (mock: không cần token; mock chưa
    có shop → một đích không shop như Phase 2)."""
    cipher = Cipher(settings.fernet_key)
    out: list[Target] = []
    for platform, adapter in adapters.items():
        shops = (
            await session.scalars(
                select(Shop)
                .where(Shop.platform == platform, Shop.auth_status == "CONNECTED")
                .order_by(Shop.created_at, Shop.id)
            )
        ).all()
        for shop in shops:
            creds = platforms.credentials(shop, cipher)
            if creds is None and _tokenless(adapter):
                expires = shop.auth_expires_at or shop.created_at
                creds = ShopCredentials(shop.platform_shop_id, "", "", expires, shop_cipher=shop.shop_cipher)
            if creds is not None:
                out.append(Target(adapter, platform, creds, shop.id, shop.name))
        if not shops and _tokenless(adapter):
            out.append(Target(adapter, platform, None, None, None))
    return out


def _matches(order: PlatformOrder, code: str, by_order_sn: bool) -> bool:
    if order.fulfilled_by_platform:
        return False  # EX-T5: đơn kho của sàn — không đóng gói / không nhận hoàn tại kho
    if code in (t.upper() for t in order.tracking_numbers):
        return True
    return by_order_sn and order.platform_order_sn.upper() == code


async def _ask(target: Target, code: str, by_order_sn: bool) -> PlatformOrder | None:
    if by_order_sn:
        found = await target.adapter.get_order(target.creds, code)
        if found is not None:
            return found
    return await target.adapter.find_by_tracking(target.creds, code)


async def find_everywhere(
    session: AsyncSession,
    code: str,
    settings: Settings,
    adapters: PlatformAdapter | Mapping[str, PlatformAdapter] | None = None,
    *,
    by_order_sn: bool = False,
) -> LookupResult:
    """BR-32: tra `code` (mã vận đơn; `by_order_sn` — bàn hoàn — thử cả mã đơn trước) ở mọi shop song song.

    Lỗi / quá hạn một shop chỉ làm shop đó "không thấy" (không ném). Kết quả trả đơn đã ánh xạ của sàn; người
    gọi ghi DB (savepoint) theo `single` / `hits`."""
    code = code.strip().upper()
    started = time.monotonic()
    # Mapping = người gọi đã chọn sàn (J-05); một adapter / None = tra khi quét (giữ hành vi mock Phase 2).
    chosen = dict(adapters) if isinstance(adapters, Mapping) else adapters_for(settings, adapters, scan=True)
    found_targets = await targets(session, settings, chosen)
    result = LookupResult(shops=len(found_targets))
    if not found_targets:
        return result
    per_shop = settings.platform_lookup_timeout_s
    answers: list[PlatformOrder | BaseException | None]
    try:
        async with asyncio.timeout(per_shop + TOTAL_SLACK_S):
            answers = await asyncio.gather(
                *(asyncio.wait_for(_ask(t, code, by_order_sn), per_shop) for t in found_targets),
                return_exceptions=True,
            )
    except TimeoutError:
        answers = [TimeoutError()] * len(found_targets)
    for target, answer in zip(found_targets, answers, strict=True):
        if isinstance(answer, BaseException):
            result.failed += 1
            if not isinstance(answer, (TimeoutError, asyncio.CancelledError)):
                log.info(
                    "platform_lookup_shop_failed", code=code, platform=target.platform,
                    shop_id=str(target.shop_id), error=type(answer).__name__,
                )  # fmt: skip
            continue
        if answer is not None and _matches(answer, code, by_order_sn):
            result.hits.append(Hit(answer, target.platform, target.shop_id, target.shop_name))
    result.duration_ms = round((time.monotonic() - started) * 1000)
    log.info(
        "platform_lookup", code=code, shops=result.shops, found=len(result.hits), ambiguous=result.ambiguous,
        failed=result.failed, duration_ms=result.duration_ms,
    )  # fmt: skip
    return result
