"""Fan-out job sàn: một task Celery / shop (02a §2 `dispatch.py`, §7; NFR-39, DEC-434, DEC-503).

Beat gọi task phân phối (`platforms.sync_orders`, `platforms.sync_shipping_status`, `platforms.sync_returns`
không tham số) → đọc shop `CONNECTED` của sàn **bật + đã cấu hình** → `send_task` một task / shop. Mỗi task
shop tự lấy khóa `sync:{shop}` / `sync_returns:{shop}` (không chờ), chạy trong `budget.time_budget` riêng —
shop chậm / lỗi chỉ hỏng task của nó, không kéo chu kỳ shop khác.

Queue: J-04 → `sync_fast`, J-06 / J-13 → `sync` (DEC-503). Hạ tầng queue `sync_fast` + `worker-sync-long` là
T-276 (M17) — tới lúc đó cả hai hằng trỏ queue `sync` mà `worker-sync` đang nghe (DEC-560).
"""

import uuid
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import commit, rollback
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import budget, registry, sync
from aicam.modules.platforms import service as platforms

log = structlog.get_logger()

ORDERS = "orders"
SHIPPING = "shipping"
RETURNS = "returns"

SHOP_TASKS = {
    ORDERS: "platforms.sync_shop_orders",
    SHIPPING: "platforms.sync_shop_shipping",
    RETURNS: "platforms.sync_shop_returns",
}
QUEUE_FAST = "sync"  # J-04 — T-276 đổi sang `sync_fast`
QUEUE_LONG = "sync"  # J-06, J-13
QUEUES = {ORDERS: QUEUE_FAST, SHIPPING: QUEUE_LONG, RETURNS: QUEUE_LONG}


def _platforms_for(kind: str, settings: Settings) -> list[str]:
    out = [p for p in registry.PLATFORM_CODES if registry.is_configured(p, settings)]
    if kind == RETURNS:
        out = [p for p in out if registry.returns_enabled(p, settings)]
    return out


def budget_s(kind: str, settings: Settings) -> float:
    return settings.sync_task_budget_s if kind == ORDERS else settings.sync_long_task_budget_s


async def send_shop_task(kind: str, shop_id: uuid.UUID | None, *extra: Any) -> None:
    from aicam.modules.media import jobs  # gửi Celery theo tên task (test thay sender)

    await jobs.send(SHOP_TASKS[kind], [str(shop_id) if shop_id else None, *extra], QUEUES[kind])


async def dispatch(session: AsyncSession, settings: Settings, kind: str) -> dict[str, Any]:
    """Task phân phối: một task / shop `CONNECTED` của sàn bật (J-13: + cờ trả hàng của sàn — EX-T1).

    J-06: chưa có shop Shopee `CONNECTED` nhưng Shopee bật (adapter mock dev / test) → một task không shop để
    kiện của đơn file vẫn được tra như Phase 2."""
    wanted = _platforms_for(kind, settings)
    if not wanted:
        return {"skipped": "not_configured"}
    rows = (
        await session.execute(
            select(Shop.id, Shop.platform)
            .where(Shop.platform.in_(wanted), Shop.auth_status == "CONNECTED")
            .order_by(Shop.platform, Shop.id)
        )
    ).all()
    await commit(session)
    sent: list[str] = []
    for shop_id, _platform in rows:
        await send_shop_task(kind, shop_id)
        sent.append(str(shop_id))
    if kind == SHIPPING and registry.SHOPEE in wanted and not any(p == registry.SHOPEE for _, p in rows):
        await send_shop_task(kind, None)
    log.info("platform_dispatch", kind=kind, platforms=wanted, shops=len(sent))
    return {"queued": len(sent), "shops": sent}


async def run_shop(
    session: AsyncSession,
    settings: Settings,
    kind: str,
    shop_id: uuid.UUID | None,
    *,
    lock_held: bool | str = False,
) -> dict[str, Any]:
    """Task một shop: chọn adapter theo `shop.platform` (registry), chạy job trong ngân sách riêng.

    `shop_id=None` chỉ cho J-06 đơn file (xem `dispatch`)."""
    held = lock_held if isinstance(lock_held, str) else None
    if shop_id is None:
        if kind != SHIPPING:
            return {"skipped": "no_shop"}
        adapter = registry.adapter_for(registry.SHOPEE, settings)
        with budget.time_budget(budget_s(kind, settings)):
            return await sync.sync_shipping_status(session, adapter, settings)
    shop = await session.get(Shop, shop_id)
    platform = shop.platform if shop is not None else None
    await commit(session)
    if platform is None:
        if lock_held:
            await platforms.release_sync_lock(shop_id, held)
        return {"skipped": "not_found"}
    adapter = registry.adapter_for(platform, settings)
    try:
        with budget.time_budget(budget_s(kind, settings)):
            if kind == ORDERS:
                return await sync.sync_orders(session, adapter, settings, shop_id, lock_held=lock_held)
            if kind == SHIPPING:
                return await sync.sync_shipping_status(session, adapter, settings, shop_id)
            return await sync.sync_returns(session, adapter, settings, shop_id)
    except Exception:
        await rollback(session)
        log.exception("platform_shop_task_crashed", kind=kind, shop_id=str(shop_id))
        raise


async def refresh_all(session: AsyncSession, settings: Settings) -> dict[str, Any]:
    """J-12 mọi sàn bật (theo grant — `sync.refresh_tokens`); lỗi một sàn không chặn sàn khác."""
    out: dict[str, Any] = {}
    for platform in registry.enabled_platforms(settings):
        try:
            out[platform] = await sync.refresh_tokens(
                session, registry.adapter_for(platform, settings), settings
            )
        except Exception as exc:
            await rollback(session)
            log.exception("platform_refresh_crashed", platform=platform)
            out[platform] = {"error": type(exc).__name__}
    return out
