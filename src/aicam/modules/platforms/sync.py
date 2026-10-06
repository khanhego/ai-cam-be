"""Job đồng bộ sàn (02a §7, FR-05.02..04, ADR-007 polling): J-04 đơn mới, J-05 xác minh lại kiện,
J-06 trạng thái vận chuyển, J-12 làm mới token.

Mọi job không làm gì khi chưa cấu hình Shopee (`SHOPEE_ENABLED=false`). Thử lại từng lời gọi HTTP nằm trong
adapter (5 lần, giãn cách mũ — FR-05.08); lỗi cuối ghi `shop.last_error` → dashboard `SYNC_ERROR` (API-32).
"""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import (
    CANCELLED_STATUSES,
    PlatformAdapter,
    PlatformAuthError,
    PlatformError,
    PlatformOrder,
    ShipmentRef,
    ShopCredentials,
)
from aicam.realtime import publish

log = structlog.get_logger()

CURSOR_OVERLAP = timedelta(minutes=10)  # 02a J-04: since = cursor − 10 phút
REFRESH_MARGIN = timedelta(hours=1)  # 02a J-12: còn < 1 giờ thì làm mới
VERIFY_BATCH = 50
VERIFY_MAX_AGE = timedelta(days=7)
SHIPPING_BATCH = 50
SHIPPING_MAX = 1000


def _error(code: str, exc: Exception) -> dict[str, Any]:
    return {"code": code, "message": str(exc)[:500], "at": clock.iso_z(clock.now())}


def _report_updated(session: AsyncSession, settings: Settings) -> None:
    """Số "Chưa bàn giao" / "Hủy sau khi đóng" trên D2 đổi → WS-02 `report.updated` sau commit."""
    from zoneinfo import ZoneInfo

    day = clock.now().astimezone(ZoneInfo(settings.tz_display)).date().isoformat()

    async def _send() -> None:
        await publish.to_dashboard("report.updated", {"date": day})

    after_commit(session, _send)


# ---------------------------------------------------------------- J-12 (dùng cả trước J-04)


async def ensure_fresh(
    session: AsyncSession,
    shop: Shop,
    adapter: PlatformAdapter,
    cipher: Cipher,
    *,
    force: bool = False,
) -> ShopCredentials | None:
    """Token còn < 1 giờ (hoặc `force`) → refresh. Refresh bị từ chối → shop `EXPIRED` (D7 "Hết hạn") → None.

    Lỗi tạm (mạng / 5xx sau khi đã thử lại) → ném `PlatformError` để người gọi ghi `last_error`.
    """
    creds = platforms.credentials(shop, cipher)
    if creds is None:
        return None
    if not force and creds.expires_at - clock.now() > REFRESH_MARGIN:
        return creds
    try:
        fresh = await adapter.refresh(creds)
    except PlatformAuthError as exc:
        shop.auth_status = "EXPIRED"
        shop.last_error = _error("AUTH_EXPIRED", exc)
        log.warning("platform_token_expired", shop_id=str(shop.id), error=str(exc))
        return None
    platforms.store_credentials(shop, fresh, cipher)
    log.info("platform_token_refreshed", shop_id=str(shop.id), expires_at=fresh.expires_at.isoformat())
    return fresh


async def refresh_tokens(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> dict[str, int]:
    """J-12 (30 phút): làm mới token các shop `CONNECTED` sắp hết hạn.

    Refresh token Shopee dùng một lần: làm mới **dưới cùng lock `sync:{shop}` với J-04** (G3-N4), đọc lại shop
    sau khi khóa — J-04 vừa làm mới thì bỏ qua. Đang bị giữ → để J-04 (đang chạy) tự làm mới trong
    `ensure_fresh`.
    """
    out = {"refreshed": 0, "expired": 0, "failed": 0, "skipped": 0}
    if not platforms.is_configured(settings):
        return out
    cipher = Cipher(settings.fernet_key)
    shop_ids = (
        await session.scalars(
            select(Shop.id).where(
                Shop.auth_status == "CONNECTED", Shop.auth_expires_at < clock.now() + REFRESH_MARGIN
            )
        )
    ).all()
    await commit(session)
    for shop_id in shop_ids:
        token = await platforms.acquire_sync_lock(shop_id, "j12")
        if token is None:
            out["skipped"] += 1
            continue
        try:
            shop = await session.scalar(
                select(Shop)
                .where(Shop.id == shop_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if (
                shop is None
                or shop.auth_status != "CONNECTED"
                or shop.auth_expires_at is None
                or shop.auth_expires_at - clock.now() > REFRESH_MARGIN
            ):
                out["skipped"] += 1
                await commit(session)
                continue
            try:
                creds = await ensure_fresh(session, shop, adapter, cipher, force=True)
            except PlatformError as exc:
                shop.last_error = _error("REFRESH_FAILED", exc)
                out["failed"] += 1
            else:
                out["refreshed" if creds else "expired"] += 1
            await commit(session)
        finally:
            await platforms.release_sync_lock(shop_id, token)
    return out


# ---------------------------------------------------------------- J-04


async def _order_packages(session: AsyncSession, order_sn: str) -> dict[Any, str]:
    rows = (
        await session.scalars(
            select(Package)
            .join(Order, Order.id == Package.order_id)
            .where(Order.platform_order_sn == order_sn)
        )
    ).all()
    return {p.id: p.warehouse_status for p in rows}


async def _upsert(session: AsyncSession, order: PlatformOrder, shop_id: Any) -> bool:
    """Một đơn trong savepoint; station vừa tra cùng đơn (IntegrityError) → thử lại một lần.

    True = có kiện đổi `warehouse_status` (vd hủy sau khi đóng) → báo dashboard tính lại.
    """
    for attempt in (1, 2):
        try:
            async with session.begin_nested():
                before = await _order_packages(session, order.platform_order_sn)
                await orders.upsert_platform_order(session, order, shop_id=shop_id)
                after = await _order_packages(session, order.platform_order_sn)
                return any(before.get(pid, status) != status for pid, status in after.items())
        except IntegrityError:
            if attempt == 2:
                raise
    return False


@dataclass
class SyncResult:
    status: str = "OK"  # OK | SKIPPED | EXPIRED | FAILED
    orders: int = 0
    changed: int = 0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.status, "orders": self.orders, "changed": self.changed, "error": self.error}


async def sync_shop_orders(
    session: AsyncSession, shop: Shop, adapter: PlatformAdapter, settings: Settings
) -> SyncResult:
    """J-04 cho một shop (đã giữ lock `sync:{shop}`). `since` = cursor − 10 phút; lần đầu lùi
    `SHOPEE_INITIAL_SYNC_DAYS` ngày. Đơn API ghi đè đơn CSV (BR-17); đơn hủy khi kho PACKED →
    CANCELLED_AFTER_PACK (EX-P10)."""
    cipher = Cipher(settings.fernet_key)
    result = SyncResult()
    try:
        creds = await ensure_fresh(session, shop, adapter, cipher)
        await commit(session)
        if creds is None:
            return SyncResult(status="EXPIRED")
        started = clock.now()
        since = (
            shop.last_sync_cursor or started - timedelta(days=settings.shopee_initial_sync_days)
        ) - CURSOR_OVERLAP
        for attempt in (1, 2):
            try:
                async for order in adapter.list_updated_orders(creds, since):
                    result.orders += 1
                    if await _upsert(session, order, shop.id):
                        result.changed += 1
                    # DEC-162 (G3-V1): commit sau MỖI đơn — nhả khóa `order:{sn}` + khóa kiện trước khi
                    # lấy đơn / trang kế (lời gọi mạng), không giữ nhiều khóa theo thứ tự Shopee trả → không
                    # khóa chéo với API-51. Cursor vẫn chỉ tiến khi đi hết danh sách (đơn đã ghi thì lần
                    # sau ghi lại idempotent).
                    await commit(session)
                break
            except PlatformAuthError:
                # 02 §10.2 architecture: token hết hạn giữa chừng → refresh một lần rồi thử lại.
                if attempt == 2:
                    raise
                creds = await ensure_fresh(session, shop, adapter, cipher, force=True)
                await commit(session)
                if creds is None:
                    return SyncResult(status="EXPIRED", orders=result.orders)
        shop.last_sync_cursor = started
        shop.last_synced_at = clock.now()
        shop.last_error = None
        if result.changed:
            _report_updated(session, settings)
        await commit(session)
    except PlatformError as exc:
        await rollback(session)
        shop = await session.get(Shop, shop.id) or shop
        if isinstance(exc, PlatformAuthError):
            shop.auth_status = "EXPIRED"
            shop.last_error = _error("AUTH_EXPIRED", exc)
            result.status = "EXPIRED"
        else:
            shop.last_error = _error("SYNC_FAILED", exc)
            result.status = "FAILED"
        result.error = str(exc)
        _report_updated(session, settings)
        await commit(session)
        log.warning("platform_sync_failed", shop_id=str(shop.id), error=str(exc), orders=result.orders)
    log.info("platform_sync", shop_id=str(shop.id), **result.as_dict())
    return result


async def sync_orders(
    session: AsyncSession,
    adapter: PlatformAdapter,
    settings: Settings,
    shop_id: Any = None,
    *,
    lock_held: bool | str = False,
) -> dict[str, Any]:
    """J-04 (5 phút / mọi shop `CONNECTED`; API-73 / sau callback cho một shop). Lock Redis `sync:{shop}`
    chống chạy chồng (02a §6); API-73 đã giữ lock thì `lock_held` = token lock (nhả compare-and-delete —
    G3-N4); `True` = message cũ không có token."""
    out: dict[str, Any] = {}
    held_token = lock_held if isinstance(lock_held, str) else None
    if not platforms.is_configured(settings):
        if shop_id is not None and lock_held:
            await platforms.release_sync_lock(shop_id, held_token)
        return {"skipped": "not_configured"}
    query = select(Shop).where(Shop.auth_status == "CONNECTED")
    if shop_id is not None:
        query = select(Shop).where(Shop.id == shop_id)
    for shop in (await session.scalars(query)).all():
        if lock_held and shop_id is not None:
            token = held_token
        else:
            token = await platforms.acquire_sync_lock(shop.id, "job")
            if token is None:
                out[str(shop.id)] = {"status": "SKIPPED", "reason": "locked"}
                continue
        try:
            if shop.auth_status != "CONNECTED":
                out[str(shop.id)] = {"status": "SKIPPED", "reason": shop.auth_status}
                continue
            out[str(shop.id)] = (await sync_shop_orders(session, shop, adapter, settings)).as_dict()
        finally:
            await platforms.release_sync_lock(shop.id, token)
    if shop_id is not None and lock_held and not out:
        await platforms.release_sync_lock(shop_id, held_token)
    return out


# ---------------------------------------------------------------- J-05


async def verify_unverified(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> dict[str, int]:
    """J-05 (10 phút): kiện `verified=false` (BR-04) (7 ngày) → tra sàn; có → gắn đơn."""
    out = {"checked": 0, "verified": 0}
    if not platforms.is_configured(settings):
        return out
    target = await platforms.lookup_target(session, adapter, settings)
    if target is None:
        return out
    packages = (
        await session.scalars(
            select(Package)
            .where(
                Package.verified.is_(False),
                Package.order_id.is_(None),
                Package.updated_at > clock.now() - VERIFY_MAX_AGE,
            )
            .order_by(Package.updated_at.desc())
            .limit(VERIFY_BATCH)
        )
    ).all()
    codes = [p.tracking_number for p in packages]
    for code in codes:
        out["checked"] += 1
        try:
            found = await adapter.find_by_tracking(target.creds, code)
        except PlatformError as exc:
            log.warning("platform_verify_failed", code=code, error=str(exc))
            break  # sàn lỗi: để lượt sau, giữ unverified
        if found is None or code.upper() not in found.tracking_numbers:
            continue
        await _upsert(session, found, target.shop_id)
        out["verified"] += 1
        await commit(session)
    await commit(session)
    return out


# ---------------------------------------------------------------- J-06


async def _apply_shipping(session: AsyncSession, package: Package, order: Order, raw: str, hint: str | None,
                          order_status: str | None) -> bool:  # fmt: skip
    changed = False
    package.platform_logistics_status = raw or package.platform_logistics_status
    if order_status:
        order.platform_status = order_status
        if order_status in CANCELLED_STATUSES:
            return await orders.apply_platform_cancel(session, package)
    steps = {
        ("PACKED", "HANDED_OVER"): ["HANDED_OVER"],
        ("PACKED", "DELIVERED"): ["HANDED_OVER", "DELIVERED"],
        ("HANDED_OVER", "DELIVERED"): ["DELIVERED"],
        # Tín hiệu hoàn (DEC-259): T-103 chỉ áp bước "đã rời kho" như Phase 1; `→ RETURN_EXPECTED` + hồ sơ
        # hàng hoàn qua `returns.attach_or_create` ở T-105 (DEC-304).
        ("PACKED", "RETURN_EXPECTED"): ["HANDED_OVER"],
    }.get((package.warehouse_status, hint or ""), [])
    for to in steps:
        changed = (
            await orders.transition(session, package, to, source="PLATFORM", actor_label="Sàn") or changed
        )
    return changed


async def sync_shipping_status(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> dict[str, int]:
    """J-06 (15 phút): kiện `PACKED` / `HANDED_OVER` → vận chuyển sàn → `HANDED_OVER` / `DELIVERED`;
    đơn bị hủy sau khi đóng → `CANCELLED_AFTER_PACK` (FR-05.04, EX-P10)."""
    out = {"checked": 0, "changed": 0}
    if not platforms.is_configured(settings):
        return out
    target = await platforms.lookup_target(session, adapter, settings)
    if target is None:
        return out
    rows = (
        await session.execute(
            select(Package, Order)
            .join(Order, Order.id == Package.order_id)
            .where(Package.warehouse_status.in_(("PACKED", "HANDED_OVER")))
            .order_by(Package.updated_at)
            .limit(SHIPPING_MAX)
        )
    ).all()
    for i in range(0, len(rows), SHIPPING_BATCH):
        chunk = rows[i : i + SHIPPING_BATCH]
        by_code = {p.tracking_number.upper(): (p, o) for p, o in chunk}
        refs = [ShipmentRef(o.platform_order_sn, p.tracking_number) for p, o in chunk]
        try:
            statuses = await adapter.get_shipping_statuses(target.creds, refs)
        except PlatformError as exc:
            log.warning("platform_shipping_failed", error=str(exc))
            break
        changed = 0
        for st in statuses:
            pair = by_code.get(st.tracking_number.upper())
            if pair is None:
                continue
            out["checked"] += 1
            if await _apply_shipping(
                session, pair[0], pair[1], st.raw_status, st.warehouse_hint, st.order_status
            ):
                changed += 1
        out["changed"] += changed
        if changed:
            _report_updated(session, settings)
        await commit(session)
    return out
