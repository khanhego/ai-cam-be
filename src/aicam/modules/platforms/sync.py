"""Job đồng bộ sàn (02a §7, FR-05.02..04, ADR-007 polling): J-04 đơn mới, J-05 xác minh lại kiện,
J-06 trạng thái vận chuyển, J-12 làm mới token, J-13 yêu cầu trả (Phase 2 — FR-05.05, 05.11, 05.12).

Mọi job không làm gì khi chưa cấu hình Shopee (`SHOPEE_ENABLED=false`). Thử lại từng lời gọi HTTP nằm trong
adapter (5 lần, giãn cách mũ — FR-05.08); lỗi cuối ghi `shop.last_error` → dashboard `SYNC_ERROR` (API-32).
"""

import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import (
    CANCEL_GROUPS,
    PlatformAdapter,
    PlatformAuthError,
    PlatformError,
    PlatformOrder,
    PlatformReturn,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.reconciliation import service as reconciliation
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase, ReturnCasePackage
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

    Phase 2 (02a §7 J-04 mở rộng, T-105): sau upsert gộp hồ sơ chưa xác định theo mã (DEC-269); đơn
    `TO_RETURN` hoặc hủy khi kiện đã giao ĐVVC → tín hiệu giao thất bại (DEC-258, 259).
    True = có kiện đổi `warehouse_status` (vd hủy sau khi đóng) → báo dashboard tính lại.
    """
    for attempt in (1, 2):
        try:
            async with session.begin_nested():
                before = await _order_packages(session, order.platform_order_sn)
                result = await orders.upsert_platform_order(session, order, shop_id=shop_id)
                merged = await returns.merge_unidentified_by_code(session, result.order)
                signalled = await _order_return_signal(session, result.order, order)
                after = await _order_packages(session, order.platform_order_sn)
                return (
                    any(before.get(pid, status) != status for pid, status in after.items())
                    or bool(merged)
                    or signalled
                )
        except IntegrityError:
            if attempt == 2:
                raise
    return False


# Đơn `TO_RETURN` trên sàn: kiện chưa ghi nhận rời kho / đang trên đường → giao thất bại (DEC-259, DEC-254).
# Kiện `DELIVERED` không tính: hoàn sau khi giao là yêu cầu trả (J-13) — DEC-316.
_TO_RETURN_FROM = ("NEW", "PACKED", "HANDED_OVER")


async def _order_return_signal(session: AsyncSession, order: Order, data: PlatformOrder) -> bool:
    """J-04: đơn `TO_RETURN` → kiện `NEW` / `PACKED` / `HANDED_OVER`; đơn hủy → kiện `HANDED_OVER` (hủy sau
    khi ĐVVC lấy hàng, boom COD — DEC-258). Người gọi đã khóa `order:{sn}`."""
    if data.status_group == "RETURNING":
        wanted: tuple[str, ...] = _TO_RETURN_FROM
    elif data.is_cancelled:
        wanted = ("HANDED_OVER",)
    else:
        return False
    packages = [p for p in await returns.packages_of_order(session, order.id) if p.warehouse_status in wanted]
    return await _failed_delivery(session, order, packages, data.updated_at)


async def _failed_delivery(
    session: AsyncSession, order: Order, packages: list[Package], at: datetime | None
) -> bool:
    """Tín hiệu giao thất bại cho các kiện (đã khóa `order:{sn}`): `PACKED → HANDED_OVER` rồi
    `returns.attach_or_create(FAILED_DELIVERY)` chuyển `→ RETURN_EXPECTED` (02 §5.3, DEC-259). Khóa hồ sơ mở
    của đơn trước kiện (DEC-266). True = có kiện đổi trạng thái."""
    if not packages:
        return False
    await returns.open_case_of_order(session, order.id, for_update=True)
    changed = False
    eligible: list[Any] = []
    for package in await returns.lock_packages(session, [p.id for p in packages]):
        if package.warehouse_status == "PACKED":
            changed = await orders.transition(
                session, package, "HANDED_OVER", source="PLATFORM", actor_label="Sàn"
            )
        if package.warehouse_status in ("NEW", "HANDED_OVER"):
            eligible.append(package.id)
    if not eligible:
        return changed
    signal = returns.Signal(
        returns.SIGNAL_FAILED_DELIVERY,
        key=returns.failed_signal_key(order.platform_order_sn, at),
        package_ids=tuple(eligible),
    )
    result = await returns.attach_or_create(session, order, signal)
    if result.case is not None and (result.created or result.moved_packages):
        returns.notify_updated(session, result.case)
        log.info(
            "failed_delivery_signal", return_case_id=str(result.case.id), order_sn=order.platform_order_sn,
            moved=len(result.moved_packages), created=result.created,
        )  # fmt: skip
        return True
    return changed


@dataclass
class SyncResult:
    status: str = "OK"  # OK | SKIPPED | EXPIRED | FAILED
    orders: int = 0
    changed: int = 0
    error: str | None = None
    skipped: int = 0  # J-13: yêu cầu trả bỏ qua lượt này (đưa vào danh sách thử lại — G3 F-6)

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"status": self.status, "orders": self.orders, "changed": self.changed,
                               "error": self.error}  # fmt: skip
        if self.skipped:
            out["skipped"] = self.skipped
        return out


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
        if not (shop.last_error and shop.last_error.get("job") == RETURNS_JOB):  # G3 F-7: không xóa lỗi J-13
            shop.last_error = None
        if result.changed:
            _report_updated(session, settings)
            reconciliation.request_run_soon(session)  # J-14 sau 30 giây (02a §7)
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
                Package.is_placeholder.is_(False),  # kiện tạm TAM- không có trên sàn (G3 SM-F7)
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


async def _apply_shipping(session: AsyncSession, package_id: Any, order_id: Any, st: ShippingStatus) -> bool:
    """Áp một trạng thái vận chuyển. Khóa `order:{sn}` → hồ sơ mở của đơn → kiện, đọc lại sau khóa (R3-4).

    - Đơn hủy: `apply_platform_cancel` (NEW / PACKED / PACKING — BR-21); kiện `HANDED_OVER` → giao thất bại
      (hủy sau khi ĐVVC lấy, DEC-258).
    - Hint `RETURN_EXPECTED` (DEC-259): `PACKED → HANDED_OVER → RETURN_EXPECTED`,
      `HANDED_OVER → RETURN_EXPECTED` qua `attach_or_create(FAILED_DELIVERY)` (chỉ tạo hồ sơ khi đơn không có
      hồ sơ mở).
    - Kiện `RETURN_EXPECTED` / `MISSING` của hồ sơ giao thất bại, hint `HANDED_OVER` / `DELIVERED` → giao lại.
    """
    order = await session.get(Order, order_id)
    if order is None:
        return False
    await orders.lock_orders(session, [order.platform_order_sn])
    order = await session.scalar(
        select(Order).where(Order.id == order_id).execution_options(populate_existing=True)
    )
    if order is None:
        return False
    case = await returns.open_case_of_order(session, order.id, for_update=True)
    locked = await returns.lock_packages(session, [package_id])
    if not locked:
        return False
    package = locked[0]
    package.platform_logistics_status = st.raw_status or package.platform_logistics_status
    if st.order_status:
        order.platform_status = st.order_status
        order.platform_status_group = st.order_status_group or "UNKNOWN"
        if order.platform_status_group in CANCEL_GROUPS:
            changed = await orders.apply_platform_cancel(session, package)
            if package.warehouse_status == "HANDED_OVER":
                changed = await _failed_delivery(session, order, [package], st.updated_at) or changed
            return changed
    hint = st.warehouse_hint
    status = package.warehouse_status
    if status in ("RETURN_EXPECTED", "RETURN_MISSING"):
        if (
            case is not None
            and case.kind == "FAILED_DELIVERY"
            and hint in ("HANDED_OVER", "DELIVERED")
            and case.id in await returns.open_case_ids_of_package(session, package.id)
            and await returns.apply_redelivery(session, case, package, hint)
        ):
            returns.notify_updated(session, case)
            return True
        return False
    if hint == "RETURN_EXPECTED" and status in ("NEW", "PACKED", "HANDED_OVER"):
        return await _failed_delivery(session, order, [package], st.updated_at)
    steps = {
        ("PACKED", "HANDED_OVER"): ["HANDED_OVER"],
        ("PACKED", "DELIVERED"): ["HANDED_OVER", "DELIVERED"],
        ("HANDED_OVER", "DELIVERED"): ["DELIVERED"],
    }.get((status, hint or ""), [])
    changed = False
    for to in steps:
        changed = (
            await orders.transition(session, package, to, source="PLATFORM", actor_label="Sàn") or changed
        )
    return changed


async def sync_shipping_status(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> dict[str, int]:
    """J-06 (15 phút): kiện `PACKED` / `HANDED_OVER` → vận chuyển sàn → `HANDED_OVER` / `DELIVERED`;
    đơn bị hủy sau khi đóng → `CANCELLED_AFTER_PACK` (FR-05.04, EX-P10). Phase 2 (T-105): giao thất bại / boom
    COD → hồ sơ `FAILED_DELIVERY`, kiện `RETURN_EXPECTED`; kiện của hồ sơ giao thất bại được giao lại."""
    out = {"checked": 0, "changed": 0}
    if not platforms.is_configured(settings):
        return out
    target = await platforms.lookup_target(session, adapter, settings)
    if target is None:
        return out
    failed_packages = (
        select(ReturnCasePackage.package_id)
        .join(ReturnCase, ReturnCase.id == ReturnCasePackage.return_case_id)
        .where(ReturnCase.kind == "FAILED_DELIVERY", ReturnCase.status.in_(OPEN_CASE_STATUSES))
    )
    rows = (
        await session.execute(
            select(Package, Order)
            .join(Order, Order.id == Package.order_id)
            .where(
                or_(
                    Package.warehouse_status.in_(("PACKED", "HANDED_OVER")),
                    and_(
                        Package.warehouse_status.in_(("RETURN_EXPECTED", "RETURN_MISSING")),
                        Package.id.in_(failed_packages),
                    ),
                )
            )
            .order_by(Package.updated_at)
            .limit(SHIPPING_MAX)
        )
    ).all()
    await commit(session)
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
            if await _apply_shipping(session, pair[0].id, pair[1].id, st):
                changed += 1
                _report_updated(session, settings)
                reconciliation.request_run_soon(session)
            await commit(session)  # nhả `order:{sn}` + khóa kiện sau mỗi kiện (như J-04 — DEC-162)
        out["changed"] += changed
    return out


# ---------------------------------------------------------------- J-13 (Phase 2, T-105)

RETURNS_LOCK_TTL_S = 600  # 02a §6: J-13 chạy chồng → lock Redis `sync_returns:{shop}` 600 giây
RETURNS_JOB = "returns"  # đánh dấu `shop.last_error.job` — J-13 thành công chỉ xóa lỗi của chính nó

_RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) end
return 0
"""


def returns_lock_key(shop_id: Any) -> str:
    return f"sync_returns:{shop_id}"


async def _acquire_returns_lock(shop_id: Any) -> str | None:
    token = secrets.token_hex(8)
    ok = await get_redis().set(returns_lock_key(shop_id), token, nx=True, ex=RETURNS_LOCK_TTL_S)
    return token if ok else None


async def _release_returns_lock(shop_id: Any, token: str) -> None:
    await get_redis().eval(_RELEASE_IF_OWNER, 1, returns_lock_key(shop_id), token)  # type: ignore[misc]


RETRY_KEY = "returns_retry:{shop}"  # Redis hash return_sn → JSON {order_sn, attempts, reason}
RETRY_MAX_ATTEMPTS = 8  # ~2 giờ với nhịp 15 phút; quá → bỏ + log error (không kẹt mãi)


class ReturnSkipped(Exception):
    """Một yêu cầu trả chưa xử lý được (đơn chưa lấy được, dữ liệu lạ) — đưa vào danh sách thử lại."""


async def _sync_one_return(
    session: AsyncSession, ret: PlatformReturn, adapter: PlatformAdapter, creds: ShopCredentials | None,
    shop_id: Any,
) -> returns.AttachResult | None:  # fmt: skip
    """Một yêu cầu trả: đơn chưa có → `get_order` + upsert (savepoint) như J-04; rồi
    `returns.upsert_from_platform` + gộp hồ sơ chưa xác định theo mã chiều về (02 §6.3 #6). Sàn không trả đơn
    → `ReturnSkipped` (G3 R10: không mất yêu cầu — thử lại ở lượt sau)."""
    order = await session.scalar(select(Order).where(Order.platform_order_sn == ret.order_sn))
    if order is None:
        data = await adapter.get_order(creds, ret.order_sn)
        if data is None:
            raise ReturnSkipped("ORDER_NOT_FOUND")
        for attempt in (1, 2):
            try:
                async with session.begin_nested():
                    order = (await orders.upsert_platform_order(session, data, shop_id=shop_id)).order
                    await returns.merge_unidentified_by_code(session, order)
                break
            except IntegrityError:
                if attempt == 2:
                    raise
        if order is None:
            raise ReturnSkipped("ORDER_NOT_FOUND")
    result = await returns.upsert_from_platform(session, order, ret)
    if (
        result.case is not None
        and result.case.return_tracking_number
        and await returns.merge_unidentified_by_code(session, order)
    ):
        result.changed = True
    if result.case is not None and result.changed:
        returns.notify_updated(session, result.case)
    return result


async def _remember_retry(shop_id: Any, return_sn: str, order_sn: str, reason: str) -> None:
    key = RETRY_KEY.format(shop=shop_id)
    redis = get_redis()
    raw = await redis.hget(key, return_sn)  # type: ignore[misc]
    attempts = (json.loads(raw)["attempts"] if raw else 0) + 1
    if attempts > RETRY_MAX_ATTEMPTS:
        await redis.hdel(key, return_sn)  # type: ignore[misc]
        log.error("returns_retry_dropped", shop_id=str(shop_id), return_sn=return_sn, order_sn=order_sn,
                  reason=reason, attempts=attempts - 1)  # fmt: skip
        return
    await redis.hset(  # type: ignore[misc]
        key, return_sn, json.dumps({"order_sn": order_sn, "attempts": attempts, "reason": reason})
    )


async def _forget_retry(shop_id: Any, return_sn: str) -> None:
    await get_redis().hdel(RETRY_KEY.format(shop=shop_id), return_sn)  # type: ignore[misc]


async def _process(
    session: AsyncSession, ret: PlatformReturn, adapter: PlatformAdapter, creds: ShopCredentials | None,
    shop_id: Any, result: SyncResult, kinds: dict[str, int], settings: Settings,
) -> None:  # fmt: skip
    """Một bản ghi: lỗi sàn (PlatformError) → ném (cả lượt dừng, cursor không tiến — như cũ); lỗi khác của bản
    ghi này (G3 F-6, R10) → rollback, log `return_sn`, đếm `skipped`, đưa vào danh sách thử lại, đi tiếp."""
    try:
        attached = await _sync_one_return(session, ret, adapter, creds, shop_id)
        if attached is not None and attached.case is not None and attached.changed:
            result.changed += 1
            kinds[attached.case.kind] = kinds.get(attached.case.kind, 0) + 1
            _report_updated(session, settings)
            reconciliation.request_run_soon(session)
        await commit(session)
        await _forget_retry(shop_id, ret.return_sn)
    except PlatformError:
        raise
    except Exception as exc:
        await rollback(session)
        result.skipped += 1
        reason = str(exc) if isinstance(exc, ReturnSkipped) else type(exc).__name__
        log.warning(
            "returns_sync_record_skipped", shop_id=str(shop_id), return_sn=ret.return_sn,
            order_sn=ret.order_sn,
            reason=reason, exc_info=not isinstance(exc, ReturnSkipped),
        )  # fmt: skip
        await _remember_retry(shop_id, ret.return_sn, ret.order_sn, reason)


async def _retry_pending(
    session: AsyncSession, shop_id: Any, adapter: PlatformAdapter, creds: ShopCredentials | None,
    result: SyncResult, kinds: dict[str, int], settings: Settings,
) -> None:  # fmt: skip
    """Yêu cầu trả lượt trước chưa xử lý được (R10): đọc lại chi tiết từ sàn rồi xử lý như bản ghi mới."""
    pending = await get_redis().hgetall(RETRY_KEY.format(shop=shop_id))  # type: ignore[misc]
    for raw_sn, raw in sorted(pending.items()):
        sn = raw_sn.decode() if isinstance(raw_sn, bytes) else str(raw_sn)
        ret = await adapter.get_return(creds, sn)
        if ret is None:
            await _remember_retry(shop_id, sn, json.loads(raw).get("order_sn", ""), "RETURN_NOT_FOUND")
            continue
        result.orders += 1
        await _process(session, ret, adapter, creds, shop_id, result, kinds, settings)


async def sync_shop_returns(
    session: AsyncSession, shop: Shop, adapter: PlatformAdapter, settings: Settings
) -> SyncResult:
    """J-13 cho một shop (đã giữ lock `sync_returns:{shop}`). `since` = cursor − 10 phút; lần đầu lùi
    `SHOPEE_RETURNS_INITIAL_DAYS` (G3 F-10). Commit sau mỗi yêu cầu (nhả `order:{sn}` trước lời gọi mạng kế
    — DEC-162).

    Không tự làm mới token (refresh token Shopee dùng một lần, chỉ J-04 / J-12 làm dưới lock `sync:{shop}` —
    DEC-316): token hết hạn → `EXPIRED`, bỏ lượt. Lỗi sàn cuối → `shop.last_error` `SYNC_FAILED` (`job =
    returns`), cursor không tiến. Lỗi riêng một yêu cầu (dữ liệu lạ, đơn chưa lấy được) → bỏ qua + danh sách
    thử lại Redis (tối đa 8 lượt), cursor vẫn tiến (G3 F-6, R10)."""
    result = SyncResult()
    creds = platforms.credentials(shop, Cipher(settings.fernet_key))
    if creds is not None and creds.expires_at <= clock.now():
        log.info("returns_sync_token_expired", shop_id=str(shop.id))
        return SyncResult(status="EXPIRED")
    started = clock.now()
    since = (
        shop.last_return_cursor or started - timedelta(days=settings.shopee_returns_initial_days)
    ) - CURSOR_OVERLAP
    kinds: dict[str, int] = {}
    shop_id = shop.id  # bản ghi lỗi → rollback làm `shop` hết hạn: không đọc thuộc tính giữa vòng
    try:
        await _retry_pending(session, shop_id, adapter, creds, result, kinds, settings)
        async for ret in adapter.list_returns(creds, since):
            result.orders += 1
            await _process(session, ret, adapter, creds, shop_id, result, kinds, settings)
        shop = await session.get(Shop, shop_id, populate_existing=True) or shop
        shop.last_return_cursor = started
        if shop.last_error and shop.last_error.get("job") == RETURNS_JOB:
            shop.last_error = None
        await commit(session)
    except PlatformError as exc:
        await rollback(session)
        shop = await session.get(Shop, shop_id) or shop
        shop.last_error = {**_error("SYNC_FAILED", exc), "job": RETURNS_JOB}
        result.status = "FAILED"
        result.error = str(exc)
        _report_updated(session, settings)
        await commit(session)
        # metric `aicam_returns_sync_errors_total{shop}` (02a §10) — log có cấu trúc
        log.warning("returns_sync_failed", shop_id=str(shop_id), error=str(exc), returns=result.orders)
    # metric `aicam_returns_synced_total{kind}` (02a §10)
    log.info("returns_sync", shop_id=str(shop_id), synced_by_kind=kinds, **result.as_dict())
    return result


async def sync_returns(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings, shop_id: Any = None
) -> dict[str, Any]:
    """J-13 (15 phút / mọi shop `CONNECTED`; sau khi kết nối cho một shop). Không làm gì khi chưa cấu hình."""
    if not platforms.is_configured(settings):
        return {"skipped": "not_configured"}
    if settings.platform_adapter == "shopee" and not settings.shopee_returns_enabled:
        return {"skipped": "returns_disabled"}  # G3 F-11: chờ T-3 xác nhận API returns thật
    query = select(Shop.id).where(Shop.auth_status == "CONNECTED")
    if shop_id is not None:
        query = query.where(Shop.id == shop_id)
    out: dict[str, Any] = {}
    # Chỉ giữ id (scalar): bản ghi lỗi → rollback làm hết hạn mọi `Shop` trong session; đọc thuộc tính shop đã
    # hết hạn ngoài greenlet → `MissingGreenlet`, chặn mọi shop sau (G3 V2-1).
    shop_ids = list((await session.scalars(query)).all())
    await commit(session)
    for sid in shop_ids:
        key = str(sid)
        token = await _acquire_returns_lock(sid)
        if token is None:
            out[key] = {"status": "SKIPPED", "reason": "locked"}
            continue
        try:
            shop = await session.get(Shop, sid, populate_existing=True)
            if shop is None or shop.auth_status != "CONNECTED":
                out[key] = {"status": "SKIPPED", "reason": "disconnected"}
                continue
            out[key] = (await sync_shop_returns(session, shop, adapter, settings)).as_dict()
        except Exception as exc:  # G3 F-6: một shop lỗi bất ngờ không chặn shop khác
            await rollback(session)
            log.exception("returns_sync_shop_crashed", shop_id=key)
            out[key] = SyncResult(status="FAILED", error=type(exc).__name__).as_dict()
        finally:
            await _release_returns_lock(sid, token)
    return out
