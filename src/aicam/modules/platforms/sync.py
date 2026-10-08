"""Job đồng bộ sàn (02a §7, FR-05.02..04, ADR-007 polling): J-04 đơn mới, J-05 xác minh lại kiện,
J-06 trạng thái vận chuyển, J-12 làm mới token, J-13 yêu cầu trả (Phase 2 — FR-05.05, 05.11, 05.12).

Mọi job không làm gì khi chưa cấu hình Shopee (`SHOPEE_ENABLED=false`). Thử lại từng lời gọi HTTP nằm trong
adapter (5 lần, giãn cách mũ — FR-05.08); lỗi cuối ghi `shop.last_error` → dashboard `SYNC_ERROR` (API-32).
"""

import json
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import after_commit, commit, rollback
from aicam.core.redis import get_redis
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.platforms import budget, grants, lookup, registry
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import (
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


# ---------------------------------------------------------------- J-12 (theo grant — DEC-433, 507)

REFRESH_MARGIN = grants.REFRESH_MARGIN


async def refresh_tokens(
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings
) -> dict[str, int]:
    """J-12 (30 phút): làm mới token theo **grant** của shop `CONNECTED` sắp hết hạn (sàn của `adapter`).

    Chỉ lấy khóa `grant:{platform}:{ref}` (không chờ — bận = người khác đang làm mới → `skipped`), không bao
    giờ lấy `sync:{shop}` (DEC-507). Shop `DISCONNECTED` không được làm mới / ghi token. Đếm theo shop. Lỗi
    một grant không chặn grant khác."""
    out = {"refreshed": 0, "expired": 0, "failed": 0, "skipped": 0}
    if not registry.is_configured(adapter.code, settings):
        return out
    cipher = Cipher(settings.fernet_key)
    rows = (
        await session.execute(
            select(Shop.id, func.coalesce(Shop.grant_ref, Shop.platform_shop_id))
            .where(
                Shop.platform == adapter.code,
                Shop.auth_status == "CONNECTED",
                Shop.auth_expires_at < clock.now() + REFRESH_MARGIN,
            )
            .order_by(Shop.id)
        )
    ).all()
    await commit(session)
    by_grant: dict[str, Any] = {}
    for shop_id, ref in rows:
        by_grant.setdefault(ref, shop_id)
    for ref, shop_id in by_grant.items():
        try:
            shop = await session.get(Shop, shop_id, populate_existing=True)
            if shop is None or shop.auth_status != "CONNECTED":
                out["skipped"] += 1
                await commit(session)
                continue
            outcome = await grants.refresh_grant(session, shop, adapter, cipher, wait_s=0)
            if outcome.refreshed:
                out["refreshed"] += outcome.shops
            elif outcome.creds is None:
                out["expired"] += max(outcome.shops, 1)
            else:
                out["skipped"] += 1  # người khác vừa làm mới (đọc lại sau khóa)
        except grants.GrantBusy:
            out["skipped"] += 1
        except PlatformError as exc:
            await rollback(session)
            shop = await session.get(Shop, shop_id, populate_existing=True)
            if shop is not None:
                set_error(shop, _error("REFRESH_FAILED", exc))
            await commit(session)
            out["failed"] += 1
            log.warning("platform_token_refresh_failed", grant=ref, error=str(exc))
        except Exception:  # một grant lỗi bất ngờ không chặn grant khác (NFR-39)
            await rollback(session)
            out["failed"] += 1
            log.exception("platform_token_refresh_crashed", grant=ref)
    return out


# ---------------------------------------------------------------- J-04


async def _order_packages(session: AsyncSession, order_id: Any) -> dict[Any, str]:
    """Kiện của **một** đơn (theo id — §5.1 #6: shop B không thấy kiện của đơn trùng mã ở shop A)."""
    if order_id is None:
        return {}
    rows = (await session.scalars(select(Package).where(Package.order_id == order_id))).all()
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
                existing = await orders.order_for_upsert(session, order.platform_order_sn, shop_id)
                before = await _order_packages(session, existing.id if existing else None)
                result = await orders.upsert_platform_order(session, order, shop_id=shop_id)
                merged = await returns.merge_unidentified_by_code(session, result.order)
                signalled = await _order_return_signal(session, result.order, order)
                after = await _order_packages(session, result.order.id)
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
    elif data.is_cancelled:  # chỉ nhóm CANCELLED (đang yêu cầu hủy không phải giao thất bại — DEC-494)
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


def initial_days(platform: str, settings: Settings, *, returns_job: bool = False) -> int:
    """Lần đồng bộ đầu lùi bao nhiêu ngày (02a §9 `*_INITIAL_SYNC_DAYS`, `*_RETURNS_INITIAL_DAYS`)."""
    if platform == registry.TIKTOK:
        return settings.tiktok_returns_initial_days if returns_job else settings.tiktok_initial_sync_days
    return settings.shopee_returns_initial_days if returns_job else settings.shopee_initial_sync_days


class ShopStopped(Exception):
    """Shop bị ngắt / hết hạn giữa lượt (Admin bấm Ngắt — 02a §6): dừng, đơn đã ghi giữ nguyên (EX-T7)."""


DISCONNECT_CHECK_EVERY = 20  # đơn — đọc một cột `auth_status` (không khóa) giữa lượt


async def _auth_status(session: AsyncSession, shop_id: Any) -> str | None:
    status: str | None = await session.scalar(select(Shop.auth_status).where(Shop.id == shop_id))
    return status


class BudgetExceeded(PlatformError):
    """Hết ngân sách thời gian của lượt (02a §7: dừng, cursor không tiến — lượt sau làm lại)."""


async def sync_shop_orders(
    session: AsyncSession, shop: Shop, adapter: PlatformAdapter, settings: Settings
) -> SyncResult:
    """J-04 cho một shop (đã giữ lock `sync:{shop}`). `since` = cursor − 10 phút; lần đầu lùi
    `*_INITIAL_SYNC_DAYS` ngày theo sàn. Đơn API ghi đè đơn CSV (BR-17); đơn hủy khi kho PACKED →
    CANCELLED_AFTER_PACK (EX-P10). Hết ngân sách (`budget.time_budget`) → `FAILED`, cursor không tiến; shop bị
    ngắt giữa lượt → `SKIPPED disconnected` (đơn đã ghi giữ nguyên)."""
    cipher = Cipher(settings.fernet_key)
    result = SyncResult()
    shop_id = shop.id
    try:
        creds = await grants.ensure_fresh(session, shop, adapter, cipher)
        await commit(session)
        if creds is None:
            return SyncResult(status="EXPIRED")
        shop = await session.get(Shop, shop_id) or shop
        started = clock.now()
        since = (
            shop.last_sync_cursor or started - timedelta(days=initial_days(shop.platform, settings))
        ) - CURSOR_OVERLAP
        for attempt in (1, 2):
            try:
                async for order in adapter.list_updated_orders(creds, since):
                    if budget.expired():
                        raise BudgetExceeded("Hết thời gian của lượt đồng bộ — lượt sau làm tiếp")
                    if (
                        result.orders
                        and result.orders % DISCONNECT_CHECK_EVERY == 0
                        and await _auth_status(session, shop_id) != "CONNECTED"
                    ):
                        raise ShopStopped()
                    result.orders += 1
                    if order.fulfilled_by_platform:
                        result.skipped += 1  # EX-T5 (AS-13): đơn kho của sàn — kho không đóng gói, chỉ đếm
                        continue
                    if await _upsert(session, order, shop_id):
                        result.changed += 1
                    # DEC-162 (G3-V1): commit sau MỖI đơn — nhả khóa `order:{sn}` + khóa kiện trước khi
                    # lấy đơn / trang kế (lời gọi mạng), không giữ nhiều khóa theo thứ tự sàn trả → không
                    # khóa chéo với API-51. Cursor vẫn chỉ tiến khi đi hết danh sách (đơn đã ghi thì lần
                    # sau ghi lại idempotent).
                    await commit(session)
                break
            except PlatformAuthError:
                # 02 §10.2 architecture: token hết hạn giữa chừng → refresh một lần rồi thử lại.
                if attempt == 2:
                    raise
                shop = await session.get(Shop, shop_id) or shop
                creds = await grants.ensure_fresh(session, shop, adapter, cipher, force=True)
                await commit(session)
                if creds is None:
                    return SyncResult(status="EXPIRED", orders=result.orders)
        shop = await session.get(Shop, shop_id, populate_existing=True) or shop
        if shop.auth_status != "CONNECTED":
            raise ShopStopped()
        shop.last_sync_cursor = started
        shop.last_synced_at = clock.now()
        if not (shop.last_error and shop.last_error.get("job") == RETURNS_JOB):  # G3 F-7: không xóa lỗi J-13
            shop.last_error = None
            shop.error_since = None
        if result.changed:
            _report_updated(session, settings)
            reconciliation.request_run_soon(session)  # J-14 sau 30 giây (02a §7)
        await commit(session)
    except ShopStopped:
        await rollback(session)
        result.status = "SKIPPED"
        result.error = "disconnected"
        log.info("platform_sync_stopped", shop_id=str(shop_id), orders=result.orders)
    except PlatformError as exc:
        await rollback(session)
        shop = await session.get(Shop, shop_id, populate_existing=True) or shop
        if isinstance(exc, PlatformAuthError):
            shop.auth_status = "EXPIRED"
            set_error(shop, _error("AUTH_EXPIRED", exc))
            result.status = "EXPIRED"
        else:
            set_error(shop, _error("SYNC_FAILED", exc))
            result.status = "FAILED"
        result.error = str(exc)
        _report_updated(session, settings)
        await commit(session)
        log.warning("platform_sync_failed", shop_id=str(shop_id), error=str(exc), orders=result.orders)
    log.info("platform_sync", platform=adapter.code, shop_id=str(shop_id), **result.as_dict())
    return result


def set_error(shop: Shop, error: dict[str, Any]) -> None:
    """Ghi `last_error`; `error_since` đặt lần đầu chuyển từ không lỗi sang lỗi (N06 — DEC-467)."""
    if shop.last_error is None or shop.error_since is None:
        shop.error_since = clock.now()
    shop.last_error = error


async def sync_orders(
    session: AsyncSession,
    adapter: PlatformAdapter,
    settings: Settings,
    shop_id: Any = None,
    *,
    lock_held: bool | str = False,
) -> dict[str, Any]:
    """J-04 cho các shop `CONNECTED` của sàn `adapter` (hoặc một shop — task fan-out `sync_shop_orders`,
    API-73, sau callback). Lock Redis `sync:{shop}` chống chạy chồng (02a §6), **không chờ** — bận →
    `SKIPPED locked`; API-73 đã giữ lock thì `lock_held` = token lock (nhả compare-and-delete — G3-N4);
    `True` = message cũ không có token. Một shop lỗi bất ngờ không chặn shop khác (NFR-39)."""
    out: dict[str, Any] = {}
    held_token = lock_held if isinstance(lock_held, str) else None
    if not registry.is_configured(adapter.code, settings):
        if shop_id is not None and lock_held:
            await platforms.release_sync_lock(shop_id, held_token)
        return {"skipped": "not_configured"}
    query = select(Shop.id).where(Shop.platform == adapter.code, Shop.auth_status == "CONNECTED")
    if shop_id is not None:
        query = select(Shop.id).where(Shop.id == shop_id, Shop.platform == adapter.code)
    shop_ids = list((await session.scalars(query.order_by(Shop.id))).all())
    await commit(session)
    for sid in shop_ids:
        key = str(sid)
        if lock_held and shop_id is not None:
            token = held_token
        else:
            token = await platforms.acquire_sync_lock(sid, "job")
            if token is None:
                out[key] = {"status": "SKIPPED", "reason": "locked"}
                continue
        try:
            shop = await session.get(Shop, sid, populate_existing=True)
            if shop is None or shop.auth_status != "CONNECTED":
                out[key] = {"status": "SKIPPED", "reason": shop.auth_status if shop else "not_found"}
                continue
            out[key] = (await sync_shop_orders(session, shop, adapter, settings)).as_dict()
        except Exception as exc:  # một shop lỗi bất ngờ không chặn shop khác (NFR-39)
            await rollback(session)
            log.exception("platform_sync_shop_crashed", shop_id=key)
            out[key] = SyncResult(status="FAILED", error=type(exc).__name__).as_dict()
        finally:
            await platforms.release_sync_lock(sid, token)
    if shop_id is not None and lock_held and not out:
        await platforms.release_sync_lock(shop_id, held_token)
    return out


# ---------------------------------------------------------------- J-05


async def verify_unverified(
    session: AsyncSession,
    adapter: PlatformAdapter | Mapping[str, PlatformAdapter] | None,
    settings: Settings,
) -> dict[str, int]:
    """J-05 (10 phút): kiện `verified=false` (BR-04) (7 ngày) → `lookup.find_everywhere` (mọi shop của mọi sàn
    bật, song song, mỗi kiện ≤ 2 giây — BR-32); đúng 1 shop có → gắn đơn của shop đó; ≥ 2 → giữ chưa xác minh
    (log `platform_verify_ambiguous`). Hết ngân sách lượt (`budget`) / mọi shop lỗi → dừng, lượt sau."""
    out = {"checked": 0, "verified": 0}
    adapters = lookup.adapters_for(settings, adapter)
    if not adapters:
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
    await commit(session)
    for code in codes:
        if budget.expired():
            log.warning("platform_verify_budget_exceeded", checked=out["checked"])
            break
        out["checked"] += 1
        found = await lookup.find_everywhere(session, code, settings, adapters)
        await commit(session)  # `credentials()` có thể vừa đánh EXPIRED (token không giải mã được)
        if found.shops == 0:
            break
        if found.ambiguous:
            log.warning("platform_verify_ambiguous", code=code, shops=found.shops_brief())
            continue
        hit = found.single
        if hit is None:
            if found.failed == found.shops:
                break  # sàn lỗi hết: để lượt sau, giữ unverified
            continue
        await _upsert(session, hit.order, hit.shop_id)
        out["verified"] += 1
        await commit(session)
    await commit(session)
    return out


# ---------------------------------------------------------------- J-06


def _keeps_cancel_requested(order: Order, st: ShippingStatus) -> bool:
    """G3-MS-2: J-06 không hạ đơn khỏi `CANCEL_REQUESTED` khi chữ trạng thái đơn trên sàn **không đổi** — yêu
    cầu hủy bị từ chối / người mua rút do J-04 áp (đọc yêu cầu hủy); J-06 chỉ áp khi chữ trạng thái đổi
    (vd sang `CANCELLED`, đang giao)."""
    return (
        order.platform_status_group == "CANCEL_REQUESTED"
        and st.order_status_group != "CANCEL_REQUESTED"
        and (st.order_status or "") == (order.platform_status or "")
    )


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
    if st.order_status and not _keeps_cancel_requested(order, st):
        old_group, group = orders.set_platform_status(order, st.order_status, st.order_status_group)
        # BR-21 làm rõ (DEC-494): chỉ nhóm CANCELLED là hủy; CANCEL_REQUESTED chỉ gắn cờ phiên đang đóng, kiện
        # vẫn theo vận chuyển như thường (sàn có thể từ chối yêu cầu hủy).
        changed = await orders.apply_status_effects(session, order, old_group, only_package_ids=[package.id])
        if group == "CANCELLED":
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
    session: AsyncSession, adapter: PlatformAdapter, settings: Settings, shop_id: Any = None
) -> dict[str, int]:
    """J-06 (15 phút): kiện `PACKED` / `HANDED_OVER` → vận chuyển sàn → `HANDED_OVER` / `DELIVERED`;
    đơn bị hủy sau khi đóng → `CANCELLED_AFTER_PACK` (FR-05.04, EX-P10). Phase 2 (T-105): giao thất bại / boom
    COD → hồ sơ `FAILED_DELIVERY`, kiện `RETURN_EXPECTED`; kiện của hồ sơ giao thất bại được giao lại.

    Phase 3 (T-205): `shop_id` = task fan-out của một shop (kiện của **đơn thuộc shop**, token shop đó —
    DEC-509); shop mặc định Shopee (mới nhất `CONNECTED`) nhận thêm kiện của đơn file như Phase 2. Không
    `shop_id` → mọi shop của sàn `adapter` tuần tự (test / gọi tay). Hết ngân sách → dừng, lượt sau."""
    out = {"checked": 0, "changed": 0}
    if not registry.is_configured(adapter.code, settings):
        return out
    by_shop = {t.shop_id: t for t in await platforms.lookup_targets(session, adapter, settings)}
    default = (
        await platforms.lookup_target(session, adapter, settings) if adapter.code == registry.SHOPEE else None
    )
    if shop_id is not None:
        by_shop = {k: v for k, v in by_shop.items() if k == shop_id}
        if default is not None and default.shop_id != shop_id:
            default = None
    if not by_shop and default is None:
        await commit(session)
        return out
    failed_packages = (
        select(ReturnCasePackage.package_id)
        .join(ReturnCase, ReturnCase.id == ReturnCasePackage.return_case_id)
        .where(ReturnCase.kind == "FAILED_DELIVERY", ReturnCase.status.in_(OPEN_CASE_STATUSES))
    )
    owner = [Order.shop_id.in_([k for k in by_shop if k is not None])]
    if default is not None:
        owner.append(Order.shop_id.is_(None))
    # G3-MS-3: rút cột thành tuple trước vòng — nhóm shop lỗi → rollback hết hạn mọi ORM của session, nhóm
    # sau không được đọc thuộc tính ORM (MissingGreenlet).
    rows = (
        await session.execute(
            select(Package.id, Order.id, Package.tracking_number, Order.platform_order_sn, Order.shop_id)
            .join(Order, Order.id == Package.order_id)
            .where(
                or_(*owner),
                or_(
                    Package.warehouse_status.in_(("PACKED", "HANDED_OVER")),
                    and_(
                        Package.warehouse_status.in_(("RETURN_EXPECTED", "RETURN_MISSING")),
                        Package.id.in_(failed_packages),
                    ),
                ),
            )
            .order_by(Package.updated_at)
            .limit(SHIPPING_MAX)
        )
    ).all()
    await commit(session)
    groups: dict[Any, list[_ShipRow]] = {}
    for package_id, order_id, tracking, order_sn, owner_id in rows:
        groups.setdefault(owner_id, []).append(_ShipRow(package_id, order_id, tracking, order_sn))
    for shop_key, shop_rows in groups.items():
        target = by_shop.get(shop_key) if shop_key is not None else default
        if target is None:
            continue  # shop không còn kết nối: không tra (token shop khác không dùng được)
        try:
            await _shipping_for_target(session, adapter, settings, target, shop_rows, out)
        except Exception:  # một shop lỗi bất ngờ không chặn shop khác (NFR-39)
            await rollback(session)
            log.exception("platform_shipping_shop_crashed", shop_id=str(shop_key))
    return out


@dataclass(frozen=True)
class _ShipRow:
    package_id: Any
    order_id: Any
    tracking_number: str
    order_sn: str


async def _shipping_for_target(
    session: AsyncSession,
    adapter: PlatformAdapter,
    settings: Settings,
    target: Any,
    rows: list[_ShipRow],
    out: dict[str, int],
) -> None:
    for i in range(0, len(rows), SHIPPING_BATCH):
        if budget.expired():
            log.warning("platform_shipping_budget_exceeded", shop_id=str(target.shop_id), done=i)
            break
        if target.shop_id is not None and await _auth_status(session, target.shop_id) != "CONNECTED":
            break  # shop vừa bị ngắt giữa lượt (02a §6)
        chunk = rows[i : i + SHIPPING_BATCH]
        by_code = {r.tracking_number.upper(): r for r in chunk}
        refs = [ShipmentRef(r.order_sn, r.tracking_number) for r in chunk]
        try:
            statuses = await adapter.get_shipping_statuses(target.creds, refs)
        except PlatformError as exc:
            log.warning("platform_shipping_failed", shop_id=str(target.shop_id), error=str(exc))
            break
        changed = 0
        for st in statuses:
            row = by_code.get(st.tracking_number.upper())
            if row is None:
                continue
            out["checked"] += 1
            if await _apply_shipping(session, row.package_id, row.order_id, st):
                changed += 1
                _report_updated(session, settings)
                reconciliation.request_run_soon(session)
            await commit(session)  # nhả `order:{sn}` + khóa kiện sau mỗi kiện (như J-04 — DEC-162)
        out["changed"] += changed


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
    # §5.1 #7 (BR-29): đơn của **shop của task** → không có: đơn **chưa gắn shop** cùng mã (đơn file / đơn cũ
    # — J-04 sẽ "nhận" khi đồng bộ; không gọi sàn thêm — DEC-568) → không có: đọc sàn + ghi vào shop này.
    # Không bao giờ lấy đơn của shop khác cùng mã.
    order = await orders.order_for_upsert(session, ret.order_sn, shop_id)
    if order is None:
        order = await _order_for_return(session, ret, adapter, creds, shop_id)
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


async def _order_for_return(
    session: AsyncSession, ret: PlatformReturn, adapter: PlatformAdapter, creds: ShopCredentials | None,
    shop_id: Any,
) -> Order:  # fmt: skip
    """Đơn chưa có (ở shop lẫn chưa gắn shop) → đọc sàn + upsert vào shop (savepoint); sàn không trả →
    `ReturnSkipped` (thử lại lượt sau)."""
    data = await adapter.get_order(creds, ret.order_sn)
    if data is None:
        raise ReturnSkipped("ORDER_NOT_FOUND")
    for attempt in (1, 2):
        try:
            async with session.begin_nested():
                order = (await orders.upsert_platform_order(session, data, shop_id=shop_id)).order
                await returns.merge_unidentified_by_code(session, order)
            return order
        except IntegrityError:
            if attempt == 2:
                raise
    raise ReturnSkipped("ORDER_NOT_FOUND")  # pragma: no cover — vòng trên luôn trả / ném


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
        shop.last_return_cursor
        or started - timedelta(days=initial_days(shop.platform, settings, returns_job=True))
    ) - CURSOR_OVERLAP
    kinds: dict[str, int] = {}
    shop_id = shop.id  # bản ghi lỗi → rollback làm `shop` hết hạn: không đọc thuộc tính giữa vòng
    try:
        await _retry_pending(session, shop_id, adapter, creds, result, kinds, settings)
        async for ret in adapter.list_returns(creds, since):
            if budget.expired():
                raise BudgetExceeded("Hết thời gian của lượt đồng bộ yêu cầu trả — lượt sau làm tiếp")
            result.orders += 1
            await _process(session, ret, adapter, creds, shop_id, result, kinds, settings)
        shop = await session.get(Shop, shop_id, populate_existing=True) or shop
        shop.last_return_cursor = started
        if shop.last_error and shop.last_error.get("job") == RETURNS_JOB:
            shop.last_error = None
            shop.error_since = None
        await commit(session)
    except PlatformError as exc:
        await rollback(session)
        shop = await session.get(Shop, shop_id) or shop
        set_error(shop, {**_error("SYNC_FAILED", exc), "job": RETURNS_JOB})
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
    if not registry.is_configured(adapter.code, settings):
        return {"skipped": "not_configured"}
    if not registry.returns_enabled(adapter.code, settings):
        return {"skipped": "returns_disabled"}  # G3 F-11 / EX-T1: chờ T-3 xác nhận API returns thật
    query = select(Shop.id).where(Shop.platform == adapter.code, Shop.auth_status == "CONNECTED")
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
