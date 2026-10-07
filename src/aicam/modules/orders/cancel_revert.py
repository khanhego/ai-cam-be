"""Kiện hủy oan (BR-21 v0.4, DEC-519 — T-285): Phase 2 coi Shopee `IN_CANCEL` (nay nhóm `CANCEL_REQUESTED`) là
hủy → kiện `NEW → CANCELLED`, `PACKED → CANCELLED_AFTER_PACK` dù người mua chỉ **xin** hủy. Trả lại:

- `revert_candidates()`: kiện `CANCELLED` / `CANCELLED_AFTER_PACK` có đơn nhóm ∉ {`CANCELLED`, `UNKNOWN`}; bỏ
  qua (kèm lý do, in "kiểm tay") khi lần vào trạng thái hủy do người (`status_history.source = MANUAL`) hoặc
  `CANCELLED_AFTER_PACK` có cảnh báo BR-11 đã được người xử lý (`RESOLVED`).
- `revert_cancel(package, trigger)`: **đường duy nhất** của 2 chuyển trạng thái ngược (`CANCELLED → NEW`,
  `CANCELLED_AFTER_PACK → PACKED` — guard ở `orders.transition`), audit `PACKAGE_CANCEL_REVERT`,
  `status_history.source = PLATFORM`.
- Lệnh `aicam fix-cancel-requests [--apply]` (ops §7.2 bước 1b) và lưới an toàn khi đồng bộ thấy đơn rời nhóm
  `CANCEL_REQUESTED` (`orders.apply_status_effects`).
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aicam.core import audit
from aicam.modules.orders.models import Order, Package, Shop, StatusHistory

log = structlog.get_logger()

REVERT_TARGET = {"CANCELLED": "NEW", "CANCELLED_AFTER_PACK": "PACKED"}
REVERT_TRIGGERS = ("COMMAND", "SYNC")
_NOT_REVERTIBLE_GROUPS = ("CANCELLED", "UNKNOWN")


@dataclass(frozen=True)
class Candidate:
    package_id: uuid.UUID
    tracking_number: str
    warehouse_status: str
    order_id: uuid.UUID
    platform_order_sn: str
    platform_status_group: str
    shop_name: str | None
    skip_reason: str | None  # None = trả lại được

    @property
    def target(self) -> str:
        return REVERT_TARGET[self.warehouse_status]


async def _entry_source(session: AsyncSession, package_id: uuid.UUID, status: str) -> str | None:
    """Nguồn của lần **vào** trạng thái hủy hiện tại (dòng lịch sử mới nhất tới trạng thái đó)."""
    source: str | None = await session.scalar(
        select(StatusHistory.source)
        .where(StatusHistory.package_id == package_id, StatusHistory.to_status == status)
        .order_by(StatusHistory.at.desc(), StatusHistory.id.desc())
        .limit(1)
    )
    return source


async def _br11_resolved_by_person(session: AsyncSession, package_id: uuid.UUID) -> bool:
    from aicam.modules.reconciliation.models import ReconAlert  # reconciliation → orders: import muộn

    found = await session.scalar(
        select(func.count())
        .select_from(ReconAlert)
        .where(
            ReconAlert.package_id == package_id,
            ReconAlert.rule == "CANCELLED_AFTER_PACK",
            ReconAlert.status == "RESOLVED",
        )
    )
    return bool(found)


async def skip_reason(session: AsyncSession, package: Package, order: Order) -> str | None:
    """Lý do **không** trả lại (None = trả lại được). Đọc trên dữ liệu người gọi đã khóa (hoặc dry-run)."""
    if package.warehouse_status not in REVERT_TARGET:
        return "Kiện không còn ở trạng thái hủy"
    if order.platform_status_group in _NOT_REVERTIBLE_GROUPS:
        return f"Đơn nhóm {order.platform_status_group} — không trả lại"
    if await _entry_source(session, package.id, package.warehouse_status) == "MANUAL":
        return "Hủy do người chỉnh tay — kiểm tay"
    if package.warehouse_status == "CANCELLED_AFTER_PACK" and await _br11_resolved_by_person(
        session, package.id
    ):
        return "Cảnh báo BR-11 đã được xử lý tay — kiểm tay"
    return None


async def revert_candidates(session: AsyncSession, order_id: uuid.UUID | None = None) -> list[Candidate]:
    """Kiện hủy của đơn nhóm ∉ {`CANCELLED`, `UNKNOWN`} (theo id kiện), kèm lý do bỏ qua nếu có."""
    query = (
        select(Package, Order, Shop.name)
        .join(Order, Order.id == Package.order_id)
        .outerjoin(Shop, Shop.id == Order.shop_id)
        .where(
            Package.warehouse_status.in_(tuple(REVERT_TARGET)),
            Order.platform_status_group.notin_(_NOT_REVERTIBLE_GROUPS),
        )
        .order_by(Package.id)
    )
    if order_id is not None:
        query = query.where(Order.id == order_id)
    out: list[Candidate] = []
    for package, order, shop_name in (await session.execute(query)).all():
        out.append(
            Candidate(
                package_id=package.id,
                tracking_number=package.tracking_number,
                warehouse_status=package.warehouse_status,
                order_id=order.id,
                platform_order_sn=order.platform_order_sn,
                platform_status_group=order.platform_status_group,
                shop_name=shop_name,
                skip_reason=await skip_reason(session, package, order),
            )
        )
    return out


async def revert_cancel(session: AsyncSession, package: Package, order: Order, *, trigger: str) -> bool:
    """Trả lại một kiện hủy oan. Người gọi giữ khóa `order:{sn}` + kiện `FOR UPDATE` (02a §6, như
    `apply_platform_cancel`); kiểm lại điều kiện dưới khóa. True = đã trả lại."""
    from aicam.modules.orders import service as orders  # service → cancel_revert: import muộn

    if trigger not in REVERT_TRIGGERS:
        raise ValueError(f"trigger không hợp lệ: {trigger}")
    if await skip_reason(session, package, order) is not None:
        return False
    before = package.warehouse_status
    target = REVERT_TARGET[before]
    await orders.transition(
        session, package, target, source="PLATFORM", actor_label="Trả lại kiện hủy oan", revert_cancel=True
    )
    audit.record(
        session, "PACKAGE_CANCEL_REVERT", user_id=None, object_type="PACKAGE", object_id=package.id,
        data={"tracking_number": package.tracking_number, "order_sn": order.platform_order_sn,
              "platform_status_group": order.platform_status_group, "from": before, "to": target,
              "trigger": trigger},
    )  # fmt: skip
    log.info("package_cancel_reverted", package_id=str(package.id), from_status=before, to_status=target,
             trigger=trigger)  # fmt: skip
    return True


async def revert_for_order(
    session: AsyncSession, order: Order, package_ids: Sequence[uuid.UUID] = ()
) -> bool:
    """Lưới an toàn (BR-21 v0.4): đồng bộ thấy đơn rời `CANCEL_REQUESTED` sang nhóm khác (`CANCELLED`,
    `UNKNOWN` loại trừ) mà ops chưa chạy lệnh → trả lại kiện hủy oan của đơn. Người gọi giữ `order:{sn}`."""
    ids = sorted(
        set(package_ids)
        | set(
            (
                await session.scalars(
                    select(Package.id).where(
                        Package.order_id == order.id, Package.warehouse_status.in_(tuple(REVERT_TARGET))
                    )
                )
            ).all()
        )
    )
    if not ids:
        return False
    locked = (
        await session.scalars(
            select(Package)
            .where(Package.id.in_(ids))
            .order_by(Package.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    changed = False
    for package in locked:
        if package.warehouse_status in REVERT_TARGET:
            changed = await revert_cancel(session, package, order, trigger="SYNC") or changed
    return changed


@dataclass
class FixReport:
    lines: list[str]
    reverted: int = 0
    skipped: int = 0
    failed: int = 0


def _line(c: Candidate, verb: str, extra: str = "") -> str:
    shop = c.shop_name or "chưa gắn shop"
    base = f"{verb} {c.tracking_number} · đơn {c.platform_order_sn} ({shop}, nhóm {c.platform_status_group})"
    return f"{base} · {c.warehouse_status} → {c.target}{extra}"


async def fix_cancel_requests(make: async_sessionmaker[AsyncSession], *, apply: bool) -> FixReport:
    """`aicam fix-cancel-requests [--apply]` (02a §7.6, ops §7.2 bước 1b). Dry-run mặc định: chỉ in danh sách.

    `--apply`: **mỗi kiện một transaction** (theo id kiện; lỗi một kiện rollback riêng, không đọc lại đối
    tượng ORM sau rollback — bài học Phase 2): khóa `order:{sn}` → kiện `FOR UPDATE` → kiểm lại →
    `revert_cancel`. Chạy lại idempotent (kiện đã trả lại không còn là ứng viên)."""
    from aicam.modules.orders import service as orders

    report = FixReport(lines=[])
    async with make() as session:
        candidates = await revert_candidates(session)
    for c in candidates:
        if c.skip_reason is not None:
            report.skipped += 1
            report.lines.append(_line(c, "BỎ QUA", f" — {c.skip_reason}"))
            continue
        if not apply:
            report.lines.append(_line(c, "SẼ TRẢ LẠI"))
            continue
        try:
            async with make() as session:
                await orders.lock_orders(session, [c.platform_order_sn])
                package = await session.scalar(
                    select(Package).where(Package.id == c.package_id).with_for_update()
                )
                order = await session.get(Order, c.order_id, populate_existing=True)
                done = (
                    package is not None
                    and order is not None
                    and await revert_cancel(session, package, order, trigger="COMMAND")
                )
                await session.commit()
        except Exception as exc:  # một kiện lỗi không chặn kiện khác
            report.failed += 1
            report.lines.append(_line(c, "LỖI", f" — {type(exc).__name__}: {exc}"))
            log.exception("fix_cancel_requests_failed", package_id=str(c.package_id))
            continue
        if done:
            report.reverted += 1
            report.lines.append(_line(c, "ĐÃ TRẢ LẠI"))
        else:
            report.skipped += 1
            report.lines.append(_line(c, "BỎ QUA", " — đã đổi trong lúc chạy, kiểm tay"))
    mode = "áp dụng" if apply else "chạy thử (thêm --apply để ghi)"
    would = sum(1 for c in candidates if c.skip_reason is None)
    report.lines.append(
        f"Tổng: {len(candidates)} kiện hủy có đơn chưa hủy trên sàn — {mode}: "
        + (f"trả lại {report.reverted}, bỏ qua {report.skipped}, lỗi {report.failed}." if apply
           else f"sẽ trả lại {would}, bỏ qua {report.skipped}.")
    )  # fmt: skip
    return report
