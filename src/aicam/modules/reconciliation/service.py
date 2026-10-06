"""Cảnh báo lệch: dựng item, tổng mở theo mức, đóng cảnh báo khi xử lý (02a §4 API-120..122, BR-26).

J-14 `run_rules` và API-120 / 121 / 123 thêm ở T-113.
"""

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.orders.models import Order, Package
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.reconciliation.schemas import (
    AlertPackage,
    OpenSummary,
    ReconAlertOut,
    Resolution,
    ResolvedBy,
)
from aicam.modules.users.queries import get_user_ref

RULE_BR = {
    "SHIPPED_NOT_PACKED": "BR-10",
    "CANCELLED_AFTER_PACK": "BR-11",
    "RETURN_OVERDUE": "BR-12",
    "RETURN_UNANNOUNCED": "BR-13",
    "PACKED_NOT_HANDED_OVER": "BR-14",
    "RETURN_DONE_NOT_RECEIVED": "BR-19",
    "UNVERIFIED_STALE": "BR-20",
}


async def lock_alert(session: AsyncSession, alert_id: uuid.UUID) -> ReconAlert | None:
    result: ReconAlert | None = await session.scalar(
        select(ReconAlert)
        .where(ReconAlert.id == alert_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result


def close_alert(
    alert: ReconAlert,
    *,
    action: str,
    note: str | None,
    by: uuid.UUID | None,
    to_status: str | None = None,
    claim_id: uuid.UUID | None = None,
) -> None:
    """`OPEN` → `RESOLVED` (người xử lý). Người gọi đã khóa dòng và kiểm `status == OPEN`."""
    alert.status = "RESOLVED"
    alert.closed_at = clock.now()
    alert.resolution_action = action
    alert.resolution_note = note
    alert.resolved_by = by
    alert.to_status = to_status
    alert.claim_id = claim_id


async def summary(session: AsyncSession) -> OpenSummary:
    rows = (
        await session.execute(
            select(ReconAlert.severity, func.count())
            .where(ReconAlert.status == "OPEN")
            .group_by(ReconAlert.severity)
        )
    ).all()
    return OpenSummary(**{severity: count for severity, count in rows})


async def alert_out(
    session: AsyncSession, alert: ReconAlert, manual_targets: dict[str, tuple[str, ...]]
) -> ReconAlertOut:
    package = await session.get(Package, alert.package_id)
    if package is None:  # FK CASCADE: không xảy ra
        raise RuntimeError(f"Cảnh báo trỏ tới kiện không tồn tại: {alert.package_id}")
    order = await session.get(Order, package.order_id) if package.order_id else None
    resolution = None
    if alert.resolution_action is not None:
        user = await get_user_ref(session, alert.resolved_by) if alert.resolved_by else None
        resolution = Resolution(
            action=alert.resolution_action,
            note=alert.resolution_note,
            by=ResolvedBy(id=user.id, display_name=user.display_name) if user else None,
            at=alert.closed_at,
            to_status=alert.to_status,
            claim_id=alert.claim_id,
        )
    return ReconAlertOut(
        id=alert.id,
        rule=alert.rule,
        br=RULE_BR[alert.rule],
        severity=alert.severity,
        status=alert.status,
        package=AlertPackage(
            id=package.id,
            tracking_number=package.tracking_number,
            warehouse_status=package.warehouse_status,
            platform_status=order.platform_status if order else None,
        ),
        context=alert.context,
        detected_at=alert.detected_at,
        closed_at=alert.closed_at,
        resolution=resolution,
        allowed_status_targets=list(manual_targets.get(package.warehouse_status, ())),
    )
