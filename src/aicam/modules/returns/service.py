"""Hồ sơ hàng hoàn — phần dùng bởi API-122 (T-102). Lõi `attach_or_create`, `recompute`… thêm ở T-104."""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.orders.models import Package
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase, ReturnCasePackage

# Kiện còn "thuộc" hồ sơ đang chờ / đang kiểm / đã nhận — hồ sơ không được hủy khi còn kiện như vậy.
_ACTIVE_PACKAGE_STATUSES = (
    "RETURN_EXPECTED",
    "RETURN_MISSING",
    "RETURN_INSPECTING",
    "RETURN_RECEIVED_OK",
    "RETURN_RECEIVED_ISSUE",
)


async def open_case_ids_of_package(session: AsyncSession, package_id: uuid.UUID) -> list[uuid.UUID]:
    """Đọc không khóa (bước trước khi khóa theo thứ tự DEC-266)."""
    rows = await session.scalars(
        select(ReturnCase.id)
        .join(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
        .where(ReturnCasePackage.package_id == package_id, ReturnCase.status.in_(OPEN_CASE_STATUSES))
        .order_by(ReturnCase.id)
    )
    return list(rows.all())


async def lock_cases(session: AsyncSession, case_ids: Sequence[uuid.UUID]) -> list[ReturnCase]:
    if not case_ids:
        return []
    rows = await session.scalars(
        select(ReturnCase)
        .where(ReturnCase.id.in_(list(case_ids)))
        .order_by(ReturnCase.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(rows.all())


async def cancel_if_no_active_package(session: AsyncSession, case: ReturnCase) -> bool:
    """API-122 `RETURN_* → DELIVERED` (02a §4): hồ sơ mở → `CANCELLED` khi không còn kiện nào của hồ sơ
    đang chờ / đang kiểm / đã nhận (DEC-303). Còn kiện như vậy → giữ, T-104 `recompute` xử lý."""
    if case.status not in OPEN_CASE_STATUSES:
        return False
    remaining = await session.scalar(
        select(Package.id)
        .join(ReturnCasePackage, ReturnCasePackage.package_id == Package.id)
        .where(
            ReturnCasePackage.return_case_id == case.id,
            Package.warehouse_status.in_(_ACTIVE_PACKAGE_STATUSES),
        )
        .limit(1)
    )
    if remaining is not None:
        return False
    case.status = "CANCELLED"
    return True
