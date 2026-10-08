"""API-104 tìm kiện hoàn thủ công (02 §6.2, 02a §4 API-104; FR-04.07, EX-R2).

Khớp chính xác mã vận đơn gốc / chiều về / mã đơn / mã yêu cầu sàn; tiền tố khi ≥ `RETURN_LOOKUP_PREFIX_MIN`
ký tự; tối đa 10 kiện mới nhất. `can_open` / `blocked_reason` dùng cùng `check_openable` với API-11.
Không có kết quả + mã ≥ 8 ký tự → tra sàn ≤ 2 giây (`platform_checked = true`).
"""

import uuid

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import commit
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms.base import PlatformAdapter
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase, ReturnCasePackage
from aicam.modules.sessions import return_scan
from aicam.modules.sessions.schemas import ReturnLookupCase, ReturnLookupItem, ReturnLookupOut
from aicam.modules.stations.models import Station

LIMIT = 10
Q_MIN, Q_MAX = 4, 40


async def _package_ids(session: AsyncSession, code: str, settings: Settings) -> list[uuid.UUID]:
    prefix = len(code) >= settings.return_lookup_prefix_min

    def _match(column: ColumnElement[str]) -> ColumnElement[bool]:
        exact = column == code
        return or_(exact, column.startswith(code, autoescape=True)) if prefix else exact

    by_package = select(Package.id).where(_match(func.upper(Package.tracking_number)))
    by_order = (
        select(Package.id)
        .join(Order, Order.id == Package.order_id)
        .where(_match(func.upper(Order.platform_order_sn)))
    )
    by_case = (
        select(Package.id)
        .join(ReturnCasePackage, ReturnCasePackage.package_id == Package.id)
        .join(ReturnCase, ReturnCase.id == ReturnCasePackage.return_case_id)
        .where(
            or_(
                _match(func.upper(ReturnCase.return_tracking_number)),
                func.upper(ReturnCase.platform_return_sn) == code,
            )
        )
    )
    ids = by_package.union(by_order, by_case).subquery()
    rows = await session.scalars(
        select(Package.id)
        .where(Package.id.in_(select(ids.c.id)), Package.is_placeholder.is_(False))
        .order_by(Package.created_at.desc(), Package.id.desc())
        .limit(LIMIT)
    )
    return list(rows.all())


async def _case_of(session: AsyncSession, package_id: uuid.UUID) -> ReturnCase | None:
    """Hồ sơ mở của kiện; không có → hồ sơ gần nhất (đã nhận / hủy) để hiện thông tin."""
    open_ids = await returns.open_case_ids_of_package(session, package_id)
    if open_ids:
        return await session.get(ReturnCase, open_ids[0])
    found: ReturnCase | None = await session.scalar(
        select(ReturnCase)
        .join(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
        .where(ReturnCasePackage.package_id == package_id)
        .order_by(ReturnCase.created_at.desc())
        .limit(1)
    )
    return found


async def lookup(
    session: AsyncSession, station: Station, q: str, adapter: PlatformAdapter, settings: Settings
) -> ReturnLookupOut:
    code = q.strip().upper()
    if not Q_MIN <= len(code) <= Q_MAX:
        raise AppError(
            "VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"q": "Nhập ít nhất 4 ký tự."}}
        )
    if station.work_mode != "RETURN":
        raise AppError("WRONG_WORK_MODE", "Station không ở chế độ nhận hàng hoàn.", 409)
    ids = await _package_ids(session, code, settings)
    checked = False
    if not ids and len(code) >= 8:
        checked = True
        if await return_scan.platform_find(session, code, adapter, settings) is not None:
            await session.flush()
            ids = await _package_ids(session, code, settings)
    items: list[ReturnLookupItem] = []
    for package_id in ids:
        package = await session.get(Package, package_id)
        if package is None:
            continue
        order = await session.get(Order, package.order_id) if package.order_id else None
        case = await _case_of(session, package.id)
        shop = await orders.shop_of(session, order) if order else None
        alert = await return_scan.check_openable(
            session, package, case if case and case.status in OPEN_CASE_STATUSES else None, order,
            package.tracking_number, settings.tz_display,
        )  # fmt: skip
        items.append(
            ReturnLookupItem(
                package_id=package.id,
                tracking_number=package.tracking_number,
                platform_order_sn=order.platform_order_sn if order else None,
                warehouse_status=package.warehouse_status,
                return_case=ReturnLookupCase(
                    id=case.id,
                    code=case.code,
                    kind=case.kind,
                    status=case.status,
                    return_tracking_number=case.return_tracking_number,
                )
                if case
                else None,
                can_open=alert is None,
                blocked_reason=alert.code if alert else None,
                platform=shop.platform if shop else None,
                shop_name=shop.name if shop else None,
            )
        )
    await commit(session)  # tra sàn có thể đã ghi đơn mới
    return ReturnLookupOut(items=items, platform_checked=checked)
