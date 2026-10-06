"""Dữ liệu hàng hoàn cho test (04-test-cases item 02 §1: đơn `2410TST000xx`, mã chiều về `SPXRTTST…`)."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, PlatformReturn, ReturnItem
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

ITEM = PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L")
SOCK = PlatformItem("Tất cổ ngắn", 1, "TAT-TRANG", "Trắng")


async def make_order(
    db: AsyncSession,
    n: int,
    *,
    packages: int = 1,
    status: str = "COMPLETED",
    warehouse_status: str = "DELIVERED",
    items: tuple[PlatformItem, ...] = (ITEM,),
) -> tuple[Order, list[Package]]:
    """Đơn `2410TST000nn`, kiện `SPXTST00000nn` (nhiều kiện: hậu tố `-1`, `-2`) ở `warehouse_status`."""
    codes = (
        (f"SPXTST{n:07d}",) if packages == 1 else tuple(f"SPXTST{n:07d}-{i}" for i in range(1, packages + 1))
    )
    data = PlatformOrder(
        platform_order_sn=f"2410TST{n:05d}",
        status=status,
        tracking_numbers=codes,
        items=items,
        created_at=datetime(2026, 10, 1, tzinfo=UTC),
    )
    result = await orders.upsert_platform_order(db, data)
    for package in result.packages:
        package.warehouse_status = warehouse_status
    await db.flush()
    return result.order, result.packages


def platform_return(
    n: int,
    *,
    needs_parcel: bool = True,
    status: str = "ACCEPTED",
    items: tuple[ReturnItem, ...] | None = None,
    tracking: str | None = None,
) -> PlatformReturn:
    return PlatformReturn(
        return_sn=f"2410RTTST{n:03d}",
        order_sn=f"2410TST{n:05d}",
        status=status,
        status_group="OPEN",
        needs_parcel=needs_parcel,
        return_tracking_number=tracking if tracking is not None else f"SPXRTTST{n:06d}",
        reason="ITEM_DAMAGED",
        reason_text="Áo bị rách ở tay",
        items=items
        if items is not None
        else (ReturnItem(quantity=2, sku="AT-DEN-L", product_name="Áo thun basic"),),
        seller_due_at=clock.now(),
        created_at=clock.now(),
        raw={"mock": True},
    )


async def buyer_return_case(db: AsyncSession, order: Order, n: int, **kw: object) -> ReturnCase:
    ret = platform_return(n, **kw)  # type: ignore[arg-type]
    result = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_PLATFORM_RETURN, key=f"RETURN:{ret.return_sn}", ret=ret)
    )
    assert result.case is not None
    return result.case


def with_status(ret: PlatformReturn, status: str) -> PlatformReturn:
    return replace(ret, status=status)


def return_session(
    station: Station,
    package: Package,
    case: ReturnCase,
    *,
    status: str = "COMPLETED",
    conclusion: str | None = "OK",
    open_code: str | None = None,
) -> PackSession:
    now = clock.now()
    return PackSession(
        id=uuid.uuid4(),
        type="RETURN",
        package_id=package.id,
        station_id=station.id,
        return_case_id=case.id,
        status=status,
        started_at=now,
        ended_at=now if status not in ("OPEN", "WAITING_APPROVAL") else None,
        open_code=open_code or package.tracking_number,
        inspection_conclusion=conclusion,
        inspection_lines_mode="FULL",
        operator_name="Lan QA",
        flags=[],
    )
