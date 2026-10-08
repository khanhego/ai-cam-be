"""Dữ liệu hàng hoàn cho test (04-test-cases item 02 §1: đơn `2410TST000xx`, mã chiều về `SPXRTTST…`)."""

import uuid
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from httpx import AsyncClient, Response
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, PlatformReturn, ReturnItem
from aicam.modules.platforms.shopee import returns_mapping
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account

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
        status_group=shopee_order_group(status),
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
        status_group=returns_mapping.status_group(status),
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


class Desk:
    """Một bàn nhận hoàn đã đăng nhập (PRE-8: chế độ nhận hoàn, người kiểm "Lan QA")."""

    def __init__(self, api: AsyncClient, headers: dict[str, str], station: Station) -> None:
        self.api, self.headers, self.station = api, headers, station

    async def scan(self, code: str, client_scan_id: str | None = None) -> Response:
        return await self.api.post(
            "/api/v1/station/scan",
            headers=self.headers,
            json={"code": code, "client_scan_id": client_scan_id or str(uuid.uuid4())},
        )

    async def state(self) -> dict[str, Any]:
        res = await self.api.get("/api/v1/station/state", headers=self.headers)
        assert res.status_code == 200
        return res.json()  # type: ignore[no-any-return]


async def make_desk(
    api: AsyncClient,
    db: AsyncSession,
    n: int = 1,
    *,
    operator: str | None = "Lan QA",
    kind: str = "BOTH",
    mode: str = "RETURN",
) -> Desk:
    user, station = await make_station_account(db, f"tst_station0{n}", f"TST Station 0{n}")
    station.kind, station.work_mode, station.operator_name = kind, mode, operator
    await db.flush()
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    return Desk(api, {"Authorization": f"Bearer {res.json()['access_token']}"}, station)


async def pack_session_with_clips(
    db: AsyncSession,
    station: Station,
    package: Package,
    ended: datetime,
    *,
    status: str = "COMPLETED",
    snapshot: bool = True,
    clip_status: str = "READY",
) -> PackSession:
    """Phiên PACK đã đóng của kiện + clip Cam 1 / Cam 2 (+ ảnh lúc đóng gói) — bằng chứng FR-08.06."""
    from datetime import timedelta

    from aicam.modules.media.models import Clip, Snapshot

    pack = PackSession(
        id=uuid.uuid4(), type="PACK", package_id=package.id, station_id=station.id, status=status,
        started_at=ended - timedelta(minutes=2), ended_at=ended, open_code=package.tracking_number,
        close_code=package.tracking_number, package_status_before="NEW", flags=[],
    )  # fmt: skip
    db.add(pack)
    await db.flush()
    for role in ("CAM1", "CAM2"):
        db.add(
            Clip(
                session_id=pack.id,
                camera_role=role,
                status=clip_status,
                start_at=pack.started_at,
                end_at=ended + timedelta(seconds=5),
                path=f"clips/{pack.id}-{role}.mp4",
                sha256="ab" * 32,
                flags=[],
                deleted_at=ended if clip_status == "DELETED" else None,
            )
        )
    if snapshot:
        db.add(Snapshot(session_id=pack.id, kind="PACK_CLOSE", camera_role="CAM1",
                        taken_at=ended - timedelta(seconds=1), path=f"snapshots/{pack.id}_pack.jpg",
                        sha256="cd" * 32, size_bytes=10, status="READY"))  # fmt: skip
    await db.flush()
    return pack
