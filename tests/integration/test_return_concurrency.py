"""Đồng thời Phase 2 trên Postgres thật (02a §6, §11 "Đồng thời"; DEC-266, DEC-303 d).

Dữ liệu commit thật, dọn bằng TRUNCATE CASCADE sau test (như `test_scan_concurrency.py`).
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings
from aicam.main import create_app
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Package
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

from .returns_helpers import make_order

pytestmark = pytest.mark.integration

TABLES = (
    "scan_dedup, session_event, approval_request, clip, export, inspection_line, session, status_history, "
    'return_case_package, return_case, order_item, package, "order", camera, station, refresh_token, shop'
)


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings
) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_rconc_%'"))
    await dispose_engine()


async def _station(name: str, *, mode: str) -> dict[str, str]:
    settings = get_settings()
    async with sessionmaker()() as db:
        user = User(username=f"tst_rconc_{name}", display_name=name, role="STATION", password_hash="x")
        db.add(user)
        await db.flush()
        station = Station(
            name=f"TST {name}", account_user_id=user.id, kind="BOTH", work_mode=mode, operator_name="Lan QA"
        )
        db.add(station)
        await db.commit()
        token, _ = encode_access_token(settings.jwt_secret, user.id, "STATION", station.id, 15)
    return {"Authorization": f"Bearer {token}"}


def _client(test_settings: Settings) -> AsyncClient:
    app = create_app(test_settings)
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()
    return AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver")


async def _scan(client: AsyncClient, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = await client.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


async def test_two_return_desks_same_package(committed: AsyncEngine, test_settings: Settings) -> None:
    """02a §6: hai bàn hoàn quét cùng kiện → một phiên, bên kia `RETURN_IN_PROGRESS_ELSEWHERE`."""
    async with sessionmaker()() as db:
        await make_order(db, 42, warehouse_status="HANDED_OVER")
        await db.commit()
    a = await _station("a", mode="RETURN")
    b = await _station("b", mode="RETURN")

    async with _client(test_settings) as client:
        results = await asyncio.gather(_scan(client, a, "SPXTST0000042"), _scan(client, b, "SPXTST0000042"))

    assert sorted(r["outcome"] for r in results) == ["ALERT", "SESSION_OPENED"]
    alert = next(r["alert"] for r in results if r["outcome"] == "ALERT")
    assert alert["code"] == "RETURN_IN_PROGRESS_ELSEWHERE"
    async with sessionmaker()() as db:
        assert await db.scalar(select(func.count()).select_from(PackSession)) == 1


async def test_pack_scan_waits_for_manual_adjust(committed: AsyncEngine, test_settings: Settings) -> None:
    """DEC-303 d (ghi chú M6): API-122 giữ khóa kiện → quét PACK chờ, thấy trạng thái mới, không ghi đè."""
    async with sessionmaker()() as db:
        await make_order(db, 21, status="READY_TO_SHIP", warehouse_status="NEW")
        await db.commit()
    headers = await _station("pack", mode="PACK")

    async with sessionmaker()() as adjust, _client(test_settings) as client:
        package = await orders.find_package(adjust, "SPXTST0000021", for_update=True)
        assert package is not None
        await orders.transition(adjust, package, "HANDED_OVER", source="MANUAL")
        scan = asyncio.create_task(_scan(client, headers, "SPXTST0000021"))
        await asyncio.sleep(0.5)
        assert not scan.done()  # bị chặn bởi khóa kiện
        await adjust.commit()
        result = await asyncio.wait_for(scan, timeout=5)

    assert result["outcome"] == "ALERT"
    assert result["alert"]["code"] == "ALREADY_HANDED_OVER"
    async with sessionmaker()() as db:
        package = await orders.find_package(db, "SPXTST0000021")
        assert package is not None
        assert package.warehouse_status == "HANDED_OVER"


async def test_flag_order_cancelled_races_closing_scan(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """TC-03.77, DEC-266, R3-8: `flag_order_cancelled` ∥ quét đóng → không deadlock, kiện cuối luôn
    `CANCELLED_AFTER_PACK` (task trước → cờ rồi đóng; đóng trước → task chuyển từ `PACKED`)."""
    from aicam.modules.sessions import service as sessions

    headers = await _station("race", mode="PACK")
    async with _client(test_settings) as client:
        for n in range(23, 28):
            code = f"SPXTST{n:07d}"
            assert (await _scan(client, headers, code))["outcome"] == "SESSION_OPENED"
            async with sessionmaker()() as db:
                package = await orders.find_package(db, code)
                assert package is not None
                package_id = package.id

            async def run_task(pid: uuid.UUID = package_id) -> str:
                async with sessionmaker()() as db:
                    return await sessions.flag_order_cancelled(db, pid, test_settings)

            await asyncio.wait_for(asyncio.gather(_scan(client, headers, code), run_task()), timeout=5)
            async with sessionmaker()() as db:
                package = await orders.find_package(db, code)
            assert package is not None
            assert package.warehouse_status == "CANCELLED_AFTER_PACK", code


async def test_scan_j13_j06_same_order_no_deadlock(committed: AsyncEngine, test_settings: Settings) -> None:
    """02a §11 "Đồng thời" (R3-4, DEC-266): API-11 RETURN mở hồ sơ mới ∥ J-13 cùng đơn ∥ J-06 cùng kiện →
    không deadlock (≤ 5 giây), đúng một hồ sơ mở mỗi đơn, kiện ở trạng thái hợp lệ."""
    from dataclasses import replace
    from datetime import timedelta

    from aicam.core import clock
    from aicam.core.security import Cipher
    from aicam.modules.orders.models import Shop
    from aicam.modules.platforms import service as platforms
    from aicam.modules.platforms import sync
    from aicam.modules.platforms.base import ShopCredentials
    from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase

    from .returns_helpers import platform_return

    test_settings.shopee_enabled = True
    async with sessionmaker()() as db:
        shop = Shop(platform="SHOPEE", platform_shop_id="990001", name="TST Shop", auth_status="CONNECTED")
        platforms.store_credentials(
            shop, ShopCredentials("990001", "a", "r", clock.now() + timedelta(hours=4)),
            Cipher(test_settings.fernet_key),
        )  # fmt: skip
        db.add(shop)
        for n in range(61, 66):
            await make_order(db, n, status="SHIPPED", warehouse_status="HANDED_OVER")
        await db.commit()
    headers = await _station("sync", mode="RETURN")
    mock = MockAdapter()
    mock.returns = {}
    for n in range(61, 66):
        code = f"SPXTST{n:07d}"
        mock.orders[f"2410TST{n:05d}"] = replace(
            mock.orders["2410TST00001"], platform_order_sn=f"2410TST{n:05d}", status="SHIPPED",
            tracking_numbers=(code,),
        )  # fmt: skip
        mock.shipping[code] = "DELIVERY_FAILED"
        mock.put_return(replace(platform_return(n), updated_at=clock.now()))

    async def j13() -> object:
        async with sessionmaker()() as db:
            return await sync.sync_returns(db, mock, test_settings)

    async def j06() -> object:
        async with sessionmaker()() as db:
            return await sync.sync_shipping_status(db, mock, test_settings)

    async with _client(test_settings) as client:
        scans = [_scan(client, headers, f"SPXTST{n:07d}") for n in (61,)]
        await asyncio.wait_for(asyncio.gather(*scans, j13(), j06()), timeout=5)

    async with sessionmaker()() as db:
        for n in range(61, 66):
            order_id = await db.scalar(select(text("id")).select_from(text('"order"')).where(
                text("platform_order_sn = :sn")).params(sn=f"2410TST{n:05d}"))  # fmt: skip
            open_cases = await db.scalar(
                select(func.count())
                .select_from(ReturnCase)
                .where(ReturnCase.order_id == order_id, ReturnCase.status.in_(OPEN_CASE_STATUSES))
            )
            assert open_cases == 1, n
            package = await orders.find_package(db, f"SPXTST{n:07d}")
            assert package is not None
            assert package.warehouse_status in ("RETURN_EXPECTED", "RETURN_INSPECTING"), n


async def test_j14_skips_locked_package(committed: AsyncEngine, test_settings: Settings) -> None:
    """02a §6 (DEC-256 / 266): J-14 dùng `SKIP LOCKED` — kiện (hoặc hồ sơ) đang bị API / job khác khóa thì
    bỏ qua lượt này, không chờ, không deadlock; lượt sau chuyển `RETURN_MISSING`."""
    from datetime import timedelta

    from aicam.core import clock
    from aicam.modules.reconciliation import service as recon

    async with sessionmaker()() as db:
        _, (package,) = await make_order(db, 71, warehouse_status="RETURN_EXPECTED")
        package.status_changed_at = clock.now() - timedelta(days=8)
        await db.commit()
        package_id = package.id

    async with sessionmaker()() as holder, sessionmaker()() as job:
        locked = await orders.find_package(holder, "SPXTST0000071", for_update=True)
        assert locked is not None
        out = await asyncio.wait_for(recon.run_rules(job, test_settings), timeout=5)
        assert out["missing"] == 0
        await holder.rollback()
        out = await asyncio.wait_for(recon.run_rules(job, test_settings), timeout=5)
        assert out["missing"] == 1
    async with sessionmaker()() as db:
        package = await db.get(Package, package_id)
        assert package is not None
        assert package.warehouse_status == "RETURN_MISSING"
