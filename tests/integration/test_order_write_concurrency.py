"""G3-F4: hai đường ghi cùng một đơn sàn song song thật (2 connection, dữ liệu commit thật, TRUNCATE sau).

Không khóa: cả hai cùng đọc đơn rồi delete + insert `order_item` → item nhân đôi; cùng tạo đơn mới → đụng
unique.
Có khóa advisory `order:{sn}`: transaction thứ hai chờ tới khi thứ nhất commit, đọc lại rồi mới ghi.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, OrderItem, Package
from aicam.modules.platforms.base import PlatformItem, PlatformOrder

pytestmark = pytest.mark.integration

TABLES = 'status_history, order_item, package, "order"'
SN = "2410TSTWC0001"
DATA = PlatformOrder(
    SN, "READY_TO_SHIP", ("SPXTSTWC00001",), (PlatformItem("Áo", 1, "SKU1"), PlatformItem("Quần", 2, "SKU2"))
)
CSV = orders.CsvOrder(SN, None, DATA.items, DATA.tracking_numbers)


@pytest.fixture
async def committed(migrated_database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
    await dispose_engine()


Writer = Callable[[AsyncSession], Awaitable[Any]]


async def _race(first: Writer, second: Writer) -> None:
    """`first` ghi rồi giữ transaction; `second` song song phải chờ khóa; `first` commit → `second` xong."""
    wrote = asyncio.Event()
    release = asyncio.Event()

    async def run_first() -> None:
        async with sessionmaker()() as db:
            await first(db)
            wrote.set()
            await release.wait()
            await db.commit()

    async def run_second() -> None:
        await wrote.wait()
        async with sessionmaker()() as db:
            await second(db)
            await db.commit()

    a, b = asyncio.create_task(run_first()), asyncio.create_task(run_second())
    await asyncio.wait_for(wrote.wait(), timeout=10)
    await asyncio.sleep(0.3)
    assert not b.done()  # transaction thứ hai đang chờ khóa đơn
    release.set()
    await asyncio.wait_for(asyncio.gather(a, b), timeout=10)


async def _counts() -> tuple[int, int, int]:
    async with sessionmaker()() as db:
        order_count = await db.scalar(
            select(func.count()).select_from(Order).where(Order.platform_order_sn == SN)
        )
        items = await db.scalar(
            select(func.count()).select_from(OrderItem).join(Order).where(Order.platform_order_sn == SN)
        )
        packages = await db.scalar(
            select(func.count()).select_from(Package).where(Package.order_id.is_not(None))
        )
        return int(order_count or 0), int(items or 0), int(packages or 0)


async def _api(db: AsyncSession) -> Any:
    return await orders.upsert_platform_order(db, DATA)


async def test_two_platform_upserts_of_existing_order_do_not_duplicate_items(committed: AsyncEngine) -> None:
    """J-04 và tra sàn khi quét cùng ghi một đơn đã có."""
    async with sessionmaker()() as db:
        await _api(db)
        await db.commit()

    await _race(_api, _api)

    assert await _counts() == (1, 2, 1)


async def test_two_platform_upserts_of_new_order_no_unique_violation(committed: AsyncEngine) -> None:
    await _race(_api, _api)

    assert await _counts() == (1, 2, 1)


async def test_csv_update_and_platform_upsert_serialize(committed: AsyncEngine) -> None:
    """Nhập file (đơn CSV đã có → cập nhật) và J-04 cùng lúc: API ghi đè sau, item không nhân đôi (BR-17)."""
    from aicam.modules.users.models import User

    async with sessionmaker()() as db:
        user = User(username="tst_wc_sup", display_name="Sup", role="SUPERVISOR", password_hash="x")
        db.add(user)
        await db.flush()
        user_id = user.id
        from aicam.core import clock
        from aicam.modules.imports.models import CsvImport

        imp = CsvImport(status="PREVIEW", file_name="x.csv", counts={}, errors=[], preview_rows=[],
                        created_by=user_id, created_at=clock.now(), expires_at=clock.now())  # fmt: skip
        db.add(imp)
        await db.flush()
        import_id = imp.id
        await orders.apply_csv_order(db, CSV, import_id=import_id, shop_id=None, actor_user_id=user_id)
        await db.commit()

    async def csv(db: AsyncSession) -> Any:
        return await orders.apply_csv_order(db, CSV, import_id=import_id, shop_id=None, actor_user_id=user_id)

    try:
        await _race(csv, _api)
        assert await _counts() == (1, 2, 1)
        async with sessionmaker()() as db:
            assert await db.scalar(select(Order.source).where(Order.platform_order_sn == SN)) == "API"
    finally:
        async with committed.begin() as conn:
            await conn.execute(text("TRUNCATE csv_import CASCADE"))
            await conn.execute(text("DELETE FROM \"user\" WHERE username = 'tst_wc_sup'"))
