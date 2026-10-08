"""J-04 không giữ khóa đơn qua lời gọi mạng kế tiếp (G3-V1, DEC-162) — commit thật, dọn bằng TRUNCATE.

Trước đây J-04 giữ tới 50 khóa `order:{sn}` (+ khóa dòng kiện) trong một transaction theo thứ tự Shopee trả,
qua các lần gọi mạng → khóa chéo hiếm với API-51 (khóa theo thứ tự sắp xếp) → Postgres 40P01. Nay commit sau
mỗi đơn: lúc adapter lấy đơn kế tiếp, khóa của đơn trước đã nhả (kiểm bằng `pg_try_advisory_xact_lock` từ
một kết nối khác).
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core import clock
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MOCK_SHOP_ID, MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)
TABLES = 'status_history, order_item, package, "order", shop'


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings
) -> AsyncIterator[AsyncEngine]:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
    await dispose_engine()


def _order(n: int) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=f"2410TSTLK{n:03d}",
        status="READY_TO_SHIP",
        tracking_numbers=(f"SPXTSTLK{n:05d}",),
        items=(PlatformItem("Áo thun", 1),),
        created_at=NOW,
        updated_at=NOW,
        status_group=shopee_order_group("READY_TO_SHIP"),
    )


async def test_j04_releases_order_lock_before_next_network_call(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    async with sessionmaker()() as db:
        shop = Shop(platform="SHOPEE", platform_shop_id=MOCK_SHOP_ID, name="TST Shop LK")
        platforms.store_credentials(
            shop,
            ShopCredentials(MOCK_SHOP_ID, "acc-1", "ref-1", datetime(2026, 10, 6, tzinfo=UTC)),
            Cipher(test_settings.fernet_key),
        )
        db.add(shop)
        await db.commit()

    orders_ = [_order(n) for n in (1, 2, 3)]
    free_before_next: list[bool] = []

    async def lock_free(sn: str) -> bool:
        async with committed.connect() as other, other.begin():
            got = await other.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtext(:k))"), {"k": f"order:{sn}"}
            )
            return bool(got)

    class Paged(MockAdapter):
        async def list_updated_orders(self, creds: Any, since: datetime) -> Any:  # type: ignore[override]
            for i, order in enumerate(orders_):
                if i:  # "lời gọi mạng" lấy đơn kế: khóa đơn trước phải đã nhả
                    free_before_next.append(await lock_free(orders_[i - 1].platform_order_sn))
                yield order

    async with sessionmaker()() as db:
        out = await sync.sync_orders(db, Paged(), test_settings)

    assert next(iter(out.values()))["orders"] == 3
    assert free_before_next == [True, True]
