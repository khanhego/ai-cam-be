# ruff: noqa: S608, E501 — DB tạm: SQL sinh dữ liệu ghép từ hằng, câu dài
"""Đo migration Phase 3 trên 1 triệu đơn (T-201; 02a §3 "đo ở T-201 trên 1 triệu đơn", ghi vào docs/ops.md §7.2).

Chạy riêng (không chạy mặc định, ~3–5 phút trên máy dev):
`RUN_PERF=1 uv run pytest -m perf tests/integration/test_perf_migration_0006.py -s`

DB **tạm** `<TEST_DATABASE_URL>_perf06` (không đụng DB dev `aicam` hay `aicam_test`), xóa sau khi đo. Dữ liệu ở 0005
(Phase 2): N đơn Shopee (trạng thái rải đủ bảng 02 §5.3 + 1 % chữ lạ + 2 % đơn file), N kiện, N phiên PACK, N dòng
`status_history`, N/50 hồ sơ hàng hoàn, N/200 hồ sơ khiếu nại có audit `CLAIM_UPDATE`. Đo `alembic upgrade` 0005 →
head (0006 [+ 0007]) — một transaction, mọi service dừng (ops §7.2). Số đo là máy dev, không phải server kho.
"""

import asyncio
import os
import time
from collections.abc import Iterator
from typing import Any

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from aicam.core.schema_guard import SCHEMA_HEAD
from aicam.core.settings import get_settings

from .conftest import TEST_DATABASE_URL, alembic_config

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perf,
    pytest.mark.skipif(
        not os.environ.get("RUN_PERF"), reason="đặt RUN_PERF=1 để đo migration 0006 trên 1 triệu đơn"
    ),
]

N = int(os.environ.get("PERF_ORDERS", "1000000"))
LIMIT_S = 300.0  # trần thô cho máy dev — số thật ghi vào ops §7.2
_BASE = make_url(TEST_DATABASE_URL)
DB = f"{_BASE.database}_perf06"
PERF_URL = _BASE.set(database=DB).render_as_string(hide_password=False)
ADMIN_URL = _BASE.set(database="postgres").render_as_string(hide_password=False)

STATUSES = (
    "'UNPAID'", "'READY_TO_SHIP'", "'PROCESSED'", "'RETRY_SHIP'", "'SHIPPED'", "'TO_CONFIRM_RECEIVE'",
    "'COMPLETED'", "'COMPLETED'", "'COMPLETED'", "'COMPLETED'", "'COMPLETED'", "'COMPLETED'", "'IN_CANCEL'",
    "'CANCELLED'", "'TO_RETURN'",
)  # fmt: skip


async def _exec(url: str, *statements: str, autocommit: bool = False) -> list[Any]:
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT" if autocommit else "READ COMMITTED")
    out: list[Any] = []
    try:
        async with engine.begin() as conn:
            for sql in statements:
                result = await conn.execute(text(sql))
                out = list(result.all()) if result.returns_rows else []
    finally:
        await engine.dispose()
    return out


def run(*statements: str) -> list[Any]:
    return asyncio.run(_exec(PERF_URL, *statements))


@pytest.fixture
def perf_db(migrated_database_url: str) -> Iterator[None]:
    asyncio.run(
        _exec(
            ADMIN_URL,
            f'DROP DATABASE IF EXISTS "{DB}" WITH (FORCE)',
            f'CREATE DATABASE "{DB}"',
            autocommit=True,
        )
    )
    os.environ["DATABASE_URL"] = PERF_URL
    get_settings.cache_clear()
    try:
        command.upgrade(alembic_config(), "0005")
        yield
    finally:
        os.environ["DATABASE_URL"] = migrated_database_url
        get_settings.cache_clear()
        asyncio.run(_exec(ADMIN_URL, f'DROP DATABASE IF EXISTS "{DB}" WITH (FORCE)', autocommit=True))


def seed(n: int) -> None:
    arr = "ARRAY[" + ", ".join(STATUSES) + "]"
    run(
        "INSERT INTO \"user\" (id, username, display_name, role, password_hash) VALUES "
        "('01960000-0000-7000-8000-000000000001', 'perf_cskh', 'Perf', 'CSKH', 'x')",
        "INSERT INTO station (id, name) VALUES ('01960000-0000-7000-8000-000000000002', 'Perf station')",
        "INSERT INTO shop (id, platform, platform_shop_id, auth_status) VALUES "
        "('01960000-0000-7000-8000-000000000003', 'SHOPEE', '990001', 'CONNECTED')",
        # 97 % đơn sàn rải trạng thái, 1 % chữ lạ, 2 % đơn file (không shop, không trạng thái).
        'INSERT INTO "order" (id, shop_id, platform_order_sn, platform_status, source, updated_at) '
        "SELECT md5('o' || g)::uuid, CASE WHEN g % 50 = 0 THEN NULL ELSE '01960000-0000-7000-8000-000000000003'::uuid END, "
        "'2410P' || lpad(g::text, 9, '0'), "
        f"CASE WHEN g % 50 = 0 THEN NULL WHEN g % 100 = 1 THEN 'WEIRD_' || (g % 7) ELSE ({arr})[1 + g % {len(STATUSES)}] END, "
        "CASE WHEN g % 50 = 0 THEN 'CSV' ELSE 'API' END, now() - (g % 365) * interval '1 day' "
        f"FROM generate_series(1, {n}) g",
        "INSERT INTO package (id, order_id, tracking_number, warehouse_status, updated_at) "
        "SELECT md5('p' || g)::uuid, md5('o' || g)::uuid, 'SPXP' || lpad(g::text, 9, '0'), "
        "CASE WHEN g % 10 = 0 THEN 'PACKED' ELSE 'DELIVERED' END, now() - (g % 365) * interval '1 day' "
        f"FROM generate_series(1, {n}) g",
        "INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, close_code) "
        "SELECT md5('s' || g)::uuid, 'PACK', md5('p' || g)::uuid, '01960000-0000-7000-8000-000000000002', 'COMPLETED', "
        "now() - (g % 365) * interval '1 day' - interval '2 minutes', now() - (g % 365) * interval '1 day', "
        f"'SPXP' || lpad(g::text, 9, '0'), 'SPXP' || lpad(g::text, 9, '0') FROM generate_series(1, {n}) g",
        "INSERT INTO status_history (id, package_id, source, from_status, to_status, at) "
        "SELECT md5('h' || g)::uuid, md5('p' || g)::uuid, 'WAREHOUSE', 'PACKING', 'PACKED', now() - (g % 365) * interval '1 day' "
        f"FROM generate_series(1, {n}) g",
        "INSERT INTO return_case (id, order_id, kind, status, source, platform_return_sn, platform_status) "
        "SELECT md5('r' || g)::uuid, md5('o' || ((g - 1) * 50 + 1))::uuid, 'BUYER_RETURN', 'RECEIVED_OK', 'PLATFORM', "
        "'RS' || g, (ARRAY['REQUESTED', 'ACCEPTED', 'REFUND_PAID', 'CLOSED'])[1 + g % 4] "
        f"FROM generate_series(1, {n // 50}) g",
        "INSERT INTO claim (id, package_id, type, counterparty, status, source) "
        "SELECT md5('c' || g)::uuid, md5('p' || g)::uuid, 'DAMAGED', 'PLATFORM', "
        f"(ARRAY['SUBMITTED', 'WON', 'LOST', 'NEW'])[1 + g % 4], 'MANUAL' FROM generate_series(1, {n // 200}) g",
        "INSERT INTO audit_log (user_id, action, object_type, object_id, at, data) "
        "SELECT '01960000-0000-7000-8000-000000000001', 'CLAIM_UPDATE', 'CLAIM', c.id::text, now() - interval '1 day', "
        "jsonb_build_object('after', jsonb_build_object('status', c.status)) FROM claim c WHERE c.status <> 'NEW'",
        'ANALYZE "order"', "ANALYZE package", "ANALYZE session", "ANALYZE status_history", "ANALYZE return_case",
        "ANALYZE claim", "ANALYZE audit_log",
    )  # fmt: skip


def test_upgrade_phase3_on_one_million_orders(perf_db: None) -> None:
    began = time.monotonic()
    seed(N)
    print(f"\n  nạp {N:,} đơn / kiện / phiên / lịch sử: {time.monotonic() - began:.1f} giây")

    began = time.monotonic()
    command.upgrade(alembic_config(), SCHEMA_HEAD)
    took = time.monotonic() - began
    print(f"  alembic upgrade 0005 → {SCHEMA_HEAD} (một transaction): {took:.1f} giây")

    counts = dict(run('SELECT platform_status_group, count(*) FROM "order" GROUP BY 1'))
    print(f"  nhóm: {counts}")
    size = run("SELECT pg_size_pretty(pg_database_size(current_database()))")[0][0]
    print(f"  dung lượng DB sau nâng cấp: {size}")
    assert sum(counts.values()) == N
    assert counts["UNKNOWN"] == N // 50 + (N - 1) // 100 + 1  # đơn file + chữ lạ
    assert took < LIMIT_S
