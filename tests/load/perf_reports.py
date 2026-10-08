# ruff: noqa: S608, E501 — script đo trên DB tạm: SQL sinh dữ liệu ghép từ hằng, câu dài, in kết quả
"""Đo NFR-37 / AC-48 (T-217; 02a §8, §11 "Hiệu năng"): báo cáo API-150..152 kỳ 92 ngày ≤ 3 giây p95, kỳ 366
ngày ≤ 10 giây, trên 500 đơn / ngày × 366 ngày (≈ 183.000 kiện, ≈ 9.150 hồ sơ hàng hoàn, ≈ 1.830 hồ sơ khiếu nại).

DB **tạm** `<TEST_DATABASE_URL>_perf_reports` (cùng Postgres với DB test, **không** đụng DB dev `aicam`), xóa sau khi
đo (trừ `--keep`). Không dùng Redis: gọi thẳng `analytics.build()` (đường không cache, có `statement_timeout`) —
mỗi tab × mỗi kỳ chạy `--runs` lần (mặc định 20), in p50 / p95 / max.

Chạy (chỉ khi đặt cờ, như Phase 2):

    RUN_PERF=1 uv run python tests/load/perf_reports.py [--per-day 500] [--days 366] [--runs 20] [--keep]

Số đo là **máy dev** (Docker Desktop), **không** phải máy kho — NFR-37 trên máy kho đo lúc go-live (02a "Rủi ro").
"""

import argparse
import asyncio
import os
import statistics
import sys
import time
from datetime import timedelta

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BASE_URL = os.environ.get("TEST_DATABASE_URL", "postgresql+asyncpg://aicam:aicam@localhost:55432/aicam_test")
_BASE = make_url(BASE_URL)
DB_NAME = f"{_BASE.database}_perf_reports"
PERF_URL = _BASE.set(database=DB_NAME).render_as_string(hide_password=False)
ADMIN_URL = _BASE.set(database="postgres").render_as_string(hide_password=False)
TZ = "Asia/Ho_Chi_Minh"
LIMITS = {92: 3.0, 366: 10.0}  # NFR-37: p95 (giây)

SHOP_A = "00000000-0000-4000-8000-00000000000a"
SHOP_B = "00000000-0000-4000-8000-00000000000b"
STATIONS = [f"00000000-0000-4000-8000-0000000000{i:02d}" for i in range(1, 5)]


def alembic_cfg() -> Config:
    cfg = Config(os.path.join(ROOT, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(ROOT, "alembic"))
    return cfg


async def exec_sql(url: str, *statements: str, autocommit: bool = False) -> None:
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT" if autocommit else "READ COMMITTED")
    try:
        async with engine.begin() as conn:
            for sql in statements:
                await conn.execute(text(sql))
    finally:
        await engine.dispose()


def seed_sql(n: int, per_day: int, days: int) -> list[str]:
    """Sinh dữ liệu theo tập. Đơn `g` (1..n) thuộc ngày `d = (g-1) / per_day` (0 = ngày xa nhất); mốc `t(g)` = 08:00
    VN của ngày đó + rải trong ngày. Hôm nay (VN) là ngày cuối."""
    day0 = f"(date_trunc('day', now() AT TIME ZONE '{TZ}') - interval '{days - 1} days')"
    t = f"(({day0} + ((g - 1) / {per_day}) * interval '1 day' + interval '8 hours' + ((g % {per_day}) * interval '50 seconds')) AT TIME ZONE '{TZ}')"
    ids = "md5('{p}' || g)::uuid"
    oid, pid = ids.format(p="o"), ids.format(p="p")
    names = "(ARRAY['Minh', 'Lan', '  minh ', 'Hà', NULL, 'Tú'])[g % 6 + 1]"
    series = f"FROM generate_series(1, {n}) g"
    rc = f"FROM generate_series(20, {n}, 20) g"  # 5 % đơn có hồ sơ hàng hoàn
    cl = f"FROM generate_series(100, {n}, 100) g"  # 1 % đơn có hồ sơ khiếu nại
    rc_kind = "(ARRAY['BUYER_RETURN','BUYER_RETURN','BUYER_RETURN','BUYER_RETURN','BUYER_RETURN','FAILED_DELIVERY','FAILED_DELIVERY','FAILED_DELIVERY','REFUND_ONLY','UNANNOUNCED'])[(g / 20) % 10 + 1]"
    rc_status = f"CASE WHEN {rc_kind} = 'REFUND_ONLY' THEN 'NO_PARCEL' ELSE (ARRAY['RECEIVED_OK','RECEIVED_OK','RECEIVED_OK','RECEIVED_OK','RECEIVED_OK','RECEIVED_ISSUE','EXPECTED'])[(g / 20) % 7 + 1] END"
    rc_created = f"LEAST({t} + interval '5 days', now())"
    cl_status = "(ARRAY['NEW','SUBMITTED','WAITING','WON','LOST','CLOSED'])[(g / 100) % 6 + 1]"
    cl_created = f"LEAST({t} + interval '6 days', now() - interval '1 hour')"
    return [
        f"INSERT INTO shop (id, platform, platform_shop_id, name, auth_status) VALUES ('{SHOP_A}', 'SHOPEE', 'PERF-A', 'Áo Đẹp', 'CONNECTED'), ('{SHOP_B}', 'TIKTOK', 'PERF-B', 'Áo Đẹp Official', 'CONNECTED')",
        "INSERT INTO station (id, name) VALUES "
        + ", ".join(f"('{s}', 'Station {i:02d}')" for i, s in enumerate(STATIONS, 1)),
        f"INSERT INTO \"order\" (id, shop_id, platform_order_sn, platform_status_group, source) SELECT {oid}, CASE WHEN g % 10 < 7 THEN '{SHOP_A}'::uuid ELSE '{SHOP_B}'::uuid END, 'PERF' || g, 'DELIVERED', 'API' {series}",
        f"INSERT INTO order_item (id, order_id, sku, product_name, variation, quantity) SELECT {ids.format(p='i')}, {oid}, CASE WHEN g % 7 = 0 THEN NULL ELSE 'SKU-' || (g % 300) END, 'Sản phẩm ' || (g % 300), 'Size ' || (g % 4), 1 {series}",
        f"INSERT INTO order_item (id, order_id, sku, product_name, variation, quantity) SELECT {ids.format(p='j')}, {oid}, 'SKU-X' || (g % 50), 'Phụ kiện ' || (g % 50), NULL, 1 FROM generate_series(1, {n}, 3) g",
        f"INSERT INTO package (id, order_id, tracking_number, warehouse_status, verified) SELECT {pid}, {oid}, 'PERFTN' || g, 'DELIVERED', true {series}",
        f"INSERT INTO status_history (id, package_id, source, from_status, to_status, at) SELECT {ids.format(p='h')}, {pid}, 'WAREHOUSE', 'NEW', 'PACKED', {t} + interval '2 minutes' {series}",
        f"INSERT INTO status_history (id, package_id, source, from_status, to_status, at) SELECT {ids.format(p='k')}, {pid}, 'PLATFORM', 'PACKED', 'HANDED_OVER', {t} + interval '4 hours' {series}",
        f"INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, flags, operator_name, cancel_reason) "
        f"SELECT {ids.format(p='s')}, 'PACK', {pid}, (ARRAY{STATIONS!r}::uuid[])[g % 4 + 1], "
        f"CASE WHEN g % 100 < 97 THEN 'COMPLETED' WHEN g % 100 < 99 THEN 'ABANDONED' ELSE 'CANCELLED' END, {t}, {t} + (60 + g % 120) * interval '1 second', "
        f"'PERFTN' || g, CASE WHEN g % 50 = 0 THEN ARRAY['HAD_MISMATCH'] WHEN g % 100 = 1 THEN ARRAY['REPACK'] ELSE ARRAY[]::text[] END, {names}, "
        f"CASE WHEN g % 100 = 99 THEN 'OTHER' END {series}",
        f"INSERT INTO approval_request (id, station_id, session_id, tracking_number, type, status, decision, created_at, decided_at) "
        f"SELECT {ids.format(p='a')}, (ARRAY{STATIONS!r}::uuid[])[g % 4 + 1], {ids.format(p='s')}, 'PERFTN' || g, 'MISMATCH', 'RESOLVED', 'CONTINUE', {t} + interval '20 seconds', {t} + interval '40 seconds' FROM generate_series(50, {n}, 50) g",
        f"INSERT INTO return_case (id, order_id, shop_id, kind, status, source, reason, conclusion, created_at, received_at, return_tracking_number) "
        f"SELECT {ids.format(p='c')}, {oid}, CASE WHEN g % 10 < 7 THEN '{SHOP_A}'::uuid ELSE '{SHOP_B}'::uuid END, {rc_kind}, {rc_status}, 'PLATFORM', "
        f"(ARRAY['ITEM_DAMAGED','CHANGE_MIND','WRONG_ITEM',NULL])[g % 4 + 1], "
        f"CASE {rc_status} WHEN 'RECEIVED_OK' THEN 'OK' WHEN 'RECEIVED_ISSUE' THEN (ARRAY['DAMAGED','EMPTY_BOX','MISSING_ITEM'])[g % 3 + 1] END, "
        f"{rc_created}, CASE WHEN {rc_status} LIKE 'RECEIVED%' THEN LEAST({rc_created} + interval '3 days', now()) END, 'PERFRT' || g {rc}",
        f"INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, operator_name, inspection_conclusion, return_case_id) "
        f"SELECT {ids.format(p='r')}, 'RETURN', {pid}, (ARRAY{STATIONS!r}::uuid[])[g % 4 + 1], 'COMPLETED', rc.received_at - interval '200 seconds', rc.received_at, "
        f"'PERFRT' || g, (ARRAY['Lan', 'Hoa', NULL])[g % 3 + 1], rc.conclusion, rc.id {rc} JOIN return_case rc ON rc.id = {ids.format(p='c')} WHERE rc.received_at IS NOT NULL",
        f"INSERT INTO claim (id, package_id, order_id, type, counterparty, status, source, created_at, deadline_at, submitted_at, result_at, recovered_amount, closed_at) "
        f"SELECT {ids.format(p='q')}, {pid}, {oid}, (ARRAY['DAMAGED','EMPTY_BOX','MISSING_ITEM','LOST_IN_TRANSIT'])[g % 4 + 1], "
        f"CASE WHEN g % 3 = 0 THEN 'CARRIER' ELSE 'PLATFORM' END, {cl_status}, 'MANUAL', {cl_created}, {cl_created} + interval '7 days', "
        f"CASE WHEN {cl_status} <> 'NEW' THEN {cl_created} + interval '1 day' END, "
        f"CASE WHEN {cl_status} IN ('WON', 'LOST', 'CLOSED') THEN LEAST({cl_created} + interval '5 days', now()) END, "
        f"CASE WHEN {cl_status} IN ('WON', 'CLOSED') THEN 100000 + (g % 7) * 50000 END, "
        f"CASE WHEN {cl_status} = 'CLOSED' THEN now() END {cl}",
        "INSERT INTO audit_log (action, object_type, object_id, at, data) SELECT 'CLAIM_UPDATE', 'CLAIM', id::text, result_at, "
        "jsonb_build_object('before', jsonb_build_object('status', 'WAITING'), 'after', jsonb_build_object('status', 'WON')) "
        "FROM claim WHERE status = 'CLOSED'",
        "ANALYZE",
    ]


async def measure(runs: int, periods: list[int]) -> list[tuple[str, int, list[float]]]:
    from aicam.modules.reports import analytics

    engine = create_async_engine(PERF_URL)
    today = analytics.today_vn(TZ)
    out = []
    try:
        for days in periods:
            f = analytics.ReportFilters(today - timedelta(days=days - 1), today)
            for name in analytics.REPORTS:
                took: list[float] = []
                for _ in range(runs):
                    async with AsyncSession(engine) as db:
                        began = time.monotonic()
                        await analytics.build(db, name, f, TZ)
                        took.append(time.monotonic() - began)
                out.append((name, days, took))
                p95 = statistics.quantiles(took, n=20)[18] if len(took) >= 2 else took[0]
                print(
                    f"  {name:<13} {days:>3} ngày: p50 {statistics.median(took):.3f} giây · p95 {p95:.3f} · max {max(took):.3f}"
                )
        # một lượt có lọc sàn / station để thấy chi phí điều kiện shop
        f = analytics.ReportFilters(today - timedelta(days=91), today, platform="TIKTOK")
        async with AsyncSession(engine) as db:
            began = time.monotonic()
            await analytics.build(db, "returns", f, TZ)
            print(f"  returns 92 ngày lọc TIKTOK (1 lần): {time.monotonic() - began:.3f} giây")
    finally:
        await engine.dispose()
    return out


def main() -> int:
    if os.environ.get("RUN_PERF") != "1":
        print("Đặt RUN_PERF=1 để chạy đo NFR-37 (tạo DB tạm, vài phút).")
        return 0
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-day", type=int, default=500)
    parser.add_argument("--days", type=int, default=366)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    n = args.per_day * args.days
    print(f"DB tạm {DB_NAME}: {n:,} đơn / kiện".replace(",", ".") + f", {args.days} ngày")
    asyncio.run(
        exec_sql(
            ADMIN_URL,
            f'DROP DATABASE IF EXISTS "{DB_NAME}" WITH (FORCE)',
            f'CREATE DATABASE "{DB_NAME}"',
            autocommit=True,
        )
    )
    try:
        os.environ["DATABASE_URL"] = PERF_URL
        from aicam.core.settings import get_settings

        get_settings.cache_clear()
        command.upgrade(alembic_cfg(), "head")
        began = time.monotonic()
        asyncio.run(exec_sql(PERF_URL, *seed_sql(n, args.per_day, args.days)))
        print(f"  sinh dữ liệu: {time.monotonic() - began:.1f} giây")
        results = asyncio.run(measure(args.runs, [92, 366]))
    finally:
        if not args.keep:
            asyncio.run(
                exec_sql(ADMIN_URL, f'DROP DATABASE IF EXISTS "{DB_NAME}" WITH (FORCE)', autocommit=True)
            )
    failed = [
        (name, days)
        for name, days, took in results
        if (statistics.quantiles(took, n=20)[18] if len(took) >= 2 else took[0]) > LIMITS[days]
    ]
    print("NFR-37 (máy dev):", "ĐẠT" if not failed else f"VƯỢT {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
