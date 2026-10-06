# ruff: noqa: S608, E501 — script đo trên DB tạm: SQL sinh dữ liệu ghép từ hằng, câu dài
"""Đo hiệu năng dữ liệu lớn (T-118; 02a §11 "Hiệu năng", RB-23, NFR-33, API-104 tiền tố).

Trên một DB **tạm** `aicam_perf` cùng Postgres của stack dev (không đụng DB `aicam`), xóa sau khi đo:

1. Dữ liệu Phase 1 ở revision 0002: N đơn / N kiện (mặc định 1 triệu; phân bố trạng thái như kho đã chạy lâu) +
   1 dòng `status_history` / kiện.
2. `alembic upgrade 0003` + `head` đo thời gian, đồng thời một kết nối khác đọc / ghi bảng `package` mỗi 200 ms →
   thời gian bị chặn (khóa của migration — RB-23).
3. Dữ liệu Phase 2: 20.000 kiện "đang về" (hồ sơ `EXPECTED`, 0–14 ngày) → J-14 `run_rules` 2 lần (lần đầu tạo
   cảnh báo, lần hai ổn định) — NFR-33 (≤ 60 giây với ≥ 100.000 kiện chưa ở trạng thái cuối).
4. API-104 (`return_lookup._package_ids`): khớp chính xác + tiền tố, không có / có index `text_pattern_ops` (0005).

Chạy (stack dev đang có postgres 55432 + redis 56379; ~5 phút với 1 triệu kiện trên máy dev):

    uv run python tests/load/perf_bigdata.py --packages 1000000 [--keep]

Số đo là **máy dev** (Docker Desktop), không phải server kho.
"""

import argparse
import asyncio
import os
import statistics
import threading
import time
from typing import Any

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

BASE_URL = os.environ.get("PERF_BASE_URL", "postgresql+asyncpg://aicam:aicam@localhost:55432/aicam")
REDIS_URL = os.environ.get("PERF_REDIS_URL", "redis://localhost:56379/14")
DB_NAME = "aicam_perf"
PERF_URL = make_url(BASE_URL).set(database=DB_NAME).render_as_string(hide_password=False)
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INDEXES = (
    ("ix_package_tracking_upper_pattern", "package", "upper(tracking_number) text_pattern_ops"),
    ("ix_order_platform_order_sn_upper_pattern", '"order"', "upper(platform_order_sn) text_pattern_ops"),
    (
        "ix_return_case_return_tracking_upper_pattern",
        "return_case",
        "upper(return_tracking_number) text_pattern_ops",
    ),
)


def alembic_cfg() -> Config:
    cfg = Config(os.path.join(ROOT, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(ROOT, "alembic"))
    return cfg


async def exec_sql(url: str, *statements: str, autocommit: bool = False) -> list[Any]:
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


def timed(label: str, fn: Any) -> float:
    began = time.monotonic()
    fn()
    took = time.monotonic() - began
    print(f"  {label}: {took:.2f} giây")
    return took


def create_db() -> None:
    admin = make_url(BASE_URL).set(database="postgres").render_as_string(hide_password=False)
    asyncio.run(exec_sql(admin, f'DROP DATABASE IF EXISTS "{DB_NAME}"', f'CREATE DATABASE "{DB_NAME}"',
                         autocommit=True))  # fmt: skip


def drop_db() -> None:
    admin = make_url(BASE_URL).set(database="postgres").render_as_string(hide_password=False)
    asyncio.run(exec_sql(admin, f'DROP DATABASE IF EXISTS "{DB_NAME}" WITH (FORCE)', autocommit=True))


def seed_phase1(n: int) -> None:
    """Đơn / kiện / lịch sử ở 0002. Trạng thái theo `i % 100`: 70 % đã giao, 10 % đã bàn giao, 8 % đã đóng gói,
    10 % mới, 2 % hủy."""
    i = "right(o.platform_order_sn, 9)::int % 100"
    status = (
        f"CASE WHEN {i} < 70 THEN 'DELIVERED' WHEN {i} < 80 THEN 'HANDED_OVER' "
        f"WHEN {i} < 88 THEN 'PACKED' WHEN {i} < 98 THEN 'NEW' ELSE 'CANCELLED' END"
    )
    asyncio.run(exec_sql(
        PERF_URL,
        'INSERT INTO "order" (id, platform_order_sn, platform_status, source, created_at_platform, updated_at) '
        f"SELECT gen_random_uuid(), '2410P' || lpad(i::text, 9, '0'), 'COMPLETED', 'API', "
        f"now() - (i % 90) * interval '1 day', now() - (i % 90) * interval '1 day' FROM generate_series(1, {n}) i",
        "INSERT INTO package (id, order_id, tracking_number, warehouse_status, updated_at) "
        f"SELECT gen_random_uuid(), o.id, 'SPXP' || right(o.platform_order_sn, 9), {status}, o.updated_at "
        'FROM "order" o',
        "INSERT INTO status_history (id, package_id, source, from_status, to_status, at) "
        "SELECT gen_random_uuid(), p.id, 'PLATFORM', NULL, p.warehouse_status, p.updated_at FROM package p",
        'ANALYZE "order"', "ANALYZE package", "ANALYZE status_history",
    ))  # fmt: skip


class LockProbe(threading.Thread):
    """Đọc + ghi một dòng `package` mỗi 200 ms trên kết nối riêng; ghi lại độ trễ (bị khóa = trễ lớn)."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop = threading.Event()
        self.reads: list[float] = []
        self.writes: list[float] = []

    def run(self) -> None:
        asyncio.run(self._loop())

    async def _loop(self) -> None:
        engine = create_async_engine(PERF_URL, isolation_level="AUTOCOMMIT")
        async with engine.connect() as conn:
            pid = (await conn.execute(text("SELECT id FROM package LIMIT 1"))).scalar()
            while not self.stop.is_set():
                began = time.monotonic()
                await conn.execute(text("SELECT warehouse_status FROM package WHERE id = :id"), {"id": pid})
                self.reads.append(time.monotonic() - began)
                began = time.monotonic()
                await conn.execute(
                    text("UPDATE package SET updated_at = updated_at WHERE id = :id"), {"id": pid}
                )
                self.writes.append(time.monotonic() - began)
                await asyncio.sleep(0.2)
        await engine.dispose()


def migrate_with_probe() -> dict[str, float]:
    os.environ["DATABASE_URL"] = PERF_URL
    probe = LockProbe()
    probe.start()
    time.sleep(1)
    took_0003 = timed("alembic upgrade 0003 (1 transaction)", lambda: command.upgrade(alembic_cfg(), "0003"))
    took_0004 = timed("alembic upgrade head (0004 + 0005)", lambda: command.upgrade(alembic_cfg(), "head"))
    time.sleep(1)
    probe.stop.set()
    probe.join()
    out = {
        "0003_s": took_0003,
        "0004_s": took_0004,
        "max_read_s": max(probe.reads),
        "max_write_s": max(probe.writes),
        "reads_blocked_gt_1s": sum(r > 1 for r in probe.reads),
        "writes_blocked_gt_1s": sum(w > 1 for w in probe.writes),
    }
    print(f"  khóa: đọc chậm nhất {out['max_read_s']:.2f} giây, ghi chậm nhất {out['max_write_s']:.2f} giây")
    return out


def seed_phase2(returns: int) -> int:
    """`returns` kiện đã giao → đang về (hồ sơ `EXPECTED`, 0–14 ngày); đối soát xét từ 90 ngày trước."""
    rows = asyncio.run(exec_sql(
        PERF_URL,
        "UPDATE setting SET recon_start_at = now() - interval '90 days'",
        "CREATE TEMP TABLE pick AS SELECT p.id AS package_id, p.order_id, row_number() OVER () AS k "
        f"FROM package p WHERE p.warehouse_status = 'DELIVERED' LIMIT {returns}",
        "INSERT INTO return_case (id, order_id, kind, status, source, expected_since, reported_at, "
        "return_tracking_number, signal_keys) SELECT gen_random_uuid(), order_id, 'BUYER_RETURN', 'EXPECTED', "
        "'PLATFORM', now() - (k % 15) * interval '1 day', now() - (k % 15) * interval '1 day', "
        "'SPXRTP' || lpad(k::text, 9, '0'), ARRAY['RETURN:P' || k] FROM pick",
        "INSERT INTO return_case_package (return_case_id, package_id) "
        "SELECT rc.id, p.package_id FROM pick p JOIN return_case rc ON rc.order_id = p.order_id",
        "UPDATE package p SET warehouse_status = 'RETURN_EXPECTED', status_changed_at = rc.expected_since "
        "FROM return_case_package rcp JOIN return_case rc ON rc.id = rcp.return_case_id "
        "WHERE rcp.package_id = p.id",
        "ANALYZE package", "ANALYZE return_case", "ANALYZE return_case_package",
        "SELECT count(*) FROM package WHERE warehouse_status NOT IN "
        "('DELIVERED', 'CANCELLED', 'CANCELLED_AFTER_PACK', 'RETURN_RECEIVED_OK', 'RETURN_RECEIVED_ISSUE')",
    ))  # fmt: skip
    return int(rows[0][0])


async def run_recon() -> list[tuple[float, dict[str, Any]]]:
    from aicam.core.redis import close_redis, init_redis
    from aicam.core.settings import Settings
    from aicam.modules.reconciliation import service as recon

    settings = Settings(app_env="test", database_url=PERF_URL, redis_url=REDIS_URL, log_json=False)
    init_redis(REDIS_URL)
    engine = create_async_engine(PERF_URL)
    out = []
    try:
        for _ in range(2):
            async with AsyncSession(engine, expire_on_commit=False) as session:
                began = time.monotonic()
                result = await recon.run_rules(session, settings)
                out.append((time.monotonic() - began, result))
                print(f"  J-14 run_rules: {out[-1][0]:.2f} giây {result}")
    finally:
        await engine.dispose()
        await close_redis()
    return out


async def lookup_times(label: str) -> dict[str, float]:
    from aicam.core.settings import Settings
    from aicam.modules.sessions.return_lookup import _package_ids

    settings = Settings(app_env="test", database_url=PERF_URL, redis_url=REDIS_URL, log_json=False)
    engine = create_async_engine(PERF_URL)
    cases = {
        "mã vận đơn chính xác": "SPXP000512345",
        "tiền tố 10 ký tự": "SPXP000512",
        "mã đơn chính xác": "2410P000512345",
        "mã chiều về chính xác": "SPXRTP000001234",
    }
    out: dict[str, float] = {}
    try:
        async with AsyncSession(engine) as session:
            for name, code in cases.items():
                samples = []
                for _ in range(7):
                    began = time.monotonic()
                    await _package_ids(session, code, settings)
                    samples.append(time.monotonic() - began)
                out[name] = statistics.median(samples[1:])
                print(f"  API-104 {label} — {name}: {out[name] * 1000:.1f} ms (trung vị)")
    finally:
        await engine.dispose()
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--packages", type=int, default=1_000_000)
    parser.add_argument("--returns", type=int, default=20_000)
    parser.add_argument("--keep", action="store_true", help="không xóa DB tạm sau khi đo")
    args = parser.parse_args()
    print(f"DB tạm {DB_NAME}: {args.packages} kiện")
    create_db()
    try:
        os.environ["DATABASE_URL"] = PERF_URL
        command.upgrade(alembic_cfg(), "0002")
        timed("sinh dữ liệu Phase 1 ở 0002", lambda: seed_phase1(args.packages))
        migrate_with_probe()
        non_final = seed_phase2(args.returns)
        print(f"  kiện chưa ở trạng thái cuối: {non_final}")
        asyncio.run(run_recon())
        asyncio.run(
            exec_sql(PERF_URL, *(f"DROP INDEX IF EXISTS {name}" for name, _, _ in INDEXES))
        )  # bỏ 0005
        asyncio.run(lookup_times("không có text_pattern_ops"))
        for name, table, expr in INDEXES:
            sql = (f"DROP INDEX IF EXISTS {name}", f"CREATE INDEX {name} ON {table} ({expr})")
            timed(f"CREATE INDEX {name}", lambda sql=sql: asyncio.run(exec_sql(PERF_URL, *sql)))
        asyncio.run(exec_sql(PERF_URL, "ANALYZE package", 'ANALYZE "order"', "ANALYZE return_case"))
        asyncio.run(lookup_times("có text_pattern_ops"))
        size = asyncio.run(exec_sql(PERF_URL, "SELECT pg_size_pretty(pg_database_size(current_database()))"))
        print(f"  dung lượng DB tạm: {size[0][0]}")
    finally:
        if not args.keep:
            drop_db()
            print(f"Đã xóa DB tạm {DB_NAME}")


if __name__ == "__main__":
    main()
