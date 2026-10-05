"""TC-N.05 (NFR-04): API-30 tra cứu kiện trên 1.000.000 kiện — p95 ≤ 2 giây.

Chạy riêng (không chạy mặc định, ~1–2 phút):
`RUN_PERF=1 uv run pytest -m perf tests/integration/test_perf_packages.py -s`

Dữ liệu nạp bằng `generate_series` vào DB test của integration (`aicam_test`, KHÔNG phải DB dev `aicam`),
trong transaction của fixture `db` → rollback sau test; teardown `VACUUM FULL` các bảng đã nạp để trả lại
dung lượng đĩa.
"""

import math
import os
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aicam.core import clock

from .factories import PASSWORD, make_station_account, make_user

pytestmark = [
    pytest.mark.integration,
    pytest.mark.perf,
    pytest.mark.skipif(
        not os.environ.get("RUN_PERF"), reason="đặt RUN_PERF=1 để chạy test hiệu năng (TC-N.05)"
    ),
]

N = 1_000_000
DAYS = 365
RUNS = 20
P95_LIMIT_S = 2.0
NOW = datetime(2026, 10, 5, 5, 0, tzinfo=UTC)  # 12:00 giờ VN
TABLES = ('"order"', "package", "session")

# Thời điểm của kiện thứ g: rải đều 365 ngày trước NOW, mỗi ngày ~2.740 kiện.
_AT = (
    "(CAST(:now AS timestamptz) - (g % CAST(:days AS int)) * interval '1 day'"
    " - (g * 7 % 86400) * interval '1 second')"
)
_SERIES = "FROM generate_series(1, CAST(:n AS int)) AS g"
_LOAD = [
    f"""
    INSERT INTO "order" (id, platform_order_sn, platform_status, source, created_at_platform, updated_at)
    SELECT md5('o' || g)::uuid, 'PERF' || lpad(g::text, 8, '0'), 'READY_TO_SHIP', 'API', {_AT}, {_AT}
    {_SERIES}
    """,
    f"""
    INSERT INTO package (id, order_id, tracking_number, warehouse_status, verified, updated_at)
    SELECT md5('p' || g)::uuid, md5('o' || g)::uuid, 'SPXPERF' || lpad(g::text, 8, '0'),
           CASE WHEN g % 10 = 0 THEN 'PACKED' ELSE 'HANDED_OVER' END, true, {_AT}
    {_SERIES}
    """,
    f"""
    INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code,
                         close_code, flags, package_status_before)
    SELECT md5('s' || g)::uuid, 'PACK', md5('p' || g)::uuid, CAST(:station_id AS uuid), 'COMPLETED',
           {_AT} - interval '2 minutes', {_AT},
           'SPXPERF' || lpad(g::text, 8, '0'), 'SPXPERF' || lpad(g::text, 8, '0'),
           CASE WHEN g % 50 = 0 THEN ARRAY['HAD_MISMATCH'] ELSE ARRAY[]::text[] END, 'NEW'
    {_SERIES}
    """,
]


@pytest.fixture
async def vacuum_after(migrated_database_url: str) -> AsyncIterator[None]:
    """Teardown sau khi `db` rollback: VACUUM FULL để file bảng / index co về kích thước trước khi nạp."""
    yield
    eng = create_async_engine(make_url(migrated_database_url), isolation_level="AUTOCOMMIT")
    try:
        async with eng.connect() as conn:
            for table in TABLES:
                await conn.execute(text(f"VACUUM FULL ANALYZE {table}"))
            left = await conn.scalar(
                text("SELECT count(*) FROM package WHERE tracking_number LIKE 'SPXPERF%'")
            )
            assert left == 0
    finally:
        await eng.dispose()


def _p95(values: list[float]) -> float:
    return sorted(values)[math.ceil(0.95 * len(values)) - 1]


async def test_tc_n05_search_one_million_packages(
    vacuum_after: None, api: AsyncClient, db: AsyncSession
) -> None:
    """TC-N.05, NFR-04: 1.000.000 kiện (đơn + kiện + phiên COMPLETED rải 365 ngày) → API-30 theo mã vận đơn,
    theo mã đơn và theo ngày: p95 ≤ 2 giây, kết quả đúng."""
    clock.freeze(NOW)
    user = await make_user(db, "tst_cskh", "CSKH")
    _, station = await make_station_account(db)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    headers = {"Authorization": f"Bearer {res.json()['access_token']}"}

    started = time.perf_counter()
    for sql in _LOAD:
        await db.execute(text(sql), {"now": NOW, "days": DAYS, "n": N, "station_id": station.id})
    await db.execute(text('ANALYZE "order", package, session'))
    load_s = time.perf_counter() - started
    assert await db.scalar(text("SELECT count(*) FROM package")) == N

    day_date = (NOW - timedelta(days=10)).astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).date()
    day = day_date.isoformat()
    per_day = await db.scalar(
        text(
            "SELECT count(*) FROM session"
            " WHERE ended_at >= (CAST(:d AS date)::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')"
            " AND ended_at < ((CAST(:d AS date) + 1)::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')"
        ),
        {"d": day_date},
    )
    cases = {
        "ma_van_don": ({"q": "spxperf00654321"}, 1),
        "ma_don": ({"q": "PERF00123457"}, 1),
        "theo_ngay": ({"date_from": day, "date_to": day}, per_day),
    }
    report = [f"TC-N.05: nạp {N} kiện trong {load_s:.1f}s"]
    p95s: dict[str, float] = {}
    for name, (params, expected_total) in cases.items():
        durations: list[float] = []
        for _ in range(RUNS):
            t0 = time.perf_counter()
            r = await api.get("/api/v1/packages", headers=headers, params=params)
            durations.append(time.perf_counter() - t0)
            assert r.status_code == 200, r.text
            assert r.json()["total"] == expected_total, (name, r.json()["total"])
        p95 = p95s[name] = _p95(durations)
        report.append(
            f"  API-30 {name} {params}: total={expected_total} n={RUNS} "
            f"min={min(durations) * 1000:.0f}ms p95={p95 * 1000:.0f}ms max={max(durations) * 1000:.0f}ms"
        )
    print("\n".join(report))
    assert all(p95 <= P95_LIMIT_S for p95 in p95s.values()), p95s
