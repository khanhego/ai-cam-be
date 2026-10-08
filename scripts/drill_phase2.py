# ruff: noqa: E501, S101 — kịch bản diễn tập trên DB / container tạm: SQL ghép từ hằng, lệnh cố định
"""Phần **Phase 2** của diễn tập nâng cấp / lùi (T-230, ops §7.2) — chạy bằng **code Phase 2** (`main`, qua
`git worktree` + venv riêng), KHÔNG bằng code của nhánh này:

    <worktree main>/.venv/bin/python scripts/drill_phase2.py seed|j06|j02 [--now ISO] [--days N]

Biến môi trường: `DATABASE_URL`, `REDIS_URL` (container tạm — không phải stack dev), `VIDEO_ROOT`,
`APP_ENV=dev`, `SHOPEE_ENABLED=true`, `PLATFORM_ADAPTER=mock`. In một dòng JSON kết quả ra stdout.

- `seed`: `aicam seed-demo` Phase 2 (MediaMTX thay bằng bản giả — không chạm MediaMTX dev) + shop Shopee `A`
  `CONNECTED` (đơn API gắn shop) + kiện hủy oan do **code Phase 2** (`upsert_platform_order` với `IN_CANCEL` →
  `NEW` → `CANCELLED`, `PACKED` → `CANCELLED_AFTER_PACK`) + 1 đơn hủy thật (`CANCELLED` — không được trả lại).
- `j06`: J-06 Phase 2 (`sync_shipping_status`) với adapter mock ghi lại mọi (mã đơn, mã vận đơn) gửi lên sàn.
- `j02`: ứng viên J-02 Phase 2 (clip + ảnh) tại `--now` với số ngày giữ `--days` — chỉ đọc, không xóa.
"""

import argparse
import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any


class _FakeMediaMTX:
    def __init__(self, *_: Any, **__: Any) -> None:
        pass

    async def upsert_path(self, name: str, source: str) -> None:
        return None

    async def delete_path(self, name: str) -> None:
        return None

    async def list_paths(self) -> dict[str, Any]:
        return {}


async def seed() -> dict[str, Any]:
    from sqlalchemy import select, text

    import aicam.modules.stations.mediamtx as mtx
    from aicam.core.db import dispose_engine, init_engine, sessionmaker
    from aicam.core.redis import close_redis, init_redis
    from aicam.core.settings import get_settings
    from aicam.entrypoints import cli
    from aicam.modules.orders import service as orders
    from aicam.modules.orders.models import Package
    from aicam.modules.platforms.base import PlatformItem, PlatformOrder

    mtx.HttpMediaMTX = _FakeMediaMTX  # type: ignore[misc,assignment]
    lines = await cli.seed_demo()
    settings = get_settings()
    init_engine(settings.database_url)
    init_redis(settings.redis_url)
    out: dict[str, Any] = {"seed_lines": len(lines)}
    try:
        async with sessionmaker()() as s:
            shop_a = await s.scalar(text("SELECT id FROM shop WHERE platform_shop_id = '880001'"))
            if shop_a is None:
                shop_a = uuid.uuid4()
                await s.execute(
                    text(
                        "INSERT INTO shop (id, platform, platform_shop_id, name, auth_status, created_at) "
                        "VALUES (:id, 'SHOPEE', '880001', 'Áo Đẹp (Phase 2)', 'CONNECTED', now() - interval '90 days')"
                    ),
                    {"id": shop_a},
                )
            await s.execute(
                text("UPDATE \"order\" SET shop_id = :id WHERE source = 'API' AND shop_id IS NULL"),
                {"id": shop_a},
            )
            # Hủy oan (BR-21 v0.4 / DEC-519): Phase 2 coi IN_CANCEL là hủy.
            pkg = await orders.find_package(s, "SPXTST0000020")
            assert pkg is not None
            if pkg.warehouse_status == "NEW":
                await orders.transition(s, pkg, "PACKING", source="WAREHOUSE", actor_label="TST Station 02")
                await orders.transition(s, pkg, "PACKED", source="WAREHOUSE", actor_label="TST Station 02")
            for n, status in ((19, "IN_CANCEL"), (20, "IN_CANCEL"), (21, "CANCELLED")):
                await orders.upsert_platform_order(
                    s,
                    PlatformOrder(
                        platform_order_sn=_sn_of(n),
                        status=status,
                        tracking_numbers=(f"SPXTST{n:07d}",),
                        items=(PlatformItem(product_name=f"Áo mẫu {n}", quantity=1, sku=f"SKU-{n}"),),
                    ),
                    shop_id=shop_a,
                )
            await s.commit()
            for sql in EVIDENCE_SQL:
                await s.execute(text(sql))
            await s.commit()
            rows = (
                await s.execute(
                    select(Package.tracking_number, Package.warehouse_status).where(
                        Package.tracking_number.in_(("SPXTST0000019", "SPXTST0000020", "SPXTST0000021"))
                    )
                )
            ).all()
            out["cancelled_by_phase2"] = {r[0]: r[1] for r in rows}
            out["shop_a"] = str(shop_a)
    finally:
        await close_redis()
        await dispose_engine()
    return out


# Bằng chứng Phase 2 (schema 0005) cho diễn tập: clip / ảnh như J-01 / chụp tay đã ghi (không có tệp — diễn tập
# chỉ kiểm DB + luật bảo vệ), phiên mở hoàn đủ loại, hồ sơ mở / đã đóng có bằng chứng.
EVIDENCE_SQL = (
    # Clip Cam 1 + Cam 2 cho mọi phiên PACK; phiên thứ n kết thúc n × 10 ngày trước (10..90 ngày).
    """WITH s AS (SELECT id, row_number() OVER (ORDER BY id) rn FROM session WHERE type = 'PACK')
    INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, path, sha256, size_bytes, duration_s)
    SELECT gen_random_uuid(), s.id, r, 'READY', now() - make_interval(days => (s.rn * 10)::int) - interval '3 minutes',
           now() - make_interval(days => (s.rn * 10)::int), 'clips/drill/' || s.id || '-' || r || '.mp4',
           md5(s.id::text || r) || md5(r || s.id::text), 1000000 + s.rn, 180
    FROM s, (VALUES ('CAM1'), ('CAM2')) v(r)""",
    # Phiên mở hoàn trên kiện của KN-000001: hoàn tất (2 clip, 2 ảnh), bỏ dở, Supervisor hủy, hủy quét nhầm.
    """WITH k AS (SELECT c.package_id, (SELECT id FROM station ORDER BY name LIMIT 1) st FROM claim c
                  WHERE c.code = 'KN-000001'),
    ins AS (
      INSERT INTO session (id, type, package_id, station_id, status, started_at, ended_at, open_code, cancel_reason,
                           inspection_conclusion, operator_name)
      SELECT gen_random_uuid(), 'RETURN', k.package_id, k.st, v.status, now() - make_interval(days => v.d),
             now() - make_interval(days => v.d) + interval '4 minutes', 'DRILL-R' || v.n, v.reason, v.concl, 'Lan'
      FROM k, (VALUES (1, 'COMPLETED', NULL, 'DAMAGED', 5), (2, 'ABANDONED', NULL, NULL, 4),
                      (3, 'CANCELLED', 'SUPERVISOR', NULL, 3), (4, 'CANCELLED', 'WRONG_SCAN', NULL, 2))
           v(n, status, reason, concl, d)
      RETURNING id, open_code, started_at)
    INSERT INTO clip (id, session_id, camera_role, status, start_at, end_at, path, sha256, size_bytes, duration_s)
    SELECT gen_random_uuid(), ins.id, r, 'READY', ins.started_at, ins.started_at + interval '4 minutes',
           'clips/drill/' || ins.id || '-' || r || '.mp4', md5(ins.id::text || r) || md5(r), 2000000, 240
    FROM ins, (VALUES ('CAM1'), ('CAM2')) v(r)
    WHERE r = 'CAM1' OR ins.open_code = 'DRILL-R1'""",
    """INSERT INTO snapshot (id, session_id, kind, camera_role, taken_at, path, sha256, size_bytes, status)
    SELECT gen_random_uuid(), s.id, 'MANUAL', 'CAM1', s.started_at + make_interval(secs => n * 30),
           'snapshots/drill/' || s.id || '-' || n || '.jpg', md5(s.id::text || n), 50000, 'READY'
    FROM session s, generate_series(1, 2) n WHERE s.open_code = 'DRILL-R1'""",
    # KN-000001 (mở): thêm phiên hoàn tất + 1 ảnh làm bằng chứng.
    """INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_at)
    SELECT gen_random_uuid(), c.id, 'SESSION', s.id, false, now() - interval '1 day'
    FROM claim c, session s WHERE c.code = 'KN-000001' AND s.open_code = 'DRILL-R1'""",
    """INSERT INTO claim_evidence (id, claim_id, kind, snapshot_id, auto, added_at)
    SELECT gen_random_uuid(), c.id, 'SNAPSHOT', (SELECT p.id FROM snapshot p JOIN session s ON s.id = p.session_id
                                                 WHERE s.open_code = 'DRILL-R1' ORDER BY p.taken_at LIMIT 1),
           false, now() - interval '1 day' FROM claim c WHERE c.code = 'KN-000001'""",
    # Hồ sơ đã đóng 10 ngày trước (phiên PACK 80 ngày) + hồ sơ đang gửi sàn (phiên PACK 20 ngày): Phase 2 giữ clip.
    """WITH s AS (SELECT id, package_id, row_number() OVER (ORDER BY id) rn FROM session WHERE type = 'PACK'),
    c AS (
      INSERT INTO claim (id, package_id, type, counterparty, status, source, closed_at, close_reason, updated_at)
      SELECT gen_random_uuid(), s.package_id, 'DAMAGED', 'PLATFORM', v.status, 'MANUAL', v.closed, v.reason, now()
      FROM s JOIN (VALUES (8, 'CLOSED', now() - interval '10 days', 'Sàn đã bồi thường'),
                          (2, 'SUBMITTED', NULL::timestamptz, NULL)) v(rn, status, closed, reason) ON v.rn = s.rn
      RETURNING id, package_id)
    INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_at)
    SELECT gen_random_uuid(), c.id, 'SESSION', s.id, false, now() - interval '20 days'
    FROM c JOIN s ON s.package_id = c.package_id""",
)


def _sn_of(n: int) -> str:
    """Mã đơn mock Phase 2 của kiện SPXTST{n} (đọc từ fixture mock — không đoán định dạng)."""
    from aicam.modules.platforms.mock.adapter import MockAdapter

    code = f"SPXTST{n:07d}"
    for order in MockAdapter().orders.values():
        if code in order.tracking_numbers:
            return order.platform_order_sn
    raise SystemExit(f"không thấy đơn mock của {code}")


async def j06() -> dict[str, Any]:
    from aicam.core.db import dispose_engine, init_engine, sessionmaker
    from aicam.core.redis import close_redis, init_redis
    from aicam.core.settings import get_settings
    from aicam.modules.platforms import sync
    from aicam.modules.platforms.mock.adapter import MockAdapter

    calls: list[tuple[str, str]] = []

    class Recording(MockAdapter):
        async def get_shipping_statuses(self, creds: Any, refs: Any) -> list[Any]:
            calls.extend((r.platform_order_sn, r.tracking_number) for r in refs)
            return []

    settings = get_settings()
    init_engine(settings.database_url)
    init_redis(settings.redis_url)
    try:
        async with sessionmaker()() as s:
            res = await sync.sync_shipping_status(s, Recording(), settings)
    finally:
        await close_redis()
        await dispose_engine()
    return {"result": res, "calls": calls}


async def j02(now: datetime, days: int) -> dict[str, Any]:
    from aicam.core.db import dispose_engine, init_engine, sessionmaker
    from aicam.core.settings import get_settings
    from aicam.modules.media import service as media

    settings = get_settings()
    init_engine(settings.database_url)
    try:
        async with sessionmaker()() as s:
            cutoff = now - timedelta(days=days)
            clips = await media.retention_clip_candidates(s, cutoff, now)
            snaps = await media.retention_snapshot_candidates(s, cutoff, now)
    finally:
        await dispose_engine()
    return {"clips": sorted(map(str, clips)), "snapshots": sorted(map(str, snaps))}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=("seed", "j06", "j02"))
    p.add_argument("--now", default=None)
    p.add_argument("--days", type=int, default=60)
    a = p.parse_args()
    if a.command == "seed":
        out = asyncio.run(seed())
    elif a.command == "j06":
        out = asyncio.run(j06())
    else:
        now = datetime.fromisoformat(a.now) if a.now else datetime.now(UTC)
        out = asyncio.run(j02(now, a.days))
    sys.stdout.write(json.dumps(out, default=str, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
