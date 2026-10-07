# ruff: noqa: E501, S108, S603, S608 — kịch bản diễn tập trên DB / container tạm: SQL ghép từ hằng, lệnh cố định
"""Diễn tập nâng cấp / lùi Phase 2 ⇄ Phase 3 trên **bản sao** DB Phase 2 (T-230, 02a §3, 02 §10, ops §7.2).

KHÔNG dùng stack dev. Cần: Postgres 16 tạm (server riêng), Redis tạm, worktree `main` (code Phase 2) đã `uv sync`:

    docker run -d --name aicam-m18-pg -p 55632:5432 -e POSTGRES_USER=aicam -e POSTGRES_PASSWORD=aicam postgres:16-alpine
    docker run -d --name aicam-m18-redis -p 56479:6379 redis:7-alpine
    git worktree add <tmp>/aicam-main origin/main && (cd <tmp>/aicam-main && uv sync)
    DRILL_PG=postgresql+asyncpg://aicam:aicam@localhost:55632 DRILL_REDIS=redis://localhost:56479/0 \\
    DRILL_P2=<tmp>/aicam-main uv run python scripts/upgrade_drill.py --out <tệp log>

Các bước theo đúng runbook ops §7.2: dựng DB Phase 2 bằng code Phase 2 (`seed-demo` + bằng chứng + kiện hủy oan do
code Phase 2) → `pg_dump` / `pg_restore` thành **bản sao** → dừng (kiểm không còn kết nối) → `alembic upgrade head`
(0006 / 0007) → `aicam fix-cancel-requests` chạy thử rồi `--apply` → kiểm → hoạt động Phase 3 (TikTok, shop Shopee
thứ 2, link, kênh, bỏ bằng chứng) → `downgrade 0005` (từ chối: link đang hoạt động; đơn ngoài; rồi cờ
`AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS=1`) → kiểm bằng **code Phase 2** (J-06 không gửi mã đơn TikTok, J-02 không
xóa thêm bằng chứng nào, đúng hạn) → nâng cấp lại → so khớp từng bảng. Mã thoát 0 = ĐẠT.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(sys.executable).parent
PG_TOOL = ROOT / "tests" / "integration" / "bin"
SRC_DB, COPY_DB = "aicam_p2_src", "aicam_p2_copy"
EVIDENCE_TABLES = ("clip", "snapshot", "session", "claim", "claim_evidence")
# Cột Phase 2 (0005) của bảng bằng chứng — so trước / sau mà không lệ thuộc cột Phase 3 thêm vào.
OUT: list[str] = []
FAILS: list[str] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(UTC):%H:%M:%SZ}] {msg}"
    OUT.append(line)
    print(line, flush=True)


def check(ok: bool, what: str) -> None:
    log(f"  {'ĐẠT' if ok else 'KHÔNG ĐẠT'} — {what}")
    if not ok:
        FAILS.append(what)


def url(db: str) -> str:
    return f"{os.environ['DRILL_PG']}/{db}"


async def q(db: str, sql: str, params: dict[str, Any] | None = None) -> list[Any]:
    engine = create_async_engine(url(db), isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            res = await conn.execute(text(sql), params or {})
            return list(res.all()) if res.returns_rows else []
    finally:
        await engine.dispose()


def run_q(db: str, sql: str, params: dict[str, Any] | None = None) -> list[Any]:
    return asyncio.run(q(db, sql, params))


def run_many(db: str, statements: list[str]) -> None:
    async def go() -> None:
        engine = create_async_engine(url(db))
        try:
            async with engine.begin() as conn:
                for sql in statements:
                    await conn.execute(text(sql))
        finally:
            await engine.dispose()

    asyncio.run(go())


def env(db: str, **extra: str) -> dict[str, str]:
    base = {k: v for k, v in os.environ.items() if not k.startswith("AICAM_DOWNGRADE_")}
    base.update(
        DATABASE_URL=url(db), REDIS_URL=os.environ["DRILL_REDIS"], APP_ENV="dev", LOG_JSON="false",
        SHOPEE_ENABLED="true", PLATFORM_ADAPTER="mock", VIDEO_ROOT=os.environ.get("DRILL_VIDEO", "/tmp/aicam-drill"),
    )  # fmt: skip
    base.update(extra)
    return base


def sh(
    cmd: list[str], *, db: str, cwd: Path = ROOT, show: str = "", **extra: str
) -> subprocess.CompletedProcess[str]:
    """Chạy lệnh, in lệnh + các dòng log đáng chú ý (`show` = regex chữ con, cách `|`)."""
    flags = " ".join(f"{k}={v}" for k, v in extra.items())
    log(
        f"$ {flags + ' ' if flags else ''}{' '.join(Path(c).name if i == 0 else c for i, c in enumerate(cmd))}"
    )
    started = time.monotonic()
    res = subprocess.run(
        cmd, cwd=cwd, env=env(db, **extra), capture_output=True, text=True, timeout=900, check=False
    )
    lines = (res.stdout + res.stderr).splitlines()
    keys = show.split("|") if show else []
    for line in lines:
        if not keys or any(k in line for k in keys):
            log(f"    {line.strip()[:400]}")
    log(f"  → mã thoát {res.returncode} ({time.monotonic() - started:.1f} giây)")
    return res


def p2(cmd: str, db: str, *args: str) -> dict[str, Any]:
    """Code **Phase 2** (`main`) — `scripts/drill_phase2.py` chạy bằng venv của worktree."""
    py = Path(os.environ["DRILL_P2"]) / ".venv" / "bin" / "python"
    res = subprocess.run(
        [str(py), str(ROOT / "scripts" / "drill_phase2.py"), cmd, *args], cwd=os.environ["DRILL_P2"],
        env=env(db), capture_output=True, text=True, timeout=600, check=False,
    )  # fmt: skip
    if res.returncode != 0:
        raise SystemExit(f"drill_phase2 {cmd} lỗi:\n{res.stderr[-3000:]}")
    return dict(json.loads(res.stdout.strip().splitlines()[-1]))


def p2_alembic(db: str, *args: str) -> subprocess.CompletedProcess[str]:
    return sh([str(Path(os.environ["DRILL_P2"]) / ".venv" / "bin" / "alembic"), *args], db=db,
              cwd=Path(os.environ["DRILL_P2"]), show="Running|0005|0007|Can't|head")  # fmt: skip


def p3_alembic(db: str, *args: str, show: str = "", **extra: str) -> subprocess.CompletedProcess[str]:
    return sh([str(BIN / "alembic"), *args], db=db, show=show, **extra)


def table_hashes(db: str) -> dict[str, str]:
    tables = [
        r[0] for r in run_q(db, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")
    ]
    out: dict[str, str] = {}
    for t in tables:
        out[t] = run_q(
            db,
            f"SELECT md5(coalesce(string_agg(to_jsonb(x)::text, '|' ORDER BY to_jsonb(x)::text), '')) "
            f'FROM "{t}" x',
        )[0][0]
    return out


PHASE2_COLS: dict[str, list[str]] = {}


def evidence(db: str) -> dict[str, list[str]]:
    """Dòng bằng chứng theo **cột Phase 2** (lấy danh sách cột ở DB Phase 2 gốc)."""
    out: dict[str, list[str]] = {}
    for t in EVIDENCE_TABLES:
        cols = ", ".join(f'"{c}"' for c in PHASE2_COLS[t])
        rows = run_q(db, f'SELECT to_jsonb(x)::text FROM (SELECT {cols} FROM "{t}") x ORDER BY 1')
        out[t] = [r[0] for r in rows]
    return out


def diff_tables(a: dict[str, str], b: dict[str, str]) -> list[str]:
    return sorted(t for t in set(a) | set(b) if a.get(t) != b.get(t))


# ------------------------------------------------------------------------------------------------ bước


def step0_build_phase2_copy(tmp: Path) -> None:
    log("== Bước 0 — DB Phase 2 dựng bằng CODE PHASE 2 (main) rồi tạo BẢN SAO bằng pg_dump / pg_restore ==")
    for db in (SRC_DB, COPY_DB):
        run_q("postgres", f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
        run_q("postgres", f'CREATE DATABASE "{db}"')
    import redis

    redis.Redis.from_url(os.environ["DRILL_REDIS"]).flushdb()
    p2_alembic(SRC_DB, "upgrade", "head")
    seeded = p2("seed", SRC_DB)
    log(f"  seed Phase 2: {seeded}")
    dump = tmp / "phase2.dump"
    pg = os.environ["DRILL_PG"].split("://", 1)[1]  # aicam:aicam@localhost:55632
    user_pw, hostport = pg.split("@")
    user, pw = user_pw.split(":")
    host, port = hostport.split(":")
    pgenv = {"PGHOST": host, "PGPORT": port, "PGUSER": user, "PGPASSWORD": pw}
    with dump.open("wb") as f:
        r = subprocess.run([str(PG_TOOL / "docker-pg_dump"), "-Fc", "-d", SRC_DB], stdout=f, env={**os.environ,
                           **pgenv}, check=False)  # fmt: skip
    log(f"$ pg_dump -Fc -d {SRC_DB} > phase2.dump → mã {r.returncode}, {dump.stat().st_size:,} byte")
    with dump.open("rb") as f:
        r2 = subprocess.run([str(PG_TOOL / "docker-pg_restore"), "--no-owner", "-d", COPY_DB], stdin=f,
                            env={**os.environ, **pgenv}, capture_output=True, check=False)  # fmt: skip
    log(f"$ pg_restore --no-owner -d {COPY_DB} < phase2.dump → mã {r2.returncode}")
    check(r.returncode == 0 and r2.returncode == 0, "bản sao DB Phase 2 tạo được")
    check(table_hashes(SRC_DB) == table_hashes(COPY_DB), "bản sao khớp DB gốc từng bảng (md5 to_jsonb)")
    for t in EVIDENCE_TABLES:
        PHASE2_COLS[t] = [
            r[0]
            for r in run_q(
                COPY_DB,
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' "
                "AND table_name = :t ORDER BY ordinal_position",
                {"t": t},
            )
        ]
    counts = {
        t: run_q(COPY_DB, f'SELECT count(*) FROM "{t}"')[0][0]
        for t in (
            "shop",
            "order",
            "package",
            "session",
            "clip",
            "snapshot",
            "return_case",
            "claim",
            "claim_evidence",
        )
    }
    log(f"  dữ liệu bản sao: {counts}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument(
        "--tmp", type=Path, default=Path(os.environ.get("TMPDIR", "/tmp")) / "aicam-upgrade-drill"
    )
    a = ap.parse_args()
    a.tmp.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    log("# T-230 diễn tập nâng cấp / lùi Phase 2 ⇄ Phase 3 — Postgres tạm "
        f"{os.environ['DRILL_PG'].rsplit('@', 1)[-1]}, Redis tạm {os.environ['DRILL_REDIS'].rsplit('@', 1)[-1]}, "
        f"code Phase 2 = worktree main {os.environ['DRILL_P2']}. KHÔNG dùng stack dev aicam-dev.")  # fmt: skip
    step0_build_phase2_copy(a.tmp)

    log("== Bước 1 — mốc trước nâng cấp (bản sao, schema 0005) ==")
    e0 = evidence(COPY_DB)
    j02_p2_before = p2("j02", COPY_DB, "--days", "5")
    j06_p2_before = p2("j06", COPY_DB)
    log(f"  bằng chứng: {{{', '.join(f'{t}: {len(v)}' for t, v in e0.items())}}}")
    log(f"  J-02 Phase 2 (giữ 5 ngày) ứng viên xóa: {len(j02_p2_before['clips'])} clip, "
        f"{len(j02_p2_before['snapshots'])} ảnh")  # fmt: skip
    log(
        f"  J-06 Phase 2 gửi sàn {len(j06_p2_before['calls'])} kiện: {sorted(c[0] for c in j06_p2_before['calls'])}"
    )
    cancelled = run_q(COPY_DB, "SELECT tracking_number, warehouse_status FROM package WHERE tracking_number IN "
                      "('SPXTST0000019', 'SPXTST0000020', 'SPXTST0000021') ORDER BY 1")  # fmt: skip
    log(f"  kiện hủy do code Phase 2 (IN_CANCEL x2, CANCELLED x1): {cancelled}")

    log("== Bước 2 — ops §7.2 bước 3: dừng api vision worker worker-sync worker-sync-long worker-export "
        "worker-backup worker-notify beat ==")  # fmt: skip
    others = run_q(COPY_DB, "SELECT count(*) FROM pg_stat_activity WHERE datname = :d AND pid <> pg_backend_pid()",
                   {"d": COPY_DB})[0][0]  # fmt: skip
    check(others == 0, f"không còn kết nối khác vào DB (đếm {others})")

    log("== Bước 3 — nâng cấp bằng code Phase 3: alembic upgrade head ==")
    p3_alembic(COPY_DB, "current", show="0005|0007")
    t0 = time.monotonic()
    up = p3_alembic(
        COPY_DB, "upgrade", "head", show="Running upgrade|0006:|0007:|backfill|WARNING|Error|error"
    )
    log(f"  upgrade 0005 → 0007: {time.monotonic() - t0:.1f} giây")
    check(up.returncode == 0, "alembic upgrade head mã 0")
    cur = run_q(COPY_DB, "SELECT version_num FROM alembic_version")
    check(cur == [("0007",)], f"alembic_version = 0007 ({cur})")
    chk = p3_alembic(COPY_DB, "check", show="No new|FAILED|Detected")
    check(chk.returncode == 0, "alembic check: model khớp schema")
    run_q(COPY_DB, 'VACUUM ANALYZE "order"')
    run_q(COPY_DB, "VACUUM ANALYZE return_case")
    log('$ VACUUM ANALYZE "order"; VACUUM ANALYZE return_case')

    log("== Bước 4 — ops §7.2 bước 1b: aicam fix-cancel-requests (chạy thử → --apply → chạy lại) ==")
    before_dry = table_hashes(COPY_DB)
    dry = sh([str(BIN / "aicam"), "fix-cancel-requests"], db=COPY_DB)
    check(dry.returncode == 0 and dry.stdout.count("SẼ TRẢ LẠI") == 2, "chạy thử liệt kê đúng 2 kiện hủy oan")
    check(table_hashes(COPY_DB) == before_dry, "chạy thử không ghi gì (md5 từng bảng không đổi)")
    app = sh([str(BIN / "aicam"), "fix-cancel-requests", "--apply"], db=COPY_DB)
    check(app.returncode == 0 and app.stdout.count("ĐÃ TRẢ LẠI") == 2, "--apply trả lại 2 kiện, mã 0")
    again = sh([str(BIN / "aicam"), "fix-cancel-requests"], db=COPY_DB)
    check("SẼ TRẢ LẠI" not in again.stdout, "chạy lại: không còn kiện nào (idempotent)")
    after = dict(run_q(COPY_DB, "SELECT tracking_number, warehouse_status FROM package WHERE tracking_number IN "
                       "('SPXTST0000019', 'SPXTST0000020', 'SPXTST0000021')"))  # fmt: skip
    check(after == {"SPXTST0000019": "NEW", "SPXTST0000020": "PACKED", "SPXTST0000021": "CANCELLED"},
          f"kiện: IN_CANCEL → trả lại, CANCELLED thật giữ nguyên ({after})")  # fmt: skip
    n_audit = run_q(COPY_DB, "SELECT count(*) FROM audit_log WHERE action = 'PACKAGE_CANCEL_REVERT'")[0][0]
    check(n_audit == 2, f"audit PACKAGE_CANCEL_REVERT = 2 ({n_audit})")

    log("== Bước 5 — kiểm bằng chứng sau nâng cấp (cột Phase 2) ==")
    e1 = evidence(COPY_DB)
    for t in ("clip", "snapshot", "session"):
        check(e1[t] == e0[t], f"{t}: {len(e1[t])} dòng y hệt trước nâng cấp")

    def _claims(rows: list[str]) -> dict[str, dict[str, Any]]:
        return {d["code"]: d for d in map(json.loads, rows)}

    c0, c1 = _claims(e0["claim"]), _claims(e1["claim"])
    bumped = sorted(k for k in c0 if c1[k]["version"] != c0[k]["version"])
    same = all({**c1[k], "version": 0} == {**c0[k], "version": 0} for k in c0) and c0.keys() == c1.keys()
    touched = sorted(r[0] for r in run_q(COPY_DB, "SELECT DISTINCT c.code FROM claim c JOIN claim_evidence ce "
                                            "ON ce.claim_id = c.id WHERE ce.backfilled"))  # fmt: skip
    check(same and bumped == touched, f"claim: {len(c1)} hồ sơ y hệt, trừ `version` + 1 đúng ở hồ sơ được 4b thêm "
          f"bằng chứng ({bumped}) — khóa lạc quan cho người đang mở hồ sơ")  # fmt: skip
    extra = sorted(set(e1["claim_evidence"]) - set(e0["claim_evidence"]))
    check(set(e0["claim_evidence"]) <= set(e1["claim_evidence"]), "claim_evidence: mọi dòng cũ còn nguyên")
    backfilled = run_q(COPY_DB, "SELECT s.open_code, s.status, s.cancel_reason FROM claim_evidence ce JOIN session s "
                       "ON s.id = ce.session_id WHERE ce.backfilled ORDER BY 1")  # fmt: skip
    log(f"  claim_evidence thêm {len(extra)} dòng do 4b (BR-39): {backfilled}")
    check(sorted(r[0] for r in backfilled) == ["DRILL-R2", "DRILL-R3"],
          "4b thêm phiên bỏ dở + phiên Supervisor hủy, KHÔNG thêm phiên hủy quét nhầm")  # fmt: skip

    log("== Bước 6 — vận hành Phase 3 (dữ liệu chỉ Phase 3 có) ==")
    admin = run_q(COPY_DB, "SELECT id FROM \"user\" WHERE role = 'ADMIN' ORDER BY username LIMIT 1")[0][0]
    removed = run_q(COPY_DB, "SELECT ce.id, ce.session_id FROM claim_evidence ce JOIN claim c ON c.id = ce.claim_id "
                    "WHERE c.status = 'SUBMITTED' AND ce.kind = 'SESSION' LIMIT 1")[0]  # fmt: skip
    share_pkg = run_q(
        COPY_DB, "SELECT package_id, id FROM session WHERE type = 'PACK' ORDER BY ended_at DESC LIMIT 1"
    )[0]
    run_many(COPY_DB, [
        "INSERT INTO shop (id, platform, platform_shop_id, name, auth_status, grant_ref, created_at) VALUES "
        "('01960000-0000-7000-8000-00000000c001', 'SHOPEE', '880009', 'Áo Mới (Phase 3)', 'CONNECTED', '880009', now()),"
        " ('01960000-0000-7000-8000-00000000f001', 'TIKTOK', '7501', 'Áo Đẹp TikTok', 'CONNECTED', 'open-1', now())",
        'INSERT INTO "order" (id, shop_id, platform_order_sn, platform_status, platform_status_group, source) VALUES '
        "('01960000-0000-7000-8000-00000000f101', '01960000-0000-7000-8000-00000000f001', '576100000000001', "
        "'AWAITING_SHIPMENT', 'AWAITING_SHIPMENT', 'API'), "
        "('01960000-0000-7000-8000-00000000f102', '01960000-0000-7000-8000-00000000f001', '576100000000002', "
        "'IN_TRANSIT', 'SHIPPED', 'API'), "
        "('01960000-0000-7000-8000-00000000c101', '01960000-0000-7000-8000-00000000c001', '2510C0000001', "
        "'READY_TO_SHIP', 'AWAITING_SHIPMENT', 'API')",
        "INSERT INTO package (id, order_id, tracking_number, warehouse_status) VALUES "
        "('01960000-0000-7000-8000-00000000f201', '01960000-0000-7000-8000-00000000f101', 'TTVN00000001', 'PACKED'), "
        "('01960000-0000-7000-8000-00000000f202', '01960000-0000-7000-8000-00000000f102', 'TTVN00000002', "
        "'HANDED_OVER'), "
        "('01960000-0000-7000-8000-00000000c201', '01960000-0000-7000-8000-00000000c101', 'SPXC000000001', 'PACKED')",
        "INSERT INTO share_link (id, status, source_type, package_id, layout, recipient, expires_at, object_prefix, "
        f"created_by) VALUES ('01960000-0000-7000-8000-00000000a001', 'ACTIVE', 'SESSION', '{share_pkg[0]}', 'CAM1', "
        f"'Shipper GHN', now() + interval '3 days', 'share/drill-token/', '{admin}')",
        "INSERT INTO share_item (share_id, ord, session_id) VALUES "
        f"('01960000-0000-7000-8000-00000000a001', 1, '{share_pkg[1]}')",
        "INSERT INTO notify_channel (id, name, type, target, events, created_by) VALUES "
        f"('01960000-0000-7000-8000-00000000b001', 'Nhóm kho', 'TELEGRAM', '-1001', '{{N01,N08}}', '{admin}')",
        # Như API-134 bỏ bằng chứng (BR-38 bỏ mềm): phiên PACK 20 ngày của hồ sơ đang gửi sàn.
        f"UPDATE claim_evidence SET removed_at = now(), removed_by = '{admin}', "
        f"removed_reason = 'Không liên quan (diễn tập)' WHERE id = '{removed[0]}'",
    ])  # fmt: skip
    log("  + shop Shopee thứ 2 (mới hơn) + shop TikTok; đơn TikTok x2 (kiện PACKED, HANDED_OVER), đơn shop Shopee mới "
        "(kiện PACKED); link ACTIVE; kênh Telegram; bỏ 1 bằng chứng phiên (BR-38)")  # fmt: skip
    by_shop = run_q(COPY_DB, "SELECT coalesce(sh.platform || ' ' || sh.name, '(đơn file / không shop)'), count(*) "
                    'FROM package p JOIN "order" o ON o.id = p.order_id LEFT JOIN shop sh ON sh.id = o.shop_id '
                    "GROUP BY 1 ORDER BY 1")  # fmt: skip
    log(f"  kiện có đơn theo shop: {by_shop}")
    p3_snapshot = table_hashes(COPY_DB)

    log("== Bước 7 — lùi về Phase 2 bằng code Phase 3 (alembic downgrade 0005) ==")
    d1 = p3_alembic(COPY_DB, "downgrade", "0005", show="RuntimeError|link|Link|từ chối|kiện")
    check(d1.returncode != 0 and table_hashes(COPY_DB) == p3_snapshot,
          "còn link đang hoạt động → TỪ CHỐI, DB không đổi")  # fmt: skip
    run_q(COPY_DB, "UPDATE share_link SET status = 'REVOKED', revoked_at = now(), revoked_by = "
          "(SELECT id FROM \"user\" WHERE role = 'ADMIN' ORDER BY username LIMIT 1) WHERE status = 'ACTIVE'")  # fmt: skip
    log("  thu hồi link (runbook: thu hồi trước khi lùi)")
    p3_snapshot = table_hashes(COPY_DB)
    e2 = evidence(COPY_DB)
    d2 = p3_alembic(COPY_DB, "downgrade", "0005", show="RuntimeError|đơn|kiện|DETACH")
    check(d2.returncode != 0 and table_hashes(COPY_DB) == p3_snapshot,
          "còn kiện của đơn ngoài (TikTok + shop Shopee bị ngắt) → TỪ CHỐI, DB không đổi")  # fmt: skip
    check(
        run_q(COPY_DB, "SELECT version_num FROM alembic_version") == [("0007",)],
        "vẫn ở 0007 sau 2 lần từ chối",
    )
    t0 = time.monotonic()
    d3 = p3_alembic(COPY_DB, "downgrade", "0005", show="downgrade|archive|0006|0007|B|LEGACY|detach",
                    AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS="1")  # fmt: skip
    log(f"  downgrade 0007 → 0005: {time.monotonic() - t0:.1f} giây")
    check(d3.returncode == 0, "AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS=1 → lùi được, mã 0")
    check(run_q(COPY_DB, "SELECT version_num FROM alembic_version") == [("0005",)], "alembic_version = 0005")

    log("== Bước 8 — kiểm bằng CODE PHASE 2 trên DB đã lùi ==")
    p2c = p2_alembic(COPY_DB, "current")
    check(
        p2c.returncode == 0 and "0005" in p2c.stdout + p2c.stderr,
        "image Phase 2 nhận DB (alembic current = 0005)",
    )
    j06 = p2("j06", COPY_DB)
    sns = sorted(c[0] for c in j06["calls"])
    log(f"  J-06 Phase 2 gửi sàn {len(sns)} kiện: {sns}")
    check(not any(s.startswith("5761") for s in sns), "J-06 Phase 2 KHÔNG gửi mã đơn TikTok (DEC-509)")
    check(
        set(sns) <= {"2510C0000001"},
        "J-06 Phase 2 chỉ gửi đơn của shop Shopee còn kết nối (kiện shop cũ đã tách)",
    )
    j02 = p2("j02", COPY_DB, "--days", "5")
    check(j02 == j02_p2_before, f"J-02 Phase 2 (giữ 5 ngày) ứng viên xóa y hệt trước nâng cấp "
          f"({len(j02['clips'])} clip, {len(j02['snapshots'])} ảnh) — bằng chứng đã bỏ vẫn được giữ")  # fmt: skip
    removed_clips = {
        r[0] for r in run_q(COPY_DB, "SELECT id::text FROM clip WHERE session_id = :s", {"s": removed[1]})
    }
    later = p2("j02", COPY_DB, "--days", "5", "--now", (datetime.now(UTC) + timedelta(days=6)).isoformat())
    check(
        removed_clips <= set(later["clips"]),
        "sau hạn giữ (bỏ lúc lùi + 5 ngày) J-02 Phase 2 mới xóa được clip đã bỏ",
    )
    legacy = run_q(COPY_DB, "SELECT status, close_reason FROM claim WHERE source = 'LEGACY_HOLD'")
    log(f"  hồ sơ hệ thống giữ bằng chứng đã bỏ: {legacy}")
    check(len(legacy) == 1 and legacy[0][0] == "CLOSED", "1 hồ sơ LEGACY_HOLD CLOSED")
    e3 = evidence(COPY_DB)
    for t in ("clip", "snapshot", "session"):
        check(e3[t] == e2[t], f"{t}: {len(e3[t])} dòng y hệt trước khi lùi")

    log("== Bước 9 — nâng cấp lại (alembic upgrade head) và so khớp từng bảng ==")
    t0 = time.monotonic()
    up2 = p3_alembic(COPY_DB, "upgrade", "head", show="Running upgrade|khôi phục|restore|0006:|WARNING|Error")
    log(f"  upgrade lại 0005 → 0007: {time.monotonic() - t0:.1f} giây")
    check(up2.returncode == 0, "nâng cấp lại mã 0")
    check(run_q(COPY_DB, "SELECT to_regnamespace('phase3_archive') IS NULL")[0][0], "phase3_archive đã drop")
    final = table_hashes(COPY_DB)
    diff = diff_tables(p3_snapshot, final)
    check(
        not diff,
        f"mọi bảng ({len(final)}) y hệt trước khi lùi (md5 to_jsonb từng bảng){' — lệch: ' + str(diff) if diff else ''}",
    )
    e4 = evidence(COPY_DB)
    check(e4 == e2, "bằng chứng (clip, ảnh, phiên, hồ sơ, claim_evidence) y hệt trước khi lùi")
    chk2 = p3_alembic(COPY_DB, "check", show="No new|FAILED|Detected")
    check(chk2.returncode == 0, "alembic check sau nâng cấp lại")

    total = time.monotonic() - started
    log(f"== KẾT QUẢ: {'ĐẠT' if not FAILS else 'KHÔNG ĐẠT — ' + '; '.join(FAILS)} (tổng {total:.0f} giây) ==")
    a.out.parent.mkdir(parents=True, exist_ok=True)
    with a.out.open("a", encoding="utf-8") as f:
        f.write("\n".join(OUT) + "\n\n")
    return 0 if not FAILS else 1


if __name__ == "__main__":
    raise SystemExit(main())
