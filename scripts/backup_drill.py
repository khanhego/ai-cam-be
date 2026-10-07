"""Diễn tập khôi phục AC-50 / NFR-40 trên DB tạm + kho S3 tạm (MinIO) — KHÔNG dùng stack dev.

    DRILL_PG=postgresql+asyncpg://aicam:aicam@localhost:55532  (server Postgres tạm, tạo DB drill_src /
    drill_dst)
    TEST_S3_ENDPOINT=… TEST_S3_ACCESS_KEY=… TEST_S3_SECRET_KEY=… TEST_S3_BUCKET=…
    uv run python scripts/backup_drill.py --out drill.txt [--packages 2000] [--phase db|full]

`db`  (T-223): dữ liệu mẫu → J-20 → khôi phục DB vào DB trống → đếm dòng khớp, khóa sai → mã 2, DB không trống
      → mã 2, `backup-verify`.
`full` (T-274 / T-284): + bằng chứng J-21 / J-22, đổi khóa giữa chừng (2 khóa), xóa 1 đối tượng clip trên kho,
      sửa 1 byte 1 đối tượng, khôi phục bằng chứng (hồ sơ mở trước), `MISSING`, CSV, `--evidence-only
      --key-file`,
      `backup-verify --accept`, `RESTORE_PENDING` hết.
"""

import argparse
import asyncio
import base64
import hashlib
import os
import shutil
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "tests" / "integration" / "bin"
KEY_1 = base64.b64encode(b"drill-key-one-32-bytes-long!!!!!").decode()
KEY_2 = base64.b64encode(b"drill-key-two-32-bytes-long!!!!!").decode()
LOG: list[str] = []


def say(msg: str) -> None:
    line = f"[{datetime.now(UTC):%H:%M:%S}Z] {msg}"
    LOG.append(line)
    print(line, flush=True)


def _tool(name: str) -> str:
    return shutil.which(name) or str(BIN / f"docker-{name}")


def settings_for(db: str, video: Path, imports: Path, tmp: Path, key: str, old: str = "") -> object:
    from aicam.core.settings import Settings

    pg = os.environ["DRILL_PG"]
    return Settings(
        app_env="test",
        log_json=False,
        database_url=f"{pg}/{db}",
        redis_url=os.environ.get("TEST_REDIS_URL", "redis://localhost:56379/15"),
        video_root=video,
        import_root=imports,
        backup_tmp_dir=tmp,
        backup_encryption_key=key,
        backup_old_keys=old,
        backup_pg_dump_bin=_tool("pg_dump"),
        backup_pg_restore_bin=_tool("pg_restore"),
        s3_endpoint=os.environ["TEST_S3_ENDPOINT"],
        s3_access_key_id=os.environ["TEST_S3_ACCESS_KEY"],
        s3_secret_access_key=os.environ["TEST_S3_SECRET_KEY"],
        s3_bucket=os.environ.get("TEST_S3_BUCKET", "aicam-test-backup"),
        s3_share_bucket=os.environ.get("TEST_S3_SHARE_BUCKET", "aicam-test-share"),
    )


async def recreate(db: str) -> None:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    admin = create_async_engine(f"{os.environ['DRILL_PG']}/postgres", isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(text(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)'))
        await conn.execute(text(f'CREATE DATABASE "{db}"'))
    await admin.dispose()


def migrate(url: str) -> None:
    from alembic import command
    from alembic.config import Config

    from aicam.core.settings import get_settings

    os.environ["DATABASE_URL"] = url
    get_settings.cache_clear()
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    command.upgrade(cfg, "head")


async def seed(session: object, settings: object, n_packages: int) -> dict[str, object]:
    """Kiện + phiên PACK + clip (tệp thật) + 1 hồ sơ khiếu nại mở (bằng chứng) + 1 hồ sơ đóng."""
    from aicam.core.security import hash_password
    from aicam.modules.claims.models import Claim, ClaimEvidence
    from aicam.modules.media.models import Clip, Snapshot
    from aicam.modules.orders.models import Package
    from aicam.modules.sessions.models import PackSession
    from aicam.modules.stations.models import Station
    from aicam.modules.users.models import User

    db = session
    now = datetime.now(UTC)
    user = User(username="tst_station01", display_name="TST Station 01", role="STATION",
                password_hash=hash_password("matkhau123"))  # fmt: skip
    db.add(user)
    await db.flush()
    station = Station(name="TST Station 01", account_user_id=user.id)
    db.add(station)
    await db.flush()
    evidence: list[uuid.UUID] = []
    for i in range(n_packages):
        package = Package(tracking_number=f"SPXDRILL{i:07d}", warehouse_status="PACKED")
        db.add(package)
        await db.flush()
        ended = now - timedelta(days=1, minutes=i)
        pack = PackSession(type="PACK", package_id=package.id, station_id=station.id, status="COMPLETED",
                           started_at=ended - timedelta(minutes=2), ended_at=ended,
                           open_code=package.tracking_number, close_code=package.tracking_number,
                           package_status_before="NEW", flags=[])  # fmt: skip
        db.add(pack)
        await db.flush()
        if i < 6:  # 6 kiện có tệp thật (3 hồ sơ mở, 3 hồ sơ đóng) — phần còn lại chỉ dòng DB
            for role in ("CAM1", "CAM2"):
                rel = f"clips/{ended:%Y/%m/%d}/{pack.id}-{role}.mp4"
                data = os.urandom(256 * 1024) + f"{rel}".encode()
                path = settings.video_root / rel
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                sha = hashlib.sha256(data).hexdigest()
                db.add(Clip(session_id=pack.id, camera_role=role, status="READY", start_at=pack.started_at,
                            end_at=ended, path=rel, sha256=sha, size_bytes=len(data), flags=[]))  # fmt: skip
            rel = f"snapshots/{ended:%Y/%m/%d}/{pack.id}_pack.jpg"
            data = os.urandom(20 * 1024)
            (settings.video_root / rel).parent.mkdir(parents=True, exist_ok=True)
            (settings.video_root / rel).write_bytes(data)
            sha = hashlib.sha256(data).hexdigest()
            db.add(Snapshot(session_id=pack.id, kind="PACK_CLOSE", camera_role="CAM1", taken_at=ended,
                            path=rel, sha256=sha, size_bytes=len(data), status="READY"))  # fmt: skip
            claim = Claim(package_id=package.id, type="OTHER", counterparty="PLATFORM", source="MANUAL",
                          status="NEW" if i < 3 else "CLOSED", closed_at=None if i < 3 else now)  # fmt: skip
            db.add(claim)
            await db.flush()
            db.add(ClaimEvidence(claim_id=claim.id, kind="SESSION", session_id=pack.id, auto=False))
            evidence.append(pack.id)
        if i % 500 == 0:
            await db.commit()
    await db.commit()
    return {"evidence_sessions": evidence}


async def counts(url: str) -> dict[str, int]:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    out: dict[str, int] = {}
    async with engine.connect() as conn:
        for table in ("package", "session", "clip", "snapshot", "claim", "claim_evidence", "backup_object"):
            sql = f'SELECT count(*) FROM "{table}"'  # noqa: S608 — tên bảng hằng
            out[table] = int(await conn.scalar(text(sql)) or 0)
    await engine.dispose()
    return out


async def run(args: argparse.Namespace) -> int:
    import aicam.db_models  # noqa: F401
    from aicam.core.db import dispose_engine, init_engine, sessionmaker
    from aicam.core.redis import close_redis, init_redis
    from aicam.modules.backup import jobs, service
    from aicam.modules.cloud import config as cloud
    from aicam.modules.settings import service as settings_service

    work = Path(tempfile.mkdtemp(prefix="aicam-drill-"))
    src_video = work / "src-video"
    src = settings_for("drill_src", src_video, work / "src-imports", work / "tmp", KEY_1)
    say(f"Diễn tập khôi phục AC-50 — phase {args.phase}, {args.packages} kiện, thư mục {work}")
    say(f"Kho S3 tạm: {src.s3_endpoint} bucket {src.s3_bucket} (MinIO container riêng)")
    store = cloud.backup_store(src)
    for info in list(store.list("backup/")):
        store.delete(info.key)  # bucket sạch trước diễn tập (delete marker)

    await recreate("drill_src")
    await asyncio.to_thread(migrate, src.database_url)
    say("DB nguồn drill_src: migrate head xong")
    (work / "src-imports" / "2026").mkdir(parents=True)
    (work / "src-imports" / "2026" / "don.csv").write_text("ma_don,ma_van_don\n2410DRILL1,SPXDRILL0000001\n")
    init_engine(src.database_url)
    init_redis(src.redis_url)
    try:
        async with sessionmaker()() as db:
            await seed(db, src, args.packages)
            cfg = await settings_service.get(db)
            cfg.backup_confirmed_fingerprint = service.current_fingerprint(src)
            cfg.backup_confirmed_at = datetime.now(UTC)
            cfg.backup_enabled = True
            cfg.backup_upload_mbps = 1000
            await db.commit()
            say(f"Dữ liệu nguồn: {await counts(src.database_url)}")
            t = time.monotonic()
            out = await jobs.run_db(db, src, store=store)
            say(f"J-20 sao lưu DB: {out} ({time.monotonic() - t:.1f} giây)")
            if out.get("status") != "SUCCESS":
                return 1
    finally:
        await close_redis()
        await dispose_engine()

    rc = await phase_db(src, work, store)
    LOG.append("")
    await asyncio.to_thread(Path(args.out).write_text, "\n".join(LOG) + "\n")
    shutil.rmtree(work, ignore_errors=True)
    return rc


async def phase_db(src: object, work: Path, store: object) -> int:
    from aicam.modules.backup import restore

    dst = settings_for("drill_dst", work / "dst-video", work / "dst-imports", work / "tmp2", KEY_2)
    await recreate("drill_dst")
    say("— Khóa sai: máy mới chỉ có khóa KHÁC (KEY_2) —")
    rep = await restore.restore(dst, store=store)
    say(f"backup-restore mã {rep.exit_code}: {' | '.join(rep.lines)}")
    tables = await counts_safe(dst.database_url)
    say(f"DB đích sau lệnh khóa sai: {tables}")
    ok = rep.exit_code == restore.EXIT_REFUSED and tables == "trống"

    dst = settings_for("drill_dst", work / "dst-video", work / "dst-imports", work / "tmp2", KEY_1)
    say("— Khôi phục DB vào DB trống với đúng khóa —")
    t = time.monotonic()
    rep = await restore.restore(dst, store=store)
    took = time.monotonic() - t
    for line in rep.lines:
        say(f"  {line}")
    say(f"backup-restore mã {rep.exit_code}, tổng {took:.1f} giây (NFR-40 RTO DB ≤ 60 phút — máy dev)")
    a, b = await counts(src.database_url), await counts(dst.database_url)
    say(f"Đếm dòng nguồn {a}")
    say(f"Đếm dòng đích  {b}")
    # `backup_object` của chính lượt J-20 ghi SAU pg_dump → bản dump không có (đúng — trạng thái lúc dump).
    a.pop("backup_object"), b.pop("backup_object")
    ok = ok and rep.exit_code == 0 and a == b
    imports_ok = (work / "dst-imports" / "2026" / "don.csv").exists()
    say(f"File nhập khôi phục: {'có' if imports_ok else 'KHÔNG'}")
    rep2 = await restore.restore(dst, store=store)
    say(f"Chạy lại vào DB không trống → mã {rep2.exit_code}: {rep2.lines[0]}")
    ok = ok and rep2.exit_code == restore.EXIT_REFUSED and imports_ok
    from aicam.core.db import dispose_engine, init_engine, sessionmaker

    init_engine(dst.database_url)
    try:
        async with sessionmaker()() as db:
            ver = await restore.verify(dst, db)
    finally:
        await dispose_engine()
    say(f"backup-verify (chưa khôi phục bằng chứng) mã {ver.exit_code}: {ver.lines[0]}")
    say("  → đúng kỳ vọng phase db: clip / ảnh chưa có tệp → 'thiếu', Chờ kiểm khôi phục giữ nguyên")
    ok = ok and ver.exit_code == restore.EXIT_VERIFY_FAILED
    say(f"KẾT QUẢ phase db: {'ĐẠT' if ok else 'KHÔNG ĐẠT'}")
    return 0 if ok else 1


async def counts_safe(url: str) -> object:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(url)
    async with engine.connect() as conn:
        n = await conn.scalar(
            text("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")
        )
    await engine.dispose()
    return "trống" if not n else f"{n} bảng"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--packages", type=int, default=2000)
    parser.add_argument("--phase", choices=["db", "full"], default="db")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
