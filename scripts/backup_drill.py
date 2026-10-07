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
import io
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
    later: tuple[uuid.UUID, uuid.UUID] | None = None
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
        # 8 kiện có tệp thật: 0–2 hồ sơ mở, 3–5 hồ sơ đóng (còn hạn giữ), 6 thành bằng chứng SAU khi đổi khóa,
        # 7 không phải bằng chứng (không lên cloud). Phần còn lại chỉ có dòng DB.
        if i == 6:
            later = (package.id, pack.id)
        if i < 8:
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
        if i < 6:
            claim = Claim(package_id=package.id, type="OTHER", counterparty="PLATFORM", source="MANUAL",
                          status="NEW" if i < 3 else "CLOSED", closed_at=None if i < 3 else now)  # fmt: skip
            db.add(claim)
            await db.flush()
            db.add(ClaimEvidence(claim_id=claim.id, kind="SESSION", session_id=pack.id, auto=False))
            evidence.append(pack.id)
        if i % 500 == 0:
            await db.commit()
    await db.commit()
    return {"evidence_sessions": evidence, "later": later}


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
            seeded = await seed(db, src, args.packages)
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

    if args.phase == "db":
        rc = await phase_db(src, work, store)
    else:
        rc = await phase_full(src, work, store, seeded)
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


async def evidence_round(settings: object, label: str, store: object) -> dict[str, object]:
    from aicam.core.db import dispose_engine, init_engine, sessionmaker
    from aicam.core.redis import close_redis, init_redis
    from aicam.modules.backup import jobs

    init_engine(settings.database_url)
    init_redis(settings.redis_url)
    try:
        async with sessionmaker()() as db:
            q = await jobs.enqueue_evidence(db, settings)
            t = time.monotonic()
            up = await jobs.upload_evidence(db, settings, store=store, budget_s=600)
            say(f"{label}: J-21 {q}; J-22 {up} ({time.monotonic() - t:.1f} giây)")
            return up
    finally:
        await close_redis()
        await dispose_engine()


async def phase_full(src: object, work: Path, store: object, seeded: dict[str, object]) -> int:
    """T-274 + T-284: bằng chứng, 2 khóa, lệch đã chấp nhận, xóa 1 đối tượng, sửa 1 byte, quên khóa cũ,
    `--evidence-only --key-file`, verify `--accept`."""
    from sqlalchemy import func, select

    from aicam.core.audit import AuditLog
    from aicam.core.db import dispose_engine, init_engine, sessionmaker
    from aicam.core.deps import Principal
    from aicam.core.redis import close_redis, init_redis
    from aicam.modules.backup import jobs, restore, service
    from aicam.modules.backup.models import BackupObject
    from aicam.modules.backup.schemas import ResolveIn
    from aicam.modules.claims.models import Claim, ClaimEvidence
    from aicam.modules.cloud import crypto
    from aicam.modules.media.models import Clip
    from aicam.modules.settings import service as settings_service

    ok = True
    # Một clip bằng chứng bị sửa trên đĩa trước khi sao lưu → lệch mã băm → Admin "Vẫn sao lưu" (EX-K6).
    init_engine(src.database_url)
    try:
        async with sessionmaker()() as db:
            sess = seeded["evidence_sessions"][1]
            bad_id, bad_path = (
                await db.execute(
                    select(Clip.id, Clip.path).where(Clip.session_id == sess, Clip.camera_role == "CAM2")
                )
            ).one()
            await db.commit()
    finally:
        await dispose_engine()
    (src.video_root / bad_path).write_bytes(b"ban-bi-sua-truoc-sao-luu")
    await evidence_round(src, "Bằng chứng khóa 1", store)
    init_engine(src.database_url)
    try:
        async with sessionmaker()() as db:
            obj = await db.scalar(select(BackupObject).where(BackupObject.clip_id == bad_id))
            say(f"Clip {bad_id} sau J-22: {obj.status}")
            admin = Principal(user_id=uuid.uuid4(), role="ADMIN", station_id=None, ip=None)
            out = await service.resolve_issue(
                db, obj.id, ResolveIn(action="UPLOAD_ANYWAY", note="Diễn tập: có còn hơn không"), admin, src
            )
            say(f"API-188 UPLOAD_ANYWAY → {out.status}")
            ok = ok and out.status == "PENDING"
    finally:
        await dispose_engine()
    fp1 = crypto.fingerprint(crypto.parse_key(KEY_1))
    # IT đổi khóa: khóa 2 hiện tại, khóa 1 vào BACKUP_OLD_KEYS; Admin xác nhận khóa mới.
    src2 = settings_for("drill_src", work / "src-video", work / "src-imports", work / "tmp", KEY_2, KEY_1)
    fp2 = crypto.fingerprint(crypto.parse_key(KEY_2))
    say(f"— Đổi khóa: {fp1} → {fp2} (khóa cũ giữ trong BACKUP_OLD_KEYS) —")
    init_engine(src2.database_url)
    init_redis(src2.redis_url)
    try:
        async with sessionmaker()() as db:
            cfg = await settings_service.get(db)
            cfg.backup_confirmed_fingerprint = fp2
            cfg.backup_confirmed_at = datetime.now(UTC)
            later = seeded["later"]
            claim = Claim(package_id=later[0], type="OTHER", counterparty="PLATFORM", source="MANUAL")
            db.add(claim)
            await db.flush()
            db.add(ClaimEvidence(claim_id=claim.id, kind="SESSION", session_id=later[1], auto=False))
            await db.commit()
            status = await service.status(db, src2)
            old = [k.model_dump() for k in status.key.old_keys]
            say(f"API-180 sau đổi khóa: state {status.state}, old_keys {old}")
            await db.commit()
    finally:
        await close_redis()
        await dispose_engine()
    await evidence_round(src2, "Bằng chứng mới sau đổi khóa (khóa 2)", store)
    init_engine(src2.database_url)
    init_redis(src2.redis_url)
    try:
        async with sessionmaker()() as db:
            out = await jobs.run_db(db, src2, store=store)
            say(f"J-20 lượt 2 (khóa 2): {out}")
            rows = (await db.execute(select(Clip.id, Clip.path, Clip.status).order_by(Clip.path))).all()
            await db.commit()
    finally:
        await close_redis()
        await dispose_engine()
    keys_by_fp: dict[str, int] = {}
    for info in list(store.list("backup/evidence/")):
        head = store.head(info.key)
        fp = head.metadata.get("key-fp", "?") if head else "?"
        keys_by_fp[fp] = keys_by_fp.get(fp, 0) + 1
    say(f"Đối tượng bằng chứng trên kho theo khóa: {keys_by_fp}")
    ok = ok and keys_by_fp.get(fp1, 0) > 0 and keys_by_fp.get(fp2, 0) > 0
    victim = rows[0]
    store.delete(jobs.evidence_key("CLIP", victim[0]))
    say(f"Xóa 1 đối tượng clip trên kho trước khi khôi phục: {victim[0]} ({victim[1]})")

    # Sửa 1 byte một đối tượng ảnh trên kho (hỏng / bị sửa) — ghi đè thành phiên bản mới.
    snap_key = next(k.key for k in store.list("backup/evidence/snapshots/"))
    head = store.head(snap_key)
    with store.get_stream(snap_key) as body:
        raw = bytearray(body.read())
    raw[100] ^= 0x01
    store.put_stream(snap_key, io.BytesIO(bytes(raw)), metadata=dict(head.metadata if head else {}))
    say(f"Sửa 1 byte đối tượng {snap_key}")

    say("— Máy mới: DB trống + thư mục video trống, CHỈ có khóa 2 (quên khóa cũ) —")
    dst = settings_for("drill_dst", work / "dst-video", work / "dst-imports", work / "tmp2", KEY_2)
    await recreate("drill_dst")
    t = time.monotonic()
    rep = await restore.restore(dst, store=store, evidence=True)
    for line in rep.lines:
        say(f"  {line}")
    say(f"backup-restore --evidence mã {rep.exit_code}, tổng {time.monotonic() - t:.1f} giây")
    ok = ok and rep.exit_code == restore.EXIT_PARTIAL
    csvs = sorted((work / "dst-video" / "restore-reports").glob("restore-failures-*.csv"))
    say(f"CSV lỗi: {csvs[-1].name if csvs else 'KHÔNG CÓ'}")
    if csvs:
        for line in csvs[-1].read_text().splitlines()[:4]:
            say(f"  csv: {line}")
    parts = list((work / "dst-video").rglob("*.part"))
    say(f"Tệp dở (.part) trên đĩa: {len(parts)}")
    ok = ok and bool(csvs) and not parts
    key_file = work / "khoa-cu.txt"
    key_file.write_text(KEY_1)
    say("— Tìm lại khóa cũ: backup-restore --evidence-only --key-file khoa-cu.txt —")
    rep = await restore.restore(dst, store=store, evidence_only=True, key_files=[key_file])
    for line in rep.lines:
        say(f"  {line}")
    say(f"backup-restore --evidence-only mã {rep.exit_code} (còn đối tượng ảnh bị sửa → vẫn mã 3)")
    ok = ok and rep.exit_code == restore.EXIT_PARTIAL
    init_engine(dst.database_url)
    try:
        async with sessionmaker()() as db:
            st = {str(cid): s for cid, s in (await db.execute(select(Clip.id, Clip.status))).all()}
            say(f"Trạng thái clip đích: { {s: list(st.values()).count(s) for s in set(st.values())} }")
            ok = ok and st[str(victim[0])] == "MISSING" and "DELETED" not in st.values()
            cfg = await settings_service.get(db)
            flags = f"backup_enabled={cfg.backup_enabled}, restore_pending={cfg.backup_restore_pending}"
            say(f"Setting đích: {flags}")
            await db.commit()
            pr = await jobs.prune(db, dst, store=store)
            say(f"J-23 trên máy mới khi chờ kiểm: {pr} (không xóa gì)")
            ok = ok and pr == {"skipped": "RESTORE_PENDING"}
            # Sửa 1 tệp sau khôi phục → verify mã 1 → --accept thiếu lý do bị từ chối → có lý do → đạt.
            ready = (
                await db.execute(select(Clip.id, Clip.path).where(Clip.status == "READY").order_by(Clip.path))
            ).all()
            await db.commit()
            changed_id, changed_path = next((i, p) for i, p in ready if i != bad_id)
            (dst.video_root / changed_path).write_bytes(b"sua-sau-khoi-phuc")
            # G3-BK-9: ghi rõ bước giả lập vào biên bản — verify "lệch 1" dưới là do bước này, không phải lỗi.
            say(
                f"Giả lập tệp bị sửa sau khôi phục: ghi đè CLIP {changed_id} ({changed_path}) "
                "→ verify kế tiếp phải báo lệch 1 (CLIP này)"
            )
            ver = await restore.verify(dst, db)
            for line in ver.lines:
                say(f"  verify: {line}")
            ok = ok and ver.exit_code == restore.EXIT_VERIFY_FAILED
            ver = await restore.verify(dst, db, accept=[str(changed_id)])
            say(f"  verify --accept (thiếu --reason) mã {ver.exit_code}: {ver.lines[0]}")
            ok = ok and ver.exit_code == restore.EXIT_REFUSED
            ver = await restore.verify(
                dst, db, accept=[str(changed_id)], reason="Diễn tập: tệp sửa sau khôi phục"
            )
            for line in ver.lines:
                say(f"  verify --accept: {line}")
            ok = ok and ver.exit_code == 0
            accepted = (
                await db.execute(
                    select(func.count())
                    .select_from(AuditLog)
                    .where(AuditLog.action == "BACKUP_VERIFY_ACCEPT")
                )
            ).scalar()
            say(f"Audit BACKUP_VERIFY_ACCEPT: {accepted}")
            cfg = await settings_service.get(db)
            say(f"Sau verify: restore_pending={cfg.backup_restore_pending}")
            await db.commit()
    finally:
        await dispose_engine()
    remaining = sum(1 for _ in store.list("backup/evidence/"))
    say(f"Đối tượng bằng chứng còn trên kho sau diễn tập: {remaining} (không đối tượng nào bị xóa thêm)")
    say(f"KẾT QUẢ phase full: {'ĐẠT' if ok else 'KHÔNG ĐẠT'}")
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
