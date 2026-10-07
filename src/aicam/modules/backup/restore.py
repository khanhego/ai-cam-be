"""Lệnh vận hành API-186 `aicam backup-restore` / `aicam backup-verify` (02 §6.2, 02a §2; FR-02.16, NFR-40;
EX-K8; DEC-495, 499, 518). Runbook: `docs/ops.md` §6.2.

Không bao giờ đánh `DELETED`: clip / ảnh không có tệp sau khôi phục → `MISSING` (J-23 không xóa bản cloud).
Sau khi khôi phục DB: `backup_enabled = false`, `backup_restore_pending = true` tới khi `backup-verify` đạt.
"""

import asyncio
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import structlog
from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aicam.core import audit, clock
from aicam.core.settings import Settings
from aicam.modules.backup import service, transfer
from aicam.modules.backup.jobs import DB_PREFIX, IMPORTS_PREFIX, pg_env
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import CloudError, ObjectStore
from aicam.modules.settings.models import Setting

log = structlog.get_logger()

EXIT_OK = 0
EXIT_VERIFY_FAILED = 1
EXIT_REFUSED = 2  # khóa DB sai / DB đích không trống / không có bản DB — không ghi gì
EXIT_PARTIAL = 3  # có đối tượng bằng chứng giải mã lỗi / thiếu khóa — đã làm hết phần còn lại


@dataclass
class Report:
    lines: list[str] = field(default_factory=list)
    exit_code: int = EXIT_OK

    def say(self, line: str) -> None:
        self.lines.append(line)


def read_key_files(paths: list[Path]) -> list[bytes]:
    """`--key-file F` (mỗi tệp một khóa base64) — khóa cũ tìm lại được sau này (DEC-495)."""
    return [crypto.parse_key(p.read_text().strip()) for p in paths]


def latest_db_key(store: ObjectStore) -> str | None:
    """`--db latest`: khóa đối tượng có tên (dấu thời gian UTC) lớn nhất dưới `backup/db/`."""
    keys = sorted(o.key for o in store.list(DB_PREFIX) if o.key.endswith(".dump.enc"))
    return keys[-1] if keys else None


async def _db_is_empty(settings: Settings) -> bool:
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as conn:
            n = await conn.scalar(
                text("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'")
            )
    finally:
        await engine.dispose()
    return int(n or 0) == 0


async def _pg_restore(settings: Settings, dump: Path, *, force: bool) -> None:
    args = [settings.backup_pg_restore_bin, "--no-owner", "--exit-on-error"]
    if force:
        args += ["--clean", "--if-exists"]
    env = pg_env(settings.database_url)
    args += ["-d", env["PGDATABASE"]]
    with dump.open("rb") as stdin:
        proc = await asyncio.create_subprocess_exec(
            *args, stdin=stdin, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env
        )
        _, err = await proc.communicate()
    if proc.returncode != 0:
        tail = (err or b"").decode(errors="replace").strip().splitlines()[-3:]
        raise RuntimeError("pg_restore lỗi: " + " | ".join(tail))


async def mark_restore_pending(db: AsyncSession) -> None:
    """DEC-499: sao lưu tắt tới khi `backup-verify` đạt (J-20..J-23 không chạy — `state =
    RESTORE_PENDING`)."""
    await db.execute(
        update(Setting).where(Setting.id == 1).values(backup_enabled=False, backup_restore_pending=True)
    )
    await db.commit()


async def restore_db(
    settings: Settings,
    report: Report,
    *,
    db_key: str,
    keys: dict[str, bytes],
    force: bool,
    store: ObjectStore,
) -> bool:
    """(1) Tải + giải mã DB dump (tệp tạm, kiểm SHA-256 với metadata) → `pg_restore` vào DB trống. Khóa không
    khớp / DB không trống → từ chối, **không ghi** gì (mã 2)."""
    object_key = latest_db_key(store) if db_key == "latest" else db_key
    if object_key is None:
        report.say("Không tìm thấy bản sao DB nào dưới backup/db/ trên kho lưu.")
        report.exit_code = EXIT_REFUSED
        return False
    head = await asyncio.to_thread(store.head, object_key)
    if head is None:
        report.say(f"Không thấy đối tượng {object_key}.")
        report.exit_code = EXIT_REFUSED
        return False

    def _peek() -> crypto.Header:
        with store.get_stream(object_key) as body:
            return crypto.read_header(body)

    header = await asyncio.to_thread(_peek)
    if header.fingerprint not in keys:
        report.say(f"Khóa giải mã không khớp (dấu vân tay {header.fingerprint}) — không ghi gì.")
        report.exit_code = EXIT_REFUSED
        return False
    if not force and not await _db_is_empty(settings):
        report.say("DB đích không trống — từ chối khôi phục (dùng --force để ghi đè có chủ đích).")
        report.exit_code = EXIT_REFUSED
        return False
    tmp = Path(tempfile.mkdtemp(prefix="aicam-restore-", dir=_tmp_root(settings)))
    try:
        dump = tmp / "aicam.dump"
        result = await asyncio.to_thread(transfer.download_decrypt_to, store, object_key, dump, keys)
        expected = head.metadata.get("sha256")
        if expected and expected != result.sha256:
            report.say("Bản DB tải về lệch SHA-256 với metadata — dừng, không ghi DB.")
            report.exit_code = EXIT_REFUSED
            return False
        started = clock.now()
        await _pg_restore(settings, dump, force=force)
        report.say(
            f"Đã khôi phục DB từ {object_key} (khóa {header.fingerprint}, "
            f"{result.plain_size:,} byte, pg_restore {(clock.now() - started).total_seconds():.1f} giây)."
        )
        await _restore_imports(settings, report, object_key, keys, store, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return True


def imports_key_for(db_object_key: str) -> str:
    """`backup/db/…/aicam-{stamp}.dump.enc` → `backup/imports/{stamp}.tgz.enc` (cùng lượt J-20)."""
    stamp = db_object_key.rsplit("/", 1)[-1].removeprefix("aicam-").removesuffix(".dump.enc")
    return f"{IMPORTS_PREFIX}{stamp}.tgz.enc"


async def _restore_imports(
    settings: Settings, report: Report, db_key: str, keys: dict[str, bytes], store: ObjectStore, tmp: Path
) -> None:
    """File nhập đơn gốc cùng lượt (FR-02.08 a) → giải nén vào `IMPORT_ROOT` (không ghi đè tệp đã có)."""
    import tarfile

    key = imports_key_for(db_key)
    if await asyncio.to_thread(store.head, key) is None:
        report.say(f"Không có bản file nhập {key} — bỏ qua.")
        return
    tgz = tmp / "imports.tgz"
    try:
        await asyncio.to_thread(transfer.download_decrypt_to, store, key, tgz, keys)
    except (crypto.CryptoError, CloudError) as exc:
        report.say(f"Không khôi phục được file nhập ({type(exc).__name__}) — DB vẫn đã khôi phục.")
        return

    def _extract() -> int:
        root = settings.import_root
        root.mkdir(parents=True, exist_ok=True)
        n = 0
        with tarfile.open(tgz, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.isfile() or not member.name.startswith("imports/"):
                    continue
                rel = member.name.removeprefix("imports/")
                target = root / rel
                if target.exists():
                    continue
                member.name = rel
                tar.extract(member, root, filter="data")
                n += 1
        return n

    try:
        n = await asyncio.to_thread(_extract)
    except (OSError, tarfile.TarError) as exc:
        report.say(f"Không giải nén được file nhập vào {settings.import_root} ({exc}) — DB vẫn đã khôi phục.")
        return
    report.say(f"Đã khôi phục {n} file nhập vào {settings.import_root}.")


def _tmp_root(settings: Settings) -> str:
    root = settings.backup_tmp_dir
    root.mkdir(parents=True, exist_ok=True)
    return str(root)


async def restore(
    settings: Settings,
    *,
    db_key: str | None = "latest",
    key_files: list[Path] | None = None,
    force: bool = False,
    store: ObjectStore | None = None,
) -> Report:
    """API-186 `aicam backup-restore`."""
    from aicam.core.db import dispose_engine, init_engine, sessionmaker

    report = Report()
    store = store or cloud.backup_store(settings)
    try:
        keys = service.keyring(settings, read_key_files(key_files or []))
    except ValueError as exc:
        report.say(f"Khóa không hợp lệ: {exc}")
        report.exit_code = EXIT_REFUSED
        return report
    if not keys:
        report.say("Chưa có khóa giải mã (BACKUP_ENCRYPTION_KEY / BACKUP_OLD_KEYS / --key-file).")
        report.exit_code = EXIT_REFUSED
        return report
    started = clock.now()
    try:
        if db_key is not None:
            if not await restore_db(settings, report, db_key=db_key, keys=keys, force=force, store=store):
                return report
            init_engine(settings.database_url)
            try:
                async with sessionmaker()() as db:
                    await mark_restore_pending(db)
            finally:
                await dispose_engine()
            report.say("Sao lưu tự động đã TẮT (Chờ kiểm khôi phục) tới khi `aicam backup-verify` đạt.")
    except CloudError as exc:
        report.say(f"Lỗi kho lưu: {exc.message}")
        report.exit_code = EXIT_REFUSED
        return report
    report.say(f"Xong sau {(clock.now() - started).total_seconds():.1f} giây.")
    log.info("backup_restore", exit_code=report.exit_code)
    return report


# ---------------------------------------------------------------- backup-verify


@dataclass
class VerifyResult:
    ok: int = 0
    accepted: int = 0
    mismatch: list[tuple[str, str]] = field(default_factory=list)  # (kind, id)
    missing_known: int = 0
    missing: list[tuple[str, str]] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.mismatch and not self.missing


async def _scan(db: AsyncSession, settings: Settings) -> VerifyResult:
    """Băm từng clip / ảnh `READY` trên đĩa so với DB; `MISSING` = thiếu đã ghi nhận."""
    from sqlalchemy import select

    from aicam.modules.backup.models import BackupObject
    from aicam.modules.media import service as media
    from aicam.modules.media.models import Clip, Snapshot

    res = VerifyResult()
    accepted = {
        (r.clip_id or r.snapshot_id): r.sha256_actual
        for r in (await db.scalars(select(BackupObject).where(BackupObject.hash_override.is_(True)))).all()
    }
    for kind, model in (("CLIP", Clip), ("SNAPSHOT", Snapshot)):
        rows = (await db.execute(select(model.id, model.status, model.path, model.sha256))).all()
        for oid, status, rel, sha in rows:
            if status == "MISSING":
                res.missing_known += 1
                continue
            if status != "READY":
                continue
            path = None
            if rel:
                try:
                    path = media.absolute(settings, rel)
                except ValueError:
                    path = None
            if path is None or not await asyncio.to_thread(path.is_file):
                res.missing.append((kind, str(oid)))
                continue
            actual, _ = await asyncio.to_thread(transfer.sha256_file, path)
            if actual == sha:
                res.ok += 1
            elif accepted.get(oid) == actual:
                res.accepted += 1
            else:
                res.mismatch.append((kind, str(oid)))
    await db.commit()
    return res


async def verify(settings: Settings, db: AsyncSession) -> Report:
    """API-186 `aicam backup-verify`: đạt (lệch = 0 và thiếu = 0) → xóa `backup_restore_pending` + audit
    `BACKUP_RESTORE_VERIFIED` → mã 0 (Admin bật lại ở D23); không đạt → mã 1."""
    from aicam.modules.settings import service as settings_service

    report = Report()
    res = await _scan(db, settings)
    report.say(
        f"khớp {res.ok} / lệch đã chấp nhận {res.accepted} / lệch {len(res.mismatch)} / "
        f"thiếu đã ghi nhận {res.missing_known} / thiếu {len(res.missing)}"
    )
    for kind, oid in (res.mismatch + res.missing)[:50]:
        state_ = "lệch" if (kind, oid) in res.mismatch else "thiếu"
        report.say(f"  {state_}: {kind} {oid}")
    if not res.passed:
        report.say("KHÔNG ĐẠT — xem từng tệp lệch / thiếu (docs/ops.md §6.2 'Lối ra').")
        report.exit_code = EXIT_VERIFY_FAILED
        return report
    cfg = await settings_service.get(db)
    if cfg.backup_restore_pending:
        cfg.backup_restore_pending = False
        audit.record(db, "BACKUP_RESTORE_VERIFIED", user_id=None, object_type="SETTING", object_id="1",
                     data={"ok": res.ok, "accepted": res.accepted, "missing_known": res.missing_known,
                           "os_user": _os_user()})  # fmt: skip
        report.say("ĐẠT — đã gỡ 'Chờ kiểm khôi phục'. Admin bật lại sao lưu ở Dashboard → Sao lưu cloud.")
    else:
        report.say("ĐẠT.")
    await db.commit()
    log.info("backup_verify", ok=res.ok, accepted=res.accepted, mismatch=0, missing_known=res.missing_known)
    return report


def _os_user() -> str:
    try:
        return os.getlogin()
    except OSError:
        return os.environ.get("USER", "?")
