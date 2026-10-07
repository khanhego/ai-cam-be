"""Job sao lưu (02a §7 J-20..J-23, §6; ADR-010). Chỉ chạy khi `backup.state = ON`.

Mỗi job khóa dòng của bảng `backup_*` (không giữ khóa nghiệp vụ khi gọi mạng); việc mạng / mã hóa chạy trong
`asyncio.to_thread` (DEC-651). Lỗi một mục không chặn mục khác.
"""

import asyncio
import os
import shutil
import tarfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import structlog
from redis import Redis
from sqlalchemy import select, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.backup import service, transfer
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud import crypto
from aicam.modules.cloud.ratelimit import TokenBucket
from aicam.modules.cloud.store import CloudError, ObjectStore
from aicam.modules.settings import service as settings_service

log = structlog.get_logger()

STALE_RUNNING = timedelta(hours=2)  # DEC-500
STALE_ERROR = "STALE_RUNNING"
DB_PREFIX = "backup/db/"
IMPORTS_PREFIX = "backup/imports/"


class BackupFailed(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def error_text(code: str, message: str) -> str:
    """`backup_run.error` / `backup_object.last_error` dạng `MÃ: lời nhắn` (API-180 `last_error.code`)."""
    return f"{code}: {message}"[:500]


def make_throttle(settings: Settings, mbps: int) -> tuple[Redis, TokenBucket]:
    redis = Redis.from_url(settings.redis_url)
    return redis, TokenBucket(redis, mbps)


async def fail_stale_runs(db: AsyncSession, now: datetime | None = None) -> int:
    """DEC-500: `RUNNING` có `started_at < now − 2 giờ` → `FAILED STALE_RUNNING` (J-20 / API-184 trước khi tạo
    lượt mới; J-23 làm lại như lưới an toàn). Không commit — người gọi commit."""
    now = now or clock.now()
    result = await db.execute(
        update(BackupRun)
        .where(BackupRun.status == "RUNNING", BackupRun.started_at < now - STALE_RUNNING)
        .values(
            status="FAILED",
            finished_at=now,
            error=error_text(STALE_ERROR, "Lượt sao lưu treo quá 2 giờ (worker dừng giữa chừng)."),
        )
        .returning(BackupRun.id)
    )
    ids = result.scalars().all()
    for run_id in ids:
        log.warning("backup_db_stale", run_id=str(run_id))
    return len(ids)


async def start_run(db: AsyncSession, trigger: str, user_id: uuid.UUID | None) -> BackupRun | None:
    """INSERT `backup_run RUNNING` (partial unique `kind` khi RUNNING → đang có lượt → None). Commit."""
    await fail_stale_runs(db)
    run = BackupRun(kind="DB", trigger=trigger, status="RUNNING", started_at=clock.now(), created_by=user_id)
    try:
        async with db.begin_nested():
            db.add(run)
            await db.flush()
    except IntegrityError:
        await db.commit()
        return None
    await db.commit()
    return run


def _pg_env(database_url: str) -> dict[str, str]:
    url = make_url(database_url)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env.update(
        {
            "PGHOST": url.host or "localhost",
            "PGPORT": str(url.port or 5432),
            "PGUSER": url.username or "",
            "PGPASSWORD": url.password or "",
            "PGDATABASE": url.database or "",
        }
    )
    return env


async def pg_dump(settings: Settings, target: Path) -> None:
    """`pg_dump -Fc --no-owner` (02a J-20) ghi ra `target`; lỗi → `BackupFailed(PG_DUMP_FAILED)`."""
    with target.open("wb") as out:
        try:
            proc = await asyncio.create_subprocess_exec(
                settings.backup_pg_dump_bin,
                "-Fc",
                "--no-owner",
                stdout=out,
                stderr=asyncio.subprocess.PIPE,
                env=_pg_env(settings.database_url),
            )
        except OSError as exc:
            raise BackupFailed("PG_DUMP_FAILED", f"Không chạy được pg_dump ({type(exc).__name__}).") from exc
        try:
            _, err = await proc.communicate()
        except asyncio.CancelledError:
            proc.kill()
            raise
    if proc.returncode != 0:
        detail = (err or b"").decode(errors="replace").strip().splitlines()
        raise BackupFailed("PG_DUMP_FAILED", f"pg_dump lỗi: {(detail[-1] if detail else '')[:200]}")
    if (await asyncio.to_thread(target.stat)).st_size == 0:
        raise BackupFailed("PG_DUMP_FAILED", "pg_dump không ghi dữ liệu.")


def tar_imports(import_root: Path, target: Path) -> None:
    """`tar czf` thư mục file nhập đơn gốc (FR-02.08 a); chưa có thư mục → tgz rỗng."""
    with tarfile.open(target, "w:gz") as tar:
        if import_root.is_dir():
            tar.add(str(import_root), arcname="imports")


def stamp(at: datetime) -> str:
    return at.strftime("%Y%m%dT%H%M%SZ")


def db_object_key(at: datetime) -> str:
    return f"{DB_PREFIX}{at:%Y/%m/%d}/aicam-{stamp(at)}.dump.enc"


def imports_object_key(at: datetime) -> str:
    return f"{IMPORTS_PREFIX}{stamp(at)}.tgz.enc"


@dataclass
class _Artifact:
    kind: str  # DB_DUMP | IMPORTS
    path: Path
    object_key: str


async def _upload_artifact(
    db: AsyncSession,
    run: BackupRun,
    art: _Artifact,
    store: ObjectStore,
    settings: Settings,
    throttle: TokenBucket,
) -> transfer.Uploaded:
    sha, _size = await asyncio.to_thread(transfer.sha256_file, art.path)
    meta = {"sha256": sha, "kind": art.kind, "id": str(run.id), "relpath": art.object_key}
    key = service.current_key(settings)
    obj = BackupObject(
        kind=art.kind, run_id=run.id, object_key=art.object_key, status="UPLOADING", sha256=sha, attempts=1
    )
    db.add(obj)
    await db.commit()
    uploaded = await asyncio.to_thread(
        transfer.upload_file, store, art.object_key, art.path, key, meta, throttle.throttle, expect_sha256=sha
    )
    # Bản đã nằm trên cloud: ghi sự thật trước khi kiểm đọc lại (DEC-522) — lượt lỗi vẫn dọn được ở J-23.
    obj.status, obj.cloud_present, obj.cloud_key_fingerprint = "UPLOADED", True, uploaded.fingerprint
    obj.size_bytes, obj.encrypted_size = uploaded.plain_size, uploaded.encrypted_size
    obj.uploaded_at = clock.now()
    await db.commit()
    verified = await asyncio.to_thread(
        transfer.verify_object, store, art.object_key, service.keyring(settings)
    )
    if verified.sha256 != sha:
        raise BackupFailed("VERIFY_FAILED", "Bản sao đọc lại không khớp mã băm.")
    return uploaded


async def run_db(
    db: AsyncSession,
    settings: Settings,
    *,
    run_id: uuid.UUID | None = None,
    trigger: str = "SCHEDULE",
    store: ObjectStore | None = None,
) -> dict[str, Any]:
    """J-20 (01, 07, 13, 19 giờ VN; API-184): `pg_dump` + `tar` file nhập → mã hóa luồng + tải lên → tải lại,
    giải mã, so SHA-256 bản rõ (DEC-466) → `SUCCESS` + `key_fingerprint`. Lỗi → `FAILED` + `error`. Luôn xóa
    file tạm. Ngân sách `BACKUP_DB_BUDGET_S`."""
    cfg = await settings_service.get(db)
    st = service.state(cfg, settings)
    mbps = cfg.backup_upload_mbps
    run: BackupRun | None
    if run_id is not None:
        run = await db.get(BackupRun, run_id)
        if run is None or run.status != "RUNNING":
            await db.commit()
            return {"skipped": "NOT_RUNNING"}
        if st != service.ON:
            _finish(run, "FAILED", error_text("STATE_" + st, "Sao lưu không ở trạng thái bật."))
            await db.commit()
            return {"status": "FAILED", "state": st}
    else:
        if st != service.ON:
            await db.commit()
            return {"skipped": st}
        run = await start_run(db, trigger, None)
        if run is None:
            return {"skipped": "RUNNING"}
    store = store or cloud.backup_store(settings)
    started = time.monotonic()
    at = run.started_at
    tmp = settings.backup_tmp_dir / str(run.id)
    redis, throttle = make_throttle(settings, mbps)
    result: dict[str, Any]
    try:
        tmp.mkdir(parents=True, exist_ok=True)
        async with asyncio.timeout(settings.backup_db_budget_s):
            dump = _Artifact("DB_DUMP", tmp / "aicam.dump", db_object_key(at))
            imports = _Artifact("IMPORTS", tmp / "imports.tgz", imports_object_key(at))
            await pg_dump(settings, dump.path)
            await asyncio.to_thread(tar_imports, settings.import_root, imports.path)
            up_dump = await _upload_artifact(db, run, dump, store, settings, throttle)
            await _upload_artifact(db, run, imports, store, settings, throttle)
        run = await _reload(db, run.id)
        _finish(run, "SUCCESS", None)
        run.size_bytes = up_dump.encrypted_size
        run.object_key, run.imports_object_key = dump.object_key, imports.object_key
        run.key_fingerprint = up_dump.fingerprint
        await db.commit()
        result = {"status": "SUCCESS", "run_id": str(run.id), "size": up_dump.encrypted_size}
    except Exception as exc:  # mọi lỗi → FAILED có mã, không để RUNNING treo
        await db.rollback()
        code, message = _classify(exc)
        run = await _reload(db, run.id)
        _finish(run, "FAILED", error_text(code, message))
        await db.execute(
            update(BackupObject)
            .where(BackupObject.run_id == run.id, BackupObject.status == "UPLOADING")
            .values(status="FAILED", last_error=error_text(code, message))
        )
        await db.commit()
        result = {"status": "FAILED", "run_id": str(run.id), "error": code}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        redis.close()
    log.info(
        "backup_db",
        run_id=result.get("run_id"),
        status=result["status"],
        size=result.get("size"),
        duration_s=round(time.monotonic() - started, 1),
        fingerprint=run.key_fingerprint,
        error=result.get("error"),
    )
    await service.publish_updated(db, settings)
    return result


def _classify(exc: BaseException) -> tuple[str, str]:
    if isinstance(exc, BackupFailed):
        return exc.code, exc.message
    if isinstance(exc, CloudError):
        return exc.code, exc.message
    if isinstance(exc, crypto.CryptoError):
        return "VERIFY_FAILED", f"Bản sao đọc lại không giải mã được: {exc}"[:200]
    if isinstance(exc, TimeoutError):
        return "TIMEOUT", "Sao lưu DB quá ngân sách thời gian."
    log.exception("backup_db_unexpected")
    return "BACKUP_ERROR", f"Lỗi không rõ ({type(exc).__name__})."


async def _reload(db: AsyncSession, run_id: uuid.UUID) -> BackupRun:
    run = await db.scalar(
        select(BackupRun).where(BackupRun.id == run_id).execution_options(populate_existing=True)
    )
    if run is None:  # dòng lượt chạy không bao giờ bị xóa khi đang chạy (J-11 chỉ xóa > 400 ngày)
        raise RuntimeError(f"backup_run {run_id} biến mất")
    return run


def _finish(run: BackupRun, status: str, error: str | None) -> None:
    run.status, run.finished_at, run.error = status, clock.now(), error
