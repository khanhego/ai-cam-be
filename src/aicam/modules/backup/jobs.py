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
from sqlalchemy import select, text, update
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


def pg_env(database_url: str) -> dict[str, str]:
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
                env=pg_env(settings.database_url),
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
    if type(exc).__module__.startswith("redis"):
        return "REDIS_UNAVAILABLE", "Không kết nối được Redis (giới hạn tốc độ tải) — thử lại lượt sau."
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


# ---------------------------------------------------------------- J-21 xếp hàng bằng chứng (BR-33)

EVIDENCE_PREFIX = "backup/evidence/"
CLIP_PREFIX = f"{EVIDENCE_PREFIX}clips/"
SNAPSHOT_PREFIX = f"{EVIDENCE_PREFIX}snapshots/"


def evidence_key(kind: str, object_id: uuid.UUID | str) -> str:
    return f"{CLIP_PREFIX if kind == 'CLIP' else SNAPSHOT_PREFIX}{object_id}.enc"


async def _clip_cutoff(db: AsyncSession, settings: Settings, now: datetime) -> datetime:
    from aicam.modules.media import service as media

    return now - timedelta(days=await media.retention_days(db, settings))


def _insert_targets(kind: str, targets: Any, reason: str, now: datetime) -> Any:
    """`INSERT … SELECT … ON CONFLICT (clip_id|snapshot_id) WHERE … IS NOT NULL` — vị từ literal (DEC-362).
    Dòng đã có mà nay thành bằng chứng (`ALL_PACK` → `EVIDENCE`) được nâng `reason` để J-22 ưu tiên."""
    from sqlalchemy import func, literal
    from sqlalchemy.dialects.postgresql import insert

    col = "clip_id" if kind == "CLIP" else "snapshot_id"
    prefix = CLIP_PREFIX if kind == "CLIP" else SNAPSHOT_PREFIX
    sub = targets.subquery()
    select_rows = select(
        func.gen_random_uuid(),
        literal(kind),
        sub.c.id,
        literal(prefix) + func.cast(sub.c.id, BackupObject.object_key.type) + literal(".enc"),
        literal("PENDING"),
        sub.c.sha256,
        sub.c.size_bytes,
        literal(0),
        literal(now),
        literal(reason),
        literal(False),
        literal(now),
        literal(now),
    )
    columns = [
        "id", "kind", col, "object_key", "status", "sha256", "size_bytes", "attempts", "next_attempt_at",
        "reason", "cloud_present", "created_at", "updated_at",
    ]  # fmt: skip
    stmt = insert(BackupObject).from_select(columns, select_rows)
    excluded = stmt.excluded
    return stmt.on_conflict_do_update(
        index_elements=[col],
        index_where=text(f"{col} IS NOT NULL"),
        set_={"reason": excluded.reason},
        where=(BackupObject.reason != "EVIDENCE") & (excluded.reason == "EVIDENCE"),
    )


async def enqueue_evidence(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    """J-21 (10 phút): xếp `backup_object PENDING` cho bằng chứng cần giữ chưa có (BR-33) + (C) clip đóng gói
    khi bật `backup_all_pack_clips` (FR-02.18). Idempotent."""
    from aicam.modules.media import protection

    cfg = await settings_service.get(db)
    st = service.state(cfg, settings)
    if st != service.ON:
        await db.commit()
        return {"skipped": st}
    now = clock.now()
    cutoff = await _clip_cutoff(db, settings, now)
    counts: dict[str, Any] = {}
    jobs_ = [
        ("clips", "CLIP", protection.evidence_clip_targets(now, cutoff), "EVIDENCE"),
        ("snapshots", "SNAPSHOT", protection.evidence_snapshot_targets(now, cutoff), "EVIDENCE"),
    ]
    if cfg.backup_all_pack_clips:
        jobs_.append(("all_pack", "CLIP", protection.all_pack_clip_targets(), "ALL_PACK"))
    for name, kind, targets, reason in jobs_:
        result = await db.execute(_insert_targets(kind, targets, reason, now))
        counts[name] = result.rowcount or 0  # type: ignore[attr-defined]
    await db.commit()
    log.info("backup_enqueue", **counts)
    return counts


# ---------------------------------------------------------------- J-22 tải bằng chứng

BACKOFF_MIN = (5, 15, 60)  # 02a J-22: 5, 15, 60 phút rồi mỗi 60 phút
LEASE_ERROR = "LEASE_EXPIRED"
SOURCE_MISSING = "SOURCE_MISSING"
HEARTBEAT_S = 30.0


def next_attempt(now: datetime, attempts: int) -> datetime:
    return now + timedelta(minutes=BACKOFF_MIN[min(max(attempts, 1), len(BACKOFF_MIN)) - 1])


async def expire_leases(db: AsyncSession, settings: Settings, now: datetime) -> int:
    """Lease (02a §6): `UPLOADING` không nhịp quá ngân sách + 60 giây (worker chết / mất điện) → `FAILED`
    `LEASE_EXPIRED`, thử lại ngay."""
    result = await db.execute(
        update(BackupObject)
        .where(
            BackupObject.status == "UPLOADING",
            BackupObject.kind.in_(service.EVIDENCE_KINDS),
            BackupObject.updated_at < now - timedelta(seconds=settings.backup_upload_budget_s + 60),
        )
        .values(
            status="FAILED",
            attempts=BackupObject.attempts + 1,
            next_attempt_at=now,
            last_error=LEASE_ERROR,
            updated_at=now,
        )
        .returning(BackupObject.id)
    )
    ids = result.scalars().all()
    for object_id in ids:
        log.warning("backup_lease_expired", object_id=str(object_id))
    await db.commit()
    return len(ids)


def _priority() -> list[Any]:
    """Hồ sơ khiếu nại chưa đóng trước (02a J-21 / J-22), rồi bằng chứng trước clip `ALL_PACK`, rồi cũ
    trước."""
    from sqlalchemy import case, exists

    from aicam.modules.claims.models import Claim, ClaimEvidence
    from aicam.modules.media.models import Clip, Snapshot

    open_claim = exists().where(
        ClaimEvidence.claim_id == Claim.id,
        Claim.status != "CLOSED",
        ClaimEvidence.removed_at.is_(None),
        (
            (ClaimEvidence.session_id == select(Clip.session_id).where(Clip.id == BackupObject.clip_id)
             .correlate(BackupObject).scalar_subquery())
            | (ClaimEvidence.session_id == select(Snapshot.session_id)
               .where(Snapshot.id == BackupObject.snapshot_id).correlate(BackupObject).scalar_subquery())
            | (ClaimEvidence.snapshot_id == BackupObject.snapshot_id)
        ),
    )  # fmt: skip
    return [
        case((open_claim, 0), else_=1),
        case((BackupObject.reason == "EVIDENCE", 0), else_=1),
        BackupObject.created_at,
    ]


async def _claim_next(db: AsyncSession, now: datetime) -> BackupObject | None:
    """Nhận **một** dòng đến hạn (`FOR UPDATE SKIP LOCKED`) → `UPLOADING` + `updated_at` → commit. Nhận
    từng dòng
    (không giữ cả lô 20 ở `UPLOADING`) để dừng giữa chừng không bỏ lại dòng chưa xử lý (DEC-656)."""
    obj = await db.scalar(
        select(BackupObject)
        .where(
            BackupObject.kind.in_(service.EVIDENCE_KINDS),
            BackupObject.status.in_(("PENDING", "FAILED")),
            (BackupObject.next_attempt_at.is_(None)) | (BackupObject.next_attempt_at <= now),
        )
        .order_by(*_priority())
        .limit(1)
        .with_for_update(skip_locked=True, of=BackupObject)
        .execution_options(populate_existing=True)
    )
    if obj is None:
        await db.commit()
        return None
    obj.status, obj.updated_at = "UPLOADING", now
    await db.commit()
    return obj


async def _source(db: AsyncSession, obj: BackupObject) -> Any:
    from aicam.modules.media.models import Clip, Snapshot

    model: Any = Clip if obj.kind == "CLIP" else Snapshot
    source_id = obj.clip_id if obj.kind == "CLIP" else obj.snapshot_id
    return await db.scalar(
        select(model).where(model.id == source_id).execution_options(populate_existing=True)
    )


def _file_of(settings: Settings, source: Any) -> Path | None:
    from aicam.modules.media import service as media

    if not source.path:
        return None
    try:
        path = media.absolute(settings, source.path)
    except ValueError:
        return None
    return path if path.is_file() else None


async def _heartbeat(db: AsyncSession, object_id: uuid.UUID, stop: asyncio.Event) -> None:
    """Lease: cập nhật `updated_at` mỗi 30 giây khi đang tải (02a §6) — dùng chung session (luồng chính chỉ
    chờ `to_thread`, không dùng session)."""
    while True:
        try:
            await asyncio.wait_for(stop.wait(), HEARTBEAT_S)
            return
        except TimeoutError:
            await db.execute(
                update(BackupObject).where(BackupObject.id == object_id).values(updated_at=clock.now())
            )
            await db.commit()


def _fail(obj: BackupObject, now: datetime, error: str) -> None:
    obj.status, obj.attempts = "FAILED", obj.attempts + 1
    obj.next_attempt_at, obj.last_error, obj.updated_at = next_attempt(now, obj.attempts), error, now


async def _upload_one(
    db: AsyncSession, obj: BackupObject, settings: Settings, store: ObjectStore, throttle: TokenBucket
) -> str:
    """Một dòng `UPLOADING`: phân loại nguồn → kiểm mã băm → mã hóa + tải → `UPLOADED` + `cloud_present`."""
    source = await _source(db, obj)
    now = clock.now()
    if source is None or source.status == "DELETED":
        # Nguồn đã bị xóa (retention / mất dòng) trước khi tải — trạng thái cuối, không tính chờ (DEC-496).
        obj.status, obj.updated_at, obj.last_error = "SOURCE_DELETED", now, None
        await db.commit()
        return "SOURCE_DELETED"
    path = await asyncio.to_thread(_file_of, settings, source)
    if path is None:
        source = await _source(db, obj)  # J-02 có thể vừa xóa giữa hai bước — đọc lại để phân loại đúng
        if source is None or source.status == "DELETED":
            obj.status, obj.updated_at, obj.last_error = "SOURCE_DELETED", now, None
            await db.commit()
            return "SOURCE_DELETED"
        first = obj.last_error != SOURCE_MISSING
        _fail(obj, now, SOURCE_MISSING)
        await db.commit()
        log.warning(
            "backup_source_missing", object_id=str(obj.id), kind=obj.kind, attempts=obj.attempts, first=first
        )
        return SOURCE_MISSING
    actual, size = await asyncio.to_thread(transfer.sha256_file, path)
    expected = source.sha256
    # "Vẫn sao lưu" (API-188 / --accept) chấp nhận **đúng nội dung** lệch đã xem (`sha256_actual`); tệp đổi
    # tiếp
    # sau quyết định → lệch mới, phải xem lại (DEC-660).
    accepted = obj.hash_override and (obj.sha256_actual is None or obj.sha256_actual == actual)
    if expected and actual != expected and not accepted:
        obj.status, obj.sha256_actual, obj.updated_at = "HASH_MISMATCH", actual, now
        obj.last_error, obj.hash_override = None, False
        obj.resolution_action = obj.resolution_note = obj.resolved_by = obj.resolved_at = None
        await db.commit()
        log.warning("backup_hash_mismatch", object_id=str(obj.id), sha_db=expected, sha_file=actual)
        return "HASH_MISMATCH"
    meta = {"sha256": actual, "kind": obj.kind, "id": str(source.id), "relpath": source.path}
    if accepted and expected and actual != expected:
        meta.update({"sha256-expected": expected, "integrity": "MISMATCH_ACCEPTED"})
    stop = asyncio.Event()
    beat = asyncio.create_task(_heartbeat(db, obj.id, stop))
    error: CloudError | None = None
    uploaded: transfer.Uploaded | None = None
    try:
        uploaded = await asyncio.to_thread(
            transfer.upload_file,
            store,
            obj.object_key,
            path,
            service.current_key(settings),
            meta,
            throttle.throttle,
            expect_sha256=actual,
        )
    except CloudError as exc:
        error = exc
    except OSError as exc:  # tệp biến mất / không đọc được giữa chừng
        error = CloudError(SOURCE_MISSING, f"Không đọc được tệp ({type(exc).__name__}).")
    finally:
        stop.set()
        await beat
    obj = await _reload_object(db, obj.id)
    now = clock.now()
    if uploaded is None:
        err = error or CloudError("CLOUD_ERROR")
        _fail(obj, now, SOURCE_MISSING if err.code == SOURCE_MISSING else error_text(err.code, err.message))
        await db.commit()
        return "FAILED"
    obj.status, obj.uploaded_at, obj.updated_at, obj.last_error = "UPLOADED", now, now, None
    obj.attempts += 1
    obj.size_bytes, obj.encrypted_size = size, uploaded.encrypted_size
    if obj.hash_override:
        obj.sha256_actual = actual
    # Sự thật trên cloud (DEC-522): ghi khi tải xong — bản cũ (khóa cũ) đã bị ghi đè cùng `object_key`.
    obj.cloud_present, obj.cloud_key_fingerprint = True, uploaded.fingerprint
    await db.commit()
    log.info(
        "backup_object",
        object_id=str(obj.id),
        kind=obj.kind,
        status="UPLOADED",
        fingerprint=uploaded.fingerprint,
    )
    return "UPLOADED"


async def _reload_object(db: AsyncSession, object_id: uuid.UUID) -> BackupObject:
    obj = await db.scalar(
        select(BackupObject).where(BackupObject.id == object_id).execution_options(populate_existing=True)
    )
    if obj is None:
        raise RuntimeError(f"backup_object {object_id} biến mất")
    return obj


async def upload_evidence(
    db: AsyncSession, settings: Settings, *, store: ObjectStore | None = None, budget_s: float | None = None
) -> dict[str, Any]:
    """J-22 (5 phút): lease → lần lượt nhận dòng đến hạn và tải (ngân sách `BACKUP_UPLOAD_BUDGET_S`); nhường
    khi có job link (`share:active`). Lỗi một dòng → `FAILED` + giãn cách, đi tiếp."""
    from aicam.modules.cloud.ratelimit import share_active

    cfg = await settings_service.get(db)
    st = service.state(cfg, settings)
    if st != service.ON:
        await db.commit()
        return {"skipped": st}
    store = store or cloud.backup_store(settings)
    started = time.monotonic()
    budget = settings.backup_upload_budget_s if budget_s is None else budget_s
    counts: dict[str, Any] = {"lease_expired": await expire_leases(db, settings, clock.now())}
    redis, throttle = make_throttle(settings, cfg.backup_upload_mbps)
    try:
        while time.monotonic() - started < budget:
            if share_active(redis):
                counts["yielded_to_share"] = True
                break
            obj = await _claim_next(db, clock.now())
            if obj is None:
                break
            try:
                outcome = await _upload_one(db, obj, settings, store, throttle)
            except Exception as exc:  # lỗi lạ một dòng: không để `UPLOADING`, đi tiếp
                await db.rollback()
                log.exception("backup_object_failed", object_id=str(obj.id))
                obj = await _reload_object(db, obj.id)
                _fail(obj, clock.now(), error_text("BACKUP_ERROR", type(exc).__name__))
                await db.commit()
                outcome = "FAILED"
            counts[outcome] = counts.get(outcome, 0) + 1
    finally:
        redis.close()
    if any(k in counts for k in ("UPLOADED", "FAILED", "HASH_MISMATCH", SOURCE_MISSING, "SOURCE_DELETED")):
        await service.publish_updated(db, settings)
    return counts


# ---------------------------------------------------------------- J-23 dọn bản cloud

DB_KEEP_DAYS = 30
DB_KEEP_MONTHS = 12
DB_KEEP_LATEST = 3  # DEC-505: luôn giữ ≥ 3 bản DB thành công mới nhất
PROBE_MAX_AGE = timedelta(days=1)
PRUNE_BUDGET_S = 1800


def retention_deleted_sql() -> Any:
    """Nguồn bị **retention** xóa (FR-02.14, DEC-499): clip `DELETED` có audit `DELETE_CLIP`
    `data.reason = 'RETENTION'` (J-02 — chỉ đường này xóa clip); ảnh `DELETED` có `deleted_at` (chỉ J-02 xóa
    ảnh). `MISSING` không bao giờ thuộc tập này."""
    from sqlalchemy import exists

    from aicam.core.audit import AuditLog
    from aicam.modules.media.models import Clip, Snapshot

    clip_ok = exists().where(
        Clip.id == BackupObject.clip_id,
        Clip.status == "DELETED",
        exists().where(
            AuditLog.action == "DELETE_CLIP",
            AuditLog.object_type == "CLIP",
            AuditLog.object_id == func_cast_text(Clip.id),
            AuditLog.data["reason"].astext == "RETENTION",
        ),
    )
    snap_ok = exists().where(
        Snapshot.id == BackupObject.snapshot_id,
        Snapshot.status == "DELETED",
        Snapshot.deleted_at.is_not(None),
    )
    return ((BackupObject.kind == "CLIP") & clip_ok) | ((BackupObject.kind == "SNAPSHOT") & snap_ok)


def func_cast_text(col: Any) -> Any:
    from sqlalchemy import Text, cast

    return cast(col, Text)


async def _prune_evidence(db: AsyncSession, store: ObjectStore, deadline: float) -> dict[str, int]:
    """(1) Bản cloud của bằng chứng bị retention xóa → delete marker → `CLOUD_DELETED` (cuối). Mỗi dòng khóa
    `SKIP LOCKED`, kiểm lại điều kiện dưới khóa; dòng `UPLOADING` bỏ qua lượt này (lease xử lý)."""
    out = {"evidence_deleted": 0, "evidence_errors": 0}
    ids = (
        await db.scalars(
            select(BackupObject.id).where(
                BackupObject.cloud_present.is_(True),
                BackupObject.status != "UPLOADING",
                BackupObject.kind.in_(service.EVIDENCE_KINDS),
                retention_deleted_sql(),
            )
        )
    ).all()
    await db.commit()
    for object_id in ids:
        if time.monotonic() > deadline:
            break
        obj = await db.scalar(
            select(BackupObject)
            .where(
                BackupObject.id == object_id,
                BackupObject.cloud_present.is_(True),
                BackupObject.status != "UPLOADING",
                retention_deleted_sql(),
            )
            .with_for_update(skip_locked=True)
            .execution_options(populate_existing=True)
        )
        if obj is None:
            await db.commit()
            continue
        try:
            await asyncio.to_thread(store.delete, obj.object_key)
        except CloudError as exc:
            await db.rollback()
            out["evidence_errors"] += 1
            log.warning("backup_prune_failed", object_id=str(object_id), code=exc.code)
            continue
        now = clock.now()
        obj.status, obj.cloud_present, obj.cloud_key_fingerprint = "CLOUD_DELETED", False, None
        obj.cloud_deleted_at, obj.updated_at = now, now
        await db.commit()
        out["evidence_deleted"] += 1
    return out


def db_runs_to_keep(runs: list[BackupRun], now: datetime, tz: str) -> set[uuid.UUID]:
    """FR-02.14 + DEC-505: `SUCCESS` < 30 ngày + lượt sớm nhất ngày 1 (giờ VN) mỗi tháng trong 12 tháng +
    luôn 3
    lượt `SUCCESS` mới nhất bất kể tuổi."""
    from zoneinfo import ZoneInfo

    zone = ZoneInfo(tz)
    ok = sorted((r for r in runs if r.status == "SUCCESS"), key=lambda r: r.started_at, reverse=True)
    keep = {r.id for r in ok[:DB_KEEP_LATEST]}
    keep |= {r.id for r in ok if r.started_at >= now - timedelta(days=DB_KEEP_DAYS)}
    monthly: dict[tuple[int, int], BackupRun] = {}
    for r in ok:
        local = r.started_at.astimezone(zone)
        if local.day == 1 and r.started_at >= now - timedelta(days=366):
            cur = monthly.get((local.year, local.month))
            if cur is None or r.started_at < cur.started_at:
                monthly[(local.year, local.month)] = r
    keep |= {r.id for r in monthly.values()}
    return keep


async def _prune_db(
    db: AsyncSession, store: ObjectStore, settings: Settings, now: datetime, deadline: float
) -> dict[str, int]:
    """(2) Bản DB ngoài chính sách → xóa đối tượng (dump + tgz) → `backup_run.cloud_deleted_at`. Lượt `FAILED`
    có bản trên cloud (lỗi sau khi tải) xóa sau 30 ngày (DEC-656)."""
    out = {"db_runs_deleted": 0, "db_errors": 0}
    runs = list(
        (
            await db.scalars(
                select(BackupRun).where(BackupRun.kind == "DB", BackupRun.cloud_deleted_at.is_(None))
            )
        ).all()
    )
    keep = db_runs_to_keep(runs, now, settings.tz_display)
    victims = [
        r
        for r in runs
        if r.id not in keep
        and (
            r.status == "SUCCESS"
            or (r.status == "FAILED" and r.started_at < now - timedelta(days=DB_KEEP_DAYS))
        )
    ]
    await db.commit()
    for run in sorted(victims, key=lambda r: r.started_at):
        if time.monotonic() > deadline:
            break
        objs = (
            await db.scalars(
                select(BackupObject)
                .where(BackupObject.run_id == run.id, BackupObject.cloud_present.is_(True))
                .with_for_update(skip_locked=True)
            )
        ).all()
        try:
            for obj in objs:
                await asyncio.to_thread(store.delete, obj.object_key)
        except CloudError as exc:
            await db.rollback()
            out["db_errors"] += 1
            log.warning("backup_prune_db_failed", run_id=str(run.id), code=exc.code)
            continue
        t = clock.now()
        for obj in objs:
            obj.status, obj.cloud_present, obj.cloud_key_fingerprint = "CLOUD_DELETED", False, None
            obj.cloud_deleted_at, obj.updated_at = t, t
        await db.execute(update(BackupRun).where(BackupRun.id == run.id).values(cloud_deleted_at=t))
        await db.commit()
        out["db_runs_deleted"] += 1
    return out


def _prune_probes(store: ObjectStore, now: datetime) -> int:
    from aicam.modules.cloud.store import PROBE_PREFIX

    n = 0
    for info in list(store.list(PROBE_PREFIX)):
        if info.last_modified < now - PROBE_MAX_AGE:
            store.delete(info.key)
            n += 1
    return n


async def prune(db: AsyncSession, settings: Settings, *, store: ObjectStore | None = None) -> dict[str, Any]:
    """J-23 (03:00 VN, sau J-02): chỉ khi `state = ON` (không chạy khi `RESTORE_PENDING`); schema guard ngay
    trước khi xóa (như J-02). Không có đường nào xóa bản cloud của bằng chứng còn cần: chỉ nguồn bị retention
    xóa (DEC-499); `MISSING` không xét."""
    from aicam.core import schema_guard

    cfg = await settings_service.get(db)
    st = service.state(cfg, settings)
    if st != service.ON:
        await db.commit()
        return {"skipped": st}
    if not await schema_guard.matches(db):
        await db.rollback()
        log.error("backup_prune_skipped_schema_mismatch")
        return {"skipped_schema_mismatch": 1}
    await db.commit()
    store = store or cloud.backup_store(settings)
    deadline = time.monotonic() + PRUNE_BUDGET_S
    now = clock.now()
    out: dict[str, Any] = {}
    out.update(await _prune_evidence(db, store, deadline))
    out.update(await _prune_db(db, store, settings, now, deadline))
    out["stale_runs"] = await fail_stale_runs(db, now)
    await db.commit()
    try:
        out["probes"] = await asyncio.to_thread(_prune_probes, store, now)
    except CloudError as exc:
        out["probes_error"] = exc.code
    log.info("backup_prune", **out)
    return out
