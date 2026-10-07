"""Sao lưu cloud — API-180..188, trạng thái `backup.state` (02 §6.2, 02a §4, ADR-010)."""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.backup.schemas import (
    BackupSettingsIn,
    BackupStatusOut,
    ConfirmKeyIn,
    DbOut,
    ErrorOut,
    EvidenceOut,
    HealthBackupOut,
    HistoryOut,
    IssueOut,
    IssuesPage,
    KeyOut,
    OldKeyOut,
    ResolutionOut,
    ResolveIn,
    ReuploadOut,
    RunNowOut,
    SettingsOut,
    StorageOut,
    TestOut,
    UserRefOut,
)
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import UNREACHABLE, CloudError
from aicam.modules.settings.models import Setting

log = structlog.get_logger()

PROBE_TIMEOUT_S = 10.0  # FR-02.17: kết quả ≤ 10 giây
_HTTP = {UNREACHABLE: 504}


def key_configured(settings: Settings) -> bool:
    return bool(settings.backup_encryption_key.strip())


def configured(settings: Settings) -> bool:
    """`configured` (API-180) — `NOT_CONFIGURED` khi thiếu `S3_*` hoặc `BACKUP_ENCRYPTION_KEY` (EX-K1)."""
    return cloud.is_configured(settings) and key_configured(settings)


def current_fingerprint(settings: Settings) -> str | None:
    """Dấu vân tay `BACKUP_ENCRYPTION_KEY` (không bao giờ trả / log khóa — FR-02.13)."""
    if not key_configured(settings):
        return None
    return crypto.fingerprint(crypto.parse_key(settings.backup_encryption_key))


def current_key(settings: Settings) -> bytes:
    return crypto.parse_key(settings.backup_encryption_key)


def keyring(settings: Settings, extra: list[bytes] | None = None) -> dict[str, bytes]:
    """Khóa giải mã: hiện tại + `BACKUP_OLD_KEYS` (+ `--key-file`) — DEC-495."""
    return crypto.keyring(settings.backup_encryption_key, settings.backup_old_keys, extra)


# `backup.state` (02 §5.2, §6.2 API-180) — ưu tiên từ trên xuống.
NOT_CONFIGURED = "NOT_CONFIGURED"
RESTORE_PENDING = "RESTORE_PENDING"
KEY_UNCONFIRMED = "KEY_UNCONFIRMED"
KEY_CHANGED = "KEY_CHANGED"
DISABLED = "DISABLED"
ON = "ON"


def state(cfg: Setting, settings: Settings) -> str:
    """Chỉ `ON` thì J-20..J-23 chạy (EX-K1, K2, K7, K8)."""
    if not configured(settings):
        return NOT_CONFIGURED
    if cfg.backup_restore_pending:
        return RESTORE_PENDING
    if not cfg.backup_confirmed_fingerprint:
        return KEY_UNCONFIRMED
    if cfg.backup_confirmed_fingerprint != current_fingerprint(settings):
        return KEY_CHANGED
    if not cfg.backup_enabled:
        return DISABLED
    return ON


def not_configured() -> AppError:
    return AppError(
        "BACKUP_NOT_CONFIGURED",
        "Chưa cấu hình kho lưu cloud hoặc khóa sao lưu — liên hệ IT (tài liệu vận hành, mục Sao lưu cloud).",
        503,
    )


async def test_connection(db: AsyncSession, settings: Settings, p: Principal) -> TestOut:
    """API-183 (FR-02.17): `store.probe()` trong 10 giây; lỗi → 502 `CLOUD_AUTH_FAILED` / `CLOUD_ERROR`, 504
    `CLOUD_UNREACHABLE`. Audit `BACKUP_TEST {ok, code}` ghi cả khi lỗi (commit trước khi trả lỗi)."""
    if not configured(settings):
        raise not_configured()
    store = cloud.backup_store(settings)
    error: CloudError | None = None
    elapsed = 0
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_S):
            elapsed = await asyncio.to_thread(store.probe)
    except TimeoutError:
        error = CloudError(UNREACHABLE)
    except CloudError as exc:
        error = exc
    except Exception as exc:  # lỗi lạ của thư viện: không 500, báo như lỗi kho lưu
        log.exception("backup_probe_failed")
        error = CloudError("CLOUD_ERROR", f"Kho lưu báo lỗi: {type(exc).__name__}")
    audit.record(db, "BACKUP_TEST", user_id=p.user_id, object_type="BACKUP", ip=p.ip,
                 data={"ok": error is None, "code": error.code if error else None,
                       "elapsed_ms": elapsed})  # fmt: skip
    await commit(db)
    if error is not None:
        log.warning("backup_probe", ok=False, code=error.code)
        raise AppError(error.code, error.message, _HTTP.get(error.code, 502))
    log.info("backup_probe", ok=True, elapsed_ms=elapsed)
    return TestOut(ok=True, elapsed_ms=elapsed)


# ---------------------------------------------------------------- số liệu chung (API-180, API-81, WS)

EVIDENCE_KINDS = ("CLIP", "SNAPSHOT")
PENDING_STATUSES = (
    "PENDING",
    "UPLOADING",
    "FAILED",
)  # `pending` không tính SOURCE_DELETED / IGNORED (DEC-496)


async def last_db_success_at(db: AsyncSession) -> datetime | None:
    return await db.scalar(
        select(func.max(BackupRun.finished_at)).where(BackupRun.kind == "DB", BackupRun.status == "SUCCESS")
    )


async def pending_count(db: AsyncSession) -> int:
    return int(
        await db.scalar(
            select(func.count())
            .select_from(BackupObject)
            .where(BackupObject.kind.in_(EVIDENCE_KINDS), BackupObject.status.in_(("PENDING", "UPLOADING")))
        )
        or 0
    )


async def publish_updated(db: AsyncSession, settings: Settings) -> None:
    """WS `backup.updated` `{state, pending, last_db_success_at}` trên `ws:admin` (02 §6.2 WS-02). Lỗi Redis /
    chưa khởi tạo → chỉ log (sự kiện là gợi ý tải lại, API-180 vẫn đúng)."""
    from aicam.modules.settings import service as settings_service  # settings → backup: import muộn
    from aicam.realtime import publish

    try:
        cfg = await settings_service.get(db)
        last = await last_db_success_at(db)
        data = {
            "state": state(cfg, settings),
            "pending": await pending_count(db),
            "last_db_success_at": clock.iso_z(last) if last else None,
        }
        await db.commit()
        await publish.to_admin("backup.updated", data)
    except Exception as exc:
        log.warning("backup_ws_publish_failed", error=type(exc).__name__)


# ---------------------------------------------------------------- API-180 trạng thái

DB_LATE_HOURS = 26  # FR-02.15
EVIDENCE_LATE = timedelta(hours=24)
HISTORY_DAYS = 14
J20_HOURS_UTC = (0, 6, 12, 18)  # beat `j20-backup-db` (01, 07, 13, 19 giờ VN)
_ERROR_MESSAGES = {
    "SOURCE_MISSING": "Không thấy tệp tại kho.",
    "LEASE_EXPIRED": "Lượt tải trước dừng giữa chừng — đã xếp tải lại.",
}


def next_db_run(now: datetime) -> datetime:
    base = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    for add in range(0, 25):
        cand = base + timedelta(hours=add)
        if cand.hour in J20_HOURS_UTC and cand > now:
            return cand
    return base + timedelta(hours=6)  # không tới đây


def parse_error(text_: str | None, at: datetime | None) -> ErrorOut | None:
    """`MÃ: lời nhắn` (DEC-655) → `{code, message, at}`."""
    if not text_ or at is None:
        return None
    code, sep, message = text_.partition(":")
    if not sep:
        return ErrorOut(code=code, message=_ERROR_MESSAGES.get(code, code), at=at)
    return ErrorOut(code=code.strip(), message=message.strip(), at=at)


@dataclass
class DbStats:
    last_success_at: datetime | None
    last_size_bytes: int | None
    running: bool
    consecutive_failures: int
    last_failed_run_id: uuid.UUID | None


async def db_stats(db: AsyncSession) -> DbStats:
    last = (
        await db.execute(
            select(BackupRun.finished_at, BackupRun.size_bytes)
            .where(BackupRun.kind == "DB", BackupRun.status == "SUCCESS")
            .order_by(BackupRun.finished_at.desc())
            .limit(1)
        )
    ).first()
    running = bool(
        await db.scalar(select(func.count()).where(BackupRun.kind == "DB", BackupRun.status == "RUNNING"))
    )
    finished = (
        await db.execute(
            select(BackupRun.id, BackupRun.status)
            .where(BackupRun.kind == "DB", BackupRun.status.in_(("SUCCESS", "FAILED")))
            .order_by(BackupRun.started_at.desc())
            .limit(10)
        )
    ).all()
    failures, second_failed = 0, None
    for run_id, status in finished:
        if status != "FAILED":
            break
        failures += 1
        if failures == 1:
            second_failed = run_id  # lượt lỗi mới nhất (dedupe N08 `backup:db2:{run_id}` — DEC-500)
    return DbStats(
        last_success_at=last[0] if last else None,
        last_size_bytes=last[1] if last else None,
        running=running,
        consecutive_failures=failures,
        last_failed_run_id=second_failed,
    )


async def evidence_stats(db: AsyncSession, now: datetime) -> EvidenceOut:
    o = BackupObject
    waiting = o.status.in_(PENDING_STATUSES)
    row = (
        await db.execute(
            select(
                func.count().filter(o.status == "UPLOADED"),
                func.count().filter(o.status.in_(("PENDING", "UPLOADING"))),
                func.count().filter(o.status == "FAILED"),
                func.min(o.created_at).filter(waiting),
                func.count().filter(waiting, o.created_at < now - EVIDENCE_LATE),
                func.count().filter(o.status == "HASH_MISMATCH"),
                func.count().filter(o.status == "IGNORED"),
                func.count().filter(o.status == "SOURCE_DELETED"),
                func.count().filter(o.status == "FAILED", o.last_error == "SOURCE_MISSING"),
            ).where(o.kind.in_(EVIDENCE_KINDS))
        )
    ).one()
    return EvidenceOut(
        uploaded=row[0],
        pending=row[1],
        failed=row[2],
        oldest_pending_at=row[3],
        late_count=row[4],
        hash_mismatch=row[5],
        ignored=row[6],
        source_deleted=row[7],
        source_missing=row[8],
    )


def db_late(cfg: Setting, stats: DbStats, now: datetime) -> tuple[bool, float | None]:
    """> 26 giờ không thành công (FR-02.15). Chưa có lượt thành công: tính từ lúc xác nhận khóa (DEC-657)."""
    ref = stats.last_success_at or cfg.backup_confirmed_at
    if ref is None:
        return False, None
    hours = (now - ref).total_seconds() / 3600
    return hours > DB_LATE_HOURS, round(hours, 1)


async def last_error(db: AsyncSession, since: datetime) -> ErrorOut | None:
    run = (
        await db.execute(
            select(BackupRun.error, BackupRun.finished_at)
            .where(BackupRun.status == "FAILED", BackupRun.finished_at >= since)
            .order_by(BackupRun.finished_at.desc())
            .limit(1)
        )
    ).first()
    obj = (
        await db.execute(
            select(BackupObject.last_error, BackupObject.updated_at)
            .where(
                BackupObject.kind.in_(EVIDENCE_KINDS),
                BackupObject.status == "FAILED",
                BackupObject.last_error.is_not(None),
                BackupObject.updated_at >= since,
            )
            .order_by(BackupObject.updated_at.desc())
            .limit(1)
        )
    ).first()
    candidates = [parse_error(r[0], r[1]) for r in (run, obj) if r is not None]
    found = [c for c in candidates if c is not None]
    return max(found, key=lambda e: e.at) if found else None


def _source_ready() -> Any:
    """Tệp nguồn còn ở kho (clip / ảnh `READY`) — điều kiện tải lại bằng khóa mới (API-187)."""
    from sqlalchemy import exists

    from aicam.modules.media.models import Clip, Snapshot

    return (
        (BackupObject.kind == "CLIP")
        & exists().where(Clip.id == BackupObject.clip_id, Clip.status == "READY")
    ) | (
        (BackupObject.kind == "SNAPSHOT")
        & exists().where(Snapshot.id == BackupObject.snapshot_id, Snapshot.status == "READY")
    )


def _reuploadable(current: str) -> Any:
    return (
        (BackupObject.status == "UPLOADED")
        & BackupObject.cloud_present.is_(True)
        & BackupObject.kind.in_(EVIDENCE_KINDS)
        & (BackupObject.cloud_key_fingerprint != current)
        & _source_ready()
    )


async def old_keys(db: AsyncSession, settings: Settings) -> list[OldKeyOut]:
    """EX-K7 (DEC-495, 522): mỗi dấu vân tay ≠ khóa hiện tại còn bản trên cloud — bằng chứng `cloud_present`
    (**bất kể `status`**: dòng đang tải lại vẫn đếm tới khi J-22 ghi đè xong) + bản DB `SUCCESS` chưa xóa;
    `reuploadable` = dòng `UPLOADED` có tệp còn ở kho."""
    current = current_fingerprint(settings)
    if current is None:
        return []
    o = BackupObject
    ev = (
        await db.execute(
            select(
                o.cloud_key_fingerprint,
                func.count(),
                func.count().filter(_reuploadable(current)),
                func.coalesce(func.sum(o.size_bytes).filter(_reuploadable(current)), 0),
            )
            .where(o.cloud_present.is_(True), o.kind.in_(EVIDENCE_KINDS), o.cloud_key_fingerprint != current)
            .group_by(o.cloud_key_fingerprint)
        )
    ).all()
    runs = (
        await db.execute(
            select(BackupRun.key_fingerprint, func.count())
            .where(
                BackupRun.status == "SUCCESS",
                BackupRun.cloud_deleted_at.is_(None),
                BackupRun.key_fingerprint.is_not(None),
                BackupRun.key_fingerprint != current,
            )
            .group_by(BackupRun.key_fingerprint)
        )
    ).all()
    out: dict[str, OldKeyOut] = {}
    for fp, n, k, b in ev:
        if fp is None:
            continue
        out[fp] = OldKeyOut(
            fingerprint=fp, evidence_objects=n, db_runs=0, reuploadable=k, reuploadable_bytes=int(b or 0)
        )
    for fp, n in runs:
        if fp is None:
            continue
        item = out.setdefault(
            fp, OldKeyOut(fingerprint=fp, evidence_objects=0, db_runs=0, reuploadable=0, reuploadable_bytes=0)
        )
        item.db_runs = n
    return sorted(out.values(), key=lambda k: k.fingerprint)


async def reupload_old_key(db: AsyncSession, p: Principal, settings: Settings) -> ReuploadOut:
    """API-187 (EX-K7, DEC-522): `state = ON` (else 409 / 503); một `UPDATE` xếp lại tệp còn ở kho đang mã hóa
    bằng khóa cũ → `PENDING` — **không** đụng `cloud_present` / `cloud_key_fingerprint` (bản cũ còn tới khi
    J-22
    ghi đè cùng `object_key`). Idempotent. Audit `BACKUP_REUPLOAD_OLD_KEY {fingerprints, queued}`."""
    from sqlalchemy import update

    from aicam.modules.settings import service as settings_service

    st = state(await settings_service.get(db), settings)
    if st != ON:
        raise _state_error(st)
    current = current_fingerprint(settings) or ""
    now = clock.now()
    rows = (
        await db.execute(
            update(BackupObject)
            .where(_reuploadable(current))
            .values(status="PENDING", attempts=0, next_attempt_at=now, last_error=None, updated_at=now)
            .returning(BackupObject.id, BackupObject.size_bytes, BackupObject.cloud_key_fingerprint)
        )
    ).all()
    total = sum(int(r[1] or 0) for r in rows)
    fps = sorted({r[2] for r in rows if r[2]})
    audit.record(db, "BACKUP_REUPLOAD_OLD_KEY", user_id=p.user_id, object_type="BACKUP", ip=p.ip,
                 data={"fingerprints": fps, "queued": len(rows), "bytes": total})  # fmt: skip
    _publish_after_commit(db, settings)
    await commit(db)
    return ReuploadOut(queued=len(rows), bytes=total)


async def _user_ref(db: AsyncSession, user_id: uuid.UUID | None) -> UserRefOut | None:
    from aicam.modules.users.queries import get_user_ref

    if user_id is None:
        return None
    ref = await get_user_ref(db, user_id)
    return UserRefOut(id=ref.id, display_name=ref.display_name) if ref else None


async def _all_pack_estimate(db: AsyncSession, now: datetime) -> float:
    """FR-02.18: trung bình 7 ngày dung lượng clip phiên PACK `COMPLETED` (GB / ngày)."""
    from aicam.modules.media.models import Clip
    from aicam.modules.sessions.models import PackSession

    total = await db.scalar(
        select(func.coalesce(func.sum(Clip.size_bytes), 0))
        .join(PackSession, PackSession.id == Clip.session_id)
        .where(
            PackSession.type == "PACK",
            PackSession.status == "COMPLETED",
            PackSession.ended_at >= now - timedelta(days=7),
        )
    )
    return round(int(total or 0) / 7 / 1e9, 1)


async def status(db: AsyncSession, settings: Settings) -> BackupStatusOut:
    """API-180 (FR-02.15, 02.17, 02.18)."""
    from aicam.modules.settings import service as settings_service

    now = clock.now()
    cfg = await settings_service.get(db)
    st = state(cfg, settings)
    is_configured = st != NOT_CONFIGURED
    stats = await db_stats(db)
    late, hours = db_late(cfg, stats, now)
    cloud_bytes = await db.scalar(
        select(func.coalesce(func.sum(BackupObject.encrypted_size), 0)).where(BackupObject.cloud_present)
    )
    history = (
        await db.scalars(
            select(BackupRun)
            .where(BackupRun.started_at >= now - timedelta(days=HISTORY_DAYS))
            .order_by(BackupRun.started_at.desc())
        )
    ).all()
    out = BackupStatusOut(
        configured=is_configured,
        storage=StorageOut(
            endpoint_host=cloud.endpoint_host(settings), bucket=cloud.backup_store(settings).bucket
        )
        if is_configured
        else None,
        key=KeyOut(
            configured=key_configured(settings),
            fingerprint=current_fingerprint(settings) if is_configured else None,
            confirmed_fingerprint=cfg.backup_confirmed_fingerprint,
            confirmed_at=cfg.backup_confirmed_at,
            confirmed_by=await _user_ref(db, cfg.backup_confirmed_by),
            old_keys=await old_keys(db, settings) if is_configured else [],
        ),
        state=st,
        enabled=cfg.backup_enabled,
        db=DbOut(
            last_success_at=stats.last_success_at,
            last_size_bytes=stats.last_size_bytes,
            next_run_at=next_db_run(now) if st == ON else None,
            hours_since_success=hours,
            late=late,
            running=stats.running,
            consecutive_failures=stats.consecutive_failures,
        ),
        evidence=await evidence_stats(db, now),
        cloud_bytes=int(cloud_bytes or 0),
        last_error=await last_error(db, now - timedelta(days=HISTORY_DAYS)),
        settings=SettingsOut(
            upload_mbps=cfg.backup_upload_mbps,
            all_pack_clips=cfg.backup_all_pack_clips,
            all_pack_clips_estimate_gb_per_day=await _all_pack_estimate(db, now),
        ),
        history=[
            HistoryOut(
                id=r.id,
                kind="DB",
                started_at=r.started_at,
                finished_at=r.finished_at,
                status=r.status,
                size_bytes=r.size_bytes,
                error=r.error,
                key_fingerprint=r.key_fingerprint,
            )
            for r in history
        ],
    )
    return out


# ---------------------------------------------------------------- API-181 / 182 / 184


def _state_error(st: str) -> AppError:
    if st == NOT_CONFIGURED:
        return not_configured()
    if st == RESTORE_PENDING:
        return AppError(
            "BACKUP_RESTORE_UNVERIFIED",
            "Hệ thống vừa được khôi phục. Sao lưu tạm dừng tới khi IT chạy lệnh kiểm khôi phục đạt.",
            409,
        )
    if st in (KEY_UNCONFIRMED, KEY_CHANGED):
        return AppError(
            "BACKUP_KEY_UNCONFIRMED", "Chưa xác nhận đã cất bản sao khóa giải mã (khóa hiện tại).", 409
        )
    return AppError("BACKUP_DISABLED", "Sao lưu đang tắt. Bật sao lưu rồi thử lại.", 409)


async def _locked_setting(db: AsyncSession) -> Setting:
    from aicam.modules.settings import service as settings_service

    row = await db.scalar(
        select(Setting).where(Setting.id == 1).with_for_update().execution_options(populate_existing=True)
    )
    return row if row is not None else await settings_service.get(db)


def _publish_after_commit(db: AsyncSession, settings: Settings) -> None:
    from aicam.core.db import after_commit

    after_commit(db, lambda: publish_updated(db, settings))


async def update_settings(
    db: AsyncSession, data: BackupSettingsIn, p: Principal, settings: Settings
) -> BackupStatusOut:
    """API-181: bật / tắt, tốc độ tải, sao lưu mọi clip đóng gói. Bật cần khóa đã xác nhận đúng dấu vân tay
    (`state ∈ {ON, DISABLED}`); `RESTORE_PENDING` → 409 `BACKUP_RESTORE_UNVERIFIED`."""
    if not configured(settings):
        raise not_configured()
    cfg = await _locked_setting(db)
    before = {"enabled": cfg.backup_enabled, "upload_mbps": cfg.backup_upload_mbps,
              "all_pack_clips": cfg.backup_all_pack_clips}  # fmt: skip
    if data.enabled is True:
        st = state(cfg, settings)
        if st not in (ON, DISABLED):
            raise _state_error(st)
    if data.enabled is not None:
        cfg.backup_enabled = data.enabled
    if data.upload_mbps is not None:
        cfg.backup_upload_mbps = data.upload_mbps
    if data.all_pack_clips is not None:
        cfg.backup_all_pack_clips = data.all_pack_clips
    after = {"enabled": cfg.backup_enabled, "upload_mbps": cfg.backup_upload_mbps,
             "all_pack_clips": cfg.backup_all_pack_clips}  # fmt: skip
    audit.record(db, "BACKUP_SETTINGS_UPDATE", user_id=p.user_id, object_type="SETTING", object_id="1",
                 ip=p.ip, data={"before": before, "after": after})  # fmt: skip
    await db.flush()
    out = await status(db, settings)
    _publish_after_commit(db, settings)
    await commit(db)
    return out


async def confirm_key(
    db: AsyncSession, data: ConfirmKeyIn, p: Principal, settings: Settings
) -> BackupStatusOut:
    """API-182 (FR-02.17): dấu vân tay gửi lên phải khớp khóa trên máy chủ → lưu xác nhận, bật sao lưu (trừ
    khi
    đang `RESTORE_PENDING`); audit `BACKUP_KEY_CONFIRM {fingerprint, previous_fingerprint}`."""
    if not configured(settings):
        raise not_configured()
    current = current_fingerprint(settings)
    if data.fingerprint.strip().upper() != current:
        raise AppError(
            "BACKUP_KEY_MISMATCH", "Khóa trên máy chủ vừa đổi — kiểm lại dấu vân tay.", 409,
            {"fingerprint": current},
        )  # fmt: skip
    cfg = await _locked_setting(db)
    previous = cfg.backup_confirmed_fingerprint
    cfg.backup_confirmed_fingerprint, cfg.backup_confirmed_at, cfg.backup_confirmed_by = (
        current,
        clock.now(),
        p.user_id,
    )
    if not cfg.backup_restore_pending:
        cfg.backup_enabled = True
    audit.record(db, "BACKUP_KEY_CONFIRM", user_id=p.user_id, object_type="SETTING", object_id="1", ip=p.ip,
                 data={"fingerprint": current, "previous_fingerprint": previous})  # fmt: skip
    await db.flush()
    out = await status(db, settings)
    _publish_after_commit(db, settings)
    await commit(db)
    return out


RUN_DB_TASK = "backup.run_db"


async def run_now(db: AsyncSession, p: Principal, settings: Settings) -> RunNowOut:
    """API-184: `state = ON` (else 409 / 503); `fail_stale_runs` (DEC-500); INSERT `RUNNING` (đang chạy → 409
    `BACKUP_RUNNING`); sau commit gửi J-20 với `run_id`; audit `BACKUP_RUN_NOW`."""
    from sqlalchemy.exc import IntegrityError

    from aicam.core.db import after_commit
    from aicam.modules.backup.jobs import fail_stale_runs
    from aicam.modules.media import jobs as task_jobs
    from aicam.modules.settings import service as settings_service

    st = state(await settings_service.get(db), settings)
    if st != ON:
        raise _state_error(st)
    await fail_stale_runs(db)
    run = BackupRun(
        kind="DB", trigger="MANUAL", status="RUNNING", started_at=clock.now(), created_by=p.user_id
    )
    try:
        async with db.begin_nested():
            db.add(run)
            await db.flush()
    except IntegrityError as exc:
        raise AppError("BACKUP_RUNNING", "Đang sao lưu, thử lại sau.", 409) from exc
    audit.record(db, "BACKUP_RUN_NOW", user_id=p.user_id, object_type="BACKUP_RUN", object_id=run.id, ip=p.ip)
    run_id = run.id

    async def _send() -> None:
        await task_jobs.send(RUN_DB_TASK, [str(run_id), "MANUAL"], "backup")

    after_commit(db, _send)
    _publish_after_commit(db, settings)
    await commit(db)
    return RunNowOut(run_id=run_id)


# ---------------------------------------------------------------- API-185 danh sách vấn đề


def issue_filter(kind: str | None, include_resolved: bool) -> Any:
    """API-185: `HASH_MISMATCH` (chưa xử lý = trạng thái `HASH_MISMATCH`); `SOURCE_MISSING` = `FAILED` +
    `last_error = SOURCE_MISSING` (từ lần đầu — DEC-517); `UPLOAD_FAILED` = `FAILED` khác, `attempts ≥ 3`.
    "Chưa xử lý" theo **trạng thái** (Thử lại mà vẫn thiếu tệp → hiện lại — DEC-657). `include_resolved` thêm
    dòng đã có `resolution` cùng loại."""
    o = BackupObject
    missing = (o.status == "FAILED") & (o.last_error == "SOURCE_MISSING")
    hash_ = o.status == "HASH_MISMATCH"
    failed = (
        (o.status == "FAILED")
        & ((o.last_error.is_(None)) | (o.last_error != "SOURCE_MISSING"))
        & (o.attempts >= 3)
    )
    resolved_hash = o.resolution_action.in_(("UPLOAD_ANYWAY", "IGNORE")) & o.sha256_actual.is_not(None)
    resolved_missing = o.resolution_action.is_not(None) & (o.last_error == "SOURCE_MISSING")
    if include_resolved:
        hash_ = hash_ | resolved_hash
        missing = missing | resolved_missing
    if kind == "HASH_MISMATCH":
        return hash_
    if kind == "SOURCE_MISSING":
        return missing
    if kind == "UPLOAD_FAILED":
        return failed
    return hash_ | missing | failed


def _detail(obj: BackupObject) -> str | None:
    if obj.status == "HASH_MISMATCH" or (
        obj.sha256_actual and obj.resolution_action in ("UPLOAD_ANYWAY", "IGNORE")
    ):
        return "Mã băm tệp khác lúc tạo — không được sao lưu."
    err = parse_error(obj.last_error, obj.updated_at)
    return err.message if err else None


async def issue_out(db: AsyncSession, obj: BackupObject) -> IssueOut:
    from aicam.modules.media.models import Clip, Snapshot
    from aicam.modules.orders.models import Package
    from aicam.modules.sessions.models import PackSession

    model: Any = Clip if obj.kind == "CLIP" else Snapshot
    source_id = obj.clip_id if obj.kind == "CLIP" else obj.snapshot_id
    row = (
        await db.execute(
            select(model.sha256, PackSession.id, Package.id, Package.tracking_number)
            .join(PackSession, PackSession.id == model.session_id)
            .join(Package, Package.id == PackSession.package_id)
            .where(model.id == source_id)
        )
    ).first()
    resolution = None
    if obj.resolution_action and obj.resolved_at:
        resolution = ResolutionOut(
            action=obj.resolution_action,
            note=obj.resolution_note,
            by=await _user_ref(db, obj.resolved_by),
            at=obj.resolved_at,
        )
    return IssueOut(
        object_id=obj.id,
        kind=obj.kind,
        status=obj.status,
        session_id=row[1] if row else None,
        package_id=row[2] if row else None,
        tracking_number=row[3] if row else None,
        detected_at=obj.updated_at,
        detail=_detail(obj),
        attempts=obj.attempts,
        sha256_expected=row[0] if row else obj.sha256,
        sha256_actual=obj.sha256_actual,
        resolution=resolution,
    )


async def issues(
    db: AsyncSession, kind: str | None, include_resolved: bool, page: int, page_size: int
) -> IssuesPage:
    """API-185 (EX-K6, EX-K9)."""
    where = (BackupObject.kind.in_(EVIDENCE_KINDS)) & issue_filter(kind, include_resolved)
    total = int(await db.scalar(select(func.count()).select_from(BackupObject).where(where)) or 0)
    rows = (
        await db.scalars(
            select(BackupObject)
            .where(where)
            .order_by(BackupObject.updated_at.desc(), BackupObject.id)
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    ).all()
    return IssuesPage(
        items=[await issue_out(db, r) for r in rows], page=page, page_size=page_size, total=total
    )


# ---------------------------------------------------------------- API-81, API-32


async def health_summary(db: AsyncSession, settings: Settings) -> HealthBackupOut:
    """API-81 `backup` (D8): `late` = DB > 26 giờ / tệp chờ > 24 giờ / lệch mã băm chưa xử lý."""
    from aicam.modules.settings import service as settings_service

    now = clock.now()
    cfg = await settings_service.get(db)
    st = state(cfg, settings)
    stats = await db_stats(db)
    ev = await evidence_stats(db, now)
    late, _ = db_late(cfg, stats, now)
    return HealthBackupOut(
        state=st,
        last_db_success_at=stats.last_success_at,
        pending=ev.pending,
        late=st == ON and (late or ev.late_count > 0 or ev.hash_mismatch > 0),
        last_error=await last_error(db, now - timedelta(days=HISTORY_DAYS)),
    )


async def stale_attention(db: AsyncSession, settings: Settings) -> list[dict[str, Any]]:
    """API-32 `BACKUP_STALE` (chỉ ADMIN — lọc sau cache): một mục mỗi lý do đang có (`DB_LATE` + `hours`,
    `DB_FAILED_TWICE`, `EVIDENCE_LATE` / `HASH_MISMATCH` / `SOURCE_MISSING` + `count`). Chỉ khi sao lưu đang
    chạy hoặc dừng vì khóa đổi (`ON`, `KEY_CHANGED`) — đã tắt / chưa cấu hình / chờ kiểm khôi phục có banner
    D23 riêng (DEC-657)."""
    from aicam.modules.settings import service as settings_service

    now = clock.now()
    cfg = await settings_service.get(db)
    if state(cfg, settings) not in (ON, KEY_CHANGED):
        return []
    stats = await db_stats(db)
    ev = await evidence_stats(db, now)
    items: list[dict[str, Any]] = []
    late, hours = db_late(cfg, stats, now)
    if late:
        items.append({"kind": "BACKUP_STALE", "reason": "DB_LATE", "hours": int(hours or 0)})
    if stats.consecutive_failures >= 2:  # DEC-500
        items.append(
            {"kind": "BACKUP_STALE", "reason": "DB_FAILED_TWICE", "count": stats.consecutive_failures}
        )
    for reason, count in (
        ("EVIDENCE_LATE", ev.late_count),
        ("HASH_MISMATCH", ev.hash_mismatch),
        ("SOURCE_MISSING", ev.source_missing),
    ):
        if count:
            items.append({"kind": "BACKUP_STALE", "reason": reason, "count": count})
    return items


# ---------------------------------------------------------------- API-188 xử lý vấn đề

ACTION_INVALID_MESSAGES = {
    "UPLOAD_ANYWAY": '"Vẫn sao lưu" chỉ dùng cho tệp lệch mã băm.',
    "RETRY": '"Thử lại ngay" chỉ dùng cho tệp không thấy tại kho.',
}


def _note_error() -> AppError:
    message = "Nhập lý do (5–500 ký tự)."
    return AppError("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", 422, {"fields": {"note": message}})


async def _source_file_exists(db: AsyncSession, obj: BackupObject, settings: Settings) -> bool:
    from aicam.modules.backup.jobs import _file_of, _source

    source = await _source(db, obj)
    if source is None:
        return False
    return await asyncio.to_thread(_file_of, settings, source) is not None


async def resolve_issue(
    db: AsyncSession, object_id: uuid.UUID, data: ResolveIn, p: Principal, settings: Settings
) -> IssueOut:
    """API-188 (EX-K6, EX-K9; DEC-496, 517): `UPLOAD_ANYWAY` (chỉ `HASH_MISMATCH`) → `PENDING` +
    `hash_override`;
    `RETRY` (chỉ `FAILED SOURCE_MISSING`) → thử lại ngay; `IGNORE` (cả hai) → `IGNORED` (cuối). Không còn
    là vấn
    đề → 409 `BACKUP_ISSUE_RESOLVED`; sai cặp → 409 `BACKUP_ISSUE_ACTION_INVALID`. Lý do 5–500 + audit."""
    from sqlalchemy.exc import NoResultFound

    note = data.note.strip()
    if not 5 <= len(note) <= 500:
        raise _note_error()
    try:
        obj = (
            await db.execute(
                select(BackupObject)
                .where(BackupObject.id == object_id, BackupObject.kind.in_(EVIDENCE_KINDS))
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one()
    except NoResultFound as exc:
        raise AppError("NOT_FOUND", "Không tìm thấy tệp sao lưu.", 404) from exc
    is_hash = obj.status == "HASH_MISMATCH"
    is_missing = obj.status == "FAILED" and obj.last_error == "SOURCE_MISSING"
    if not (is_hash or is_missing):
        raise AppError("BACKUP_ISSUE_RESOLVED", "Tệp này đã được xử lý hoặc không còn lỗi.", 409,
                       {"status": obj.status})  # fmt: skip
    if (data.action == "UPLOAD_ANYWAY" and not is_hash) or (data.action == "RETRY" and not is_missing):
        raise AppError("BACKUP_ISSUE_ACTION_INVALID", ACTION_INVALID_MESSAGES[data.action], 409)
    if data.action == "IGNORE" and is_missing and await _source_file_exists(db, obj, settings):
        raise AppError("BACKUP_ISSUE_ACTION_INVALID", "Tệp đã có lại tại kho — bấm Thử lại ngay.", 409)
    now = clock.now()
    before = {"status": obj.status, "last_error": obj.last_error}
    if data.action == "UPLOAD_ANYWAY":
        obj.status, obj.hash_override, obj.attempts, obj.next_attempt_at = "PENDING", True, 0, now
    elif data.action == "RETRY":
        obj.attempts, obj.next_attempt_at = 0, now
    else:
        obj.status = "IGNORED"
    obj.resolution_action, obj.resolution_note, obj.resolved_by, obj.resolved_at = (
        data.action,
        note,
        p.user_id,
        now,
    )
    obj.updated_at = now
    expected = (await issue_out(db, obj)).sha256_expected
    data_ = {
        "object_id": str(obj.id),
        "action": data.action,
        "note": note,
        "sha256_expected": expected,
        "sha256_actual": obj.sha256_actual,
        "last_error": before["last_error"],
        "status_before": before["status"],
    }
    audit.record(db, "BACKUP_ISSUE_RESOLVE", user_id=p.user_id, object_type="BACKUP_OBJECT", object_id=obj.id,
                 ip=p.ip, data=data_)  # fmt: skip
    await db.flush()
    out = await issue_out(db, obj)
    _publish_after_commit(db, settings)
    await commit(db)
    log.info("backup_issue_resolved", object_id=str(obj.id), action=data.action)
    return out
