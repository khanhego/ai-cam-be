"""Media: clip phiên (J-01), index segment (J-10), URL ký (API-40/41), giữ clip (API-42), cắt lại (API-46).

Clip gốc bất biến (ADR-008): cắt stream copy, `chmod 0444`, SHA-256; không có API sửa / xóa (DEC-27).
"""

import os
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import after_commit, commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.media import ffmpeg, jobs, signing
from aicam.modules.media.models import Clip, VideoSegment
from aicam.modules.media.schemas import HoldOut, PlayUrlOut, RebuildOut, UserBrief
from aicam.modules.media.segments import (
    Segment,
    candidates,
    gaps,
    is_closed,
    is_incomplete,
    list_segment_files,
    parse_start,
    plan_cut,
    timeline_bounds,
)
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings import service as settings_service
from aicam.modules.stations import service as stations
from aicam.modules.stations.models import Camera
from aicam.modules.users.queries import get_user_ref

log = structlog.get_logger()

ROLES = ("CAM1", "CAM2")
FINAL_SESSION_STATUSES = ("COMPLETED", "CANCELLED", "ABANDONED", "SUPERSEDED")


# ---------------------------------------------------------------- đường dẫn


def camera_dir(settings: Settings, camera: Camera) -> Path:
    return settings.video_root / "raw" / camera.mediamtx_path


def clip_rel_path(session_id: uuid.UUID, role: str, at: datetime) -> str:
    return f"clips/{at:%Y/%m/%d}/{session_id}-{role}.mp4"


def absolute(settings: Settings, rel: str) -> Path:
    path = (settings.video_root / rel).resolve()
    if not path.is_relative_to(settings.video_root.resolve()):  # phòng đường dẫn lạ trong DB
        raise ValueError(f"Đường dẫn ngoài VIDEO_ROOT: {rel}")
    return path


def _rel(settings: Settings, path: Path) -> str:
    try:
        return path.relative_to(settings.video_root).as_posix()
    except ValueError:
        return path.as_posix()


def retention_until(clip: Clip, retention_clip_days: int) -> datetime | None:
    """DEC-30: không lưu, tính từ setting hiện tại. Clip đang giữ hoặc đã xóa → None."""
    if clip.held or clip.status == "DELETED":
        return None
    return clip.end_at + timedelta(days=retention_clip_days)


# ---------------------------------------------------------------- J-10 index segment


async def index_camera(
    db: AsyncSession, camera: Camera, settings: Settings, *, since: datetime, limit: int = 500
) -> int:
    """Đồng bộ `video_segment` của một camera với đĩa (idempotent theo (camera_id, start_at)).

    - Thêm file đã ghi xong chưa có trong index.
    - Bỏ dòng index mà file đã biến mất (MediaMTX dev tự xóa sau 1 giờ, qa-reset, xóa tay) — index không
      được trỏ tới file không tồn tại.
    """
    files = list_segment_files(camera_dir(settings, camera), since=since)
    on_disk = {f.start for f in files}
    existing = set(
        (
            await db.scalars(
                select(VideoSegment.start_at).where(
                    VideoSegment.camera_id == camera.id,
                    VideoSegment.start_at >= since - timedelta(hours=1),  # cùng phạm vi list_segment_files
                )
            )
        ).all()
    )
    vanished = existing - on_disk
    if vanished:
        await db.execute(
            delete(VideoSegment).where(
                VideoSegment.camera_id == camera.id, VideoSegment.start_at.in_(sorted(vanished))
            )
        )
        log.info("segments_vanished", camera_id=str(camera.id), count=len(vanished))
    now_ts = time.time()  # so với mtime của file (giờ hệ điều hành), không phải giờ nghiệp vụ
    added = 0
    for index, f in enumerate(files):
        if added >= limit:
            break
        if f.start in existing or not is_closed(files, index, now_ts, settings.segment_closed_after_s):
            continue
        try:
            duration = await ffmpeg.probe_duration(settings.ffprobe_bin, f.path)
            size = f.path.stat().st_size
        except (ffmpeg.FFmpegError, FileNotFoundError) as exc:
            log.warning("segment_probe_failed", path=str(f.path), error=str(exc))
            continue
        await db.execute(
            insert(VideoSegment)
            .values(
                id=uuid.uuid4(),
                camera_id=camera.id,
                start_at=f.start,
                end_at=f.start + timedelta(seconds=duration),
                path=_rel(settings, f.path),
                size_bytes=size,
            )
            .on_conflict_do_nothing(index_elements=["camera_id", "start_at"])
        )
        added += 1
    return added


async def index_segments(db: AsyncSession, settings: Settings) -> int:
    """J-10 (mỗi phút): index segment mới của mọi camera; commit từng camera."""
    cfg = await settings_service.get(db)
    since = clock.now() - timedelta(days=cfg.retention_raw_days + 1)
    cameras = (await db.scalars(select(Camera))).all()
    await db.commit()
    total = 0
    for camera in cameras:
        try:
            total += await index_camera(db, camera, settings, since=since)
            await db.commit()
        except Exception:
            await db.rollback()
            log.exception("index_segments_failed", camera_id=str(camera.id))
    return total


# ---------------------------------------------------------------- J-01 cắt clip


@dataclass
class BuildResult:
    retry_in: float | None = None
    ready: list[uuid.UUID] = field(default_factory=list)
    failed: list[uuid.UUID] = field(default_factory=list)


class _NoCamera(Exception):
    """Station không có camera vai này: lỗi cố định, FAILED ngay (không thử lại)."""


class _Retry(Exception):
    """Video chưa đủ (MediaMTX chưa ghi tới cuối khoảng): thử lại sau."""


async def _segments_for(
    db: AsyncSession, camera: Camera, t0: datetime, t1: datetime, settings: Settings
) -> tuple[list[Segment], bool]:
    """Segment giao [t0, t1] đọc thẳng từ đĩa (gồm segment đang ghi) + segment cuối đã đóng chưa.

    Không chờ J-10: segment 60 giây đang ghi vẫn đọc được (fMP4, part 1 giây) → clip sẵn sàng vài giây sau
    `ended_at + đệm` thay vì chờ segment đóng (spike S3, NFR-03).
    """
    files = list_segment_files(camera_dir(settings, camera), since=t0)
    picked = candidates(files, t0, t1)
    segments: list[Segment] = []
    for f in picked:
        try:
            duration = await ffmpeg.probe_duration(settings.ffprobe_bin, f.path)
        except ffmpeg.FFmpegError as exc:
            log.warning("segment_probe_failed", path=str(f.path), error=str(exc))
            continue
        segments.append(Segment(f.path, f.start, duration))
    last_closed = True
    if picked:
        last_closed = is_closed(files, files.index(picked[-1]), time.time(), settings.segment_closed_after_s)
    return segments, last_closed


async def _cut(
    clip: Clip, segments: list[Segment], t0: datetime, t1: datetime, settings: Settings
) -> tuple[Path, float, list[dict[str, object]]]:
    plan = plan_cut(segments, t0, t1)
    final = absolute(settings, clip_rel_path(clip.session_id, clip.camera_role, t1))
    final.parent.mkdir(parents=True, exist_ok=True)
    partial = final.with_name(final.stem + ".part.mp4")
    with tempfile.TemporaryDirectory(prefix="aicam-cut-") as tmp:
        listing = Path(tmp) / "list.txt"
        listing.write_text(ffmpeg.concat_listing([s.path for s in plan.segments]))
        await ffmpeg.run(
            ffmpeg.cut_command(settings.ffmpeg_bin, listing, plan.ss, plan.to, partial),
            settings.clip_cut_timeout_s,
        )
    duration = await ffmpeg.probe_duration(settings.ffprobe_bin, partial)
    os.chmod(partial, 0o444)  # clip gốc chỉ đọc (ADR-008, TC-02.15)
    os.replace(partial, final)
    return final, duration, plan.timeline(duration)


async def _build_one(
    db: AsyncSession, clip: Clip, camera: Camera | None, t0: datetime, t1: datetime, settings: Settings,
    *, final: bool,
) -> bool:  # fmt: skip
    """Cắt một clip. Trả True nếu thiếu video (VIDEO_INCOMPLETE). Ném `_Retry` khi chưa đủ dữ liệu."""
    if camera is None:
        raise _NoCamera(f"Station chưa cấu hình {clip.camera_role}")
    segments, last_closed = await _segments_for(db, camera, t0, t1, settings)
    if not segments:
        if not final:
            raise _Retry
        raise ffmpeg.FFmpegError("Không có video của camera trong khoảng phiên")
    if segments[-1].end < t1 and not last_closed and not final:
        raise _Retry  # segment đang ghi chưa tới cuối khoảng
    missing = gaps(segments, t0, t1)
    incomplete = is_incomplete(missing, settings.clip_gap_tolerance_s)
    path, duration, timeline = await _cut(clip, segments, t0, t1, settings)
    clip.status = "READY"
    clip.path = _rel(settings, path)
    clip.duration_s = Decimal(f"{duration:.2f}")
    clip.size_bytes = path.stat().st_size
    clip.sha256 = ffmpeg.sha256_file(path)
    clip.timeline = timeline
    clip.start_at, clip.end_at = timeline_bounds(timeline, duration)
    clip.flags = ["VIDEO_INCOMPLETE"] if incomplete else []
    clip.error = None
    if incomplete:
        log.info(
            "clip_incomplete", clip_id=str(clip.id), gaps=[(a.isoformat(), b.isoformat()) for a, b in missing]
        )
    return incomplete


async def _lock_clip(db: AsyncSession, session_id: uuid.UUID, role: str, t0: datetime, t1: datetime) -> Clip:
    """Tạo clip PENDING nếu chưa có (UNIQUE (session_id, camera_role)) rồi khóa dòng — chạy 2 lần an toàn."""
    await db.execute(
        insert(Clip)
        .values(
            id=uuid.uuid4(),
            session_id=session_id,
            camera_role=role,
            status="PENDING",
            start_at=t0,
            end_at=t1,
            flags=[],
            held=False,
        )
        .on_conflict_do_nothing(index_elements=["session_id", "camera_role"])
    )
    clip: Clip = (
        await db.scalars(
            select(Clip)
            .where(Clip.session_id == session_id, Clip.camera_role == role)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one()
    return clip


async def build_session_clips(
    db: AsyncSession, session_id: uuid.UUID, settings: Settings, *, final: bool = False
) -> BuildResult:
    """J-01: cắt clip Cam 1 + Cam 2 `[started_at − đệm, ended_at + đệm]` (FR-02.02, AC-02).

    Mỗi camera commit riêng. Thiếu đoạn → cờ `VIDEO_INCOMPLETE` cho clip và phiên.
    Lỗi ở lần thử cuối (`final`) → clip FAILED.
    """
    result = BuildResult()
    pack = await db.get(PackSession, session_id)
    if pack is None or pack.ended_at is None or pack.status not in FINAL_SESSION_STATUSES:
        log.warning("build_clips_skipped", session_id=str(session_id))
        return result
    pad = timedelta(seconds=settings.clip_padding_s)
    t0, t1 = pack.started_at - pad, pack.ended_at + pad
    wait = (t1 + timedelta(seconds=settings.clip_settle_s) - clock.now()).total_seconds()
    if wait > 0:
        result.retry_in = wait
        return result
    cameras = {c.role: c for c in await stations.cameras_of(db, pack.station_id)}
    await db.commit()
    incomplete_any = False
    for role in ROLES:
        clip = await _lock_clip(db, session_id, role, t0, t1)
        if clip.status in ("READY", "DELETED"):
            await db.commit()
            continue
        began = time.monotonic()
        try:
            incomplete_any |= await _build_one(db, clip, cameras.get(role), t0, t1, settings, final=final)
            result.ready.append(clip.id)
            log.info(
                "clip_built",
                clip_id=str(clip.id),
                session_id=str(session_id),
                role=role,
                build_s=round(time.monotonic() - began, 2),
                after_close_s=round((clock.now() - pack.ended_at).total_seconds(), 1),
            )
        except _Retry:
            result.retry_in = 10.0
        except _NoCamera as exc:
            clip.status, clip.error = "FAILED", str(exc)
            result.failed.append(clip.id)
        except (ffmpeg.FFmpegError, OSError, ValueError) as exc:
            if not final:
                log.warning("clip_build_retry", clip_id=str(clip.id), error=str(exc))
                result.retry_in = 30.0
            else:
                clip.status = "FAILED"
                clip.error = str(exc)[:500]
                result.failed.append(clip.id)
                incomplete_any |= "Không có video" in clip.error
                log.error("clip_failed", clip_id=str(clip.id), error=clip.error)
        await db.commit()
    if incomplete_any:
        await sessions.add_flag(db, session_id, "VIDEO_INCOMPLETE")
    if result.ready:
        _notify_clip_ready(db, pack.station_id, session_id, result.ready)
    await commit(db)
    return result


def _notify_clip_ready(
    db: AsyncSession, station_id: uuid.UUID, session_id: uuid.UUID, clip_ids: list[uuid.UUID]
) -> None:
    from aicam.realtime import publish

    data = {"session_id": str(session_id), "clip_ids": [str(c) for c in clip_ids]}

    async def _send() -> None:
        await publish.to_station(station_id, "session.clip_ready", data)
        await publish.to_dashboard("session.clip_ready", data)  # D4 làm mới chi tiết kiện (DEC-102)

    after_commit(db, _send)


async def sessions_missing_clips(
    db: AsyncSession, older_than: timedelta, lookback: timedelta
) -> list[uuid.UUID]:
    """J-11: phiên đã kết thúc > `older_than` mà chưa có đủ clip hoặc còn clip PENDING (job bị mất)."""
    now = clock.now()
    rows = await db.execute(
        select(PackSession.id)
        .where(
            PackSession.status.in_(FINAL_SESSION_STATUSES),
            PackSession.ended_at.is_not(None),
            PackSession.ended_at < now - older_than,
            PackSession.ended_at > now - lookback,
        )
        .where(
            select(Clip.id)
            .where(Clip.session_id == PackSession.id, Clip.status != "PENDING")
            .correlate(PackSession)
            .limit(1)
            .scalar_subquery()
            .is_(None)
            | select(Clip.id)
            .where(Clip.session_id == PackSession.id, Clip.status == "PENDING")
            .correlate(PackSession)
            .limit(1)
            .exists()
        )
        .limit(200)
    )
    return [r[0] for r in rows.all()]


# ---------------------------------------------------------------- API-40 / 41


def _clip_unavailable(clip: Clip, retention_clip_days: int) -> AppError | None:
    if clip.status == "DELETED":
        return AppError(
            "CLIP_DELETED",
            f"Clip đã bị xóa theo chính sách lưu trữ {retention_clip_days} ngày.",
            410,
            {"deleted_at": clip.deleted_at.isoformat() if clip.deleted_at else None,
             "retention_clip_days": retention_clip_days},
        )  # fmt: skip
    if clip.status != "READY":
        # FAILED dùng chung mã CLIP_NOT_READY (02 không có mã riêng), phân biệt qua details.status (DEC-102).
        return AppError(
            "CLIP_NOT_READY",
            "Clip đang được cắt, sẵn sàng trong khoảng 1 phút."
            if clip.status == "PENDING"
            else "Không cắt được clip. Quản lý có thể bấm Thử lại.",
            409,
            {"status": clip.status},
        )
    return None


async def _require_clip(db: AsyncSession, clip_id: uuid.UUID) -> Clip:
    clip = await db.get(Clip, clip_id)
    if clip is None:
        raise AppError("NOT_FOUND", "Không tìm thấy clip.", 404)
    return clip


async def play_url(db: AsyncSession, clip_id: uuid.UUID, p: Principal, settings: Settings) -> PlayUrlOut:
    """API-40. STATION chỉ xem clip phiên của station mình, bắt đầu trong ngày (giờ VN) — 01 §5.1."""
    clip = await _require_clip(db, clip_id)
    if p.role == "STATION":
        station = await sessions.require_station(db, p.station_id, p.user_id)
        pack = await db.get(PackSession, clip.session_id)
        tz = settings.tz_display
        today = sessions.vn_day_start(clock.now().astimezone(ZoneInfo(tz)).date(), tz)
        if pack is None or pack.station_id != station.id or pack.started_at < today:
            raise AppError("FORBIDDEN", "Tài khoản không có quyền thực hiện thao tác này.", 403)
    cfg = await settings_service.get(db)
    error = _clip_unavailable(clip, cfg.retention_clip_days)
    if error:
        raise error
    exp = signing.expiry(settings.media_url_ttl_s)
    return PlayUrlOut(
        url=signing.clip_url(settings.media_signing_key, clip.id, p.user_id, exp),
        expires_at=signing.expires_at(exp),
    )


def _signature_invalid() -> AppError:
    return AppError("SIGNATURE_INVALID", "Liên kết đã hết hạn hoặc không hợp lệ. Tải lại trang.", 403)


def is_first_byte(range_header: str | None) -> bool:
    """DEC-13: chỉ ghi audit khi phát từ đầu (không Range, hoặc Range bắt đầu từ byte 0)."""
    if not range_header:
        return True
    spec = range_header.strip().lower()
    return spec.startswith("bytes=0-") or spec == "bytes=0"


async def open_clip_media(
    db: AsyncSession,
    clip_id: uuid.UUID,
    *,
    uid: uuid.UUID,
    exp: int,
    sig: str,
    range_header: str | None,
    ip: str | None,
    settings: Settings,
) -> Path:
    """API-41: kiểm chữ ký + hạn; audit VIEW_CLIP theo `uid` khi request bắt đầu từ byte 0."""
    if not signing.is_valid(settings.media_signing_key, signing.clip_message(clip_id, uid, exp), sig, exp):
        raise _signature_invalid()
    clip = await _require_clip(db, clip_id)
    cfg = await settings_service.get(db)
    error = _clip_unavailable(clip, cfg.retention_clip_days)
    if error:
        raise error
    path = absolute(settings, clip.path or "")
    if not path.is_file():
        log.error("clip_file_missing", clip_id=str(clip.id), path=str(path))
        raise AppError("NOT_FOUND", "Không tìm thấy file clip.", 404)
    if is_first_byte(range_header):
        audit.record(db, "VIEW_CLIP", user_id=uid, object_type="CLIP", object_id=clip.id, ip=ip,
                     data={"session_id": str(clip.session_id), "camera_role": clip.camera_role})  # fmt: skip
        await commit(db)
    return path


# ---------------------------------------------------------------- API-42 / 46


async def set_hold(db: AsyncSession, clip_id: uuid.UUID, held: bool, p: Principal) -> HoldOut:
    """API-42 (FR-02.09, BR-09): clip giữ không bị retention xóa. Khóa dòng để không tranh với J-02."""
    clip = await db.scalar(
        select(Clip).where(Clip.id == clip_id).with_for_update().execution_options(populate_existing=True)
    )
    if clip is None:
        raise AppError("NOT_FOUND", "Không tìm thấy clip.", 404)
    cfg = await settings_service.get(db)
    if clip.status == "DELETED":
        error = _clip_unavailable(clip, cfg.retention_clip_days)
        assert error is not None  # noqa: S101
        raise error
    if clip.held != held:
        clip.held = held
        clip.held_by = p.user_id if held else None
        clip.held_at = clock.now() if held else None
        audit.record(db, "HOLD_CLIP" if held else "UNHOLD_CLIP", user_id=p.user_id, object_type="CLIP",
                     object_id=clip.id, ip=p.ip, data={"session_id": str(clip.session_id)})  # fmt: skip
    holder = await get_user_ref(db, clip.held_by) if clip.held_by else None
    out = HoldOut(
        id=clip.id,
        held=clip.held,
        held_by=UserBrief(id=holder.id, display_name=holder.display_name) if holder else None,
        held_at=clip.held_at,
        retention_until=retention_until(clip, cfg.retention_clip_days),
    )
    await commit(db)
    return out


async def rebuild(db: AsyncSession, session_id: uuid.UUID, p: Principal) -> RebuildOut:
    """API-46: clip FAILED → PENDING rồi chạy lại J-01 (FR-02.02)."""
    pack = await db.get(PackSession, session_id)
    if pack is None:
        raise AppError("NOT_FOUND", "Không tìm thấy phiên.", 404)
    failed = (
        await db.scalars(
            select(Clip)
            .where(Clip.session_id == session_id, Clip.status == "FAILED")
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    if not failed:
        raise AppError("CLIP_NOT_FAILED", "Clip không ở trạng thái lỗi.", 409)
    for clip in failed:
        clip.status = "PENDING"
        clip.error = None
    audit.record(db, "REBUILD_CLIP", user_id=p.user_id, object_type="SESSION", object_id=session_id, ip=p.ip,
                 data={"clip_ids": [str(c.id) for c in failed]})  # fmt: skip
    jobs.enqueue_build_clips(db, session_id, pack.ended_at or clock.now())
    await commit(db)
    return RebuildOut(queued=True)


# ---------------------------------------------------------------- J-02 retention


def _unlink(path: Path) -> None:
    path.unlink(missing_ok=True)


def sweep_raw_files(video_root: Path, cutoff: datetime) -> int:
    """Xóa file video thô có giờ bắt đầu trước `cutoff − 2 phút` (segment ≤ 60 giây đã kết thúc trước mốc).

    Quét theo đĩa, không theo index: dọn cả video của camera đã xóa khỏi DB (ổ dev đầy 2026-10-05).
    """
    raw = video_root / "raw"
    if not raw.is_dir():
        return 0
    limit = cutoff - timedelta(minutes=2)
    removed = 0
    for f in raw.glob("*/*/*/*/*.mp4"):
        start = parse_start(f)
        if start is not None and start < limit:
            _unlink(f)
            removed += 1
    for day in sorted(raw.glob("*/*/*/*"), reverse=True):  # dọn thư mục ngày / tháng / năm rỗng
        for d in (day, day.parent, day.parent.parent):
            try:
                d.rmdir()
            except OSError:
                break
    return removed


async def enforce_retention(db: AsyncSession, settings: Settings) -> dict[str, int]:
    """J-02 (02:00 giờ VN): video thô quá `retention_raw_days`; clip không giữ quá `retention_clip_days`.

    Setting đọc lúc chạy (DEC-30, AC-20). Clip: khóa dòng → kiểm lại (API-42 vừa giữ thì bỏ qua — BR-09) →
    xóa file → `DELETED` + audit `DELETE_CLIP` (actor hệ thống), commit từng clip.
    """
    cfg = await settings_service.get(db)
    now = clock.now()
    raw_cutoff = now - timedelta(days=cfg.retention_raw_days)
    clip_cutoff = now - timedelta(days=cfg.retention_clip_days)
    await db.commit()

    raw_files = sweep_raw_files(settings.video_root, raw_cutoff)
    rows = await db.execute(delete(VideoSegment).where(VideoSegment.end_at < raw_cutoff))
    await db.commit()

    candidates_ = (
        await db.scalars(
            select(Clip.id).where(Clip.held.is_(False), Clip.status == "READY", Clip.end_at < clip_cutoff)
        )
    ).all()
    await db.commit()
    deleted = 0
    for clip_id in candidates_:
        try:
            clip = await db.scalar(
                select(Clip)
                .where(Clip.id == clip_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if clip is None or clip.held or clip.status != "READY" or clip.end_at >= clip_cutoff:
                await db.rollback()
                continue
            if clip.path:
                _unlink(absolute(settings, clip.path))
            clip.status = "DELETED"
            clip.deleted_at = now
            audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=clip.id,
                         data={"session_id": str(clip.session_id), "camera_role": clip.camera_role,
                               "sha256": clip.sha256, "reason": "RETENTION",
                               "retention_clip_days": cfg.retention_clip_days})  # fmt: skip
            await db.commit()
            deleted += 1
        except Exception:
            await db.rollback()
            log.exception("retention_clip_failed", clip_id=str(clip_id))
    result = {"raw_files": raw_files, "segment_rows": rows.rowcount or 0, "clips": deleted}  # type: ignore[attr-defined]
    log.info("retention_done", **result)
    return result
