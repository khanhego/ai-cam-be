"""Ảnh Cam 1 (02a §2 `media/snapshots.py`): chụp tay ở phiên RETURN (API-103), tải ảnh ký (API-106),
ảnh lúc đóng gói trích từ clip gốc (J-17, L8, DEC-227). FR-04.04, FR-02.11.

File `snapshots/YYYY/MM/DD/{session_id}_{n:02d}.jpg` (`_pack.jpg` cho ảnh lúc đóng gói), quyền 0444, SHA-256
(bằng chứng như clip gốc — ADR-008). Không có API sửa / xóa ảnh.
"""

import asyncio
import os
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path

import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.errors import AppError
from aicam.core.ids import uuid7
from aicam.core.settings import Settings
from aicam.modules.media import ffmpeg, signing
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Camera
from aicam.modules.stations.probe import CameraUnreachable, grab_frame
from aicam.modules.users.models import User

log = structlog.get_logger()

Grabber = Callable[[str, float], Awaitable[bytes]]
PACK_CLOSE_BEFORE_END = timedelta(seconds=0.5)  # khung Cam 1 tại `ended_at − 0,5 giây` (02a J-17)


def rel_path(session_id: uuid.UUID, taken_at: datetime, suffix: str) -> str:
    return f"snapshots/{taken_at:%Y/%m/%d}/{session_id}_{suffix}.jpg"


def _absolute(settings: Settings, rel: str) -> Path:
    path = (settings.video_root / rel).resolve()
    if not path.is_relative_to(settings.video_root.resolve()):
        raise ValueError(f"Đường dẫn ngoài VIDEO_ROOT: {rel}")
    return path


def _write_readonly(path: Path, data: bytes) -> str:
    """Ghi file tạm rồi đổi tên (không để ảnh dở), chmod 0444; trả SHA-256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    partial.write_bytes(data)
    os.chmod(partial, 0o444)
    os.replace(partial, path)
    return ffmpeg.sha256_file(path)


def url_for(settings: Settings, snapshot_id: uuid.UUID, uid: uuid.UUID) -> str:
    exp = signing.expiry(settings.media_url_ttl_s)
    return signing.snapshot_url(settings.media_signing_key, snapshot_id, uid, exp)


async def of_session(
    session: AsyncSession, session_id: uuid.UUID, kind: str = "MANUAL"
) -> Sequence[Snapshot]:
    return (
        await session.scalars(
            select(Snapshot)
            .where(Snapshot.session_id == session_id, Snapshot.kind == kind, Snapshot.status == "READY")
            .order_by(Snapshot.taken_at, Snapshot.id)
        )
    ).all()


async def pack_close_of(session: AsyncSession, session_id: uuid.UUID) -> Snapshot | None:
    result: Snapshot | None = await session.scalar(
        select(Snapshot).where(
            Snapshot.session_id == session_id, Snapshot.kind == "PACK_CLOSE", Snapshot.status == "READY"
        )
    )
    return result


# ---------------------------------------------------------------- API-103


async def _manual_count(session: AsyncSession, session_id: uuid.UUID) -> int:
    return int(
        await session.scalar(
            select(func.count()).where(Snapshot.session_id == session_id, Snapshot.kind == "MANUAL")
        )
        or 0
    )


async def _require_open_return(
    session: AsyncSession, station_id: uuid.UUID, session_id: uuid.UUID
) -> PackSession:
    pack: PackSession | None = await session.scalar(
        select(PackSession).where(PackSession.id == session_id).execution_options(populate_existing=True)
    )
    if pack is None or pack.station_id != station_id or pack.status != "OPEN" or pack.type != "RETURN":
        raise AppError("SESSION_NOT_OPEN", "Phiên không còn mở.", 409)
    return pack


def _limit_error(settings: Settings) -> AppError:
    return AppError(
        "SNAPSHOT_LIMIT",
        f"Đã đủ {settings.snapshot_max_per_session} ảnh.",
        409,
        {"max": settings.snapshot_max_per_session},
    )


async def take(
    session: AsyncSession,
    station_id: uuid.UUID,
    session_id: uuid.UUID,
    settings: Settings,
    *,
    lock: Callable[[AsyncSession, uuid.UUID], Awaitable[None]],
    grab: Grabber | None = None,
) -> Snapshot:
    """API-103: kiểm phiên + giới hạn → lấy khung relay Cam 1 **ngoài** khóa (≤ `SNAPSHOT_TIMEOUT_S`) →
    khóa station, kiểm lại phiên `OPEN` + đếm, ghi file + INSERT (02a §4 API-103, §6 "chụp chậm 3 giây")."""
    await lock(session, station_id)
    await _require_open_return(session, station_id, session_id)
    if await _manual_count(session, session_id) >= settings.snapshot_max_per_session:
        raise _limit_error(settings)
    camera = await session.scalar(
        select(Camera).where(Camera.station_id == station_id, Camera.role == "CAM1")
    )
    await commit(session)  # nhả khóa station trong lúc chờ camera (không có gì để ghi)
    if camera is None:
        raise AppError(
            "CAMERA_UNREACHABLE", "Không chụp được ảnh từ Cam 1. Thử lại.", 422, {"reason": "NO_CAMERA"}
        )
    began = time.monotonic()
    taken_at = clock.now()
    try:
        data = await (grab or _grab)(
            f"{settings.mediamtx_rtsp_url}/{camera.mediamtx_path}", settings.snapshot_timeout_s
        )
    except CameraUnreachable as exc:
        log.warning("snapshot_failed", session_id=str(session_id), reason=exc.reason)
        raise AppError(
            "CAMERA_UNREACHABLE", "Không chụp được ảnh từ Cam 1. Thử lại.", 422, {"reason": exc.reason}
        ) from exc

    await lock(session, station_id)
    await _require_open_return(session, station_id, session_id)  # phiên đóng trong lúc chụp → 409, không ghi
    count = await _manual_count(session, session_id)
    if count >= settings.snapshot_max_per_session:
        raise _limit_error(settings)
    rel = rel_path(session_id, taken_at, f"{count + 1:02d}")
    path = _absolute(settings, rel)
    sha = await asyncio.to_thread(_write_readonly, path, data)
    snapshot = Snapshot(
        session_id=session_id, kind="MANUAL", camera_role="CAM1", taken_at=taken_at, path=rel, sha256=sha,
        size_bytes=len(data), status="READY",
    )  # fmt: skip
    session.add(snapshot)
    await session.flush()
    log.info("snapshot_taken", session_id=str(session_id), snapshot_id=str(snapshot.id),
             seconds=round(time.monotonic() - began, 2))  # fmt: skip
    return snapshot


async def _grab(url: str, timeout_s: float) -> bytes:
    return await grab_frame(url, timeout_s=timeout_s)


# ---------------------------------------------------------------- API-106


def _signature_invalid() -> AppError:
    return AppError("SIGNATURE_INVALID", "Liên kết đã hết hạn hoặc không hợp lệ. Tải lại trang.", 403)


async def open_snapshot(
    session: AsyncSession,
    snapshot_id: uuid.UUID,
    *,
    uid: uuid.UUID,
    exp: int,
    sig: str,
    ip: str | None,
    settings: Settings,
) -> Path:
    """API-106: kiểm chữ ký `snapshot:{id}:{uid}:{exp}`; ảnh `DELETED` → 410; audit `VIEW_SNAPSHOT` khi
    người xem không phải tài khoản STATION."""
    message = signing.snapshot_message(snapshot_id, uid, exp)
    if not signing.is_valid(settings.media_signing_key, message, sig, exp):
        raise _signature_invalid()
    snapshot = await session.get(Snapshot, snapshot_id)
    if snapshot is None:
        raise AppError("NOT_FOUND", "Không tìm thấy ảnh.", 404)
    if snapshot.status == "DELETED":
        raise AppError(
            "SNAPSHOT_DELETED",
            "Ảnh đã bị xóa theo chính sách lưu trữ.",
            410,
            {"deleted_at": clock.iso_z(snapshot.deleted_at) if snapshot.deleted_at else None},
        )
    path = _absolute(settings, snapshot.path or "")
    if not path.is_file():
        log.error("snapshot_file_missing", snapshot_id=str(snapshot.id))
        raise AppError("NOT_FOUND", "Không tìm thấy file ảnh.", 404)
    viewer = await session.get(User, uid)
    if viewer is None or viewer.role != "STATION":
        audit.record(
            session, "VIEW_SNAPSHOT", user_id=uid, object_type="SNAPSHOT", object_id=snapshot.id, ip=ip,
            data={"session_id": str(snapshot.session_id), "kind": snapshot.kind},
        )  # fmt: skip
        await commit(session)
    return path


# ---------------------------------------------------------------- J-17 ảnh lúc đóng gói


def clip_offset(clip: Clip, at: datetime) -> float:
    """Giây trong clip ứng với giờ thực `at` (theo `timeline` của J-01; không có → tính từ `start_at`)."""
    duration = float(clip.duration_s or 0)
    offset = (at - clip.start_at).total_seconds()
    for piece in clip.timeline or []:
        wall = datetime.fromisoformat(str(piece["wall"]))
        if wall <= at:
            offset = float(piece["t"]) + (at - wall).total_seconds()
    upper = max(0.0, duration - 0.1) if duration else offset
    return round(min(max(0.0, offset), upper), 3)


def frame_command(ffmpeg_bin: str, src: Path, offset: float, quality: int, out: Path) -> list[str]:
    """Một khung JPEG tại `offset` giây; `quality` 1–100 → `-q:v` 2–31 của mjpeg (cao = đẹp)."""
    qv = max(2, min(31, round(31 - (quality / 100) * 29)))
    return [ffmpeg_bin, "-hide_banner", "-loglevel", "error", "-y", "-ss", f"{offset:.3f}", "-i", str(src),
            "-frames:v", "1", "-q:v", str(qv), "-f", "image2", str(out)]  # fmt: skip


async def capture_pack_snapshot(session: AsyncSession, session_id: uuid.UUID, settings: Settings) -> str:
    """J-17: phiên PACK `COMPLETED` có clip Cam 1 READY → khung tại `ended_at − 0,5 giây` → `PACK_CLOSE`.

    Đã có ảnh → bỏ qua (unique). Thiếu clip / phiên khác → bỏ qua (không chặn gì). Lỗi ffmpeg → ném.
    """
    pack = await session.get(PackSession, session_id)
    if (
        pack is None
        or pack.type != "PACK"
        or pack.status not in ("COMPLETED", "SUPERSEDED")
        or pack.ended_at is None
    ):
        return "skipped"
    if await pack_close_of(session, session_id) is not None:
        return "exists"
    clip = await session.scalar(
        select(Clip).where(Clip.session_id == session_id, Clip.camera_role == "CAM1", Clip.status == "READY")
    )
    if clip is None or not clip.path:
        return "no_clip"
    target = pack.ended_at - PACK_CLOSE_BEFORE_END
    src = _absolute(settings, clip.path)
    rel = rel_path(session_id, pack.ended_at, "pack")
    path = _absolute(settings, rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.stem + ".part.jpg")
    offset = clip_offset(clip, target)
    await ffmpeg.run(
        frame_command(settings.ffmpeg_bin, src, offset, settings.snapshot_jpeg_quality, partial), 30
    )
    data = partial.read_bytes()
    partial.unlink(missing_ok=True)
    sha = await asyncio.to_thread(_write_readonly, path, data)
    await session.execute(
        insert(Snapshot)
        .values(
            id=uuid7(),
            session_id=session_id,
            kind="PACK_CLOSE",
            camera_role="CAM1",
            taken_at=target,
            path=rel,
            sha256=sha,
            size_bytes=len(data),
            status="READY",
            created_at=clock.now(),
        )
        .on_conflict_do_nothing(index_elements=["session_id"], index_where=Snapshot.kind == "PACK_CLOSE")
    )
    await commit(session)
    log.info("pack_snapshot_captured", session_id=str(session_id))
    return "captured"
