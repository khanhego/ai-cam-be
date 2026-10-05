"""Bản xuất bằng chứng: API-43..45, J-03 `media.render_export` (FR-07.04, FR-02.03, FR-02.07; ADR-008).

Clip gốc giữ nguyên; bản xuất encode lại H.264 có overlay (mã vận đơn, mã đơn sàn, giờ thực tới giây, tên
station) + `info.json` (SHA-256 clip gốc, SHA-256 bản xuất, người xuất, thời điểm).
File giữ `EXPORT_TTL_HOURS`.
"""

import json
import shutil
import tempfile
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam import __version__
from aicam.core import audit, clock
from aicam.core.db import commit
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.media import ffmpeg, jobs, signing
from aicam.modules.media.models import Clip, Export
from aicam.modules.media.schemas import ExportCreated, ExportFiles, ExportOut
from aicam.modules.media.segments import timeline_bounds
from aicam.modules.media.service import _clip_unavailable, absolute, is_first_byte
from aicam.modules.orders.models import Order, Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings import service as settings_service
from aicam.modules.stations.models import Station
from aicam.modules.users.queries import get_user_ref

log = structlog.get_logger()


def roles_of(layout: str) -> list[str]:
    return ["CAM1", "CAM2"] if layout == "SIDE_BY_SIDE" else [layout]


def export_dir(export_id: uuid.UUID) -> str:
    return f"exports/{export_id}"


async def _clips_by_role(db: AsyncSession, session_id: uuid.UUID) -> dict[str, Clip]:
    rows = (await db.scalars(select(Clip).where(Clip.session_id == session_id))).all()
    return {c.camera_role: c for c in rows}


# ---------------------------------------------------------------- API-43


async def create_export(
    db: AsyncSession, session_id: uuid.UUID, layout: str, p: Principal, settings: Settings
) -> ExportCreated:
    pack = await db.get(PackSession, session_id)
    if pack is None:
        raise AppError("NOT_FOUND", "Không tìm thấy phiên.", 404)
    cfg = await settings_service.get(db)
    clips = await _clips_by_role(db, session_id)
    for role in roles_of(layout):
        clip = clips.get(role)
        if clip is None:
            raise AppError("CLIP_NOT_READY", "Clip đang được cắt, sẵn sàng trong khoảng 1 phút.", 409,
                           {"camera_role": role, "status": None})  # fmt: skip
        error = _clip_unavailable(clip, cfg.retention_clip_days)
        if error is not None:
            error.details["camera_role"] = role
            raise error
    export = Export(
        session_id=session_id,
        layout=layout,
        status="QUEUED",
        progress=0,
        created_by=p.user_id,
        expires_at=clock.now() + timedelta(hours=settings.export_ttl_hours),
    )
    db.add(export)
    await db.flush()
    audit.record(db, "EXPORT_CLIP", user_id=p.user_id, object_type="SESSION", object_id=session_id, ip=p.ip,
                 data={"export_id": str(export.id), "layout": layout})  # fmt: skip
    jobs.enqueue_render_export(db, export.id)
    out = ExportCreated(id=export.id, status="QUEUED", progress=0)
    await commit(db)
    return out


# ---------------------------------------------------------------- API-44


async def export_out(db: AsyncSession, export: Export, uid: uuid.UUID, settings: Settings) -> ExportOut:
    clips = await _clips_by_role(db, export.session_id)
    files = None
    if export.status == "READY" and export.expires_at and export.expires_at > clock.now():
        exp = signing.expiry(settings.media_url_ttl_s)
        key = settings.media_signing_key
        files = ExportFiles(
            video=signing.export_url(key, export.id, "video.mp4", uid, exp),
            info=signing.export_url(key, export.id, "info.json", uid, exp),
        )
    return ExportOut(
        id=export.id,
        session_id=export.session_id,
        layout=export.layout,
        status=export.status,
        progress=export.progress,
        sha256=export.sha256,
        source_clip_sha256={r: clips[r].sha256 if r in clips else None for r in roles_of(export.layout)},
        files=files,
        expires_at=export.expires_at,
    )


async def get_export(db: AsyncSession, export_id: uuid.UUID, p: Principal, settings: Settings) -> ExportOut:
    """Người tạo hoặc ADMIN; người khác → 404 (không lộ bản xuất của người khác)."""
    export = await db.get(Export, export_id)
    if export is None or (export.created_by != p.user_id and p.role != "ADMIN"):
        raise AppError("NOT_FOUND", "Không tìm thấy bản xuất.", 404)
    return await export_out(db, export, p.user_id, settings)


# ---------------------------------------------------------------- API-45


async def open_export_file(
    db: AsyncSession,
    export_id: uuid.UUID,
    file: str,
    *,
    uid: uuid.UUID,
    exp: int,
    sig: str,
    range_header: str | None,
    ip: str | None,
    settings: Settings,
) -> tuple[Path, str]:
    """Kiểm chữ ký `export:{id}:{file}:{uid}:{exp}`; audit DOWNLOAD_EXPORT theo `uid` khi tải từ byte 0."""
    message = signing.export_message(export_id, file, uid, exp)
    if file not in signing.EXPORT_FILES or not signing.is_valid(
        settings.media_signing_key, message, sig, exp
    ):
        raise AppError("SIGNATURE_INVALID", "Liên kết đã hết hạn hoặc không hợp lệ. Tải lại trang.", 403)
    export = await db.get(Export, export_id)
    if (
        export is None
        or export.status != "READY"
        or not export.expires_at
        or export.expires_at <= clock.now()
    ):
        raise AppError("NOT_FOUND", "Bản xuất không còn. Tạo bản xuất mới.", 404)
    rel = export.path_video if file == "video.mp4" else export.path_info
    path = absolute(settings, rel or "")
    if not path.is_file():
        raise AppError("NOT_FOUND", "Bản xuất không còn. Tạo bản xuất mới.", 404)
    if is_first_byte(range_header):
        audit.record(db, "DOWNLOAD_EXPORT", user_id=uid, object_type="EXPORT", object_id=export.id, ip=ip,
                     data={"file": file, "session_id": str(export.session_id)})  # fmt: skip
        await commit(db)
    package = await db.scalar(
        select(Package)
        .join(PackSession, PackSession.package_id == Package.id)
        .where(PackSession.id == export.session_id)
    )
    stem = f"{package.tracking_number if package else export.session_id}-{export.layout}"
    return path, f"{stem}.mp4" if file == "video.mp4" else f"{stem}.json"


# ---------------------------------------------------------------- J-03


def clock_pieces(
    timeline: list[dict[str, Any]] | None, start_at: datetime, skip: float, duration: float, tz: str
) -> list[tuple[float, float, float]]:
    """Đoạn giờ thực trên bản xuất (giây bắt đầu, giây kết thúc, epoch giờ hiển thị), đã bỏ `skip` giây đầu.

    Epoch cộng chênh múi giờ hiển thị để drawtext dùng `gmtime` ra đúng giờ VN.
    """
    pieces = timeline or [{"t": 0.0, "wall": start_at.isoformat()}]
    out: list[tuple[float, float, float]] = []
    for i, piece in enumerate(pieces):
        t = float(piece["t"]) - skip
        t_end = (float(pieces[i + 1]["t"]) - skip) if i + 1 < len(pieces) else duration
        wall = datetime.fromisoformat(piece["wall"])
        if t_end <= 0 or t >= duration:
            continue
        if t < 0:  # đoạn bắt đầu trước điểm cắt đầu
            wall += timedelta(seconds=-t)
            t = 0.0
        offset = wall.astimezone(ZoneInfo(tz)).utcoffset() or timedelta(0)
        out.append((t, min(t_end, duration), wall.timestamp() + offset.total_seconds()))
    return out


def _clip_bounds(clip: Clip) -> tuple[datetime, datetime]:
    if clip.timeline and clip.duration_s is not None:
        return timeline_bounds(clip.timeline, float(clip.duration_s))
    return clip.start_at, clip.end_at


async def _publish(db: AsyncSession, export: Export, settings: Settings) -> None:
    from aicam.realtime import publish

    data = (await export_out(db, export, export.created_by, settings)).model_dump(mode="json")
    await publish.to_user(export.created_by, "export.updated", data)


async def render_export(db: AsyncSession, export_id: uuid.UUID, settings: Settings) -> str:
    """J-03 (queue `export`, concurrency 1): encode, ghi info.json, cập nhật tiến độ, WS `export.updated`."""
    export = await db.get(Export, export_id)
    if export is None or export.status not in ("QUEUED", "RUNNING"):
        return "SKIPPED"
    export.status, export.progress = "RUNNING", 0
    await commit(db)
    await _publish(db, export, settings)
    out_dir = absolute(settings, export_dir(export.id))
    try:
        pack = await db.get(PackSession, export.session_id)
        assert pack is not None  # noqa: S101 — FK CASCADE
        package = await db.get(Package, pack.package_id)
        order = await db.get(Order, package.order_id) if package and package.order_id else None
        station = await db.get(Station, pack.station_id)
        clips = await _clips_by_role(db, export.session_id)
        sources = [clips[r] for r in roles_of(export.layout)]
        for c in sources:
            if c.status != "READY" or not c.path:
                raise ffmpeg.FFmpegError(f"Clip {c.camera_role} không còn sẵn sàng")
        bounds = [_clip_bounds(c) for c in sources]
        media = [float(c.duration_s or 0) for c in sources]
        if len(sources) == 2 and all(len(c.timeline or []) <= 1 for c in sources):
            # Ghép: bỏ phần đầu của camera bắt đầu sớm hơn để hai hình cùng giờ (TC-07.11, ≤ 0,5 giây).
            common_start = max(b[0] for b in bounds)
            skips = [(common_start - b[0]).total_seconds() for b in bounds]
            duration = min(m - s for m, s in zip(media, skips, strict=True))
        else:
            # 1 camera, hoặc clip có khe hở (dòng thời gian không tuyến tính): không dịch, giờ theo clip đầu.
            skips = [0.0] * len(sources)
            duration = min(media)
        if duration <= 0:
            raise ffmpeg.FFmpegError("Hai camera không có khoảng thời gian chung")
        common_start = bounds[0][0] + timedelta(seconds=skips[0])
        linear = len(sources[0].timeline or []) <= 1
        common_end = common_start + timedelta(seconds=duration) if linear else bounds[0][1]
        out_dir.mkdir(parents=True, exist_ok=True)
        video = out_dir / "video.mp4"
        font = settings.export_font_file if settings.export_font_file.is_file() else None
        lines = [package.tracking_number if package else "?"]
        if order is not None:
            lines.append(f"Đơn {order.platform_order_sn}")
        lines.append(station.name if station else "")
        last_pct = -10

        async def _progress(pct: int) -> None:
            nonlocal last_pct
            if pct - last_pct >= 5:
                last_pct = pct
                export.progress = pct
                await commit(db)
                await _publish(db, export, settings)

        with tempfile.TemporaryDirectory(prefix="aicam-export-") as tmp:
            text_file = Path(tmp) / "overlay.txt"
            text_file.write_text(" · ".join(x for x in lines if x))
            clock_ = ffmpeg.clock_overlays(
                clock_pieces(sources[0].timeline, bounds[0][0], skips[0], duration, settings.tz_display), font
            )
            cmd = ffmpeg.export_command(
                settings.ffmpeg_bin,
                [(absolute(settings, c.path or ""), s) for c, s in zip(sources, skips, strict=True)],
                video,
                text_file=text_file,
                clock=clock_,
                font_file=font,
                duration=duration,
                preset=settings.export_preset,
                side_scale=settings.export_side_scale,
            )
            await ffmpeg.run_with_progress(cmd, duration, settings.export_timeout_s, _progress)
        sha = ffmpeg.sha256_file(video)
        creator = await get_user_ref(db, export.created_by)
        info = {
            "export_id": str(export.id),
            "session_id": str(pack.id),
            "layout": export.layout,
            "tracking_number": package.tracking_number if package else None,
            "platform_order_sn": order.platform_order_sn if order else None,
            "station_name": station.name if station else None,
            "session_started_at": pack.started_at.isoformat(),
            "session_ended_at": pack.ended_at.isoformat() if pack.ended_at else None,
            "video_start_at": common_start.isoformat(),
            "video_end_at": common_end.isoformat(),
            "sha256": sha,
            "source_clip_sha256": {c.camera_role: c.sha256 for c in sources},
            "exported_by": {
                "id": str(export.created_by),
                "display_name": creator.display_name if creator else None,
            },
            "exported_at": clock.now().isoformat(),
            "generator": f"Hệ thống X (aicam {__version__})",
        }
        (out_dir / "info.json").write_text(json.dumps(info, ensure_ascii=False, indent=2))
        export.status, export.progress, export.sha256 = "READY", 100, sha
        export.path_video = f"{export_dir(export.id)}/video.mp4"
        export.path_info = f"{export_dir(export.id)}/info.json"
        export.error = None
    except (ffmpeg.FFmpegError, OSError) as exc:
        log.error("export_failed", export_id=str(export.id), error=str(exc))
        export.status, export.error = "FAILED", str(exc)[:500]
        shutil.rmtree(out_dir, ignore_errors=True)
    await commit(db)
    await _publish(db, export, settings)
    return export.status


async def cleanup_expired(db: AsyncSession, settings: Settings) -> int:
    """Một phần J-10: xóa bản xuất quá `expires_at` (file + dòng)."""
    expired = (
        await db.scalars(
            select(Export).where(Export.expires_at.is_not(None), Export.expires_at < clock.now())
        )
    ).all()
    for export in expired:
        folder = Path(export.path_video).parent.as_posix() if export.path_video else export_dir(export.id)
        shutil.rmtree(absolute(settings, folder), ignore_errors=True)
    if expired:
        await db.execute(delete(Export).where(Export.id.in_([e.id for e in expired])))
    await db.commit()
    return len(expired)
