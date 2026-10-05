"""Dữ liệu media cho test: segment fMP4 thật (FFmpeg) đặt theo cấu trúc thư mục MediaMTX, phiên + camera."""

import os
import shutil
import subprocess
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Camera, Station

SEGMENT_S = 10  # segment ngắn cho test nhanh (MediaMTX thật: 60 giây)
HAS_FFMPEG = shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None
needs_ffmpeg = pytest.mark.skipif(not HAS_FFMPEG, reason="máy không có ffmpeg/ffprobe")


def make_sample(dest: Path) -> Path:
    """Một segment fMP4 10 giây, 320×240, 10 fps, keyframe mỗi 1 giây (như MediaMTX ghi)."""
    out = dest / "sample.mp4"
    if not out.exists():
        subprocess.run(  # noqa: S603 — ffmpeg trong PATH như tiến trình worker
            [
                shutil.which("ffmpeg") or "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "lavfi",
                "-i",
                f"testsrc=size=320x240:rate=10:duration={SEGMENT_S}",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "10",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+frag_keyframe+empty_moov+default_base_moof",
                str(out),
            ],
            check=True,
        )
    return out


def put_segments(video_root: Path, mediamtx_path: str, sample: Path, starts: list[datetime]) -> list[Path]:
    """Chép segment mẫu vào `raw/<path>/YYYY/MM/DD/HH-MM-SS-ffffff.mp4`; mtime cũ = đã ghi xong."""
    out = []
    for start in starts:
        target = video_root / "raw" / mediamtx_path / f"{start:%Y/%m/%d/%H-%M-%S-%f}.mp4"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(sample, target)
        old = start.timestamp() if start.timestamp() < datetime.now().timestamp() - 60 else 0
        os.utime(target, (old, old))
        out.append(target)
    return out


async def make_cameras(db: AsyncSession, station: Station) -> dict[str, Camera]:
    cams = {}
    for role in ("CAM1", "CAM2"):
        cam_id = uuid.uuid4()
        cam = Camera(
            id=cam_id,
            station_id=station.id,
            role=role,
            rtsp_url="rtsp://10.0.0.1/s",
            mediamtx_path=f"cam-{cam_id}",
        )
        db.add(cam)
        cams[role] = cam
    await db.flush()
    return cams


async def make_closed_session(
    db: AsyncSession,
    station: Station,
    code: str,
    started: datetime,
    ended: datetime,
    status: str = "COMPLETED",
) -> PackSession:
    package = Package(tracking_number=code, warehouse_status="PACKED" if status == "COMPLETED" else "NEW")
    db.add(package)
    await db.flush()
    pack = PackSession(
        package_id=package.id,
        station_id=station.id,
        status=status,
        started_at=started,
        ended_at=ended,
        open_code=code,
        close_code=code if status == "COMPLETED" else None,
        package_status_before="NEW",
    )
    db.add(pack)
    await db.flush()
    return pack


def starts_every(base: datetime, count: int, step_s: float = SEGMENT_S) -> list[datetime]:
    return [base + timedelta(seconds=step_s * i) for i in range(count)]
