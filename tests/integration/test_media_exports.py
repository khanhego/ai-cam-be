"""API-43..45, J-03 (T-15) — FR-07.04, FR-02.03, FR-02.07; TC-07.09, TC-07.10, TC-02.14, TC-P.05.

Encode thật cần FFmpeg có filter `drawtext` (libfreetype): máy không có → skip, đã kiểm live trong container.
"""

import json
import shutil
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.media import exports, jobs
from aicam.modules.media.models import Clip, Export
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .media_fixtures import HAS_FFMPEG, make_closed_session, make_sample

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 5, 3, 0, tzinfo=UTC)


def _has_drawtext() -> bool:
    if not HAS_FFMPEG:
        return False
    out = subprocess.run(  # noqa: S603
        [shutil.which("ffmpeg") or "ffmpeg", "-hide_banner", "-filters"],
        capture_output=True,
        text=True,
        check=False,
    )
    return " drawtext " in out.stdout


needs_drawtext = pytest.mark.skipif(
    not _has_drawtext(), reason="ffmpeg máy này không có drawtext (libfreetype)"
)


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


async def _login(
    api: AsyncClient, db: AsyncSession, role: str, name: str | None = None
) -> tuple[dict[str, str], Any]:
    user = await make_user(db, name or f"tst_{role.lower()}", role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


async def _session_with_clips(
    db: AsyncSession,
    settings: Settings,
    station: Station,
    statuses: dict[str, str],
    source: Path | None = None,
) -> uuid.UUID:
    pack = await make_closed_session(db, station, "SPXTST0000001", T0, T0 + timedelta(seconds=20))
    for role, status in statuses.items():
        rel = f"clips/{pack.id}-{role}.mp4"
        if source is not None:
            (settings.video_root / "clips").mkdir(exist_ok=True)
            shutil.copyfile(source, settings.video_root / rel)
        db.add(Clip(session_id=pack.id, camera_role=role, status=status, start_at=T0 - timedelta(seconds=5),
                    end_at=T0 + timedelta(seconds=5), duration_s=10, path=rel, sha256=f"{role}-sha",
                    timeline=[{"t": 0.0, "wall": (T0 - timedelta(seconds=5)).isoformat()}]))  # fmt: skip
    await db.flush()
    return pack.id


async def test_create_export_validates_clips(
    api: AsyncClient, db: AsyncSession, media_settings: Settings, sent_jobs: list[Any]
) -> None:
    """TC-07.09: clip nguồn đã xóa → 410; đang cắt → 409; hợp lệ → 202 QUEUED, J-03 queue export, audit."""
    headers, uid = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    session_id = await _session_with_clips(db, media_settings, station, {"CAM1": "READY", "CAM2": "DELETED"})
    url = f"/api/v1/sessions/{session_id}/exports"

    deleted = await api.post(url, headers=headers, json={"layout": "SIDE_BY_SIDE"})
    assert (deleted.status_code, deleted.json()["error"]["code"]) == (410, "CLIP_DELETED")
    ok = await api.post(url, headers=headers, json={"layout": "CAM1"})

    assert ok.status_code == 202
    assert ok.json()["status"] == "QUEUED"
    assert sent_jobs == [(jobs.RENDER_EXPORT, [ok.json()["id"]], "export", 0.0)]
    entry = await db.scalar(select(AuditLog).where(AuditLog.action == "EXPORT_CLIP"))
    assert entry is not None
    assert entry.user_id == uid
    assert (await db.scalar(select(Export).where(Export.session_id == session_id))) is not None
    bad = await api.post(url, headers=headers, json={"layout": "CAM9"})
    assert bad.status_code == 422
    missing = await api.post(
        f"/api/v1/sessions/{uuid.uuid4()}/exports", headers=headers, json={"layout": "CAM1"}
    )
    assert missing.status_code == 404


async def test_pending_clip_blocks_export(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    headers, _ = await _login(api, db, "SUPERVISOR")
    _, station = await make_station_account(db)
    session_id = await _session_with_clips(db, media_settings, station, {"CAM1": "PENDING"})
    res = await api.post(
        f"/api/v1/sessions/{session_id}/exports", headers=headers, json={"layout": "SIDE_BY_SIDE"}
    )
    assert (res.status_code, res.json()["error"]["code"]) == (409, "CLIP_NOT_READY")


async def test_get_export_owner_admin_and_download(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """API-44 người tạo / ADMIN; người khác 404. API-45 URL ký, audit DOWNLOAD_EXPORT; đổi file → 403."""
    clock.freeze(datetime.now(UTC))
    owner, owner_id = await _login(api, db, "CSKH")
    other, _ = await _login(api, db, "CSKH", "tst_cskh2")
    admin, _ = await _login(api, db, "ADMIN")
    _, station = await make_station_account(db)
    session_id = await _session_with_clips(db, media_settings, station, {"CAM1": "READY", "CAM2": "READY"})
    export = Export(session_id=session_id, layout="SIDE_BY_SIDE", status="READY", progress=100,
                    sha256="e" * 64,
                    created_by=owner_id, expires_at=clock.now() + timedelta(hours=24),
                    path_video="exports/x/video.mp4", path_info="exports/x/info.json")  # fmt: skip
    db.add(export)
    await db.flush()
    folder = media_settings.video_root / "exports/x"
    folder.mkdir(parents=True)
    (folder / "video.mp4").write_bytes(b"\x00" * 2048)
    (folder / "info.json").write_text('{"sha256": "e"}')

    body = (await api.get(f"/api/v1/exports/{export.id}", headers=owner)).json()
    assert body["status"] == "READY"
    assert body["source_clip_sha256"] == {"CAM1": "CAM1-sha", "CAM2": "CAM2-sha"}
    assert (await api.get(f"/api/v1/exports/{export.id}", headers=admin)).status_code == 200
    assert (await api.get(f"/api/v1/exports/{export.id}", headers=other)).status_code == 404

    video = await api.get(body["files"]["video"])
    assert video.status_code == 200
    assert video.headers["content-disposition"].endswith('"SPXTST0000001-SIDE_BY_SIDE.mp4"')
    info = await api.get(body["files"]["info"])
    assert info.json() == {"sha256": "e"}
    downloads = (await db.scalars(select(AuditLog.user_id).where(AuditLog.action == "DOWNLOAD_EXPORT"))).all()
    assert list(downloads) == [owner_id, owner_id]
    forged = body["files"]["info"].replace("info.json", "video.mp4")
    res = await api.get(forged)
    assert (res.status_code, res.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")

    clock.advance(timedelta(hours=25))
    assert await exports.cleanup_expired(db, media_settings) == 1
    assert not folder.exists()
    assert await db.get(Export, export.id, populate_existing=True) is None


def test_clock_pieces_skip_and_gap() -> None:
    """Giờ VN trên overlay: bỏ 2 giây đầu; đoạn sau khe hở giữ giờ thật."""
    timeline = [
        {"t": 0.0, "wall": T0.isoformat()},
        {"t": 10.0, "wall": (T0 + timedelta(seconds=30)).isoformat()},
    ]
    pieces = exports.clock_pieces(timeline, T0, 2.0, 15.0, "Asia/Ho_Chi_Minh")
    vn = 7 * 3600
    assert pieces == [
        (0.0, 8.0, T0.timestamp() + 2 + vn),
        (8.0, 15.0, T0.timestamp() + 30 + vn),
    ]


@needs_drawtext
async def test_render_export_side_by_side(
    db: AsyncSession, redis_client: object, media_settings: Settings, tmp_path: Path
) -> None:
    """J-03 thật: READY, SHA-256 khớp file, info.json có hash clip gốc (TC-02.14)."""
    sample = make_sample(tmp_path)
    user = await make_user(db, "tst_cskh", "CSKH")
    _, station = await make_station_account(db)
    session_id = await _session_with_clips(
        db, media_settings, station, {"CAM1": "READY", "CAM2": "READY"}, sample
    )
    export = Export(session_id=session_id, layout="SIDE_BY_SIDE", created_by=user.id,
                    expires_at=clock.now() + timedelta(hours=1))  # fmt: skip
    db.add(export)
    await db.flush()
    media_settings.export_font_file = Path("/nonexistent.ttf")  # dùng font mặc định của fontconfig

    assert await exports.render_export(db, export.id, media_settings) == "READY"
    video = media_settings.video_root / f"exports/{export.id}/video.mp4"
    info = json.loads((video.parent / "info.json").read_text())
    import hashlib

    assert info["sha256"] == hashlib.sha256(video.read_bytes()).hexdigest()
    assert info["source_clip_sha256"] == {"CAM1": "CAM1-sha", "CAM2": "CAM2-sha"}


async def test_render_export_fails_cleanly(
    db: AsyncSession, redis_client: object, media_settings: Settings
) -> None:
    """TC-07.10: FFmpeg lỗi (file clip nguồn hỏng) → FAILED + error, không để file dở."""
    user = await make_user(db, "tst_cskh", "CSKH")
    _, station = await make_station_account(db)
    session_id = await _session_with_clips(db, media_settings, station, {"CAM1": "READY"})
    (media_settings.video_root / "clips").mkdir()
    (media_settings.video_root / f"clips/{session_id}-CAM1.mp4").write_bytes(b"khong phai video")
    export = Export(session_id=session_id, layout="CAM1", created_by=user.id)
    db.add(export)
    await db.flush()

    assert await exports.render_export(db, export.id, media_settings) == "FAILED"
    reloaded = await db.get(Export, export.id, populate_existing=True)
    assert reloaded is not None
    assert reloaded.error
    assert not (media_settings.video_root / f"exports/{export.id}").exists()


# ---------------------------------------------------------------- G3-F2: ghép 2 camera khi clip có khe hở


def _wall_at(parts: list[Any], timeline: list[dict[str, Any]], w0: datetime, x: float) -> datetime | None:
    """Giờ thực của khung ở giây `x` trên nửa hình dựng từ `parts` (None = khung đen của khe hở)."""
    pos = 0.0
    for part in parts:
        if pos <= x < pos + part.duration:
            if part.kind == "GAP":
                return None
            clip_t = part.start + (x - pos)
            piece = max((p for p in timeline if float(p["t"]) <= clip_t), key=lambda p: float(p["t"]))
            return datetime.fromisoformat(piece["wall"]) + timedelta(seconds=clip_t - float(piece["t"]))
        pos += part.duration
    raise AssertionError(f"giây {x} ngoài bản xuất")


def test_align_parts_lines_up_wall_clock() -> None:
    """TC-07.11: CAM1 mất 20 giây giữa phiên, CAM2 liền mạch → mỗi giây hai nửa cùng giờ thực (≤ 0,5 s)."""
    cam1 = [{"t": 0.0, "wall": clock.iso_z(T0)}, {"t": 10.0, "wall": clock.iso_z(T0 + timedelta(seconds=30))}]
    cam2 = [{"t": 0.0, "wall": clock.iso_z(T0 - timedelta(seconds=2))}]
    w0, w1 = T0, T0 + timedelta(seconds=40)  # chung: CAM1 T0..T0+40 (có khe), CAM2 T0−2..T0+48

    p1 = exports.align_parts(cam1, T0, 20.0, w0, w1)
    p2 = exports.align_parts(cam2, T0 - timedelta(seconds=2), 50.0, w0, w1)

    assert [(p.kind, round(p.duration, 3)) for p in p1] == [("VIDEO", 10.0), ("GAP", 20.0), ("VIDEO", 10.0)]
    assert [(p.kind, p.start, p.end) for p in p2] == [("VIDEO", 2.0, 42.0)]
    assert sum(p.duration for p in p1) == sum(p.duration for p in p2) == 40.0
    for x in (0.0, 5.5, 9.9, 15.0, 30.0, 39.9):
        expected = w0 + timedelta(seconds=x)
        a, b = _wall_at(p1, cam1, w0, x), _wall_at(p2, cam2, w0, x)
        assert b is not None
        assert abs((b - expected).total_seconds()) <= 0.5
        if a is not None:  # khe hở CAM1 (10..30) là khung đen
            assert abs((a - b).total_seconds()) <= 0.5
        else:
            assert 10.0 <= x < 30.0
    gaps = exports.wall_gaps(p1, w0)
    assert gaps == [(T0 + timedelta(seconds=10), T0 + timedelta(seconds=30))]
    assert exports.wall_gaps(p2, w0) == []


@pytest.mark.skipif(not HAS_FFMPEG, reason="máy không có ffmpeg/ffprobe")
async def test_aligned_export_command_runs(tmp_path: Path) -> None:
    """Lệnh ghép căn giờ chạy với FFmpeg thật (bỏ chữ: máy test có thể thiếu drawtext); độ dài = cửa sổ."""
    from aicam.modules.media import ffmpeg

    sample = make_sample(tmp_path)
    parts1 = [ffmpeg.Part("VIDEO", 0.0, 3.0), ffmpeg.Part("GAP", 0.0, 2.0), ffmpeg.Part("VIDEO", 5.0, 8.0)]
    parts2 = [ffmpeg.Part("VIDEO", 1.0, 9.0)]
    out = tmp_path / "out.mp4"
    cmd = ffmpeg.aligned_export_command(
        shutil.which("ffmpeg") or "ffmpeg", [(sample, parts1), (sample, parts2)], out,
        text_file=None, gap_text_file=None, clock=[], font_file=None, duration=8.0,
        preset="ultrafast", side_scale="320:240",
    )  # fmt: skip
    await ffmpeg.run(cmd, 60)
    duration = await ffmpeg.probe_duration(shutil.which("ffprobe") or "ffprobe", out)
    assert duration == pytest.approx(8.0, abs=0.2)


async def test_render_side_by_side_with_gap_aligns_and_reports(
    db: AsyncSession, redis_client: object, media_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """J-03 với CAM1 có khe hở: dùng lệnh căn giờ (khung đen + 1 đồng hồ từ giờ chung), info.json ghi
    `video_gaps`, mốc giờ ISO-8601 'Z' (G3-F10)."""
    from aicam.modules.media import ffmpeg

    user = await make_user(db, "tst_cskh", "CSKH")
    _, station = await make_station_account(db)
    session_id = await _session_with_clips(db, media_settings, station, {"CAM1": "READY", "CAM2": "READY"})
    (media_settings.video_root / "clips").mkdir(exist_ok=True)
    for role in ("CAM1", "CAM2"):
        (media_settings.video_root / f"clips/{session_id}-{role}.mp4").write_bytes(b"x")
    clips = {c.camera_role: c for c in (await db.scalars(select(Clip).where(Clip.session_id == session_id)))}
    clips["CAM1"].timeline = [
        {"t": 0.0, "wall": clock.iso_z(T0 - timedelta(seconds=5))},
        {"t": 4.0, "wall": clock.iso_z(T0 + timedelta(seconds=2))},  # mất 3 giây
    ]
    clips["CAM1"].duration_s = 7  # T0−5 … T0−1, T0+2 … T0+5
    export = Export(session_id=session_id, layout="SIDE_BY_SIDE", created_by=user.id,
                    expires_at=clock.now() + timedelta(hours=1))  # fmt: skip
    db.add(export)
    await db.flush()
    seen: list[list[str]] = []

    async def fake_run(cmd: list[str], *_: Any) -> None:
        seen.append(cmd)
        Path(cmd[-1]).write_bytes(b"video")  # noqa: ASYNC240

    monkeypatch.setattr(ffmpeg, "run_with_progress", fake_run)

    assert await exports.render_export(db, export.id, media_settings) == "READY"

    graph = seen[0][seen[0].index("-filter_complex") + 1]
    assert "concat=n=3" in graph  # CAM1: video, khung đen, video
    assert "color=c=black" in graph
    assert seen[0][seen[0].index("-t") + 1] == "10.000"
    info = json.loads((media_settings.video_root / f"exports/{export.id}/info.json").read_text())
    assert info["video_gaps"] == [{"camera_role": "CAM1", "from": clock.iso_z(T0 - timedelta(seconds=1)),
                                   "to": clock.iso_z(T0 + timedelta(seconds=2)), "seconds": 3.0}]  # fmt: skip
    for key in ("session_started_at", "session_ended_at", "video_start_at", "video_end_at", "exported_at"):
        assert info[key].endswith("Z"), key
    assert info["video_start_at"] == clock.iso_z(T0 - timedelta(seconds=5))
