"""J-01 cắt clip, J-10 index segment, API-40/41, cờ VIDEO_INCOMPLETE (T-14).

TC-02.01 (logic), TC-02.03, TC-02.04, TC-02.11, TC-02.12, TC-02.13 (API), TC-02.15, TC-02.19, TC-P.04.
FFmpeg thật trên segment fMP4 sinh bằng `testsrc` (máy không có ffmpeg → skip có lý do).
"""

import hashlib
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.media import jobs
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip, VideoSegment
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession, SessionEvent
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations import service as stations
from aicam.modules.stations.mediamtx import PathStat

from .factories import PASSWORD, make_station_account, make_user
from .media_fixtures import (
    make_cameras,
    make_closed_session,
    make_sample,
    needs_ffmpeg,
    put_segments,
    starts_every,
)

pytestmark = pytest.mark.integration

T = datetime(2026, 10, 5, 3, 0, 0, tzinfo=UTC)  # 10:00 giờ VN


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


@pytest.fixture(scope="session")
def sample(tmp_path_factory: pytest.TempPathFactory) -> Path:
    if not media_ffmpeg_available():
        pytest.skip("máy không có ffmpeg/ffprobe")
    return make_sample(tmp_path_factory.mktemp("sample"))


def media_ffmpeg_available() -> bool:
    from .media_fixtures import HAS_FFMPEG

    return HAS_FFMPEG


async def _station_with_cams(db: AsyncSession, n: int = 1) -> tuple[Any, Any, dict[str, Any]]:
    user, station = await make_station_account(db, f"tst_station0{n}", f"TST Station 0{n}")
    return user, station, await make_cameras(db, station)


async def _clip(db: AsyncSession, session_id: uuid.UUID, role: str = "CAM1") -> Clip:
    clip = await db.scalar(
        select(Clip)
        .where(Clip.session_id == session_id, Clip.camera_role == role)
        .execution_options(populate_existing=True)
    )
    assert clip is not None
    return clip


# ---------------------------------------------------------------- J-01


@needs_ffmpeg
async def test_build_clips_cover_window_hash_and_readonly(
    db: AsyncSession, redis_client: object, media_settings: Settings, sample: Path
) -> None:
    """TC-02.01 (logic), TC-02.04, TC-02.15: clip phủ [mở − 5s, đóng + 5s], SHA-256 khớp file, quyền 444."""
    _, station, cams = await _station_with_cams(db)
    for cam in cams.values():
        put_segments(media_settings.video_root, cam.mediamtx_path, sample, starts_every(T, 6))
    pack = await make_closed_session(
        db, station, "SPXTST0000001", T + timedelta(seconds=15), T + timedelta(seconds=40)
    )
    clock.freeze(T + timedelta(seconds=49))

    result = await media.build_session_clips(db, pack.id, media_settings)

    assert result.retry_in is None
    assert len(result.ready) == 2
    for role in ("CAM1", "CAM2"):
        clip = await _clip(db, pack.id, role)
        assert clip.status == "READY"
        path = media_settings.video_root / (clip.path or "")
        assert clip.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
        assert oct(path.stat().st_mode & 0o777) == "0o444"
        assert clip.start_at <= T + timedelta(seconds=10)  # lùi về keyframe, không hụt đầu
        assert clip.end_at >= T + timedelta(seconds=44.9)
        assert float(clip.duration_s or 0) >= 34.9
        assert clip.flags == []
    refreshed = await db.get(PackSession, pack.id, populate_existing=True)
    assert refreshed is not None
    assert "VIDEO_INCOMPLETE" not in refreshed.flags


@needs_ffmpeg
async def test_missing_segment_flags_video_incomplete(
    db: AsyncSession, redis_client: object, media_settings: Settings, sample: Path
) -> None:
    """TC-02.03: camera rớt giữa phiên → clip vẫn READY, cờ VIDEO_INCOMPLETE cho clip và phiên."""
    _, station, cams = await _station_with_cams(db)
    starts = starts_every(T, 6)
    put_segments(media_settings.video_root, cams["CAM1"].mediamtx_path, sample, starts[:2] + starts[3:])
    put_segments(media_settings.video_root, cams["CAM2"].mediamtx_path, sample, starts)
    pack = await make_closed_session(
        db, station, "SPXTST0000002", T + timedelta(seconds=15), T + timedelta(seconds=40)
    )
    clock.freeze(T + timedelta(seconds=49))

    await media.build_session_clips(db, pack.id, media_settings)

    cam1 = await _clip(db, pack.id, "CAM1")
    assert (cam1.status, cam1.flags) == ("READY", ["VIDEO_INCOMPLETE"])
    assert cam1.timeline is not None
    assert len(cam1.timeline) == 2  # giờ thực sau khe hở không bị lệch
    assert datetime.fromisoformat(cam1.timeline[1]["wall"]) == T + timedelta(seconds=30)
    assert (await _clip(db, pack.id, "CAM2")).flags == []
    refreshed = await db.get(PackSession, pack.id, populate_existing=True)
    assert refreshed is not None
    assert "VIDEO_INCOMPLETE" in refreshed.flags


async def test_waits_until_padding_after_close(db: AsyncSession, media_settings: Settings) -> None:
    _, station, _ = await _station_with_cams(db)
    pack = await make_closed_session(
        db, station, "SPXTST0000003", T + timedelta(seconds=15), T + timedelta(seconds=40)
    )
    clock.freeze(T + timedelta(seconds=44))

    result = await media.build_session_clips(db, pack.id, media_settings)

    assert result.retry_in == pytest.approx(4.0)  # 40 + đệm 5 + chờ ghi 3 − 44
    assert await db.scalar(select(func.count()).select_from(Clip).where(Clip.session_id == pack.id)) == 0


@needs_ffmpeg
async def test_retry_while_recording_then_fail_without_camera(
    db: AsyncSession, redis_client: object, media_settings: Settings, sample: Path
) -> None:
    """Segment đang ghi chưa tới cuối khoảng → thử lại; camera không có video ở lần cuối → FAILED."""
    _, station, cams = await _station_with_cams(db)
    files = put_segments(media_settings.video_root, cams["CAM1"].mediamtx_path, sample, starts_every(T, 4))
    now = time.time()
    os.utime(files[-1], (now, now))  # file cuối vừa được MediaMTX ghi (T+30..T+40, cần tới T+45)
    pack = await make_closed_session(
        db, station, "SPXTST0000004", T + timedelta(seconds=15), T + timedelta(seconds=40)
    )
    clock.freeze(T + timedelta(seconds=49))

    first = await media.build_session_clips(db, pack.id, media_settings)
    assert first.retry_in is not None
    assert (await _clip(db, pack.id, "CAM1")).status == "PENDING"

    final = await media.build_session_clips(db, pack.id, media_settings, final=True)
    cam1, cam2 = await _clip(db, pack.id, "CAM1"), await _clip(db, pack.id, "CAM2")
    assert cam1.status == "READY"  # lần cuối: cắt phần đã có
    assert cam1.flags == ["VIDEO_INCOMPLETE"]
    assert cam2.status == "FAILED"
    assert cam2.error is not None
    assert "Không có video" in cam2.error
    assert final.failed == [cam2.id]


@needs_ffmpeg
async def test_rerun_keeps_ready_clip(
    db: AsyncSession, redis_client: object, media_settings: Settings, sample: Path
) -> None:
    """Job chạy 2 lần (J-11 quét lại): clip READY giữ nguyên, không cắt lại (02a §6)."""
    _, station, cams = await _station_with_cams(db)
    for cam in cams.values():
        put_segments(media_settings.video_root, cam.mediamtx_path, sample, starts_every(T, 6))
    pack = await make_closed_session(
        db, station, "SPXTST0000005", T + timedelta(seconds=15), T + timedelta(seconds=40)
    )
    clock.freeze(T + timedelta(seconds=49))
    await media.build_session_clips(db, pack.id, media_settings)
    before = (await _clip(db, pack.id)).sha256

    again = await media.build_session_clips(db, pack.id, media_settings)

    assert again.ready == []
    assert (await _clip(db, pack.id)).sha256 == before


# ---------------------------------------------------------------- enqueue + cờ camera


@pytest.fixture
async def station_headers(api: AsyncClient, db: AsyncSession) -> tuple[dict[str, str], Any]:
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()  # type: ignore[attr-defined]
    user, station = await make_station_account(db)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, station


async def _scan(api: AsyncClient, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = await api.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    return res.json()  # type: ignore[no-any-return]


async def test_closing_or_cancelling_enqueues_clip_job(
    api: AsyncClient, station_headers: tuple[dict[str, str], Any], sent_jobs: list[Any]
) -> None:
    """Đóng phiên và hủy phiên đều đẩy J-01 (queue video) sau commit (FR-02.02)."""
    headers, _ = station_headers
    opened = await _scan(api, headers, "SPXTST0000001")
    assert sent_jobs == []
    await _scan(api, headers, "SPXTST0000001")
    second = await _scan(api, headers, "SPXTST0000002")
    await api.post(
        f"/api/v1/station/sessions/{second['state']['session']['id']}/cancel",
        headers=headers,
        json={"reason": "WRONG_SCAN"},
    )

    assert [(t, q) for t, _, q, _ in sent_jobs] == [(jobs.BUILD_CLIPS, "video")] * 2
    assert sent_jobs[0][1] == [opened["state"]["session"]["id"]]
    assert sent_jobs[0][3] == pytest.approx(8.0, abs=1.0)  # đệm 5 + chờ ghi 3 giây


async def test_camera_offline_flags_open_session(
    api: AsyncClient, db: AsyncSession, station_headers: tuple[dict[str, str], Any]
) -> None:
    """Review M1 #15 / EX-P7: camera mất giữa phiên → phiên đang mở có cờ VIDEO_INCOMPLETE."""
    headers, station = station_headers
    opened = await _scan(api, headers, "SPXTST0000003")
    session_id = uuid.UUID(opened["state"]["session"]["id"])

    flagged = await sessions.mark_camera_lost(db, station.id, "CAM1")
    await db.flush()

    assert flagged == session_id
    pack = await db.get(PackSession, session_id, populate_existing=True)
    assert pack is not None
    assert "VIDEO_INCOMPLETE" in pack.flags
    events = (await db.scalars(select(SessionEvent.type).where(SessionEvent.session_id == session_id))).all()
    assert "CAMERA_OFFLINE" in events
    assert await sessions.mark_camera_lost(db, uuid.uuid4(), "CAM1") is None


# ---------------------------------------------------------------- J-10


@needs_ffmpeg
async def test_index_segments_adds_closed_and_prunes_vanished(
    db: AsyncSession, media_settings: Settings, sample: Path
) -> None:
    """J-10: index file đã đóng; file biến mất (MediaMTX dev tự xóa 1 giờ, qa-reset) → bỏ khỏi index."""
    _, _, cams = await _station_with_cams(db)
    cam = cams["CAM1"]
    files = put_segments(media_settings.video_root, cam.mediamtx_path, sample, starts_every(T, 3))
    now = time.time()
    os.utime(files[-1], (now, now))  # đang ghi
    since = T - timedelta(days=1)

    assert await media.index_camera(db, cam, media_settings, since=since) == 2
    assert await media.index_camera(db, cam, media_settings, since=since) == 0  # idempotent
    files[0].unlink()
    await media.index_camera(db, cam, media_settings, since=since)

    rows = (await db.scalars(select(VideoSegment).where(VideoSegment.camera_id == cam.id))).all()
    assert [r.start_at for r in rows] == [T + timedelta(seconds=10)]
    assert rows[0].end_at == T + timedelta(seconds=20)
    assert rows[0].path == f"raw/{cam.mediamtx_path}/2026/10/05/03-00-10-000000.mp4"


class FakeMediaMTX:
    def __init__(self, paths: list[str]) -> None:
        self.paths = {p: "" for p in paths}
        self.deleted: list[str] = []

    async def upsert_path(self, name: str, source: str) -> None:
        self.paths[name] = source

    async def delete_path(self, name: str) -> None:
        self.deleted.append(name)
        self.paths.pop(name, None)

    async def list_paths(self) -> dict[str, PathStat]:
        return {p: PathStat(p, True, 0) for p in self.paths}


async def test_reconcile_mediamtx_readds_missing_and_drops_orphans(
    db: AsyncSession, media_settings: Settings
) -> None:
    """J-10 (DEC-102): MediaMTX khởi động lại mất path → thêm lại; path camera không còn trong DB → xóa."""
    _, _, cams = await _station_with_cams(db)
    orphan = f"cam-{uuid.uuid4()}"
    fake = FakeMediaMTX([cams["CAM2"].mediamtx_path, orphan, "cam-fake1"])

    out = await stations.reconcile_mediamtx(db, fake, media_settings)

    assert out == {"added": 1, "removed": 1}
    assert fake.paths[cams["CAM1"].mediamtx_path] == "rtsp://10.0.0.1/s"
    assert fake.deleted == [orphan]
    assert "cam-fake1" in fake.paths


# ---------------------------------------------------------------- API-40 / 41


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> tuple[dict[str, str], uuid.UUID]:
    user = await make_user(db, f"tst_{role.lower()}", role, display_name="Lan")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


async def _ready_clip(
    db: AsyncSession, settings: Settings, station: Any, code: str, started: datetime, status: str = "READY"
) -> Clip:
    pack = await make_closed_session(db, station, code, started, started + timedelta(seconds=30))
    rel = f"clips/{code}.mp4"
    path = settings.video_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00" * 4096)
    clip = Clip(
        session_id=pack.id,
        camera_role="CAM1",
        status=status,
        start_at=started - timedelta(seconds=5),
        end_at=started + timedelta(seconds=35),
        path=rel,
        sha256="x" * 64,
        deleted_at=clock.now() if status == "DELETED" else None,
    )
    db.add(clip)
    await db.flush()
    return clip


async def test_play_url_roles_and_states(
    api: AsyncClient, db: AsyncSession, media_settings: Settings, station_headers: tuple[dict[str, str], Any]
) -> None:
    """TC-P.04, TC-02.11, TC-02.06 bước 6: quyền theo vai, clip đang cắt 409, đã xóa 410."""
    clock.freeze(datetime.now(UTC))
    station_h, station = station_headers
    _, other = await make_station_account(db, "tst_station02", "TST Station 02")
    cskh, _ = await _login(api, db, "CSKH")
    today = clock.now() - timedelta(minutes=5)
    own = await _ready_clip(db, media_settings, station, "SPXTST0000011", today)
    foreign = await _ready_clip(db, media_settings, other, "SPXTST0000012", today)
    old = await _ready_clip(db, media_settings, station, "SPXTST0000013", today - timedelta(days=2))
    pending = await _ready_clip(db, media_settings, station, "SPXTST0000014", today, status="PENDING")
    deleted = await _ready_clip(db, media_settings, station, "SPXTST0000015", today, status="DELETED")

    ok = await api.get(f"/api/v1/clips/{own.id}/play-url", headers=cskh)
    assert ok.status_code == 200
    body = ok.json()
    assert body["url"].startswith(f"/api/v1/media/clips/{own.id}?uid=")
    assert datetime.fromisoformat(body["expires_at"]) == clock.now().replace(microsecond=0) + timedelta(
        seconds=600
    )
    assert (await api.get(f"/api/v1/clips/{own.id}/play-url", headers=station_h)).status_code == 200
    for clip in (foreign, old):
        res = await api.get(f"/api/v1/clips/{clip.id}/play-url", headers=station_h)
        assert res.status_code == 403
        assert res.json()["error"]["code"] == "FORBIDDEN"
    res = await api.get(f"/api/v1/clips/{pending.id}/play-url", headers=cskh)
    assert (res.status_code, res.json()["error"]["code"]) == (409, "CLIP_NOT_READY")
    res = await api.get(f"/api/v1/clips/{deleted.id}/play-url", headers=cskh)
    assert (res.status_code, res.json()["error"]["code"]) == (410, "CLIP_DELETED")
    assert res.json()["error"]["details"]["retention_clip_days"] == 90
    assert res.json()["error"]["details"]["deleted_at"] is not None
    assert (await api.get(f"/api/v1/clips/{uuid.uuid4()}/play-url", headers=cskh)).status_code == 404


async def test_media_signature_range_and_view_audit(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-02.12, TC-02.13 (API), TC-02.19, DEC-13: phát từ byte 0 → 1 dòng VIEW_CLIP theo uid; Range giữa
    file không ghi thêm; đổi uid / quá hạn → 403 SIGNATURE_INVALID."""
    clock.freeze(datetime.now(UTC))
    _, station = await make_station_account(db)
    cskh, cskh_id = await _login(api, db, "CSKH")
    clip = await _ready_clip(db, media_settings, station, "SPXTST0000021", clock.now() - timedelta(minutes=1))
    url = (await api.get(f"/api/v1/clips/{clip.id}/play-url", headers=cskh)).json()["url"]

    first = await api.get(url, headers={"Range": "bytes=0-"})
    middle = await api.get(url, headers={"Range": "bytes=1024-2047"})

    assert (first.status_code, first.headers["content-type"]) == (206, "video/mp4")
    assert middle.status_code == 206
    assert len(middle.content) == 1024
    views = (await db.scalars(select(AuditLog).where(AuditLog.action == "VIEW_CLIP"))).all()
    assert [(v.user_id, v.object_id) for v in views] == [(cskh_id, str(clip.id))]

    tampered = url.replace(str(cskh_id), str(uuid.uuid4()))
    res = await api.get(tampered)
    assert (res.status_code, res.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")
    clock.advance(timedelta(minutes=11))
    res = await api.get(url)
    assert (res.status_code, res.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")


async def test_no_api_to_modify_or_delete_clip(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-02.05 (AC-11, DEC-27): không có DELETE / PUT / PATCH trên clip."""
    _, station = await make_station_account(db)
    admin, _ = await _login(api, db, "ADMIN")
    clip = await _ready_clip(db, media_settings, station, "SPXTST0000031", clock.now())
    for method in ("DELETE", "PUT", "PATCH"):
        res = await api.request(method, f"/api/v1/clips/{clip.id}", headers=admin)
        assert res.status_code in (404, 405)
    assert (await _clip(db, clip.session_id)).status == "READY"
