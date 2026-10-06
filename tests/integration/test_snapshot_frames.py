"""T-121 (NFR-32, RB-21; DEC-320): ảnh lấy khung mới nhất vision giữ trong Redis thay vì mở RTSP mỗi lần.

`frame:{camera_id}` (TTL 5 giây) — API-103 dùng khung ≤ 2 giây tuổi, cũ / thiếu → `grab_frame` như cũ; đóng
phiên PACK → ảnh lúc đóng gói ngay từ khung (J-17 thấy đã có, bỏ qua); vision giữ khung cả Cam 1 lẫn Cam 2.
"""

import asyncio
import hashlib
import re
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.media import frames, snapshots
from aicam.modules.media.models import Snapshot
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions import service as sessions_service
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations.models import Camera
from aicam.modules.vision.runner import FrameOptions, Target, TrayRunner

from .returns_helpers import Desk, buyer_return_case, make_desk, make_order

pytestmark = pytest.mark.integration

JPEG = b"\xff\xd8\xff\xe0" + b"frame-from-vision" * 8 + b"\xff\xd9"


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    monkeypatch.setattr(sessions_service, "get_settings", lambda: test_settings)
    return test_settings


@pytest.fixture
def grabs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    async def fake(url: str, timeout_s: float) -> bytes:
        calls.append(url)
        return b"\xff\xd8rtsp\xff\xd9"

    monkeypatch.setattr(snapshots, "_grab", fake)
    return calls


async def _desk(api: AsyncClient, db: AsyncSession, mode: str) -> tuple[Desk, Camera]:
    api._transport.app.dependency_overrides[get_platform_adapter] = MockAdapter  # type: ignore[attr-defined]
    desk = await make_desk(api, db, mode=mode)
    cam = Camera(station_id=desk.station.id, role="CAM1", rtsp_url="rtsp://x", mediamtx_path="cam-f1")
    db.add(cam)
    await db.flush()
    return desk, cam


# ---------------------------------------------------------------- Redis `frame:{camera_id}`


async def test_store_and_latest_age(redis_client: Redis) -> None:
    """Khung ≤ 2 giây → dùng; cũ hơn / ở tương lai quá 0,5 giây / hỏng / không có → None; TTL 5 giây."""
    camera_id = uuid.uuid4()
    now = clock.now()
    assert await frames.latest(redis_client, camera_id, max_age_s=2) is None
    await frames.store(redis_client, camera_id, JPEG, now - timedelta(seconds=1.5))
    found = await frames.latest(redis_client, camera_id, max_age_s=2, at=now)
    assert found is not None
    assert (found.jpeg, found.taken_at) == (JPEG, now - timedelta(seconds=1.5))
    assert 0 < await redis_client.ttl(frames.frame_key(camera_id)) <= frames.FRAME_TTL_S
    assert await frames.latest(redis_client, camera_id, max_age_s=2, at=now + timedelta(seconds=1)) is None
    assert await frames.latest(redis_client, camera_id, max_age_s=2, at=now - timedelta(seconds=3)) is None
    await redis_client.set(frames.frame_key(camera_id), "{not json")
    assert await frames.latest(redis_client, camera_id, max_age_s=2) is None


async def test_runner_reads_cam1_frames_only_and_writer_stores(redis_client: Redis) -> None:
    """Vision: Cam 1 có reader lấy khung (không đọc mã, không theo dõi khay); Cam 2 như cũ; tắt khung → chỉ
    Cam 2. `FrameWriter` giữ khung mới nhất mỗi camera và ghi Redis."""
    made: list[tuple[Target, Any]] = []

    class _Reader:
        def __init__(self, target: Target) -> None:
            self.target = target

        def start(self) -> None: ...

        def stop(self) -> None: ...

    def factory(target: Target, emit: Any) -> Any:
        made.append((target, emit))
        return _Reader(target)

    station = uuid.uuid4()
    cam1, cam2 = (
        Target(uuid.uuid4(), station, "rtsp://m/c1", None, "CAM1"),
        Target(uuid.uuid4(), station, "rtsp://m/c2", None, "CAM2"),
    )
    runner = TrayRunner(redis_client, re.compile(r".*"), factory)
    await runner.apply([cam1, cam2])
    assert {t.role for t, _ in made} == {"CAM1", "CAM2"}
    assert set(runner.tracker.cameras) == {cam2.camera_id}

    now = clock.now()
    runner.frames.offer(cam1.camera_id, b"old", now - timedelta(seconds=1))
    runner.frames.offer(cam1.camera_id, JPEG, now)
    await asyncio.sleep(0)  # call_soon_threadsafe chạy trong vòng lặp
    assert await runner.frames.flush() == 1
    found = await frames.latest(redis_client, cam1.camera_id, max_age_s=2)
    assert found is not None
    assert found.jpeg == JPEG

    made.clear()
    off = TrayRunner(redis_client, re.compile(r".*"), factory, frame_options=FrameOptions(enabled=False))
    await off.apply([cam1, cam2])
    assert [t.role for t, _ in made] == ["CAM2"]


# ---------------------------------------------------------------- API-103


async def test_snapshot_uses_cached_frame(
    api: AsyncClient, db: AsyncSession, redis_client: Redis, media_settings: Settings, grabs: list[str]
) -> None:
    """TC-04.40 (T-121): có khung ≤ 2 giây → ảnh = đúng khung đó (SHA-256 khớp, `taken_at` = giờ chụp khung,
    file 0444), **không** mở RTSP."""
    desk, cam = await _desk(api, db, "RETURN")
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = (await desk.scan("SPXRTTST000041")).json()["state"]["session"]
    taken = clock.now() - timedelta(milliseconds=700)
    await frames.store(redis_client, cam.id, JPEG, taken)

    res = await desk.api.post(f"/api/v1/station/sessions/{session['id']}/snapshots", headers=desk.headers)

    assert res.status_code == 201, res.text
    shot = res.json()["snapshot"]
    assert grabs == []
    assert shot["sha256"] == hashlib.sha256(JPEG).hexdigest()
    assert shot["taken_at"] == clock.iso_z(taken)
    row = await db.get(Snapshot, uuid.UUID(shot["id"]))
    assert row is not None
    assert row.path is not None
    path = media_settings.video_root / row.path
    assert path.read_bytes() == JPEG
    assert oct(path.stat().st_mode & 0o777) == "0o444"


async def test_snapshot_falls_back_when_frame_stale(
    api: AsyncClient, db: AsyncSession, redis_client: Redis, media_settings: Settings, grabs: list[str]
) -> None:
    """Khung cũ hơn 2 giây (vision chậm / camera rớt) → `grab_frame` qua relay như trước."""
    desk, cam = await _desk(api, db, "RETURN")
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = (await desk.scan("SPXRTTST000041")).json()["state"]["session"]
    await frames.store(redis_client, cam.id, JPEG, clock.now() - timedelta(seconds=3))
    res = await desk.api.post(f"/api/v1/station/sessions/{session['id']}/snapshots", headers=desk.headers)
    assert res.status_code == 201, res.text
    assert grabs == [f"{media_settings.mediamtx_rtsp_url}/cam-f1"]
    assert res.json()["snapshot"]["sha256"] != hashlib.sha256(JPEG).hexdigest()


# ---------------------------------------------------------------- ảnh lúc đóng gói (L8)


async def test_pack_close_snapshot_from_cached_frame(
    api: AsyncClient, db: AsyncSession, redis_client: Redis, media_settings: Settings
) -> None:
    """Đóng phiên PACK khi có khung Cam 1 mới → `PACK_CLOSE` có ngay (không chờ J-01 + J-17); J-17 sau đó thấy
    đã có → bỏ qua. Không có khung → không tạo (J-17 trích từ clip như cũ)."""
    desk, cam = await _desk(api, db, "PACK")
    await make_order(db, 12, status="READY_TO_SHIP", warehouse_status="NEW")
    await make_order(db, 13, status="READY_TO_SHIP", warehouse_status="NEW")
    assert (await desk.scan("SPXTST0000012")).json()["outcome"] == "SESSION_OPENED"
    await frames.store(redis_client, cam.id, JPEG, clock.now())
    closed = (await desk.scan("SPXTST0000012")).json()
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    session_id = uuid.UUID(closed["closed_session"]["id"])
    row = await db.scalar(select(Snapshot).where(Snapshot.session_id == session_id))
    assert row is not None
    assert (row.kind, row.sha256) == ("PACK_CLOSE", hashlib.sha256(JPEG).hexdigest())
    assert await snapshots.capture_pack_snapshot(db, session_id, media_settings) == "exists"

    await redis_client.delete(frames.frame_key(cam.id))
    assert (await desk.scan("SPXTST0000013")).json()["outcome"] == "SESSION_OPENED"
    closed = (await desk.scan("SPXTST0000013")).json()
    other = uuid.UUID(closed["closed_session"]["id"])
    assert await db.scalar(select(Snapshot).where(Snapshot.session_id == other)) is None
