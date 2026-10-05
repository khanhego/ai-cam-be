"""API-42 giữ clip, API-46 cắt lại, J-02 retention (T-21) — BR-09, AC-15, AC-20.

TC-02.06, TC-02.07, TC-02.08, TC-02.10, TC-P.05.
"""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.media import jobs
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip, VideoSegment
from aicam.modules.settings.models import Setting
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .media_fixtures import make_cameras, make_closed_session

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 5, 3, 0, tzinfo=UTC)


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> tuple[dict[str, str], uuid.UUID]:
    user = await make_user(db, f"tst_{role.lower()}", role, display_name=f"Người {role}")
    if role == "STATION":
        db.add(Station(name="TST Station Z", account_user_id=user.id))
        await db.flush()
    res = await api.post(
        "/api/v1/auth/login",
        json={"username": user.username, "password": PASSWORD,
              "client": "STATION" if role == "STATION" else "DASHBOARD"},
    )  # fmt: skip
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


async def _clip(
    db: AsyncSession, settings: Settings, station: Station, code: str, ended: datetime, status: str = "READY"
) -> tuple[Clip, Path]:
    pack = await make_closed_session(db, station, code, ended - timedelta(minutes=2), ended)
    rel = f"clips/{ended:%Y/%m/%d}/{pack.id}-CAM1.mp4"
    path = settings.video_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00" * 1024)
    path.chmod(0o444)
    clip = Clip(session_id=pack.id, camera_role="CAM1", status=status, start_at=ended - timedelta(minutes=2),
                end_at=ended + timedelta(seconds=5), path=rel if status == "READY" else None,
                sha256="ab" * 32,
                error="ffmpeg lỗi" if status == "FAILED" else None)  # fmt: skip
    db.add(clip)
    await db.flush()
    return clip, path


async def _reload(db: AsyncSession, clip_id: uuid.UUID) -> Clip:
    clip = await db.get(Clip, clip_id, populate_existing=True)
    assert clip is not None
    return clip


# ---------------------------------------------------------------- API-42


async def test_hold_and_unhold(api: AsyncClient, db: AsyncSession, media_settings: Settings) -> None:
    """FR-02.09: CSKH giữ → retention_until null + audit; bỏ giữ → tính lại từ setting."""
    clock.freeze(T0)
    headers, uid = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    clip, _ = await _clip(db, media_settings, station, "SPXTST0000001", T0)

    held = await api.put(f"/api/v1/clips/{clip.id}/hold", headers=headers, json={"held": True})
    assert held.status_code == 200
    body = held.json()
    assert (body["held"], body["retention_until"]) == (True, None)
    assert body["held_by"] == {"id": str(uid), "display_name": "Người CSKH"}
    unheld = (await api.put(f"/api/v1/clips/{clip.id}/hold", headers=headers, json={"held": False})).json()
    assert (unheld["held"], unheld["held_by"], unheld["held_at"]) == (False, None, None)
    assert datetime.fromisoformat(unheld["retention_until"]) == clip.end_at + timedelta(days=90)
    actions = (await db.scalars(select(AuditLog.action).where(AuditLog.object_id == str(clip.id)))).all()
    assert list(actions) == ["HOLD_CLIP", "UNHOLD_CLIP"]


async def test_hold_permissions_and_deleted(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-P.05: Station ⛔ giữ clip; clip đã xóa → 410 CLIP_DELETED."""
    station_h, _ = await _login(api, db, "STATION")
    sup, _ = await _login(api, db, "SUPERVISOR")
    _, station = await make_station_account(db)
    clip, _ = await _clip(db, media_settings, station, "SPXTST0000002", T0, status="DELETED")

    assert (
        await api.put(f"/api/v1/clips/{clip.id}/hold", headers=station_h, json={"held": True})
    ).status_code == 403
    res = await api.put(f"/api/v1/clips/{clip.id}/hold", headers=sup, json={"held": True})
    assert (res.status_code, res.json()["error"]["code"]) == (410, "CLIP_DELETED")


# ---------------------------------------------------------------- J-02


async def test_retention_keeps_held_clip_deletes_others(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-02.06 / AC-15 / BR-09: +91 ngày, clip giữ còn READY; clip không giữ DELETED, mất file, 410."""
    clock.freeze(T0)
    headers, _ = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    clip_a, path_a = await _clip(db, media_settings, station, "SPXTST0000001", T0)
    clip_b, path_b = await _clip(db, media_settings, station, "SPXTST0000002", T0)
    await api.put(f"/api/v1/clips/{clip_a.id}/hold", headers=headers, json={"held": True})

    clock.advance(timedelta(days=91))
    out = await media.enforce_retention(db, media_settings)

    assert out["clips"] == 1
    a, b = await _reload(db, clip_a.id), await _reload(db, clip_b.id)
    assert (a.status, path_a.exists()) == ("READY", True)
    assert (b.status, path_b.exists(), b.deleted_at) == ("DELETED", False, clock.now())
    entry = await db.scalar(select(AuditLog).where(AuditLog.action == "DELETE_CLIP"))
    assert entry is not None
    assert (entry.user_id, entry.object_id) == (None, str(clip_b.id))
    login = await api.post(  # token cũ đã hết hạn sau khi tua 91 ngày
        "/api/v1/auth/login", json={"username": "tst_cskh", "password": PASSWORD, "client": "DASHBOARD"}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
    res = await api.get(f"/api/v1/clips/{clip_b.id}/play-url", headers=headers)
    assert (res.status_code, res.json()["error"]["code"]) == (410, "CLIP_DELETED")


async def test_retention_uses_current_setting(db: AsyncSession, media_settings: Settings) -> None:
    """TC-02.07 / AC-20: clip 100 ngày tuổi, tăng clip_days 90 → 180 trước J-02 → không bị xóa."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    clip, path = await _clip(db, media_settings, station, "SPXTST0000003", T0 - timedelta(days=100))
    await db.execute(update(Setting).where(Setting.id == 1).values(retention_clip_days=180))

    out = await media.enforce_retention(db, media_settings)

    assert out["clips"] == 0
    assert ((await _reload(db, clip.id)).status, path.exists()) == ("READY", True)
    await db.execute(update(Setting).where(Setting.id == 1).values(retention_clip_days=90))
    assert (await media.enforce_retention(db, media_settings))["clips"] == 1


async def test_retention_raw_video(db: AsyncSession, media_settings: Settings) -> None:
    """TC-02.08: segment kết thúc 31 ngày trước bị xóa (file + dòng index); segment 1 ngày trước còn.

    Quét theo đĩa nên cả file của camera không còn trong DB cũng bị dọn."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    cam = (await make_cameras(db, station))["CAM1"]
    files = {}
    for name, age in (("old", 31), ("new", 1), ("orphan", 40)):
        start = T0 - timedelta(days=age)
        folder = cam.mediamtx_path if name != "orphan" else f"cam-{uuid.uuid4()}"
        f = media_settings.video_root / "raw" / folder / f"{start:%Y/%m/%d/%H-%M-%S-%f}.mp4"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"x")
        files[name] = f
        if name != "orphan":
            rel = str(f.relative_to(media_settings.video_root))
            end = start + timedelta(seconds=60)
            db.add(VideoSegment(camera_id=cam.id, start_at=start, end_at=end, path=rel, size_bytes=1))
    await db.flush()

    out = await media.enforce_retention(db, media_settings)

    assert out["raw_files"] == 2
    assert out["segment_rows"] == 1
    assert [f.exists() for f in files.values()] == [False, True, False]
    left = (await db.scalars(select(VideoSegment.start_at).where(VideoSegment.camera_id == cam.id))).all()
    assert left == [T0 - timedelta(days=1)]


# ---------------------------------------------------------------- API-46


async def test_rebuild_failed_clip(
    api: AsyncClient, db: AsyncSession, media_settings: Settings, sent_jobs: list[Any]
) -> None:
    """TC-02.10: FAILED → 202 queued, clip PENDING, J-01 được đẩy; không còn FAILED → 409; CSKH ⛔."""
    sup, _ = await _login(api, db, "SUPERVISOR")
    cskh, _ = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    clip, _ = await _clip(db, media_settings, station, "SPXTST0000004", T0, status="FAILED")
    url = f"/api/v1/sessions/{clip.session_id}/clips/rebuild"

    assert (await api.post(url, headers=cskh)).status_code == 403
    res = await api.post(url, headers=sup)

    assert (res.status_code, res.json()) == (202, {"queued": True})
    reloaded = await _reload(db, clip.id)
    assert (reloaded.status, reloaded.error) == ("PENDING", None)
    assert [(t, a) for t, a, _, _ in sent_jobs] == [(jobs.BUILD_CLIPS, [str(clip.session_id)])]
    again = await api.post(url, headers=sup)
    assert (again.status_code, again.json()["error"]["code"]) == (409, "CLIP_NOT_FAILED")
    assert (await api.post(f"/api/v1/sessions/{uuid.uuid4()}/clips/rebuild", headers=sup)).status_code == 404


async def test_retention_keeps_raw_video_of_failed_clip(db: AsyncSession, media_settings: Settings) -> None:
    """G3-F7: video thô quá hạn nhưng chồng [mở − đệm, đóng + đệm] của phiên có clip FAILED → giữ (API-46
    cắt lại được); file cũ khác của cùng camera vẫn bị xóa."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    cam = (await make_cameras(db, station))["CAM1"]
    ended = T0 - timedelta(days=40)
    clip, _ = await _clip(db, media_settings, station, "SPXTST0000031", ended, status="FAILED")
    files = {}
    for name, start in (
        ("before", ended - timedelta(minutes=2, seconds=50)),  # segment chứa đầu phiên (mở = đóng − 2 phút)
        ("inside", ended - timedelta(seconds=30)),
        ("other", ended - timedelta(hours=3)),
    ):
        f = media_settings.video_root / "raw" / cam.mediamtx_path / f"{start:%Y/%m/%d/%H-%M-%S-%f}.mp4"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"x")
        files[name] = f

    out = await media.enforce_retention(db, media_settings)

    assert out["raw_files"] == 1
    assert {k: f.exists() for k, f in files.items()} == {"before": True, "inside": True, "other": False}
    # Clip cắt lại thành công → lượt sau được xóa.
    await db.execute(update(Clip).where(Clip.id == clip.id).values(status="READY", end_at=T0))
    assert (await media.enforce_retention(db, media_settings))["raw_files"] == 2


async def test_retention_commits_before_unlink_and_retries_file(
    db: AsyncSession, media_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-F8: DELETED + audit commit trước; xóa file lỗi → clip vẫn DELETED, lượt J-02 sau xóa nốt file."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    clip, path = await _clip(db, media_settings, station, "SPXTST0000032", T0 - timedelta(days=100))
    original = media._unlink

    def _fail(p: Path) -> None:
        if "clips" in p.parts:
            raise PermissionError("ổ chỉ đọc")
        original(p)

    monkeypatch.setattr(media, "_unlink", _fail)
    first = await media.enforce_retention(db, media_settings)
    assert (first["clips"], first["clip_files_retried"]) == (1, 0)
    assert ((await _reload(db, clip.id)).status, path.exists()) == ("DELETED", True)

    monkeypatch.setattr(media, "_unlink", original)
    second = await media.enforce_retention(db, media_settings)
    assert (second["clips"], second["clip_files_retried"]) == (0, 1)
    assert not path.exists()
