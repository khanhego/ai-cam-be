"""T-109: ảnh Cam 1 — API-103 (chụp), API-106 (tải ký), J-17 ảnh lúc đóng gói, `pack_reference`, API-40 luật
STATION — 02a §4, §7; FR-04.04, 04.12, 02.11; TC-04.40..04.43, TC-02.42, TC-P2.04, TC-N2.08."""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.media import service as media
from aicam.modules.media import signing, snapshots
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations.models import Camera
from aicam.modules.stations.probe import CameraUnreachable

from .factories import PASSWORD, make_station_account, make_user
from .media_fixtures import (
    make_cameras,
    make_closed_session,
    make_sample,
    needs_ffmpeg,
    put_segments,
    starts_every,
)
from .returns_helpers import Desk, buyer_return_case, make_desk, make_order, pack_session_with_clips

pytestmark = pytest.mark.integration

JPEG = b"\xff\xd8\xff\xe0" + b"0" * 64 + b"\xff\xd9"
T = datetime(2026, 10, 5, 3, 0, 0, tzinfo=UTC)


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
def grabs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []

    async def fake(url: str, timeout_s: float) -> bytes:
        calls.append(url)
        return JPEG

    monkeypatch.setattr(snapshots, "_grab", fake)
    return calls


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter, media_settings: Settings) -> Desk:
    desk = await make_desk(api, db)
    db.add(Camera(station_id=desk.station.id, role="CAM1", rtsp_url="rtsp://x", mediamtx_path="cam-desk1"))
    await db.flush()
    return desk


async def _open_41(desk: Desk, db: AsyncSession) -> dict[str, Any]:
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    body = (await desk.scan("SPXRTTST000041")).json()
    assert body["outcome"] == "SESSION_OPENED", body
    return body["state"]["session"]  # type: ignore[no-any-return]


async def _snap(desk: Desk, session_id: str) -> Any:
    return await desk.api.post(f"/api/v1/station/sessions/{session_id}/snapshots", headers=desk.headers)


# ---------------------------------------------------------------- API-103 / API-106


async def test_take_snapshot_and_download(
    desk: Desk, db: AsyncSession, grabs: list[str], media_settings: Settings
) -> None:
    """TC-04.40: ảnh 201, `sha256` 64 ký tự, file 0444 đúng thư mục; state có ảnh; API-106 tải được."""
    session = await _open_41(desk, db)

    res = await _snap(desk, session["id"])

    assert res.status_code == 201, res.text
    shot = res.json()["snapshot"]
    assert (shot["kind"], shot["camera_role"]) == ("MANUAL", "CAM1")
    assert len(shot["sha256"]) == 64
    assert grabs == [f"{media_settings.mediamtx_rtsp_url}/cam-desk1"]
    row = await db.get(Snapshot, uuid.UUID(shot["id"]))
    assert row is not None
    assert row.path is not None
    assert row.path.endswith(f"{session['id']}_01.jpg")
    assert row.path.startswith("snapshots/")
    path = media_settings.video_root / row.path
    assert oct(path.stat().st_mode & 0o777) == "0o444"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == shot["sha256"]
    state = await desk.state()
    assert [s["id"] for s in state["session"]["snapshots"]] == [shot["id"]]

    image = await desk.api.get(shot["url"])
    assert image.status_code == 200
    assert image.headers["content-type"] == "image/jpeg"
    assert image.content == JPEG
    views = (await db.scalars(select(AuditLog).where(AuditLog.action == "VIEW_SNAPSHOT"))).all()
    assert views == []  # tài khoản STATION xem không ghi audit


async def test_snapshot_view_by_staff_is_audited(
    desk: Desk, db: AsyncSession, grabs: list[str], media_settings: Settings
) -> None:
    admin = await make_user(db, "tst_admin", "ADMIN")
    session = await _open_41(desk, db)
    shot = (await _snap(desk, session["id"])).json()["snapshot"]
    exp = signing.expiry(600)
    url = signing.snapshot_url(media_settings.media_signing_key, uuid.UUID(shot["id"]), admin.id, exp)

    res = await desk.api.get(url)

    assert res.status_code == 200
    views = (await db.scalars(select(AuditLog).where(AuditLog.action == "VIEW_SNAPSHOT"))).all()
    assert [v.user_id for v in views] == [admin.id]


async def test_snapshot_limit(
    desk: Desk, db: AsyncSession, grabs: list[str], media_settings: Settings
) -> None:
    """TC-04.41: đủ ảnh → `409 SNAPSHOT_LIMIT`, `details.max`."""
    media_settings.snapshot_max_per_session = 2
    session = await _open_41(desk, db)
    assert (await _snap(desk, session["id"])).status_code == 201
    assert (await _snap(desk, session["id"])).status_code == 201

    res = await _snap(desk, session["id"])

    assert res.status_code == 409
    assert res.json()["error"] == {"code": "SNAPSHOT_LIMIT", "message": "Đã đủ 2 ảnh.", "details": {"max": 2}}


async def test_camera_unreachable(desk: Desk, db: AsyncSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """TC-04.42: không lấy được khung Cam 1 → `422 CAMERA_UNREACHABLE`, không ghi ảnh."""

    async def broken(url: str, timeout_s: float) -> bytes:
        raise CameraUnreachable("TIMEOUT")

    monkeypatch.setattr(snapshots, "_grab", broken)
    session = await _open_41(desk, db)

    res = await _snap(desk, session["id"])

    assert res.status_code == 422
    assert res.json()["error"]["code"] == "CAMERA_UNREACHABLE"
    assert (await db.scalars(select(Snapshot))).all() == []


async def test_snapshot_session_not_open(desk: Desk, db: AsyncSession, grabs: list[str]) -> None:
    session = await _open_41(desk, db)
    await desk.api.post(
        f"/api/v1/station/sessions/{session['id']}/cancel",
        headers=desk.headers,
        json={"reason": "WRONG_SCAN"},
    )

    res = await _snap(desk, session["id"])

    assert (res.status_code, res.json()["error"]["code"]) == (409, "SESSION_NOT_OPEN")
    assert grabs == []


async def test_snapshot_signature_and_deleted(desk: Desk, db: AsyncSession, grabs: list[str]) -> None:
    session = await _open_41(desk, db)
    shot = (await _snap(desk, session["id"])).json()["snapshot"]

    tampered = await desk.api.get(shot["url"].replace("sig=", "sig=00"))
    assert (tampered.status_code, tampered.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")

    row = await db.get(Snapshot, uuid.UUID(shot["id"]))
    assert row is not None
    row.status, row.deleted_at = "DELETED", clock.now()
    await db.flush()
    gone = await desk.api.get(shot["url"])
    assert (gone.status_code, gone.json()["error"]["code"]) == (410, "SNAPSHOT_DELETED")


async def test_snapshot_url_expires_after_ten_minutes(
    desk: Desk, db: AsyncSession, grabs: list[str], media_settings: Settings
) -> None:
    """TC-N2.08, NFR-36: URL ảnh ký (API-106) hạn 10 phút — 9 phút vẫn tải được, tua đồng hồ 11 phút →
    `403 SIGNATURE_INVALID`."""
    clock.freeze(datetime.now(UTC))
    session = await _open_41(desk, db)
    shot = (await _snap(desk, session["id"])).json()["snapshot"]
    assert shot["url"].startswith("/api/v1/media/snapshots/")

    assert (await desk.api.get(shot["url"])).status_code == 200
    clock.advance(timedelta(minutes=9))
    assert (await desk.api.get(shot["url"])).status_code == 200
    clock.advance(timedelta(minutes=2))
    expired = await desk.api.get(shot["url"])

    assert (expired.status_code, expired.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")


# ---------------------------------------------------------------- J-17 + pack_reference + API-40


@needs_ffmpeg
async def test_j17_pack_snapshot_after_clips(
    db: AsyncSession, redis_client: object, media_settings: Settings, sent_jobs: list[Any], tmp_path: Path
) -> None:
    """TC-02.42, AC-31, DEC-227: J-01 xong clip Cam 1 → J-17 trích khung `ended_at − 0,5 s` → `PACK_CLOSE`."""
    sample = make_sample(tmp_path)
    _, station = await make_station_account(db)
    cams = await make_cameras(db, station)
    for cam in cams.values():
        put_segments(media_settings.video_root, cam.mediamtx_path, sample, starts_every(T, 6))
    pack = await make_closed_session(
        db, station, "SPXTST0000001", T + timedelta(seconds=15), T + timedelta(seconds=40)
    )
    clock.freeze(T + timedelta(seconds=49))

    await media.build_session_clips(db, pack.id, media_settings)

    assert ("media.capture_pack_snapshot", [str(pack.id)], "video", 0.0) in sent_jobs
    assert await snapshots.capture_pack_snapshot(db, pack.id, media_settings) == "captured"
    assert await snapshots.capture_pack_snapshot(db, pack.id, media_settings) == "exists"
    shot = await snapshots.pack_close_of(db, pack.id)
    assert shot is not None
    assert shot.taken_at == T + timedelta(seconds=39.5)
    path = media_settings.video_root / (shot.path or "")
    assert path.read_bytes()[:2] == b"\xff\xd8"  # JPEG
    assert oct(path.stat().st_mode & 0o777) == "0o444"


async def test_pack_reference_snapshot_and_station_clip_access(
    api: AsyncClient, desk: Desk, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-04.43 (API), 02 API-40: R2 có ảnh lúc đóng gói; STATION xem clip PACK của kiện đang kiểm hoàn."""
    order, (package,) = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    _, other = await make_station_account(db, "tst_station09", "TST Station 09")
    pack = PackSession(
        type="PACK", package_id=package.id, station_id=other.id, status="COMPLETED",
        open_code="SPXTST0000041", flags=[], started_at=T, ended_at=T + timedelta(seconds=30),
    )  # fmt: skip
    db.add(pack)
    await db.flush()
    clip = Clip(
        session_id=pack.id, camera_role="CAM1", status="READY", start_at=T, end_at=T + timedelta(seconds=35),
        duration_s=Decimal("35"), path="clips/x.mp4",
    )  # fmt: skip
    db.add(clip)
    db.add(
        Snapshot(
            session_id=pack.id,
            kind="PACK_CLOSE",
            taken_at=T + timedelta(seconds=29.5),
            path="snapshots/p.jpg",
            sha256="0" * 64,
            size_bytes=10,
        )
    )
    await db.flush()

    before = await api.get(f"/api/v1/clips/{clip.id}/play-url", headers=desk.headers)
    assert before.status_code == 403  # chưa có phiên hoàn của kiện ở station này
    session = (await desk.scan("SPXRTTST000041")).json()["state"]["session"]

    ref = session["pack_reference"]
    assert ref["session_id"] == str(pack.id)
    assert ref["station_name"] == "TST Station 09"
    assert ref["clips"] == [{"id": str(clip.id), "camera_role": "CAM1", "status": "READY"}]
    assert ref["snapshot"]["url"].startswith("/api/v1/media/snapshots/")
    assert "NO_PACK_CLIP" not in session["flags"]
    during = await api.get(f"/api/v1/clips/{clip.id}/play-url", headers=desk.headers)
    assert during.status_code == 200, during.text


async def _play(api: AsyncClient, desk: Desk, clip_id: uuid.UUID) -> int:
    return (await api.get(f"/api/v1/clips/{clip_id}/play-url", headers=desk.headers)).status_code


async def test_station_clip_access_ends_when_return_session_closed_or_cancelled(
    api: AsyncClient, desk: Desk, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-P2.04, AC-35, 02 API-40: STATION xem clip PACK của kiện chỉ khi phiên RETURN của kiện đó đang
    `OPEN` / `WAITING_APPROVAL` tại station mình; phiên đã đóng → 403; phiên đã hủy (API-12) → 403; phiên kiện
    khác đang mở không mở quyền cho clip kiện trước."""
    await make_user(db, "tst_sup_p204", "SUPERVISOR")
    sup_login = await api.post(
        "/api/v1/auth/login", json={"username": "tst_sup_p204", "password": PASSWORD, "client": "DASHBOARD"}
    )
    sup = {"Authorization": f"Bearer {sup_login.json()['access_token']}"}
    _, other = await make_station_account(db, "tst_station09", "TST Station 09")
    clip_of: dict[int, uuid.UUID] = {}
    for n in (41, 42):
        order, (package,) = await make_order(db, n)
        await buyer_return_case(db, order, n)
        pack = await pack_session_with_clips(db, other, package, T - timedelta(days=2), snapshot=False)
        clip = await db.scalar(select(Clip).where(Clip.session_id == pack.id, Clip.camera_role == "CAM1"))
        assert clip is not None
        clip_of[n] = clip.id

    # Kiện 41: mở → 200; kết luận + quét đóng → 403.
    assert await _play(api, desk, clip_of[41]) == 403
    opened = (await desk.scan("SPXRTTST000041")).json()
    session = opened["state"]["session"]
    assert await _play(api, desk, clip_of[41]) == 200
    lines = [{"order_item_id": i["order_item_id"], "quantity_received": i["quantity_received"],
              "condition": "OK"} for i in session["inspection"]["lines"]]  # fmt: skip
    saved = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection",
        headers=desk.headers,
        json={"conclusion": "OK", "note": "", "lines": lines},
    )
    assert saved.status_code == 200, saved.text
    closed = (await desk.scan("SPXRTTST000041")).json()
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    assert await _play(api, desk, clip_of[41]) == 403

    # Kiện 42: mở → 200; gọi quản lý (WAITING_APPROVAL) → 200; tiếp tục rồi hủy phiên (API-12) → 403.
    session = (await desk.scan("SPXRTTST000042")).json()["state"]["session"]
    assert await _play(api, desk, clip_of[42]) == 200
    assert await _play(api, desk, clip_of[41]) == 403  # phiên đang mở là của kiện khác
    req = await api.post(
        "/api/v1/station/approval-requests",
        headers=desk.headers,
        json={"type": "ASSIST", "session_id": session["id"]},
    )
    assert req.status_code == 201, req.text
    assert (await desk.state())["session"]["status"] == "WAITING_APPROVAL"
    assert await _play(api, desk, clip_of[42]) == 200
    decided = await api.post(
        f"/api/v1/approval-requests/{req.json()['approval_request']['id']}/decision",
        headers=sup,
        json={"action": "CONTINUE"},
    )
    assert decided.status_code == 200, decided.text
    cancelled = await desk.api.post(
        f"/api/v1/station/sessions/{session['id']}/cancel",
        headers=desk.headers,
        json={"reason": "WRONG_SCAN"},
    )
    assert cancelled.status_code == 200, cancelled.text
    assert await _play(api, desk, clip_of[42]) == 403


def test_clip_offset_uses_timeline() -> None:
    clip = Clip(
        start_at=T, end_at=T + timedelta(seconds=40), duration_s=Decimal("40"),
        timeline=[{"t": 0, "wall": "2026-10-05T03:00:00Z"}, {"t": 20, "wall": "2026-10-05T03:00:30Z"}],
    )  # fmt: skip

    assert snapshots.clip_offset(clip, T + timedelta(seconds=10)) == 10.0
    assert snapshots.clip_offset(clip, T + timedelta(seconds=35)) == 25.0  # sau khe hở 10 giây
    assert snapshots.clip_offset(clip, T + timedelta(minutes=5)) == 39.9


async def test_duplicate_pack_close_does_not_overwrite_file(
    db: AsyncSession, media_settings: Settings
) -> None:
    """G3 B-3: J-17 giao trùng (cả hai qua bước kiểm "đã có") — bên chèn sau không ghi đè file ảnh đã là bằng
    chứng: INSERT trước, chỉ đổi tên file khi chèn được; trùng → xóa file tạm."""
    _, station = await make_station_account(db)
    pack = await make_closed_session(db, station, "SPXTST0000009", T, T + timedelta(seconds=30))
    rel = snapshots.rel_path(pack.id, T + timedelta(seconds=30), "pack")
    path = media_settings.video_root / rel
    first_tmp, first_sha = snapshots._write_temp(path, b"\xff\xd8first")
    second_tmp, second_sha = snapshots._write_temp(path, b"\xff\xd8second")
    assert first_tmp != second_tmp

    assert await snapshots._insert_pack_close(db, pack.id, T, rel, first_tmp, first_sha, 7)
    assert not await snapshots._insert_pack_close(db, pack.id, T, rel, second_tmp, second_sha, 8)

    assert path.read_bytes() == b"\xff\xd8first"
    assert not second_tmp.exists()
    assert not first_tmp.exists()
    shot = await snapshots.pack_close_of(db, pack.id)
    assert shot is not None
    assert shot.sha256 == first_sha == hashlib.sha256(path.read_bytes()).hexdigest()


async def test_snapshot_missing_is_409_g3_ev5(desk: Desk, db: AsyncSession, grabs: list[str]) -> None:
    """G3-EV-5: ảnh `MISSING` (thiếu tệp — EX-K8 / K9) → API-106 `409 SNAPSHOT_MISSING` (như clip `MISSING`),
    kể cả khi tệp vẫn nằm trên đĩa (không phục vụ tệp chưa được kiểm lại)."""
    session = await _open_41(desk, db)
    shot = (await _snap(desk, session["id"])).json()["snapshot"]
    row = await db.get(Snapshot, uuid.UUID(shot["id"]))
    assert row is not None
    row.status = "MISSING"
    await db.flush()
    res = await desk.api.get(shot["url"])
    assert (res.status_code, res.json()["error"]["code"]) == (409, "SNAPSHOT_MISSING")
    assert res.json()["error"]["details"] == {"status": "MISSING"}
