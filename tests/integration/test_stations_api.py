"""API-60..65 — 02 §6; TC-01.xx, TC-P.07."""

import asyncio
import json

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.modules.stations.mediamtx import MediaMTXError, PathStat
from aicam.modules.stations.models import Camera
from aicam.modules.stations.router import get_mediamtx
from aicam.modules.stations.service import apply_camera_health

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration


class FakeMediaMTX:
    def __init__(self, fail: bool = False) -> None:
        self.paths: dict[str, str] = {}
        self.fail = fail

    async def upsert_path(self, name: str, source: str) -> None:
        if self.fail:
            raise MediaMTXError("down")
        self.paths[name] = source

    async def delete_path(self, name: str) -> None:
        self.paths.pop(name, None)

    async def list_paths(self) -> dict[str, PathStat]:
        return {}


@pytest.fixture
def mediamtx(api: AsyncClient) -> FakeMediaMTX:
    fake = FakeMediaMTX()
    api._transport.app.dependency_overrides[get_mediamtx] = lambda: fake  # type: ignore[attr-defined]
    return fake


async def _auth(api: AsyncClient, db: AsyncSession, role: str = "ADMIN") -> dict[str, str]:
    username = f"tst_{role.lower()}"
    await make_user(db, username, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _create_station(api: AsyncClient, headers: dict[str, str], name: str = "TST Station 01") -> dict:  # type: ignore[type-arg]
    res = await api.post("/api/v1/stations", headers=headers, json={"name": name})
    assert res.status_code == 201, res.text
    return res.json()  # type: ignore[no-any-return]


async def test_create_and_list_station(api: AsyncClient, db: AsyncSession) -> None:
    headers = await _auth(api, db)
    account = await make_user(db, "station01", "STATION")

    created = await api.post(
        "/api/v1/stations",
        headers=headers,
        json={"name": "TST Station 01", "account_user_id": str(account.id)},
    )
    listed = await api.get("/api/v1/stations", headers=headers)

    assert created.status_code == 201
    assert created.json()["account"] == {"id": str(account.id), "username": "station01"}
    assert created.json()["cameras"] == []
    assert [s["name"] for s in listed.json()["items"]] == ["TST Station 01"]


async def test_station_name_taken_case_insensitive(api: AsyncClient, db: AsyncSession) -> None:
    """TC-01.02."""
    headers = await _auth(api, db)
    await _create_station(api, headers)

    res = await api.post("/api/v1/stations", headers=headers, json={"name": "tst station 01"})

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "NAME_TAKEN"


async def test_account_in_use_and_wrong_role(api: AsyncClient, db: AsyncSession) -> None:
    """TC-01.03."""
    headers = await _auth(api, db)
    user, _ = await make_station_account(db)
    cskh = await make_user(db, "tst_cskh", "CSKH")

    in_use = await api.post(
        "/api/v1/stations", headers=headers, json={"name": "S3", "account_user_id": str(user.id)}
    )
    wrong = await api.post(
        "/api/v1/stations", headers=headers, json={"name": "S4", "account_user_id": str(cskh.id)}
    )

    assert in_use.status_code == 409
    assert in_use.json()["error"]["code"] == "ACCOUNT_IN_USE"
    assert wrong.status_code == 422
    assert "account_user_id" in wrong.json()["error"]["details"]["fields"]


async def test_patch_station_deactivate_and_rename(api: AsyncClient, db: AsyncSession) -> None:
    headers = await _auth(api, db)
    station = await _create_station(api, headers)

    res = await api.patch(
        f"/api/v1/stations/{station['id']}", headers=headers, json={"name": "Bàn 1", "is_active": False}
    )

    assert res.status_code == 200
    assert res.json()["name"] == "Bàn 1"
    assert res.json()["is_active"] is False


@pytest.mark.parametrize("role", ["SUPERVISOR", "CSKH"])
async def test_station_admin_api_forbidden(api: AsyncClient, db: AsyncSession, role: str) -> None:
    """TC-P.07: chỉ ADMIN (02 API-60 sau review)."""
    headers = await _auth(api, db, role)

    assert (await api.get("/api/v1/stations", headers=headers)).status_code == 403
    assert (await api.post("/api/v1/stations", headers=headers, json={"name": "x"})).status_code == 403


async def test_set_camera_registers_mediamtx_path_and_hides_password(
    api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX
) -> None:
    """TC-01.01 (phần API)."""
    headers = await _auth(api, db)
    station = await _create_station(api, headers)

    res = await api.put(
        f"/api/v1/stations/{station['id']}/cameras/CAM2",
        headers=headers,
        json={"rtsp_url": "rtsp://192.168.20.12:554/stream1", "username": "admin", "password": "bimat123"},
    )

    assert res.status_code == 200
    body = res.json()
    assert body["role"] == "CAM2"
    assert body["status"] == "OFFLINE"
    assert body["rtsp_url_masked"] == "rtsp://192.168.20.12:554/stream1"
    assert "bimat123" not in res.text
    assert mediamtx.paths == {f"cam-{body['id']}": "rtsp://admin:bimat123@192.168.20.12:554/stream1"}
    camera = await db.get(Camera, body["id"])
    assert camera is not None
    assert camera.password_enc is not None
    assert b"bimat123" not in camera.password_enc
    log = await db.scalar(select(AuditLog).where(AuditLog.action == "CAMERA_UPDATE"))
    assert log is not None
    assert "bimat123" not in json.dumps(log.data)


async def test_set_camera_twice_updates_same_row(
    api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX
) -> None:
    headers = await _auth(api, db)
    station = await _create_station(api, headers)
    url = f"/api/v1/stations/{station['id']}/cameras/CAM1"

    first = await api.put(url, headers=headers, json={"rtsp_url": "rtsp://10.0.0.1/a"})
    second = await api.put(url, headers=headers, json={"rtsp_url": "rtsp://10.0.0.2/b"})

    assert first.json()["id"] == second.json()["id"]
    assert list(mediamtx.paths.values()) == ["rtsp://10.0.0.2/b"]


async def test_set_camera_saves_even_if_mediamtx_down(api: AsyncClient, db: AsyncSession) -> None:
    api._transport.app.dependency_overrides[get_mediamtx] = lambda: FakeMediaMTX(fail=True)  # type: ignore[attr-defined]
    headers = await _auth(api, db)
    station = await _create_station(api, headers)

    res = await api.put(
        f"/api/v1/stations/{station['id']}/cameras/CAM1",
        headers=headers,
        json={"rtsp_url": "rtsp://10.0.0.1/a"},
    )

    assert res.status_code == 200


async def test_camera_url_validation(api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX) -> None:
    headers = await _auth(api, db)
    station = await _create_station(api, headers)

    res = await api.put(
        f"/api/v1/stations/{station['id']}/cameras/CAM1", headers=headers, json={"rtsp_url": "http://x/a"}
    )
    bad_role = await api.put(
        f"/api/v1/stations/{station['id']}/cameras/CAM3", headers=headers, json={"rtsp_url": "rtsp://x/a"}
    )

    assert res.status_code == bad_role.status_code == 422


async def test_roi_only_cam2_and_notifies_vision(
    api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX, redis_client: Redis
) -> None:
    """TC-01.05, TC-01.06."""
    headers = await _auth(api, db)
    station = await _create_station(api, headers)
    cam1 = (
        await api.put(
            f"/api/v1/stations/{station['id']}/cameras/CAM1", headers=headers, json={"rtsp_url": "rtsp://h/1"}
        )
    ).json()
    cam2 = (
        await api.put(
            f"/api/v1/stations/{station['id']}/cameras/CAM2", headers=headers, json={"rtsp_url": "rtsp://h/2"}
        )
    ).json()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("vision.config")
    await pubsub.get_message(timeout=1)

    roi = {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4}
    on_cam1 = await api.put(f"/api/v1/cameras/{cam1['id']}/roi", headers=headers, json=roi)
    too_small = await api.put(f"/api/v1/cameras/{cam2['id']}/roi", headers=headers, json={**roi, "w": 0.04})
    ok = await api.put(f"/api/v1/cameras/{cam2['id']}/roi", headers=headers, json=roi)
    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
    await pubsub.aclose()  # type: ignore[no-untyped-call]

    assert on_cam1.status_code == 409
    assert on_cam1.json()["error"]["code"] == "ROI_ONLY_CAM2"
    assert too_small.status_code == 422
    assert ok.status_code == 200
    assert ok.json()["roi"] == roi
    assert message is not None
    assert json.loads(message["data"]) == {"camera_id": cam2["id"]}


async def test_live_lists_whep_urls(api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX) -> None:
    headers = await _auth(api, db)
    station = await _create_station(api, headers)
    cam = (
        await api.put(
            f"/api/v1/stations/{station['id']}/cameras/CAM1", headers=headers, json={"rtsp_url": "rtsp://h/1"}
        )
    ).json()
    sup = await _auth(api, db, "SUPERVISOR")

    res = await api.get("/api/v1/live", headers=sup)

    assert res.status_code == 200
    assert res.json()["stations"][0]["cameras"] == [
        {"id": cam["id"], "role": "CAM1", "status": "OFFLINE", "whep_url": f"/live/cam-{cam['id']}/whep"}
    ]


async def test_live_and_snapshot_forbidden_for_cskh(api: AsyncClient, db: AsyncSession) -> None:
    headers = await _auth(api, db, "CSKH")

    assert (await api.get("/api/v1/live", headers=headers)).status_code == 403
    assert (
        await api.get("/api/v1/cameras/00000000-0000-0000-0000-000000000000/snapshot", headers=headers)
    ).status_code == 403


async def test_apply_camera_health(api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX) -> None:
    """J-08 phía api: ghi ONLINE/OFFLINE + last_seen_at."""
    headers = await _auth(api, db)
    station = await _create_station(api, headers)
    cam = (
        await api.put(
            f"/api/v1/stations/{station['id']}/cameras/CAM1", headers=headers, json={"rtsp_url": "rtsp://h/1"}
        )
    ).json()

    changed = await apply_camera_health(db, f"cam-{cam['id']}", "ONLINE")
    again = await apply_camera_health(db, f"cam-{cam['id']}", "ONLINE")

    assert changed is not None
    assert changed.status == "ONLINE"
    assert changed.last_seen_at is not None
    assert again is None


async def test_probe_unreachable_camera(api: AsyncClient, db: AsyncSession) -> None:
    """TC-01.04: IP không tồn tại → CAMERA_UNREACHABLE."""
    headers = await _auth(api, db)

    res = await api.post(
        "/api/v1/cameras/test", headers=headers, json={"rtsp_url": "rtsp://127.0.0.1:9/none"}
    )

    assert res.status_code == 422
    assert res.json()["error"]["code"] == "CAMERA_UNREACHABLE"
    assert res.json()["error"]["details"]["reason"] in {"TIMEOUT", "STREAM"}


async def _fake_cam_up() -> bool:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection("localhost", 58554), timeout=1)
    except (OSError, TimeoutError):
        return False
    writer.close()
    return True


async def test_probe_real_fake_camera(api: AsyncClient, db: AsyncSession) -> None:
    """Chạy khi stack dev có MediaMTX + fake-cam (58554)."""
    if not await _fake_cam_up():
        pytest.skip("MediaMTX dev không chạy")
    headers = await _auth(api, db)

    res = await api.post(
        "/api/v1/cameras/test", headers=headers, json={"rtsp_url": "rtsp://localhost:58554/cam-fake2"}
    )

    assert res.status_code == 200, res.text
    assert res.json()["snapshot"].startswith("data:image/jpeg;base64,")
    assert res.json()["clock_offset_ms"] is None


async def test_update_camera_without_password_keeps_old_one(
    api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX
) -> None:
    """Review M1 #11: form sửa không hiện lại tài khoản / mật khẩu → không gửi = giữ; "" = xóa."""
    headers = await _auth(api, db)
    station = await _create_station(api, headers)
    url = f"/api/v1/stations/{station['id']}/cameras/CAM1"
    await api.put(
        url,
        headers=headers,
        json={"rtsp_url": "rtsp://10.0.0.1/a", "username": "admin", "password": "bimat123"},
    )

    kept = await api.put(url, headers=headers, json={"rtsp_url": "rtsp://10.0.0.1/b"})
    assert list(mediamtx.paths.values()) == ["rtsp://admin:bimat123@10.0.0.1/b"]

    await api.put(
        url, headers=headers, json={"rtsp_url": "rtsp://10.0.0.1/b", "username": "", "password": ""}
    )
    camera = await db.get(Camera, kept.json()["id"])
    assert camera is not None
    assert camera.password_enc is None
    assert list(mediamtx.paths.values()) == ["rtsp://10.0.0.1/b"]


async def test_camera_url_with_inline_credentials_rejected(
    api: AsyncClient, db: AsyncSession, mediamtx: FakeMediaMTX
) -> None:
    """Review M1 #12."""
    headers = await _auth(api, db)
    station = await _create_station(api, headers)

    res = await api.put(
        f"/api/v1/stations/{station['id']}/cameras/CAM1",
        headers=headers,
        json={"rtsp_url": "rtsp://admin:bimat123@10.0.0.1/a"},
    )

    assert res.status_code == 422
    assert "bimat123" not in res.text
    assert mediamtx.paths == {}
