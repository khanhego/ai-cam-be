"""Cam 2 → khay → phiên (T-12): TrayTracker ghi Redis, `on_tray_changed` (BR-06, BR-18, FR-03.06, 03.07).

TC-03.21..03.23 phần logic (HW thật cần T-4); TC-03.25 (vision dừng → UNAVAILABLE).
"""

import json
import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.settings import Settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession, SessionEvent
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.sessions.tray import TRAY_CHANGED_CHANNEL, clear_tray, tray_key, write_tray
from aicam.modules.vision.capture import Observation
from aicam.modules.vision.runner import TrayTracker

from .factories import PASSWORD, make_station_account

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------- TrayTracker (vision → Redis)


async def _messages(pubsub: Any, n: int = 5) -> list[dict[str, Any]]:
    out = []
    for _ in range(n):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.3)
        if msg is not None:
            out.append(json.loads(msg["data"]))
    return out


async def test_tracker_writes_tray_and_announces_changes(redis_client: Redis) -> None:
    camera_id, station_id = uuid.uuid4(), uuid.uuid4()
    tracker = TrayTracker(redis_client)
    tracker.track(camera_id, station_id)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(TRAY_CHANGED_CHANNEL)
    await pubsub.get_message(timeout=1)

    await tracker.handle(Observation(camera_id, ("SPXTST0000001",), 100.0))
    assert await redis_client.get(tray_key(station_id)) is None  # chưa đủ 2 khung
    await tracker.handle(Observation(camera_id, ("SPXTST0000001",), 100.25))
    first = json.loads(await redis_client.get(tray_key(station_id)))  # type: ignore[arg-type]
    await tracker.handle(Observation(camera_id, ("SPXTST0000001",), 100.5))
    again = json.loads(await redis_client.get(tray_key(station_id)))  # type: ignore[arg-type]
    ttl = await redis_client.ttl(tray_key(station_id))

    assert first["codes"] == ["SPXTST0000001"]
    assert again["updated_at"] == first["updated_at"]  # khung giống nhau không đổi updated_at
    assert 0 < ttl <= 5
    mine = {"station_id": str(station_id)}
    assert [m for m in await _messages(pubsub) if m == mine] == [mine]  # chỉ phát khi đổi

    # Mất stream > 3 giây → xóa khóa (UNAVAILABLE) + báo
    await tracker.handle(Observation(camera_id, None, 102.0))
    assert await redis_client.get(tray_key(station_id)) is not None
    await tracker.tick(103.6)
    assert await redis_client.get(tray_key(station_id)) is None
    assert [m for m in await _messages(pubsub) if m == mine] == [mine]
    await pubsub.aclose()  # type: ignore[no-untyped-call]


async def test_tracker_forget_clears_tray(redis_client: Redis) -> None:
    camera_id, station_id = uuid.uuid4(), uuid.uuid4()
    tracker = TrayTracker(redis_client)
    tracker.track(camera_id, station_id)
    for t in (1.0, 1.25):
        await tracker.handle(Observation(camera_id, (), t))
    for t in (1.5, 1.75):
        await tracker.handle(Observation(camera_id, (), t))
    assert json.loads(await redis_client.get(tray_key(station_id)) or "{}")["codes"] == []  # NOT_SEEN

    await tracker.forget(camera_id)

    assert await redis_client.get(tray_key(station_id)) is None
    assert camera_id not in tracker.cameras


# ---------------------------------------------------------------- on_tray_changed (api)


@pytest.fixture
async def station(api: AsyncClient, db: AsyncSession) -> tuple[dict[str, str], uuid.UUID]:
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()  # type: ignore[attr-defined]
    user, st = await make_station_account(db)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, st.id


async def _scan(api: AsyncClient, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = await api.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    return res.json()  # type: ignore[no-any-return]


async def _tray(redis: Redis, station_id: uuid.UUID, *codes: str) -> None:
    from aicam.core import clock

    await write_tray(redis, station_id, codes, clock.now(), ttl_s=60)


async def _pack(db: AsyncSession, code: str) -> PackSession:
    pack = await db.scalar(
        select(PackSession).where(PackSession.open_code == code).execution_options(populate_existing=True)
    )
    assert pack is not None
    return pack


async def test_wrong_label_on_tray_turns_session_mismatch(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """TC-03.21 (logic), BR-06: phiên …14, khay …15 → MISMATCH CAM2 + WS station.state, report.updated."""
    headers, station_id = station
    await _scan(api, headers, "SPXTST0000014")
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{station_id}", "ws:dashboard")
    await pubsub.get_message(timeout=1)
    await pubsub.get_message(timeout=1)
    await _tray(redis_client, station_id, "SPXTST0000015")

    result = await sessions.on_tray_changed(db, station_id, test_settings)

    assert result == "MISMATCH"
    pack = await _pack(db, "SPXTST0000014")
    assert pack.status == "MISMATCH"
    assert pack.mismatch == {"source": "CAM2", "expected": "SPXTST0000014", "actual": "SPXTST0000015"}
    assert "HAD_MISMATCH" in pack.flags
    msgs = await _messages(pubsub)
    await pubsub.aclose()  # type: ignore[no-untyped-call]
    # Kênh ws:dashboard dùng chung Redis với stack dev đang chạy: chỉ xét sự kiện của station test.
    states = [m["data"] for m in msgs if m["type"] == "station.state"]
    assert len(states) == 1
    assert any(m["type"] == "report.updated" for m in msgs)
    state = states[0]
    assert state["state"] == "MISMATCH"
    assert state["tray"] == {**state["tray"], "codes": ["SPXTST0000015"], "match": "DIFFERENT"}
    assert state["session"]["mismatch"]["source"] == "CAM2"


async def test_correct_scan_blocked_until_wrong_label_removed(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """TC-03.22, TC-03.23 (logic): khay còn phiếu sai → quét đúng mã vẫn MISMATCH; bỏ phiếu → OPEN → đóng."""
    headers, station_id = station
    await _scan(api, headers, "SPXTST0000014")
    await _tray(redis_client, station_id, "SPXTST0000015")
    await sessions.on_tray_changed(db, station_id, test_settings)

    for _ in range(2):
        res = await _scan(api, headers, "SPXTST0000014")
        assert (res["outcome"], res["state"]["session"]["mismatch"]["source"]) == ("MISMATCH", "CAM2")

    await _tray(redis_client, station_id)  # khay trống
    assert await sessions.on_tray_changed(db, station_id, test_settings) == "OPEN"
    pack = await _pack(db, "SPXTST0000014")
    assert (pack.status, pack.mismatch) == ("OPEN", None)

    done = await _scan(api, headers, "SPXTST0000014")
    assert done["outcome"] == "SESSION_COMPLETED"
    events = (await db.scalars(select(SessionEvent.type).where(SessionEvent.session_id == pack.id))).all()
    assert "MISMATCH_CLEARED" in events


async def test_two_labels_on_tray_is_multiple(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """2 phiếu (…02 + …03) khi phiên …02 → MISMATCH (MULTIPLE); khay về chỉ …02 → OPEN (MATCH)."""
    headers, station_id = station
    await _scan(api, headers, "SPXTST0000002")
    await _tray(redis_client, station_id, "SPXTST0000002", "SPXTST0000003")

    assert await sessions.on_tray_changed(db, station_id, test_settings) == "MISMATCH"
    assert (await _pack(db, "SPXTST0000002")).mismatch["actual"] == "SPXTST0000003"  # type: ignore[index]

    await _tray(redis_client, station_id, "SPXTST0000002")
    assert await sessions.on_tray_changed(db, station_id, test_settings) == "OPEN"
    assert (await _pack(db, "SPXTST0000002")).cam2_seen_match is True


async def test_match_marks_cam2_seen(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """BR-18: Cam 2 khớp trong phiên → đóng không cờ CAM2_UNVERIFIED (phiếu đã dán, khay trống)."""
    headers, station_id = station
    await _scan(api, headers, "SPXTST0000016")
    assert (await _pack(db, "SPXTST0000016")).cam2_seen_match is False
    await _tray(redis_client, station_id, "SPXTST0000016")

    assert await sessions.on_tray_changed(db, station_id, test_settings) is None
    await _tray(redis_client, station_id)
    await sessions.on_tray_changed(db, station_id, test_settings)

    await _scan(api, headers, "SPXTST0000016")
    pack = await _pack(db, "SPXTST0000016")
    assert pack.status == "COMPLETED"
    assert "CAM2_UNVERIFIED" not in pack.flags


async def test_vision_stopped_closes_with_cam2_unverified(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """TC-03.24, TC-03.25 (logic), EX-P6: không có dữ liệu khay → UNAVAILABLE, không chặn, CAM2_UNVERIFIED."""
    headers, station_id = station
    opened = await _scan(api, headers, "SPXTST0000017")
    assert opened["state"]["tray"]["match"] == "UNAVAILABLE"
    await clear_tray(redis_client, station_id)
    assert await sessions.on_tray_changed(db, station_id, test_settings) is None

    done = await _scan(api, headers, "SPXTST0000017")

    assert done["outcome"] == "SESSION_COMPLETED"
    assert "CAM2_UNVERIFIED" in (await _pack(db, "SPXTST0000017")).flags


async def test_scan_mismatch_and_waiting_approval_unchanged_by_tray(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """Lệch do quét (SCAN) không tự hết khi khay đổi; phiên chờ duyệt chỉ cập nhật `tray`."""
    headers, station_id = station
    await _scan(api, headers, "SPXTST0000001")
    await _scan(api, headers, "SPXTST0000002")  # MISMATCH SCAN
    await _tray(redis_client, station_id)
    assert await sessions.on_tray_changed(db, station_id, test_settings) is None
    pack = await _pack(db, "SPXTST0000001")
    assert pack.mismatch["source"] == "SCAN"  # type: ignore[index]

    pack.status = "WAITING_APPROVAL"
    db.add(ApprovalRequest(station_id=station_id, session_id=pack.id, tracking_number="SPXTST0000001",
                           type="MISMATCH"))  # fmt: skip
    await db.flush()
    await _tray(redis_client, station_id, "SPXTST0000009")
    assert await sessions.on_tray_changed(db, station_id, test_settings) is None
    assert (await _pack(db, "SPXTST0000001")).status == "WAITING_APPROVAL"


async def test_idle_station_only_pushes_tray(
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    _, station_id = station
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{station_id}")
    await pubsub.get_message(timeout=1)
    await _tray(redis_client, station_id, "SPXTST0000001")

    assert await sessions.on_tray_changed(db, station_id, test_settings) is None
    assert await sessions.on_tray_changed(db, uuid.uuid4(), test_settings) is None  # station không tồn tại

    msgs = await _messages(pubsub)
    await pubsub.aclose()  # type: ignore[no-untyped-call]
    assert [m["type"] for m in msgs] == ["station.state"]
    assert msgs[0]["data"]["state"] == "READY"
    assert await redis_client.exists(tray_key(station_id))


async def test_open_while_tray_shows_other_label_is_mismatch(
    api: AsyncClient,
    db: AsyncSession,
    redis_client: Redis,
    station: tuple[dict[str, str], uuid.UUID],
    test_settings: Settings,
) -> None:
    """BR-06 (DEC-111): khay đang thấy …02 khi quét mở …01 → phiên mở nhưng MISMATCH nguồn CAM2 ngay."""
    headers, station_id = station
    await _tray(redis_client, station_id, "SPXTST0000002")

    res = await _scan(api, headers, "SPXTST0000001")

    assert res["outcome"] == "MISMATCH"
    assert res["state"]["state"] == "MISMATCH"
    assert res["state"]["session"]["mismatch"] == {
        "source": "CAM2", "expected": "SPXTST0000001", "actual": "SPXTST0000002",
    }  # fmt: skip
    pack = await _pack(db, "SPXTST0000001")
    assert "HAD_MISMATCH" in pack.flags
    # Bỏ phiếu …02, đặt phiếu đúng → OPEN, đóng không cờ Cam 2.
    await _tray(redis_client, station_id, "SPXTST0000001")
    assert await sessions.on_tray_changed(db, station_id, test_settings) == "OPEN"
    await _tray(redis_client, station_id)
    done = await _scan(api, headers, "SPXTST0000001")
    assert done["outcome"] == "SESSION_COMPLETED"
    assert "CAM2_UNVERIFIED" not in (await _pack(db, "SPXTST0000001")).flags
