"""WS-01, WS-02 (T-11) — 02 §6 WS; TC-03.56.

Chạy uvicorn thật + client `websockets`: TestClient của Starlette hủy task app lúc đóng.
"""

import json
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import timedelta

import jwt
import pytest
import uvicorn
from redis import Redis as SyncRedis
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import ClientConnection, connect

from aicam.core import clock
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings
from aicam.main import create_app

from .conftest import TEST_REDIS_URL

pytestmark = pytest.mark.integration

PORT = 18765


@pytest.fixture(scope="module")
def server_url() -> Iterator[str]:
    settings = Settings(app_env="test", log_json=False, redis_url=TEST_REDIS_URL)
    app = create_app(settings)
    app.dependency_overrides[get_settings] = lambda: settings
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.02)
    yield f"ws://127.0.0.1:{PORT}"
    server.should_exit = True
    thread.join(5)


@pytest.fixture
def redis_sync() -> Iterator[SyncRedis]:
    r = SyncRedis.from_url(TEST_REDIS_URL, decode_responses=True)
    yield r
    r.close()


def _token(role: str, station_id: uuid.UUID | None = None) -> str:
    token, _ = encode_access_token(get_settings().jwt_secret, uuid.uuid4(), role, station_id, 15)
    return token


def _subscribers(redis: SyncRedis, channel: str) -> int:
    return int(redis.pubsub_numsub(channel)[0][1])  # type: ignore[index]


def _publish_when_subscribed(redis: SyncRedis, channel: str, payload: dict[str, object]) -> None:
    """Chờ hub subscribe kênh rồi mới publish (tránh mất message do race)."""
    for _ in range(200):
        if _subscribers(redis, channel) >= 1:
            redis.publish(channel, json.dumps(payload))
            return
        time.sleep(0.02)
    raise AssertionError(f"Hub chưa subscribe {channel}")


def _close_code(ws: ClientConnection) -> int | None:
    with pytest.raises(ConnectionClosed) as exc:
        ws.recv(timeout=5)
    return exc.value.rcvd.code if exc.value.rcvd else None


def test_station_receives_its_channel_and_pong(server_url: str, redis_sync: SyncRedis) -> None:
    station_id = uuid.uuid4()
    channel = f"ws:station:{station_id}"
    with connect(f"{server_url}/ws/station?token={_token('STATION', station_id)}") as ws:
        ws.send(json.dumps({"type": "ping"}))
        assert json.loads(ws.recv(timeout=5))["type"] == "pong"

        _publish_when_subscribed(redis_sync, channel, {"type": "station.state", "data": {}})
        assert json.loads(ws.recv(timeout=5))["type"] == "station.state"

    for _ in range(100):  # đóng kết nối → hủy subscribe, không rò
        if _subscribers(redis_sync, channel) == 0:
            break
        time.sleep(0.02)
    assert _subscribers(redis_sync, channel) == 0


def test_invalid_token_closed_4401(server_url: str) -> None:
    with connect(f"{server_url}/ws/station?token=sai") as ws:
        assert _close_code(ws) == 4401


@pytest.mark.parametrize(("path", "role"), [("/ws/dashboard", "STATION"), ("/ws/station", "CSKH")])
def test_wrong_role_closed_4403(server_url: str, path: str, role: str) -> None:
    station_id = uuid.uuid4() if role == "STATION" else None
    with connect(f"{server_url}{path}?token={_token(role, station_id)}") as ws:
        assert _close_code(ws) == 4403


def test_cskh_does_not_get_approvals(server_url: str, redis_sync: SyncRedis) -> None:
    """TC-03.56."""
    with connect(f"{server_url}/ws/dashboard?token={_token('CSKH')}") as ws:
        _publish_when_subscribed(redis_sync, "ws:dashboard", {"type": "report.updated", "data": {}})
        assert json.loads(ws.recv(timeout=5))["type"] == "report.updated"
        assert _subscribers(redis_sync, "ws:approvals") == 0


def test_supervisor_gets_approvals(server_url: str, redis_sync: SyncRedis) -> None:
    with connect(f"{server_url}/ws/dashboard?token={_token('SUPERVISOR')}") as ws:
        _publish_when_subscribed(redis_sync, "ws:approvals", {"type": "approval.created", "data": {}})
        assert json.loads(ws.recv(timeout=5))["type"] == "approval.created"


def test_token_expiry_closes_4401(server_url: str) -> None:
    expires = clock.now() + timedelta(seconds=1)
    token = jwt.encode(
        {"sub": str(uuid.uuid4()), "role": "CSKH", "exp": int(expires.timestamp()) + 1, "typ": "access"},
        get_settings().jwt_secret,
    )
    with connect(f"{server_url}/ws/dashboard?token={token}") as ws:
        assert _close_code(ws) == 4401
