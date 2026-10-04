"""WS-01 `/ws/station`, WS-02 `/ws/dashboard` (02 §6, 02a §4 WS).

Mỗi kết nối nghe các kênh Redis `ws:*` của nó và chuyển xuống client.
Client gửi `{"type":"ping"}` mỗi 20 giây, server trả `pong`.
Token hết hạn → đóng mã 4401 (client refresh rồi nối lại).
"""

import asyncio
import contextlib
import json

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis

from aicam.core import clock
from aicam.core.redis import get_redis
from aicam.core.security import AccessClaims, TokenError, decode_access_token
from aicam.core.settings import get_settings

log = structlog.get_logger()

CLOSE_TOKEN_EXPIRED = 4401
CLOSE_FORBIDDEN = 4403
DASHBOARD_ROLES = ("ADMIN", "SUPERVISOR", "CSKH")
APPROVER_ROLES = ("ADMIN", "SUPERVISOR")

router = APIRouter()


def channels_for(path: str, claims: AccessClaims) -> list[str] | None:
    """Kênh Redis cho một kết nối; None = không có quyền với endpoint này."""
    if path == "station":
        if claims.role != "STATION" or claims.station_id is None:
            return None
        return [f"ws:station:{claims.station_id}"]
    if claims.role not in DASHBOARD_ROLES:
        return None
    channels = ["ws:dashboard", f"ws:user:{claims.user_id}"]
    if claims.role in APPROVER_ROLES:
        channels.append("ws:approvals")
    return channels


async def _authenticate(ws: WebSocket) -> AccessClaims | None:
    token = ws.query_params.get("token", "")
    try:
        return decode_access_token(get_settings().jwt_secret, token)
    except TokenError:
        return None


async def _forward(ws: WebSocket, redis: Redis, channels: list[str]) -> None:
    pubsub = redis.pubsub()
    await pubsub.subscribe(*channels)
    try:
        async for message in pubsub.listen():
            if message.get("type") == "message":
                await ws.send_text(str(message["data"]))
    finally:
        await pubsub.aclose()  # type: ignore[no-untyped-call]


async def _receive(ws: WebSocket) -> None:
    while True:
        raw = await ws.receive_text()
        with contextlib.suppress(json.JSONDecodeError, AttributeError):
            if json.loads(raw).get("type") == "ping":
                await ws.send_text(json.dumps({"type": "pong", "data": None, "at": clock.now().isoformat()}))


async def _serve(ws: WebSocket, path: str) -> None:
    # Accept trước rồi mới đóng: đóng trước accept thành HTTP 403, trình duyệt chỉ thấy 1006
    # và client không biết phải refresh token (02b-station §8: 4401 → refresh → nối lại).
    await ws.accept()
    claims = await _authenticate(ws)
    if claims is None:
        await ws.close(code=CLOSE_TOKEN_EXPIRED)
        return
    channels = channels_for(path, claims)
    if channels is None:
        await ws.close(code=CLOSE_FORBIDDEN)
        return
    ttl = max(0.0, (claims.expires_at - clock.now()).total_seconds())
    tasks = [
        asyncio.create_task(_forward(ws, get_redis(), channels)),
        asyncio.create_task(_receive(ws)),
        asyncio.create_task(asyncio.sleep(ttl)),
    ]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                log.error("ws_task_failed", path=path, error=repr(exc))
        if tasks[2] in done:
            await ws.close(code=CLOSE_TOKEN_EXPIRED)
    except WebSocketDisconnect:
        pass
    finally:
        for task in tasks:
            task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks, return_exceptions=True)
        log.info("ws_closed", path=path, user_id=str(claims.user_id))


@router.websocket("/ws/station")
async def ws_station(ws: WebSocket) -> None:
    await _serve(ws, "station")


@router.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket) -> None:
    await _serve(ws, "dashboard")
