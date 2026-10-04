"""Đẩy sự kiện realtime qua Redis (02a §4 WS).

Kênh: `ws:station:{id}`, `ws:dashboard`, `ws:approvals`, `ws:user:{id}`.

Hub WebSocket trong api (T-11) nghe các kênh này và gửi xuống client. Gọi sau commit (`after_commit`).
"""

import json
import uuid
from typing import Any

from aicam.core import clock
from aicam.core.redis import get_redis


def _message(event_type: str, data: Any) -> str:
    return json.dumps({"type": event_type, "data": data, "at": clock.now().isoformat()}, default=str)


async def to_station(station_id: uuid.UUID, event_type: str, data: Any) -> None:
    await get_redis().publish(f"ws:station:{station_id}", _message(event_type, data))


async def to_dashboard(event_type: str, data: Any) -> None:
    await get_redis().publish("ws:dashboard", _message(event_type, data))


async def to_approvers(event_type: str, data: Any) -> None:
    await get_redis().publish("ws:approvals", _message(event_type, data))


async def to_user(user_id: uuid.UUID, event_type: str, data: Any) -> None:
    await get_redis().publish(f"ws:user:{user_id}", _message(event_type, data))
