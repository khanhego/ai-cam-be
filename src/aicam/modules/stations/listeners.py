"""Handler sự kiện Redis cho module stations (đăng ký trong api)."""

from typing import Any

from aicam.core.db import sessionmaker
from aicam.modules.stations import service

CAMERA_HEALTH_CHANNEL = "camera.health"


async def on_camera_health(payload: dict[str, Any]) -> None:
    """J-08 phía api: ghi trạng thái camera, đẩy WS (station.state + dashboard camera.status).

    Cờ VIDEO_INCOMPLETE cho phiên đang mở do media (T-14) tính từ segment thiếu.
    """
    from aicam.core.settings import get_settings
    from aicam.modules.sessions.service import publish_state
    from aicam.realtime import publish

    async with sessionmaker()() as session:
        camera = await service.apply_camera_health(session, str(payload["path"]), str(payload["status"]))
        if camera is None:
            return
        await publish.to_dashboard("camera.status", {"camera_id": str(camera.id), "status": camera.status})
        await publish_state(session, camera.station_id, get_settings())
