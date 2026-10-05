"""Handler sự kiện Redis cho module stations (đăng ký trong api)."""

from typing import Any

from aicam.core.db import sessionmaker
from aicam.modules.stations import service

CAMERA_HEALTH_CHANNEL = "camera.health"


async def on_camera_health(payload: dict[str, Any]) -> None:
    """J-08 phía api: ghi trạng thái camera, đẩy WS (station.state + dashboard camera.status).

    Camera OFFLINE giữa phiên → cờ VIDEO_INCOMPLETE cho phiên đang mở của station (review M1 #15).
    J-01 còn kiểm khe hở segment khi cắt clip (cờ trên clip + phiên).
    """
    from aicam.core.db import commit
    from aicam.core.settings import get_settings
    from aicam.modules.sessions.service import mark_camera_lost, publish_state
    from aicam.realtime import publish

    async with sessionmaker()() as session:
        camera = await service.apply_camera_health(session, str(payload["path"]), str(payload["status"]))
        if camera is None:
            return
        if camera.status == "OFFLINE":
            await mark_camera_lost(session, camera.station_id, camera.role)
            await commit(session)
        await publish.to_dashboard("camera.status", {"camera_id": str(camera.id), "status": camera.status})
        await publish_state(session, camera.station_id, get_settings())
