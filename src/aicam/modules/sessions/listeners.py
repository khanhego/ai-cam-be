"""Handler sự kiện Redis cho module sessions (đăng ký trong api)."""

import uuid
from typing import Any

from aicam.core.db import sessionmaker
from aicam.core.settings import get_settings
from aicam.modules.sessions import service


async def on_tray_changed(payload: dict[str, Any]) -> None:
    """`tray.changed` từ vision → đánh giá lại phiên đang mở của station (BR-06)."""
    async with sessionmaker()() as session:
        await service.on_tray_changed(session, uuid.UUID(str(payload["station_id"])), get_settings())
