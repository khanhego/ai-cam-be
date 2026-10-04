"""J-08: vòng lặp 2 giây trong tiến trình vision (DEC-31).

Đọc MediaMTX, phát `camera.health` khi camera đổi trạng thái.
"""

import asyncio
import contextlib
import json
import time

import structlog
from redis.asyncio import Redis

from aicam.core.db import sessionmaker
from aicam.modules.stations import service as stations
from aicam.modules.stations.health import HealthTracker
from aicam.modules.stations.listeners import CAMERA_HEALTH_CHANNEL
from aicam.modules.stations.mediamtx import MediaMTX, MediaMTXError

log = structlog.get_logger()
INTERVAL_S = 2.0
# Đọc lại danh sách camera mỗi vòng: camera mới phải lên ONLINE ≤ 10 giây (TC-01.01, BUG-2 QA M1).
WATCH_REFRESH_S = INTERVAL_S


async def run_health_loop(redis: Redis, mediamtx: MediaMTX, stop: asyncio.Event) -> None:
    tracker = HealthTracker()
    watched: list[str] = []
    refreshed_at = 0.0
    while not stop.is_set():
        now = time.monotonic()
        if now - refreshed_at >= WATCH_REFRESH_S or not watched:
            async with sessionmaker()() as session:
                watched = await stations.watched_paths(session)
            refreshed_at = now
        try:
            paths = await mediamtx.list_paths()
        except MediaMTXError as exc:
            log.warning("mediamtx_unavailable", error=str(exc))
            paths = {}
        for path, status in tracker.update(now, paths, watched).items():
            await redis.publish(CAMERA_HEALTH_CHANNEL, json.dumps({"path": path, "status": status}))
            log.info("camera_health", path=path, status=status)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=INTERVAL_S)
