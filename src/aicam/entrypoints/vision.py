"""Tiến trình `vision`: J-08 theo dõi camera (T-8) + đọc mã khay Cam 2 (T-12, ADR-005) + giữ khung mới nhất
mọi camera trong Redis cho ảnh chụp (T-121)."""

import asyncio
import re
import signal
from typing import Any

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam.core import schema_guard
from aicam.core.db import dispose_engine, init_engine
from aicam.core.logging import configure_logging
from aicam.core.redis import close_redis, init_redis
from aicam.core.settings import get_settings
from aicam.modules.stations.mediamtx import HttpMediaMTX
from aicam.modules.stations.service import VISION_CONFIG_CHANNEL
from aicam.modules.vision.health_loop import run_health_loop
from aicam.modules.vision.runner import FrameOptions, run_tray_loop
from aicam.realtime.bus import Bus


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    await schema_guard.enforce(settings, "vision")  # G3 M-F1
    init_engine(settings.database_url)
    redis = init_redis(settings.redis_url)
    stop = asyncio.Event()
    reload = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async def _on_vision_config(_: dict[str, Any]) -> None:
        reload.set()  # API-64 đổi ROI → nạp lại ngay

    bus = Bus()
    bus.on(VISION_CONFIG_CHANNEL, _on_vision_config)
    tasks = [
        asyncio.create_task(bus.run(redis), name="vision-bus"),
        asyncio.create_task(run_health_loop(redis, HttpMediaMTX(settings.mediamtx_api_url), stop)),
        asyncio.create_task(
            run_tray_loop(
                redis,
                settings.mediamtx_rtsp_url,
                re.compile(settings.scan_code_regex),
                stop,
                reload,
                FrameOptions(
                    settings.vision_frames_enabled,
                    settings.vision_frame_interval_s,
                    settings.snapshot_jpeg_quality,
                ),
            )
        ),
    ]
    stopper = asyncio.create_task(stop.wait())
    try:
        # Một vòng chết (lỗi không lường trước) → thoát để Docker khởi động lại cả tiến trình.
        done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
        for task in done - {stopper}:
            exc = task.exception()
            if exc is not None:
                raise exc
    finally:
        stopper.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close_redis()
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
