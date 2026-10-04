"""Tiến trình `vision`: J-08 theo dõi camera (T-8). Đọc mã Cam 2 thêm ở T-12."""

import asyncio
import signal

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam.core.db import dispose_engine, init_engine
from aicam.core.logging import configure_logging
from aicam.core.redis import close_redis, init_redis
from aicam.core.settings import get_settings
from aicam.modules.stations.mediamtx import HttpMediaMTX
from aicam.modules.vision.health_loop import run_health_loop


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    init_engine(settings.database_url)
    redis = init_redis(settings.redis_url)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await run_health_loop(redis, HttpMediaMTX(settings.mediamtx_api_url), stop)
    finally:
        await close_redis()
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
