"""Đăng ký task Celery. Mỗi task mở engine riêng (Celery chạy đồng bộ, gọi coroutine qua asyncio.run)."""

import asyncio
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.settings import get_settings
from aicam.modules.sessions import service as sessions
from aicam.modules.stations import service as stations
from aicam.workers.celery_app import app


def _run[T](job: Callable[..., Awaitable[T]]) -> T:
    async def _wrapped() -> T:
        init_engine(get_settings().database_url)
        try:
            async with sessionmaker()() as session:
                return await job(session)
        finally:
            await dispose_engine()

    return asyncio.run(_wrapped())


@app.task(name="stations.check_clock_drift", soft_time_limit=120)  # type: ignore[untyped-decorator]
def check_clock_drift() -> int:
    """J-09 (02a §7): mỗi 10 phút."""
    return _run(stations.update_clock_offsets)


@app.task(name="sessions.check_timeouts", soft_time_limit=25)  # type: ignore[untyped-decorator]
def check_timeouts() -> dict[str, int]:
    """J-07 (02a §7): mỗi 30 giây, BR-16."""
    from aicam.core.redis import close_redis, init_redis

    async def _job(session: AsyncSession) -> dict[str, int]:
        init_redis(get_settings().redis_url)
        try:
            return await sessions.check_timeouts(session, get_settings())
        finally:
            await close_redis()

    return _run(_job)
