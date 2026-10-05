"""Đăng ký task Celery. Mỗi task mở engine + Redis riêng (Celery đồng bộ, gọi coroutine qua asyncio.run)."""

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.redis import close_redis, init_redis
from aicam.core.settings import get_settings
from aicam.modules.media import service as media
from aicam.modules.sessions import service as sessions
from aicam.modules.stations import service as stations
from aicam.modules.stations.mediamtx import HttpMediaMTX, MediaMTXError
from aicam.workers.celery_app import app

log = structlog.get_logger()


def _run[T](job: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def _wrapped() -> T:
        settings = get_settings()
        init_engine(settings.database_url)
        init_redis(settings.redis_url)
        try:
            async with sessionmaker()() as session:
                return await job(session)
        finally:
            await close_redis()
            await dispose_engine()

    return asyncio.run(_wrapped())


@app.task(name="stations.check_clock_drift", soft_time_limit=120)  # type: ignore[untyped-decorator]
def check_clock_drift() -> int:
    """J-09 (02a §7): mỗi 10 phút."""
    return _run(stations.update_clock_offsets)


@app.task(name="sessions.check_timeouts", soft_time_limit=25)  # type: ignore[untyped-decorator]
def check_timeouts() -> dict[str, int]:
    """J-07 (02a §7): mỗi 30 giây, BR-16."""
    return _run(lambda db: sessions.check_timeouts(db, get_settings()))


@app.task(  # type: ignore[untyped-decorator]
    name="media.build_session_clips", bind=True, max_retries=3, soft_time_limit=120, time_limit=150
)
def build_session_clips(self: Any, session_id: str) -> dict[str, Any]:
    """J-01 (queue `video`): cắt clip sau khi phiên kết thúc. Thử lại 3 lần; lần cuối lỗi → clip FAILED."""
    final = self.request.retries >= self.max_retries
    result = _run(
        lambda db: media.build_session_clips(db, uuid.UUID(session_id), get_settings(), final=final)
    )
    if result.retry_in is not None and not final:
        raise self.retry(countdown=result.retry_in)
    return {"ready": len(result.ready), "failed": len(result.failed)}


@app.task(name="media.index_segments", soft_time_limit=55, time_limit=58)  # type: ignore[untyped-decorator]
def index_segments() -> dict[str, int]:
    """J-10 (mỗi phút): đồng bộ path MediaMTX với DB, index segment mới, dọn bản xuất hết hạn."""

    async def _job(db: AsyncSession) -> dict[str, int]:
        settings = get_settings()
        out: dict[str, int] = {}
        try:
            out.update(
                await stations.reconcile_mediamtx(db, HttpMediaMTX(settings.mediamtx_api_url), settings)
            )
        except MediaMTXError as exc:
            log.warning("mediamtx_unreachable", error=str(exc))
        out["indexed"] = await media.index_segments(db, settings)
        return out

    return _run(_job)
