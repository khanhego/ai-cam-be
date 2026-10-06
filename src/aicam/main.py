"""FastAPI app factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import aicam.db_models  # noqa: F401 — nạp mọi model để khóa ngoại giữa module phân giải được
from aicam import __version__
from aicam.core.db import dispose_engine, init_engine
from aicam.core.errors import install_error_handlers
from aicam.core.limits import BodySizeLimitMiddleware
from aicam.core.logging import configure_logging
from aicam.core.redis import close_redis, init_redis
from aicam.core.settings import Settings, get_settings
from aicam.modules.approvals.router import router as approvals_router
from aicam.modules.claims.router import router as claims_router
from aicam.modules.imports.router import router as imports_router
from aicam.modules.media.router import router as media_router
from aicam.modules.orders.router import router as orders_router
from aicam.modules.platforms.router import router as platforms_router
from aicam.modules.reports.router import router as reports_router
from aicam.modules.returns.router import router as returns_router
from aicam.modules.sessions.listeners import on_tray_changed
from aicam.modules.sessions.router import router as sessions_router
from aicam.modules.sessions.tray import TRAY_CHANGED_CHANNEL
from aicam.modules.settings.router import router as settings_router
from aicam.modules.stations.listeners import CAMERA_HEALTH_CHANNEL, on_camera_health
from aicam.modules.stations.router import router as stations_router
from aicam.modules.users.router import router as users_router
from aicam.realtime.bus import Bus, start
from aicam.realtime.hub import router as ws_router


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # Engine và Redis kết nối lười: app vẫn khởi động khi DB chưa sẵn sàng (/healthz luôn trả lời).
        init_engine(settings.database_url)
        redis = init_redis(settings.redis_url)
        bus = Bus()
        bus.on(CAMERA_HEALTH_CHANNEL, on_camera_health)
        bus.on(TRAY_CHANGED_CHANNEL, on_tray_changed)
        listener = start(bus, redis)
        try:
            yield
        finally:
            listener.cancel()
            await close_redis()
            await dispose_engine()

    app = FastAPI(
        title="Hệ thống X API",
        version=__version__,
        docs_url="/api/docs",
        openapi_url="/api/v1/openapi.json",
        lifespan=lifespan,
    )
    install_error_handlers(app)
    app.add_middleware(BodySizeLimitMiddleware)  # G3-N2: chặn body quá lớn trước khi Starlette spool ra đĩa
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.include_router(users_router, prefix="/api/v1")
    app.include_router(stations_router, prefix="/api/v1")
    app.include_router(sessions_router, prefix="/api/v1")
    app.include_router(approvals_router, prefix="/api/v1")
    app.include_router(orders_router, prefix="/api/v1")
    app.include_router(media_router, prefix="/api/v1")
    app.include_router(imports_router, prefix="/api/v1")
    app.include_router(platforms_router, prefix="/api/v1")
    app.include_router(reports_router, prefix="/api/v1")
    app.include_router(returns_router, prefix="/api/v1")
    app.include_router(claims_router, prefix="/api/v1")
    app.include_router(settings_router, prefix="/api/v1")
    app.include_router(ws_router)

    @app.get("/healthz", tags=["system"])
    async def healthz() -> dict[str, str]:
        """Liveness, không cần đăng nhập (02a §10)."""
        return {"status": "ok", "version": __version__}

    return app
