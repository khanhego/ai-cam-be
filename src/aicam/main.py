"""FastAPI app factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from aicam import __version__
from aicam.core.db import dispose_engine, init_engine
from aicam.core.errors import install_error_handlers
from aicam.core.logging import configure_logging
from aicam.core.redis import close_redis, init_redis
from aicam.core.settings import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level, settings.log_json)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # Engine và Redis kết nối lười: app vẫn khởi động khi DB chưa sẵn sàng (/healthz luôn trả lời).
        init_engine(settings.database_url)
        init_redis(settings.redis_url)
        try:
            yield
        finally:
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
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.get("/healthz", tags=["system"])
    async def healthz() -> dict[str, str]:
        """Liveness, không cần đăng nhập (02a §10)."""
        return {"status": "ok", "version": __version__}

    return app
