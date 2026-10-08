"""Thông báo (M17): settings + client API + đăng nhập theo vai. Mặc định `NOTIFY_TRANSPORT=mock` (tin ghi
Redis `notify:mock:{type}`); test Telegram / Zalo thật dùng server giả (respx)."""

import json
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.redis import get_redis
from aicam.core.settings import Settings, get_settings
from aicam.modules.notify.models import NotifyChannel
from aicam.modules.notify.providers.mock import mock_key

from .conftest import TEST_DATABASE_URL, TEST_REDIS_URL
from .factories import PASSWORD, make_user

TG_BASE = "https://tg.test"
TG_TOKEN = "123456:TESTtokenABCdef_ghi-JKL"


def make_notify_settings(tmp: Path, **kw: object) -> Settings:
    base: dict[str, object] = {
        "app_env": "test",
        "log_json": False,
        "database_url": TEST_DATABASE_URL,
        "redis_url": TEST_REDIS_URL,
        "video_root": tmp / "video",
        "notify_transport": "mock",
        "site_address": "x.local",
    }
    base.update(kw)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture
def notify_settings(tmp_path: Path) -> Settings:
    (tmp_path / "video").mkdir(exist_ok=True)
    return make_notify_settings(tmp_path)


@pytest.fixture
async def notify_api(
    db: AsyncSession, redis_client: object, notify_settings: Settings
) -> AsyncIterator[AsyncClient]:
    from aicam.core.db import get_session
    from aicam.main import create_app

    app = create_app(notify_settings)
    app.dependency_overrides[get_session] = lambda: db
    app.dependency_overrides[get_settings] = lambda: notify_settings
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver") as client:
        yield client


async def login(api: AsyncClient, db: AsyncSession, role: str = "ADMIN") -> tuple[dict[str, str], uuid.UUID]:
    user = await make_user(db, f"tst_ntf_{role.lower()}_{uuid.uuid4().hex[:6]}", role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


async def make_channel(
    db: AsyncSession,
    name: str = "Kho",
    *,
    type_: str = "TELEGRAM",
    target: str = "-1001234567890",
    events: list[str] | None = None,
    enabled: bool = True,
) -> NotifyChannel:
    ch = NotifyChannel(
        name=name, type=type_, target=target, events=events or ["N01", "N02", "N03", "N09"], enabled=enabled
    )
    db.add(ch)
    await db.flush()
    return ch


async def mock_sent(channel_type: str = "TELEGRAM") -> list[dict[str, Any]]:
    """Tin mock đã gửi (02a §7.2 `notify:mock:{type}`)."""
    rows = await get_redis().lrange(mock_key(channel_type), 0, -1)  # type: ignore[misc]
    return [json.loads(r) for r in rows]
