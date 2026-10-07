"""Fixture sao lưu cloud (M15): kho `MemoryStore` thay S3, settings có khóa sao lưu, client API Admin.

MinIO thật (tùy chọn): đặt `TEST_S3_ENDPOINT` (+ `TEST_S3_ACCESS_KEY`, `TEST_S3_SECRET_KEY`, `TEST_S3_BUCKET`,
`TEST_S3_SHARE_BUCKET`, `TEST_S3_ROOT_KEY`, `TEST_S3_ROOT_SECRET`) — container riêng, không phải stack dev.
"""

import base64
import os
import shutil
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.settings import Settings, get_settings
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import MemoryStore

from .conftest import TEST_DATABASE_URL, TEST_REDIS_URL
from .factories import PASSWORD, make_user

KEY_A = base64.b64encode(b"A" * 32).decode()
KEY_B = base64.b64encode(b"B" * 32).decode()


@pytest.fixture
def memory_store() -> Iterator[MemoryStore]:
    store = MemoryStore("test-backup")
    cloud.use_store(cloud.BACKUP, store)
    yield store
    cloud.use_store(cloud.BACKUP, None)


def make_backup_settings(**kw: object) -> Settings:
    base: dict[str, object] = {
        "app_env": "test",
        "log_json": False,
        "database_url": TEST_DATABASE_URL,
        "redis_url": TEST_REDIS_URL,
        "backup_encryption_key": KEY_A,
    }
    base.update(kw)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture
def backup_settings(tmp_path: object) -> Settings:
    return make_backup_settings(backup_tmp_dir=f"{tmp_path}/backup-tmp")


@pytest.fixture
async def backup_api(
    db: AsyncSession, redis_client: object, backup_settings: Settings
) -> AsyncIterator[AsyncClient]:
    from aicam.core.db import get_session
    from aicam.main import create_app

    app = create_app(backup_settings)
    app.dependency_overrides[get_session] = lambda: db
    app.dependency_overrides[get_settings] = lambda: backup_settings
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver") as client:
        yield client


async def login(api: AsyncClient, db: AsyncSession, role: str = "ADMIN") -> tuple[dict[str, str], uuid.UUID]:
    user = await make_user(db, f"tst_{role.lower()}_{uuid.uuid4().hex[:6]}", role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


S3_ENDPOINT = os.environ.get("TEST_S3_ENDPOINT", "")
needs_minio = pytest.mark.skipif(not S3_ENDPOINT, reason="cần MinIO tạm (TEST_S3_ENDPOINT) — chưa test")


def minio_settings(**kw: object) -> Settings:
    base: dict[str, object] = {
        "s3_endpoint": S3_ENDPOINT,
        "s3_access_key_id": os.environ.get("TEST_S3_ACCESS_KEY", ""),
        "s3_secret_access_key": os.environ.get("TEST_S3_SECRET_KEY", ""),
        "s3_bucket": os.environ.get("TEST_S3_BUCKET", "aicam-test-backup"),
        "s3_share_bucket": os.environ.get("TEST_S3_SHARE_BUCKET", "aicam-test-share"),
    }
    base.update(kw)
    return make_backup_settings(**base)


BIN = Path(__file__).resolve().parent / "bin"


def pg_tool(name: str) -> str:
    """`pg_dump` / `pg_restore` / `psql` 16: trong PATH, không có thì wrapper chạy container
    (`bin/docker-*`)."""
    found = shutil.which(name)
    if found:
        return found
    if shutil.which("docker"):
        return str(BIN / f"docker-{name}")
    pytest.skip(f"cần {name} hoặc docker — chưa test")


def fake_pg_dump(tmp: Path, payload: str = "PGDMP-fake-dump", code: int = 0) -> str:
    """pg_dump giả (không cần docker): in `payload` ra stdout, thoát `code`."""
    script = tmp / f"fake-pg-dump-{code}"
    script.write_text(f"#!/bin/sh\nprintf '%s' '{payload}'\necho 'lỗi giả' >&2\nexit {code}\n")
    script.chmod(0o755)
    return str(script)


async def enable_backup(db: AsyncSession, settings: Settings) -> None:
    """Đưa `backup.state` về `ON`: đã xác nhận đúng dấu vân tay khóa hiện tại, bật."""
    from aicam.modules.backup import service
    from aicam.modules.settings import service as settings_service

    cfg = await settings_service.get(db)
    cfg.backup_confirmed_fingerprint = service.current_fingerprint(settings)
    cfg.backup_enabled = True
    cfg.backup_restore_pending = False
    await db.flush()
