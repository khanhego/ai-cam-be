"""API-183 kiểm tra kết nối kho lưu (FR-02.17): ghi → đọc → xóa 1 KB ≤ 10 giây; lỗi kho → 502 / 504 có mã;
chưa cấu hình → 503; chỉ ADMIN; audit `BACKUP_TEST {ok, code}` kể cả khi lỗi."""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.core.settings import get_settings
from aicam.modules.cloud.store import AUTH_FAILED, PROBE_PREFIX, UNREACHABLE, CloudError, MemoryStore

from .backup_fixtures import login, make_backup_settings

pytestmark = pytest.mark.integration


async def _audits(db: AsyncSession) -> list[AuditLog]:
    return list((await db.scalars(select(AuditLog).where(AuditLog.action == "BACKUP_TEST"))).all())


async def test_probe_ok(backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore) -> None:
    headers, user_id = await login(backup_api, db)
    res = await backup_api.post("/api/v1/backup/test", headers=headers)
    assert res.status_code == 200, res.text
    assert res.json()["ok"] is True
    assert memory_store.keys(PROBE_PREFIX) == []
    (row,) = await _audits(db)
    assert row.user_id == user_id
    assert row.data is not None
    assert row.data["ok"] is True
    assert row.data["code"] is None


@pytest.mark.parametrize(("code", "status"), [(AUTH_FAILED, 502), (UNREACHABLE, 504), ("CLOUD_ERROR", 502)])
async def test_probe_errors(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, code: str, status: int
) -> None:
    headers, _ = await login(backup_api, db)
    memory_store.fail = CloudError(code)
    res = await backup_api.post("/api/v1/backup/test", headers=headers)
    assert res.status_code == status
    assert res.json()["error"]["code"] == code
    (row,) = await _audits(db)
    assert row.data == {"ok": False, "code": code, "elapsed_ms": 0}


async def test_probe_not_configured_503(
    backup_api: AsyncClient, db: AsyncSession, backup_settings: object
) -> None:
    headers, _ = await login(backup_api, db)
    res = await backup_api.post("/api/v1/backup/test", headers=headers)  # không S3, không kho thay
    assert res.status_code == 503
    assert res.json()["error"]["code"] == "BACKUP_NOT_CONFIGURED"


@pytest.mark.parametrize("role", ["SUPERVISOR", "CSKH"])
async def test_probe_admin_only(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, role: str
) -> None:
    headers, _ = await login(backup_api, db, role)
    res = await backup_api.post("/api/v1/backup/test", headers=headers)
    assert res.status_code == 403


def test_settings_fixture_has_key() -> None:
    assert make_backup_settings().backup_encryption_key
    assert get_settings is not None
