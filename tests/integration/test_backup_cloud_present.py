"""`cloud_present` / `cloud_key_fingerprint` độc lập `status` (DEC-522, T-287): CHECK cặp cột; J-23 xóa bản
cloud của nguồn bị retention xóa với **mọi** trạng thái trừ `UPLOADING`; API-184 / API-187 `DISABLED` →
409."""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.modules.backup import jobs
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud.store import MemoryStore
from aicam.modules.settings import service as settings_service

from .backup_fixtures import NOW, World, login

pytestmark = pytest.mark.integration


async def test_check_cloud_present_matches_fingerprint(db: AsyncSession, world: World) -> None:
    clip = world.clips["CAM1"]
    db.add(BackupObject(kind="CLIP", clip_id=clip.id, object_key="x", cloud_present=True))
    with pytest.raises(IntegrityError):
        async with db.begin_nested():
            await db.flush()


@pytest.mark.parametrize(
    "status", ["PENDING", "FAILED", "HASH_MISMATCH", "IGNORED", "SOURCE_DELETED", "UPLOADED"]
)
async def test_j23_deletes_cloud_copy_regardless_of_status(
    db: AsyncSession, world: World, memory_store: MemoryStore, status: str
) -> None:
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    cam1 = world.clips["CAM1"]
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == cam1.id))
    assert row is not None
    assert row.cloud_present is True
    row.status = status
    cam1.status, cam1.deleted_at = "DELETED", NOW
    audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=cam1.id,
                 data={"reason": "RETENTION"})  # fmt: skip
    await db.flush()
    out = await jobs.prune(db, world.settings, store=memory_store)
    assert out["evidence_deleted"] == 1
    await db.refresh(row)
    assert row.status == "CLOUD_DELETED"
    assert row.cloud_present is False
    assert row.cloud_key_fingerprint is None
    assert row.cloud_deleted_at is not None
    assert memory_store.head(row.object_key) is None


async def test_disabled_409_for_run_now_and_reupload(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    headers, _ = await login(backup_api, db)
    cfg = await settings_service.get(db)
    cfg.backup_enabled = False
    await db.flush()
    for path in ("/api/v1/backup/run-db", "/api/v1/backup/reupload-old-key"):
        res = await backup_api.post(path, headers=headers)
        assert res.status_code == 409, path
        assert res.json()["error"]["code"] == "BACKUP_DISABLED"
        assert res.json()["error"]["message"] == "Sao lưu đang tắt. Bật sao lưu rồi thử lại."
