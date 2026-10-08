"""Cứng hóa J-22 / API-185 / API-188 (EX-K6; DEC-496): "Vẫn sao lưu" tải đúng bản lệch đã xem kèm metadata
`integrity=MISMATCH_ACCEPTED`; tệp đổi tiếp → lệch mới; "Bỏ qua" là trạng thái cuối, không tính chờ; mọi lỗi
409 / 422 / 404; audit; `SOURCE_DELETED`, `IGNORED` không tính `pending`."""

import hashlib
import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.modules.backup import jobs
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud.store import MemoryStore

from .backup_fixtures import World, login, use_settings

pytestmark = pytest.mark.integration


async def _mismatch(
    db: AsyncSession, world: World, store: MemoryStore, content: bytes = b"ban-bi-sua"
) -> BackupObject:
    clip = world.clips["CAM1"]
    assert clip.path is not None
    (world.settings.video_root / clip.path).write_bytes(content)
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=store)
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == clip.id))
    assert row is not None
    assert row.status == "HASH_MISMATCH"
    return row


async def test_upload_anyway_uploads_accepted_mismatch(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    headers, user_id = await login(backup_api, db)
    row = await _mismatch(db, world, memory_store)
    url = f"/api/v1/backup/issues/{row.id}/resolve"
    res = await backup_api.post(url, headers=headers, json={"action": "RETRY", "note": "thu lai"})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_ISSUE_ACTION_INVALID"
    res = await backup_api.post(url, headers=headers, json={"action": "UPLOAD_ANYWAY", "note": "  ab "})
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"]["note"] == "Nhập lý do (5–500 ký tự)."
    res = await backup_api.post(
        url, headers=headers, json={"action": "UPLOAD_ANYWAY", "note": "Có còn hơn không"}
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "PENDING"
    assert body["resolution"]["action"] == "UPLOAD_ANYWAY"
    assert body["resolution"]["by"]["id"] == str(user_id)
    assert body["sha256_actual"] == hashlib.sha256(b"ban-bi-sua").hexdigest()
    res = await backup_api.post(url, headers=headers, json={"action": "IGNORE", "note": "doi y roi"})
    assert res.json()["error"]["code"] == "BACKUP_ISSUE_RESOLVED"
    (audit_row,) = (await db.scalars(select(AuditLog).where(AuditLog.action == "BACKUP_ISSUE_RESOLVE"))).all()
    assert audit_row.data is not None
    assert audit_row.data["action"] == "UPLOAD_ANYWAY"
    assert audit_row.data["sha256_expected"] == world.clips["CAM1"].sha256

    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["UPLOADED"] == 1
    await db.refresh(row)
    assert row.status == "UPLOADED"
    assert row.cloud_present is True
    head = memory_store.head(row.object_key)
    assert head is not None
    assert head.metadata["integrity"] == "MISMATCH_ACCEPTED"
    assert head.metadata["sha256-expected"] == world.clips["CAM1"].sha256
    assert head.metadata["sha256"] == row.sha256_actual

    params = {"kind": "HASH_MISMATCH", "include_resolved": True}
    issues = (await backup_api.get("/api/v1/backup/issues", headers=headers, params=params)).json()
    assert [i["object_id"] for i in issues["items"]] == [str(row.id)]
    assert (await backup_api.get("/api/v1/backup/issues", headers=headers,
                                 params={"kind": "HASH_MISMATCH"})).json()["total"] == 0  # fmt: skip


async def test_accepted_content_changes_again_is_new_mismatch(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    headers, _ = await login(backup_api, db)
    row = await _mismatch(db, world, memory_store)
    res = await backup_api.post(f"/api/v1/backup/issues/{row.id}/resolve", headers=headers,
                                json={"action": "UPLOAD_ANYWAY", "note": "chap nhan ban nay"})  # fmt: skip
    assert res.status_code == 200
    clip = world.clips["CAM1"]
    assert clip.path is not None
    (world.settings.video_root / clip.path).write_bytes(b"lai-bi-sua-tiep")
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["HASH_MISMATCH"] == 1
    await db.refresh(row)
    assert row.status == "HASH_MISMATCH"
    assert row.hash_override is False
    assert row.resolution_action is None
    assert row.sha256_actual == hashlib.sha256(b"lai-bi-sua-tiep").hexdigest()
    assert memory_store.head(row.object_key) is None


async def test_ignore_is_final_and_not_pending(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    headers, _ = await login(backup_api, db)
    row = await _mismatch(db, world, memory_store)
    res = await backup_api.post(f"/api/v1/backup/issues/{row.id}/resolve", headers=headers,
                                json={"action": "IGNORE", "note": "Tệp hỏng, bỏ qua"})  # fmt: skip
    assert res.json()["status"] == "IGNORED"
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    await db.refresh(row)
    assert row.status == "IGNORED"
    cam2 = world.clips["CAM2"]
    cam2.status = "DELETED"
    other = await db.scalar(select(BackupObject).where(BackupObject.clip_id == cam2.id))
    assert other is not None
    other.status = "PENDING"
    other.cloud_present, other.cloud_key_fingerprint = False, None
    await db.flush()
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    ev = (await backup_api.get("/api/v1/backup", headers=headers)).json()["evidence"]
    assert ev["ignored"] == 1
    assert ev["source_deleted"] == 1
    assert ev["pending"] == 0
    assert ev["hash_mismatch"] == 0


async def test_resolve_404_and_admin_only(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    headers, _ = await login(backup_api, db)
    res = await backup_api.post(f"/api/v1/backup/issues/{uuid.uuid4()}/resolve", headers=headers,
                                json={"action": "IGNORE", "note": "khong co"})  # fmt: skip
    assert res.status_code == 404
    sup, _ = await login(backup_api, db, "SUPERVISOR")
    res = await backup_api.post(f"/api/v1/backup/issues/{uuid.uuid4()}/resolve", headers=sup,
                                json={"action": "IGNORE", "note": "khong co"})  # fmt: skip
    assert res.status_code == 403
