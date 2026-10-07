"""Đổi khóa sao lưu (EX-K7, FR-02.17; DEC-495, 522): `old_keys` đếm đúng bằng chứng + bản DB còn trên cloud
bằng khóa cũ (bất kể `status`), API-187 xếp đúng tệp còn ở kho, ngay sau API-187 vẫn đếm đủ, sau J-22 bản
cloud
mang khóa mới; nguồn bị retention xóa khi dòng còn `PENDING` → J-23 xóa bản cloud; khôi phục cần cả 2 khóa."""

import io
from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.core.audit import AuditLog
from aicam.core.settings import get_settings
from aicam.modules.backup import jobs, service
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import MemoryStore

from .backup_fixtures import KEY_A, KEY_B, NOW, World, login

pytestmark = pytest.mark.integration


async def _rotate(backup_api: AsyncClient, db: AsyncSession, world: World) -> dict[str, str]:
    """IT đổi khóa: khóa hiện tại = KEY_B, KEY_A vào BACKUP_OLD_KEYS (API dùng settings của `world`)."""
    world.settings.backup_encryption_key = KEY_B
    world.settings.backup_old_keys = KEY_A
    app = backup_api._transport.app  # type: ignore[attr-defined]
    app.dependency_overrides[get_settings] = lambda: world.settings
    headers, _ = await login(backup_api, db)
    return headers


async def test_rotation_old_keys_reupload_and_prune(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    fp_a = crypto.fingerprint(crypto.parse_key(KEY_A))
    fp_b = crypto.fingerprint(crypto.parse_key(KEY_B))
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    db.add(BackupRun(kind="DB", trigger="SCHEDULE", status="SUCCESS", started_at=NOW - timedelta(hours=1),
                     finished_at=NOW, key_fingerprint=fp_a))  # fmt: skip
    await db.flush()

    headers = await _rotate(backup_api, db, world)
    body = (await backup_api.get("/api/v1/backup", headers=headers)).json()
    assert body["state"] == "KEY_CHANGED"  # sao lưu dừng tới khi xác nhận khóa mới
    res = await backup_api.post("/api/v1/backup/reupload-old-key", headers=headers)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_KEY_UNCONFIRMED"
    body = (
        await backup_api.post("/api/v1/backup/confirm-key", headers=headers, json={"fingerprint": fp_b})
    ).json()
    assert body["state"] == "ON"
    (old,) = body["key"]["old_keys"]
    assert old["fingerprint"] == fp_a
    assert old["evidence_objects"] == 3
    assert old["db_runs"] == 1
    assert old["reuploadable"] == 3
    paths = [world.clips["CAM1"].path, world.clips["CAM2"].path, world.snapshot.path]
    assert old["reuploadable_bytes"] == sum(len(world.files[str(p)]) for p in paths)

    # Clip CAM2 bị retention xóa sau khi xếp tải lại (dòng còn PENDING, bản cũ còn trên cloud).
    res = await backup_api.post("/api/v1/backup/reupload-old-key", headers=headers)
    assert res.status_code == 202
    assert res.json()["queued"] == 3
    again = await backup_api.post("/api/v1/backup/reupload-old-key", headers=headers)
    assert again.json()["queued"] == 0  # idempotent
    (old,) = (await backup_api.get("/api/v1/backup", headers=headers)).json()["key"]["old_keys"]
    assert old["evidence_objects"] == 3  # vẫn đếm đủ khi đang PENDING (DEC-522)
    assert old["reuploadable"] == 0
    rows = {r.clip_id or r.snapshot_id: r for r in (await db.scalars(
        select(BackupObject).where(BackupObject.kind.in_(("CLIP", "SNAPSHOT"))))).all()}  # fmt: skip
    assert {r.status for r in rows.values()} == {"PENDING"}
    assert all(r.cloud_present and r.cloud_key_fingerprint == fp_a for r in rows.values())
    audit_row, noop = (
        await db.scalars(
            select(AuditLog).where(AuditLog.action == "BACKUP_REUPLOAD_OLD_KEY").order_by(AuditLog.id)
        )
    ).all()
    assert noop.data is not None
    assert noop.data["queued"] == 0
    assert audit_row.data is not None
    assert audit_row.data["fingerprints"] == [fp_a]
    assert audit_row.data["queued"] == 3

    cam2 = world.clips["CAM2"]
    cam2.status, cam2.deleted_at = "DELETED", NOW
    audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=cam2.id,
                 data={"reason": "RETENTION"})  # fmt: skip
    await db.flush()
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["UPLOADED"] == 2
    assert out["SOURCE_DELETED"] == 1
    for r in rows.values():
        await db.refresh(r)
    cam2_row = rows[cam2.id]
    assert cam2_row.status == "SOURCE_DELETED"
    assert cam2_row.cloud_present is True  # bản khóa cũ vẫn trên cloud
    assert cam2_row.cloud_key_fingerprint == fp_a
    for key in (world.clips["CAM1"].id, world.snapshot.id):
        assert rows[key].cloud_key_fingerprint == fp_b
        out_ = io.BytesIO()
        crypto.decrypt_stream(io.BytesIO(memory_store.raw(rows[key].object_key)), out_, crypto.keyring(KEY_B))

    (old,) = (await backup_api.get("/api/v1/backup", headers=headers)).json()["key"]["old_keys"]
    assert old["evidence_objects"] == 1  # chỉ còn CAM2 (đã bị xóa tại kho — vẫn cần khóa cũ)
    assert old["db_runs"] == 1

    pruned = await jobs.prune(db, world.settings, store=memory_store)
    assert pruned["evidence_deleted"] == 1  # SOURCE_DELETED + cloud_present → J-23 vẫn xóa bản cloud
    await db.refresh(cam2_row)
    assert cam2_row.cloud_present is False
    assert cam2_row.status == "CLOUD_DELETED"
    (old,) = (await backup_api.get("/api/v1/backup", headers=headers)).json()["key"]["old_keys"]
    assert old["evidence_objects"] == 0
    assert old["db_runs"] == 1


async def test_reupload_disabled_and_restore_pending(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    from aicam.modules.settings import service as settings_service

    headers, _ = await login(backup_api, db)
    cfg = await settings_service.get(db)
    cfg.backup_enabled = False
    await db.flush()
    res = await backup_api.post("/api/v1/backup/reupload-old-key", headers=headers)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_DISABLED"
    cfg.backup_restore_pending = True
    await db.flush()
    res = await backup_api.post("/api/v1/backup/reupload-old-key", headers=headers)
    assert res.json()["error"]["code"] == "BACKUP_RESTORE_UNVERIFIED"
    assert service.current_fingerprint(world.settings)
