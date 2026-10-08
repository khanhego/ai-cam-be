"""T-291 (02a §5.2 #16, EX-K9 v0.5, DEC-530): J-22 đặt nguồn `MISSING` ở lần thử liền thứ 4 không thấy tệp, về
`READY` khi tệp có lại băm khớp; lệch → `HASH_MISMATCH` (nguồn giữ `MISSING`) → `UPLOAD_ANYWAY` → `READY`
(`clip.sha256` giữ gốc); API-188 `IGNORE` → `MISSING` ngay; audit `MEDIA_MARK_MISSING` /
`MEDIA_MISSING_RECOVERED`.
"""

from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.modules.backup import jobs
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud.store import MemoryStore
from aicam.modules.media.models import Clip

from .backup_fixtures import World, login, use_settings

pytestmark = pytest.mark.integration


async def _start(db: AsyncSession, world: World, store: MemoryStore) -> tuple[BackupObject, Clip, bytes]:
    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    path = world.settings.video_root / cam1.path
    content = path.read_bytes()
    path.unlink()
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=store)
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == cam1.id))
    assert row is not None
    return row, cam1, content


async def _audits(db: AsyncSession, action: str) -> list[AuditLog]:
    return list(
        (await db.scalars(select(AuditLog).where(AuditLog.action == action).order_by(AuditLog.id))).all()
    )


async def _retry_round(db: AsyncSession, world: World, store: MemoryStore) -> None:
    clock.advance(timedelta(minutes=61))
    await jobs.upload_evidence(db, world.settings, store=store)


async def test_mark_missing_after_four_then_recover(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    row, cam1, content = await _start(db, world, memory_store)
    for expected_attempts in (2, 3):
        await _retry_round(db, world, memory_store)
        await db.refresh(row)
        await db.refresh(cam1)
        assert row.attempts == expected_attempts
        assert cam1.status == "READY"  # chưa đủ 4 lần liền
    assert await _audits(db, "MEDIA_MARK_MISSING") == []
    await _retry_round(db, world, memory_store)
    await db.refresh(row)
    await db.refresh(cam1)
    assert row.attempts == 4
    assert cam1.status == "MISSING"
    (entry,) = await _audits(db, "MEDIA_MARK_MISSING")
    assert entry.user_id is None
    assert entry.data == {"clip_id": str(cam1.id), "cause": "SOURCE_MISSING", "object_id": str(row.id)}
    # Vẫn thử lại mỗi giờ.
    assert row.status == "FAILED"
    assert row.last_error == "SOURCE_MISSING"
    assert row.next_attempt_at == clock.now() + timedelta(minutes=60)
    await _retry_round(db, world, memory_store)
    assert len(await _audits(db, "MEDIA_MARK_MISSING")) == 1  # không ghi lại khi đã MISSING
    # IT chép lại đúng tệp → về READY + tải lên.
    (world.settings.video_root / (cam1.path or "")).write_bytes(content)
    await _retry_round(db, world, memory_store)
    await db.refresh(row)
    await db.refresh(cam1)
    assert cam1.status == "READY"
    assert row.status == "UPLOADED"
    (rec,) = await _audits(db, "MEDIA_MISSING_RECOVERED")
    assert rec.user_id is None
    assert rec.data is not None
    assert rec.data["accepted_mismatch"] is False


async def test_missing_counter_restarts_after_other_error(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    """Đếm lần **liền**: lỗi khác xen giữa (vd mất mạng) → lần không thấy tệp kế tiếp đếm lại từ 1."""
    row, cam1, _ = await _start(db, world, memory_store)
    await _retry_round(db, world, memory_store)
    await _retry_round(db, world, memory_store)
    await db.refresh(row)
    assert row.attempts == 3
    row.last_error = "CLOUD_UNREACHABLE: mất mạng"  # lượt lỗi mạng xen giữa
    await db.flush()
    await _retry_round(db, world, memory_store)
    await db.refresh(row)
    await db.refresh(cam1)
    assert row.attempts == 1
    assert cam1.status == "READY"


async def test_changed_file_needs_upload_anyway(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    row, cam1, _ = await _start(db, world, memory_store)
    original_sha = cam1.sha256
    for _ in range(3):
        await _retry_round(db, world, memory_store)
    await db.refresh(cam1)
    assert cam1.status == "MISSING"
    (world.settings.video_root / (cam1.path or "")).write_bytes(b"noi-dung-khac")
    await _retry_round(db, world, memory_store)
    await db.refresh(row)
    await db.refresh(cam1)
    assert row.status == "HASH_MISMATCH"
    assert cam1.status == "MISSING"  # lệch → nguồn giữ MISSING
    headers, _ = await login(backup_api, db)
    res = await backup_api.post(
        f"/api/v1/backup/issues/{row.id}/resolve", headers=headers,
        json={"action": "UPLOAD_ANYWAY", "note": "Đã xem, đúng clip"},
    )  # fmt: skip
    assert res.status_code == 200, res.text
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    await db.refresh(row)
    await db.refresh(cam1)
    assert cam1.status == "READY"
    assert cam1.sha256 == original_sha  # lệch đã chấp nhận — giữ băm gốc (như EX-K6)
    assert row.status == "UPLOADED"
    (rec,) = await _audits(db, "MEDIA_MISSING_RECOVERED")
    assert rec.data is not None
    assert rec.data["accepted_mismatch"] is True


async def test_ignore_marks_missing_now(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    row, cam1, _ = await _start(db, world, memory_store)
    headers, admin_id = await login(backup_api, db)
    res = await backup_api.post(
        f"/api/v1/backup/issues/{row.id}/resolve", headers=headers,
        json={"action": "IGNORE", "note": "Ổ hỏng, mất hẳn"},
    )  # fmt: skip
    assert res.status_code == 200
    await db.refresh(cam1)
    assert cam1.status == "MISSING"
    (entry,) = await _audits(db, "MEDIA_MARK_MISSING")
    assert entry.user_id == admin_id
    assert entry.data == {"clip_id": str(cam1.id), "cause": "BACKUP_IGNORE", "object_id": str(row.id)}
    # Phát / cắt lại / link sau khi MISSING → 409 (T-286).
    res = await backup_api.get(f"/api/v1/clips/{cam1.id}/play-url", headers=headers)
    assert res.status_code == 409
    assert res.json()["error"]["details"]["status"] == "MISSING"
