"""Review G3 Phase 3 — sao lưu (G3-BK-*). Kho `MemoryStore` thay S3 (MinIO thật: xem `backup_fixtures`)."""

import io
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.backup import jobs, transfer
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import CloudError, MemoryStore

from .backup_fixtures import KEY_A, KEY_B, NOW, World, enable_backup

pytestmark = pytest.mark.integration


async def _rotated_pending(db: AsyncSession, world: World, memory_store: MemoryStore) -> BackupObject:
    """Bằng chứng đã lên cloud bằng KEY_A → đổi khóa sang KEY_B, dòng CAM1 xếp tải lại (như API-187)."""
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == world.clips["CAM1"].id))
    assert row is not None
    assert row.cloud_present
    assert row.cloud_key_fingerprint == crypto.fingerprint(crypto.parse_key(KEY_A))
    world.settings.backup_encryption_key = KEY_B
    world.settings.backup_old_keys = KEY_A
    await enable_backup(db, world.settings)
    others = (await db.scalars(select(BackupObject).where(BackupObject.id != row.id))).all()
    for other in others:
        other.status = "UPLOADED"
    row.status, row.attempts, row.next_attempt_at, row.last_error = "PENDING", 0, NOW, None
    await db.flush()
    return row


def _cloud_plain(store: MemoryStore, key: str) -> bytes:
    out = io.BytesIO()
    crypto.decrypt_stream(io.BytesIO(store.raw(key)), out, crypto.keyring(KEY_B, KEY_A))
    return out.getvalue()


async def test_bk1_reupload_file_changes_between_hash_and_upload_keeps_original(
    db: AsyncSession, world: World, memory_store: MemoryStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-BK-1 (HIGH): tải lại đối tượng đã `cloud_present` (đổi khóa); tệp bị ghi đè ngay sau lúc băm → bản
    cloud hiện hành vẫn là nội dung gốc (đúng mã băm), `cloud_key_fingerprint` khớp bản thật trên kho."""
    row = await _rotated_pending(db, world, memory_store)
    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    original = world.files[cam1.path]
    target = world.settings.video_root / cam1.path
    real_hash = transfer.sha256_file
    tampered: list[Path] = []

    def hash_then_tamper(path: Path) -> tuple[str, int]:
        out = real_hash(path)
        if not tampered:
            target.write_bytes(b"BI-GHI-DE" * 40)  # tệp gốc đổi sau lúc băm, trước lúc tải
            tampered.append(path)
        return out

    monkeypatch.setattr(transfer, "sha256_file", hash_then_tamper)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    await db.refresh(row)
    assert _cloud_plain(memory_store, row.object_key) == original
    head = memory_store.head(row.object_key)
    assert head is not None
    assert row.cloud_present is True
    assert row.cloud_key_fingerprint == head.metadata["key-fp"]


async def test_bk1_failed_reupload_syncs_fingerprint_with_cloud(
    db: AsyncSession, world: World, memory_store: MemoryStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tải lại ghi lên kho xong nhưng bước kiểm sau đó lỗi → dòng `FAILED` nhưng `cloud_key_fingerprint` đọc
    lại từ HEAD (khớp bản hiện hành), bản cloud vẫn đúng nội dung gốc."""
    row = await _rotated_pending(db, world, memory_store)
    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    real_upload = transfer.upload_file

    def upload_then_fail(*args: object, **kwargs: object) -> transfer.Uploaded:
        real_upload(*args, **kwargs)  # type: ignore[arg-type]
        raise CloudError("CLOUD_ERROR", "giả: kiểm sau tải lỗi")

    monkeypatch.setattr(transfer, "upload_file", upload_then_fail)
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    await db.refresh(row)
    head = memory_store.head(row.object_key)
    assert head is not None
    assert row.status == "FAILED"
    assert row.cloud_present is True
    assert row.cloud_key_fingerprint == head.metadata["key-fp"]
    assert _cloud_plain(memory_store, row.object_key) == world.files[cam1.path]
