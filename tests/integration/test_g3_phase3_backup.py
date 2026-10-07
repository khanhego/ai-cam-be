"""Review G3 Phase 3 — sao lưu (G3-BK-*). Kho `MemoryStore` thay S3 (MinIO thật: xem `backup_fixtures`)."""

import hashlib
import io
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aicam.modules.backup import jobs, restore, transfer
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import CloudError, MemoryStore

from .backup_fixtures import KEY_A, KEY_B, NOW, World, enable_backup, make_backup_settings
from .test_backup_restore import TARGET_URL, _drop, _make_dump, _recreate, _scalar, _target_settings

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


# ---------------------------------------------------------------- G3-BK-2 / BK-3 (backup-restore)

FAR = datetime(2099, 1, 1, 1, 0, tzinfo=UTC)


@pytest.fixture
async def target(migrated_database_url: str) -> AsyncIterator[str]:
    """DB đích trống `aicam_test_restore` (như `test_backup_restore.target`)."""
    await _recreate(TARGET_URL)
    yield TARGET_URL
    await _drop(TARGET_URL)


def _put_dump(store: MemoryStore, at: datetime, payload: bytes | None, *, complete: bool = True) -> str:
    """Bản DB giả trên kho: `payload` None = rác (không phải AICAMENC); `complete` = có bản file nhập cùng
    lượt."""
    key = jobs.db_object_key(at)
    if payload is None:
        store.put_stream(key, io.BytesIO(b"rac-khong-phai-aicamenc" * 4), metadata={"sha256": "x"})
    else:
        sha = hashlib.sha256(payload).hexdigest()
        transfer.upload_encrypted(store, key, io.BytesIO(payload), crypto.parse_key(KEY_A), {"sha256": sha})
    if complete:
        transfer.upload_encrypted(
            store, jobs.imports_object_key(at), io.BytesIO(b"tgz"), crypto.parse_key(KEY_A), {}
        )
    return key


def _fake_pg_restore(tmp: Path, code: int) -> str:
    script = tmp / f"fake-pg-restore-{code}"
    script.write_text(f"#!/bin/sh\ncat >/dev/null\necho 'pg_restore: lỗi giả giữa chừng' >&2\nexit {code}\n")
    script.chmod(0o755)
    return str(script)


async def test_bk3_latest_skips_incomplete_and_falls_back_on_corrupt(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, target: str
) -> None:
    """`--db latest`: bỏ qua bản của lượt chưa hoàn tất (mồ côi / lỗi), bản hoàn tất mới nhất hỏng → tự dùng
    bản kế, in rõ bản đã dùng; `--list` đánh dấu."""
    await _make_dump(db, tmp_path, memory_store)
    (good,) = [d.key for d in restore.db_dumps(memory_store)]
    corrupt = _put_dump(memory_store, FAR, None)
    orphan = _put_dump(memory_store, FAR + timedelta(days=1), b"PGDMP-mo-coi", complete=False)
    listed = restore.list_dumps(_target_settings(target, tmp_path), store=memory_store)
    text_ = "\n".join(listed.lines)
    assert f"{orphan}" in text_
    assert "CHƯA HOÀN TẤT" in text_
    assert f"{corrupt}" in text_.split("← --db latest")[0].splitlines()[-1]

    settings = _target_settings(target, tmp_path, import_root=tmp_path / "imports-new")
    report = await restore.restore(settings, store=memory_store)
    out = "\n".join(report.lines)
    assert report.exit_code == restore.EXIT_OK, out
    assert f"Bỏ qua {orphan}" in out
    assert f"Bản DB {corrupt} hỏng" in out
    assert f"DÙNG BẢN KẾ: {good}" in out
    assert await _scalar(target, "SELECT backup_restore_pending FROM setting WHERE id = 1") is True


async def test_bk2_explicit_corrupt_dump_exit_code(
    memory_store: MemoryStore, tmp_path: Path, target: str
) -> None:
    corrupt = _put_dump(memory_store, FAR, None)
    report = await restore.restore(_target_settings(target, tmp_path), db_key=corrupt, store=memory_store)
    assert report.exit_code == restore.EXIT_CORRUPT
    assert "hỏng / không giải mã được" in "\n".join(report.lines)


async def test_bk2_pg_restore_failure_marks_pending_and_warns(
    memory_store: MemoryStore, tmp_path: Path, target: str
) -> None:
    """`pg_restore` lỗi giữa chừng: mã 5, hướng dẫn KHÔNG `dc up -d`; bảng `setting` đã có (ghi đè `--force`)
    → vẫn tắt sao lưu + chờ kiểm (không để state ON)."""
    _put_dump(memory_store, FAR, b"PGDMP-gia")
    settings = make_backup_settings(
        database_url=target,
        backup_tmp_dir=tmp_path / "rst",
        backup_pg_restore_bin=_fake_pg_restore(tmp_path, 1),
    )
    report = await restore.restore(settings, store=memory_store)
    out = "\n".join(report.lines)
    assert report.exit_code == restore.EXIT_RESTORE_FAILED, out
    assert "KHÔNG chạy `dc up -d`" in out
    assert "chưa có bảng setting" in out

    engine = create_async_engine(target)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "CREATE TABLE setting (id int PRIMARY KEY, backup_enabled bool, "
                    "backup_restore_pending bool)"
                )
            )
            await conn.execute(text("INSERT INTO setting VALUES (1, true, false)"))
    finally:
        await engine.dispose()
    report = await restore.restore(settings, store=memory_store, force=True)
    assert report.exit_code == restore.EXIT_RESTORE_FAILED
    assert "Đã tắt sao lưu tự động" in "\n".join(report.lines)
    assert await _scalar(target, "SELECT backup_enabled FROM setting WHERE id = 1") is False
    assert await _scalar(target, "SELECT backup_restore_pending FROM setting WHERE id = 1") is True
