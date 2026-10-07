"""API-186 `aicam backup-restore` (DB) + `aicam backup-verify` (FR-02.16, NFR-40, AC-50; DEC-499, 518).

DB nguồn = DB test đã migrate; DB đích = `aicam_test_restore` (tạo / xóa trong test, cùng server test — không
phải DB dev `aicam`). pg_dump / pg_restore 16 chạy qua container tạm nếu máy không cài.
"""

import hashlib
import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.schema_guard import SCHEMA_HEAD
from aicam.core.settings import Settings
from aicam.modules.backup import jobs, restore
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud.store import MemoryStore
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package
from aicam.modules.settings import service as settings_service

from .backup_fixtures import KEY_A, KEY_B, enable_backup, make_backup_settings, pg_tool
from .conftest import TEST_DATABASE_URL
from .factories import make_station_account
from .returns_helpers import pack_session_with_clips

pytestmark = pytest.mark.integration

TARGET_URL = os.environ.get(
    "TEST_RESTORE_DATABASE_URL",
    make_url(TEST_DATABASE_URL).set(database="aicam_test_restore").render_as_string(hide_password=False),
)


async def _recreate(url: str) -> None:
    target = make_url(url)
    admin = create_async_engine(target.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{target.database}" WITH (FORCE)'))
            await conn.execute(text(f'CREATE DATABASE "{target.database}"'))
    finally:
        await admin.dispose()


async def _drop(url: str) -> None:
    target = make_url(url)
    admin = create_async_engine(target.set(database="postgres"), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{target.database}" WITH (FORCE)'))
    finally:
        await admin.dispose()


@pytest.fixture
async def empty_target(migrated_database_url: str) -> AsyncIterator[str]:
    await _recreate(TARGET_URL)
    yield TARGET_URL
    await _drop(TARGET_URL)


async def _scalar(url: str, sql: str) -> object:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(text(sql))
    finally:
        await engine.dispose()


async def _make_dump(db: AsyncSession, tmp_path: Path, store: MemoryStore, key: str = KEY_A) -> Settings:
    src = make_backup_settings(
        backup_encryption_key=key,
        backup_tmp_dir=tmp_path / "bk",
        import_root=tmp_path / "imports",
        backup_pg_dump_bin=pg_tool("pg_dump"),
    )
    await enable_backup(db, src)
    (tmp_path / "imports" / "2026").mkdir(parents=True, exist_ok=True)
    (tmp_path / "imports" / "2026" / "don.csv").write_text("ma_don\n2410TST001\n")
    out = await jobs.run_db(db, src, store=store)
    assert out["status"] == "SUCCESS", out
    return src


def _target_settings(url: str, tmp_path: Path, **kw: object) -> Settings:
    return make_backup_settings(
        database_url=url, backup_tmp_dir=tmp_path / "rst", backup_pg_restore_bin=pg_tool("pg_restore"), **kw
    )


async def test_restore_db_into_empty_then_refuse_non_empty(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, empty_target: str
) -> None:
    await _make_dump(db, tmp_path, memory_store)
    settings = _target_settings(empty_target, tmp_path, import_root=tmp_path / "imports-new")
    report = await restore.restore(settings, store=memory_store)
    assert report.exit_code == restore.EXIT_OK, report.lines
    assert (tmp_path / "imports-new" / "2026" / "don.csv").read_text() == "ma_don\n2410TST001\n"
    assert await _scalar(empty_target, "SELECT version_num FROM alembic_version") == SCHEMA_HEAD
    assert await _scalar(empty_target, "SELECT backup_restore_pending FROM setting WHERE id = 1") is True
    assert await _scalar(empty_target, "SELECT backup_enabled FROM setting WHERE id = 1") is False
    assert not list((tmp_path / "rst").glob("aicam-restore-*"))  # tệp tạm đã xóa

    again = await restore.restore(settings, store=memory_store)
    assert again.exit_code == restore.EXIT_REFUSED
    assert "không trống" in "\n".join(again.lines)


async def test_restore_wrong_key_writes_nothing(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, empty_target: str
) -> None:
    await _make_dump(db, tmp_path, memory_store)
    settings = _target_settings(
        empty_target, tmp_path, backup_encryption_key=KEY_B, import_root=tmp_path / "imports-new"
    )
    report = await restore.restore(settings, store=memory_store)
    assert report.exit_code == restore.EXIT_REFUSED
    assert "Khóa giải mã không khớp (dấu vân tay" in report.lines[0]
    count = await _scalar(
        empty_target, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"
    )
    assert count == 0
    # Đưa thêm khóa cũ qua --key-file → khôi phục được (DEC-495).
    key_file = tmp_path / "old.key"
    key_file.write_text(KEY_A)
    report = await restore.restore(settings, store=memory_store, key_files=[key_file])
    assert report.exit_code == restore.EXIT_OK, report.lines


async def test_restore_no_dump(memory_store: MemoryStore, tmp_path: Path) -> None:
    report = await restore.restore(_target_settings(TARGET_URL, tmp_path), store=memory_store)
    assert report.exit_code == restore.EXIT_REFUSED


NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)


async def test_verify_categories_and_clears_pending(db: AsyncSession, tmp_path: Path) -> None:
    clock.freeze(NOW)
    settings = make_backup_settings(video_root=tmp_path / "video")
    _, station = await make_station_account(db)
    package = Package(tracking_number="SPXTSTVF00001", warehouse_status="PACKED")
    db.add(package)
    await db.flush()
    pack = await pack_session_with_clips(db, station, package, NOW - timedelta(days=1), snapshot=False)
    cam1, cam2 = (
        await db.scalars(select(Clip).where(Clip.session_id == pack.id).order_by(Clip.camera_role))
    ).all()
    for clip in (cam1, cam2):
        assert clip.path is not None
        path = settings.video_root / clip.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{clip.camera_role}".encode())
        clip.sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    cfg = await settings_service.get(db)
    cfg.backup_restore_pending = True
    await db.flush()

    ok = await restore.verify(settings, db)
    assert ok.exit_code == restore.EXIT_OK, ok.lines
    assert ok.lines[0].startswith("khớp 2 /")
    assert (await settings_service.get(db)).backup_restore_pending is False
    assert await db.scalar(select(AuditLog).where(AuditLog.action == "BACKUP_RESTORE_VERIFIED"))

    assert cam2.path is not None
    (settings.video_root / cam2.path).write_bytes(b"sua")
    bad = await restore.verify(settings, db)
    assert bad.exit_code == restore.EXIT_VERIFY_FAILED
    assert "lệch 1" in bad.lines[0]

    # Lệch đã chấp nhận (API-188 UPLOAD_ANYWAY trước đó) không làm trượt.
    db.add(BackupObject(kind="CLIP", clip_id=cam2.id, object_key="k", status="PENDING", hash_override=True,
                        sha256_actual=hashlib.sha256(b"sua").hexdigest()))  # fmt: skip
    assert cam1.path is not None
    (settings.video_root / cam1.path).unlink()
    cam1.status = "MISSING"  # thiếu đã ghi nhận
    await db.flush()
    res = await restore.verify(settings, db)
    assert res.exit_code == restore.EXIT_OK, res.lines
    assert res.lines[0] == "khớp 0 / lệch đã chấp nhận 1 / lệch 0 / thiếu đã ghi nhận 1 / thiếu 0"


def test_keys_fixture_distinct() -> None:
    assert KEY_A != KEY_B
