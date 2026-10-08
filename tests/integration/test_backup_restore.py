"""API-186 `aicam backup-restore` (DB) + `aicam backup-verify` (FR-02.16, NFR-40, AC-50; DEC-499, 518).

DB nguồn = DB test đã migrate; DB đích = `aicam_test_restore` (tạo / xóa trong test, cùng server test — không
phải DB dev `aicam`). pg_dump / pg_restore 16 chạy qua container tạm nếu máy không cài.
"""

import hashlib
import io
import os
import uuid
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
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Package
from aicam.modules.settings import service as settings_service

from .backup_fixtures import KEY_A, KEY_B, World, enable_backup, make_backup_settings, pg_tool
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


# ---------------------------------------------------------------- bằng chứng (T-274: EX-K8, DEC-499)


async def _backed_up(db: AsyncSession, world: World, store: MemoryStore) -> None:
    await jobs.enqueue_evidence(db, world.settings)
    out = await jobs.upload_evidence(db, world.settings, store=store)
    assert out["UPLOADED"] == 3


async def test_restore_evidence_browse_cloud_missing_never_deleted(
    db: AsyncSession, redis_client: object, world: World, memory_store: MemoryStore, tmp_path: Path
) -> None:
    import io as _io

    from aicam.modules.cloud import crypto as _crypto

    await _backed_up(db, world, memory_store)
    cam1, cam2 = world.clips["CAM1"], world.clips["CAM2"]
    # Xóa 1 đối tượng clip trên cloud trước khi khôi phục (AC-50).
    memory_store.delete(jobs.evidence_key("CLIP", cam2.id))
    # Đối tượng "ngoài DB" (tải lên sau bản dump).
    ghost = uuid.uuid4()
    memory_store.put_stream(jobs.evidence_key("CLIP", ghost),
                            _crypto.EncryptingReader(_io.BytesIO(b"ghost"), _crypto.parse_key(KEY_A)),  # type: ignore[arg-type]
                            metadata={"id": str(ghost), "kind": "CLIP", "relpath": f"clips/ghost/{ghost}.mp4",
                                      "sha256": hashlib.sha256(b"ghost").hexdigest()})  # fmt: skip
    new_root = tmp_path / "may-moi"
    settings = make_backup_settings(video_root=new_root)
    report = restore.Report()
    stats = await restore.restore_evidence(
        settings, db, report, keys=_crypto.keyring(KEY_A), store=memory_store
    )
    assert stats.downloaded == 3  # CAM1 + ảnh + ghost
    assert stats.outside_db == 1
    assert cam1.path is not None
    assert (new_root / cam1.path).read_bytes() == world.files[cam1.path]
    assert world.snapshot.path is not None
    assert (new_root / world.snapshot.path).read_bytes() == world.files[world.snapshot.path]
    assert (new_root / f"clips/ghost/{ghost}.mp4").read_bytes() == b"ghost"
    statuses = {c.id: c.status for c in (await db.scalars(
        select(Clip).execution_options(populate_existing=True))).all()}  # fmt: skip
    assert statuses[cam2.id] == "MISSING"  # không có bản cloud, không có tệp → thiếu, KHÔNG xóa
    assert statuses[cam1.id] == "READY"
    loose = (await db.scalars(select(Clip.id).where(Clip.session_id == world.loose.id))).all()
    assert {statuses[c] for c in loose} == {"MISSING"}
    assert "DELETED" not in statuses.values()
    gets = [k for op, k in memory_store.calls if op == "get" and "/evidence/" in k]
    assert gets[0] in {jobs.evidence_key("CLIP", cam1.id), jobs.evidence_key("SNAPSHOT", world.snapshot.id)}
    assert gets[-1] == jobs.evidence_key("CLIP", ghost)  # hồ sơ khiếu nại mở trước


async def test_restore_evidence_unknown_key_then_evidence_only(
    db: AsyncSession, redis_client: object, world: World, memory_store: MemoryStore, tmp_path: Path
) -> None:
    from aicam.modules.cloud import crypto as _crypto

    await _backed_up(db, world, memory_store)
    new_root = tmp_path / "may-moi"
    settings = make_backup_settings(video_root=new_root, backup_encryption_key=KEY_B)
    report = restore.Report()
    stats = await restore.restore_evidence(
        settings, db, report, keys=_crypto.keyring(KEY_B), store=memory_store
    )
    assert stats.downloaded == 0
    assert {f[3] for f in stats.failures} == {"UNKNOWN_KEY"}
    assert stats.marked_missing >= 3
    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    assert not (new_root / cam1.path).exists()  # không để tệp dở
    assert not list(new_root.rglob("*.part"))
    stats = await restore.restore_evidence(
        settings, db, report, keys=_crypto.keyring(KEY_B, KEY_A), store=memory_store, evidence_only=True
    )
    assert stats.recovered == 3
    refreshed = await db.scalar(
        select(Clip).where(Clip.id == cam1.id).execution_options(populate_existing=True)
    )
    assert refreshed is not None
    assert refreshed.status == "READY"
    loose = (await db.scalars(select(Clip.status).where(Clip.session_id == world.loose.id))).all()
    assert set(loose) == {"MISSING"}  # không có bản cloud → vẫn thiếu


async def test_restore_skips_retention_deleted(
    db: AsyncSession, redis_client: object, world: World, memory_store: MemoryStore, tmp_path: Path
) -> None:
    from aicam.modules.cloud import crypto as _crypto

    await _backed_up(db, world, memory_store)
    cam1 = world.clips["CAM1"]
    cam1.status = "DELETED"
    await db.flush()
    settings = make_backup_settings(video_root=tmp_path / "may-moi")
    stats = await restore.restore_evidence(
        settings, db, restore.Report(), keys=_crypto.keyring(KEY_A), store=memory_store
    )
    assert stats.skipped_deleted == 1
    assert cam1.path is not None
    assert not (tmp_path / "may-moi" / cam1.path).exists()


# ---------------------------------------------------------------- T-284: lối ra (DEC-518)


async def test_decrypt_failed_csv_exit3_no_partial_file(
    db: AsyncSession, redis_client: object, world: World, memory_store: MemoryStore, tmp_path: Path
) -> None:
    from aicam.modules.cloud import crypto as _crypto

    await _backed_up(db, world, memory_store)
    cam1 = world.clips["CAM1"]
    key = jobs.evidence_key("CLIP", cam1.id)
    data = bytearray(memory_store.raw(key))
    data[200] ^= 0x01  # sửa 1 byte đối tượng cloud
    memory_store.tamper(key, bytes(data))
    settings = make_backup_settings(video_root=tmp_path / "may-moi")
    report = restore.Report()
    stats = await restore.restore_evidence(
        settings, db, report, keys=_crypto.keyring(KEY_A), store=memory_store
    )
    restore._report_failures(settings, report, stats)
    assert report.exit_code == restore.EXIT_PARTIAL
    assert stats.downloaded == 2  # chạy hết phần còn lại
    assert [f[3] for f in stats.failures] == ["DECRYPT_FAILED"]
    (csv_path,) = (tmp_path / "may-moi" / "restore-reports").glob("restore-failures-*.csv")
    lines = csv_path.read_text().splitlines()
    assert lines[0] == "kind,id,object_key,reason,key_fp"
    assert lines[1].startswith(f"CLIP,{cam1.id},{key},DECRYPT_FAILED,")
    assert cam1.path is not None
    assert not (tmp_path / "may-moi" / cam1.path).exists()
    assert not list((tmp_path / "may-moi").rglob("*.part"))
    status = await db.scalar(select(Clip.status).where(Clip.id == cam1.id))
    assert status == "MISSING"


async def test_verify_accept_requires_reason_and_clears_pending(
    db: AsyncSession, redis_client: object, world: World, memory_store: MemoryStore
) -> None:
    from aicam.modules.cloud import crypto as _crypto

    await _backed_up(db, world, memory_store)
    cfg = await settings_service.get(db)
    cfg.backup_restore_pending = True
    loose = (await db.scalars(select(Clip).where(Clip.session_id == world.loose.id))).all()
    for clip in loose:  # clip không thuộc BR-33: đã ghi nhận thiếu
        clip.status = "MISSING"
    await db.flush()
    cam1, cam2 = world.clips["CAM1"], world.clips["CAM2"]
    assert cam1.path is not None
    assert cam2.path is not None
    (world.settings.video_root / cam1.path).write_bytes(b"sua-sau-khoi-phuc")  # lệch
    (world.settings.video_root / cam2.path).unlink()  # thiếu
    loose_snap = await db.scalar(select(Snapshot).where(Snapshot.session_id == world.loose.id))
    assert loose_snap is not None
    loose_snap.status = "MISSING"
    await db.flush()

    rep = await restore.verify(world.settings, db)
    assert rep.exit_code == restore.EXIT_VERIFY_FAILED
    assert rep.lines[0] == "khớp 1 / lệch đã chấp nhận 0 / lệch 1 / thiếu đã ghi nhận 3 / thiếu 1"
    assert list((world.settings.video_root / "restore-reports").glob("verify-*.csv"))

    rep = await restore.verify(world.settings, db, accept=[str(cam1.id)], reason=None)
    assert rep.exit_code == restore.EXIT_REFUSED
    rep = await restore.verify(
        world.settings, db, accept=[str(cam1.id), str(cam2.id), str(uuid.uuid4())],
        reason="Tệp sửa khi chép lại sau sự cố ổ",
    )  # fmt: skip
    assert rep.exit_code == restore.EXIT_OK, rep.lines
    assert "khớp 1 / lệch đã chấp nhận 1 / lệch 0 / thiếu đã ghi nhận 4 / thiếu 0" in rep.lines
    assert any("bỏ qua" in line for line in rep.lines)
    assert (await settings_service.get(db)).backup_restore_pending is False
    audits = (await db.scalars(select(AuditLog).where(AuditLog.action == "BACKUP_VERIFY_ACCEPT"))).all()
    assert len(audits) == 2
    assert {a.data["category"] for a in audits if a.data} == {"MISMATCH", "MISSING"}
    assert all(a.user_id is None and a.data and a.data["reason"] for a in audits)
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == cam1.id))
    assert row is not None
    assert row.resolution_action == "ACCEPT_RESTORED"
    assert row.hash_override is True
    assert (await db.scalar(select(Clip.status).where(Clip.id == cam2.id))) == "MISSING"

    # Bản lệch đã chấp nhận KHÔNG ghi đè bản gốc trên cloud (DEC-663), kể cả khi xếp tải lại.
    original = memory_store.raw(row.object_key)
    row.status = "PENDING"
    await db.flush()
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out.get("KEPT_CLOUD_ORIGINAL") == 1
    assert memory_store.raw(row.object_key) == original
    plain = io.BytesIO()
    _crypto.decrypt_stream(io.BytesIO(original), plain, _crypto.keyring(KEY_A))
    assert plain.getvalue() == world.files[cam1.path]

    cloud = await restore.verify(world.settings, db, from_cloud=True, store=memory_store)
    assert cloud.lines[0].startswith("[bản cloud] khớp 3 /")
