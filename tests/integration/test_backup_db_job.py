"""J-20 sao lưu DB (02a §7, FR-02.08 a, NFR-40, DEC-466, DEC-500): pg_dump + file nhập → mã hóa → tải →
kiểm đọc lại → `SUCCESS` + dấu vân tay khóa; lỗi → `FAILED` có mã, không để file tạm / `RUNNING` treo; chỉ
chạy
khi `state = ON`; một lượt chạy; lượt treo > 2 giờ → `FAILED STALE_RUNNING` trước lượt mới; khóa không vào
log."""

import io
import tarfile
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.backup import jobs, service
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import UNREACHABLE, MemoryStore

from .backup_fixtures import KEY_A, enable_backup, fake_pg_dump, make_backup_settings, pg_tool

pytestmark = pytest.mark.integration


@pytest.fixture
def import_root(tmp_path: Path) -> Path:
    root = tmp_path / "imports"
    (root / "2026" / "10").mkdir(parents=True)
    (root / "2026" / "10" / "don-shopee.csv").write_text("ma_don,ma_van_don\n2410TST001,SPXTST0000001\n")
    return root


def _settings(tmp_path: Path, import_root: Path, pg_dump_bin: str) -> Settings:
    return make_backup_settings(
        backup_tmp_dir=tmp_path / "bk-tmp", import_root=import_root, backup_pg_dump_bin=pg_dump_bin
    )


def _decrypt(store: MemoryStore, key: str) -> bytes:
    out = io.BytesIO()
    crypto.decrypt_stream(io.BytesIO(store.raw(key)), out, crypto.keyring(KEY_A))
    return out.getvalue()


async def _runs(db: AsyncSession) -> list[BackupRun]:
    return list((await db.scalars(select(BackupRun).order_by(BackupRun.started_at))).all())


async def test_j20_real_pg_dump_success(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, import_root: Path
) -> None:
    settings = _settings(tmp_path, import_root, pg_tool("pg_dump"))
    await enable_backup(db, settings)
    out = await jobs.run_db(db, settings, store=memory_store)
    assert out["status"] == "SUCCESS", out
    (run,) = await _runs(db)
    assert run.status == "SUCCESS"
    assert run.trigger == "SCHEDULE"
    assert run.key_fingerprint == service.current_fingerprint(settings)
    assert run.object_key is not None
    assert run.object_key.startswith(f"backup/db/{run.started_at:%Y/%m/%d}/aicam-")
    assert run.object_key.endswith(".dump.enc")
    assert run.imports_object_key is not None
    assert run.imports_object_key.startswith("backup/imports/")
    dump = _decrypt(memory_store, run.object_key)
    assert dump.startswith(b"PGDMP")  # pg_dump -Fc
    assert run.size_bytes == len(memory_store.raw(run.object_key))
    with tarfile.open(fileobj=io.BytesIO(_decrypt(memory_store, run.imports_object_key))) as tgz:
        assert "imports/2026/10/don-shopee.csv" in tgz.getnames()
    head = memory_store.head(run.object_key)
    assert head is not None
    assert head.metadata["kind"] == "DB_DUMP"
    assert head.metadata["key-fp"] == run.key_fingerprint
    objs = (await db.scalars(select(BackupObject).where(BackupObject.run_id == run.id))).all()
    assert sorted(o.kind for o in objs) == ["DB_DUMP", "IMPORTS"]
    assert all(o.cloud_present and o.status == "UPLOADED" for o in objs)
    assert all(o.cloud_key_fingerprint == run.key_fingerprint for o in objs)
    assert not (settings.backup_tmp_dir / str(run.id)).exists()


async def test_j20_skips_unless_state_on(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, import_root: Path
) -> None:
    settings = _settings(tmp_path, import_root, fake_pg_dump(tmp_path))
    out = await jobs.run_db(db, settings, store=memory_store)  # chưa xác nhận khóa
    assert out == {"skipped": "KEY_UNCONFIRMED"}
    assert await _runs(db) == []
    assert memory_store.keys() == []


async def test_j20_single_run_and_stale_running(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, import_root: Path
) -> None:
    settings = _settings(tmp_path, import_root, fake_pg_dump(tmp_path))
    await enable_backup(db, settings)
    busy = BackupRun(
        kind="DB", trigger="MANUAL", status="RUNNING", started_at=clock.now() - timedelta(hours=1)
    )
    db.add(busy)
    await db.flush()
    assert await jobs.run_db(db, settings, store=memory_store) == {"skipped": "RUNNING"}
    busy.started_at = clock.now() - timedelta(hours=3)  # treo > 2 giờ (worker chết)
    await db.flush()
    out = await jobs.run_db(db, settings, store=memory_store)
    assert out["status"] == "SUCCESS"
    await db.refresh(busy)
    assert busy.status == "FAILED"
    assert busy.error is not None
    assert busy.error.startswith("STALE_RUNNING")


async def test_j20_pg_dump_failure_marks_failed_and_cleans(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, import_root: Path
) -> None:
    settings = _settings(tmp_path, import_root, fake_pg_dump(tmp_path, code=1))
    await enable_backup(db, settings)
    out = await jobs.run_db(db, settings, store=memory_store)
    assert out["status"] == "FAILED"
    (run,) = await _runs(db)
    assert run.status == "FAILED"
    assert run.error is not None
    assert run.error.startswith("PG_DUMP_FAILED")
    assert run.finished_at is not None
    assert memory_store.keys() == []
    assert not (settings.backup_tmp_dir / str(run.id)).exists()


async def test_j20_cloud_unreachable(
    db: AsyncSession, redis_client: object, memory_store: MemoryStore, tmp_path: Path, import_root: Path
) -> None:
    settings = _settings(tmp_path, import_root, fake_pg_dump(tmp_path))
    await enable_backup(db, settings)
    memory_store.fail_on_put_after = 1
    out = await jobs.run_db(db, settings, store=memory_store)
    assert out == {"status": "FAILED", "run_id": out["run_id"], "error": UNREACHABLE}
    (run,) = await _runs(db)
    assert run.error is not None
    assert run.error.startswith(UNREACHABLE)
    obj = await db.scalar(select(BackupObject).where(BackupObject.run_id == run.id))
    assert obj is not None
    assert obj.status == "FAILED"
    assert obj.cloud_present is False


class _CorruptingStore(MemoryStore):
    def get_stream(self, key: str) -> io.BytesIO:  # type: ignore[override]
        data = bytearray(self.raw(key))
        data[-5] ^= 0xFF
        return io.BytesIO(bytes(data))


async def test_j20_verify_readback_failure(
    db: AsyncSession, redis_client: object, tmp_path: Path, import_root: Path
) -> None:
    from aicam.modules.cloud import config as cloud

    store = _CorruptingStore()
    cloud.use_store(cloud.BACKUP, store)
    try:
        settings = _settings(tmp_path, import_root, fake_pg_dump(tmp_path))
        await enable_backup(db, settings)
        out = await jobs.run_db(db, settings, store=store)
    finally:
        cloud.use_store(cloud.BACKUP, None)
    assert out["status"] == "FAILED"
    assert out["error"] == "VERIFY_FAILED"
    obj = await db.scalar(select(BackupObject).where(BackupObject.kind == "DB_DUMP"))
    assert obj is not None
    assert obj.cloud_present is True  # bản lỗi vẫn ghi nhận có trên cloud → J-23 dọn được (DEC-522)


async def test_j20_key_never_logged(
    db: AsyncSession,
    redis_client: object,
    memory_store: MemoryStore,
    tmp_path: Path,
    import_root: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(tmp_path, import_root, fake_pg_dump(tmp_path))
    await enable_backup(db, settings)
    await jobs.run_db(db, settings, store=memory_store)
    captured = capsys.readouterr()
    text = captured.out + captured.err + caplog.text
    key = crypto.parse_key(KEY_A)
    assert KEY_A not in text
    assert key.hex() not in text
    assert KEY_A.encode() not in memory_store.raw(next(iter(memory_store.keys("backup/db/"))))
