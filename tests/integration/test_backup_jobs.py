"""J-21 / J-22 / J-23 (02a §6, §7; BR-33, FR-02.08 b, 02.14, 02.18; EX-K3, K6; DEC-496, 499, 505, 517, 522).

Kho `MemoryStore` (versioning), tệp clip / ảnh thật trong `VIDEO_ROOT` tạm. Phần MinIO: `test_cloud_minio.py`.
"""

import hashlib
import io
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.modules.backup import jobs
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.cloud import crypto
from aicam.modules.cloud.store import UNREACHABLE, CloudError, MemoryStore
from aicam.modules.media.models import Clip
from aicam.modules.settings import service as settings_service

from .backup_fixtures import KEY_A, NOW, World

pytestmark = pytest.mark.integration


async def _objects(db: AsyncSession) -> list[BackupObject]:
    rows = await db.scalars(
        select(BackupObject)
        .where(BackupObject.kind.in_(("CLIP", "SNAPSHOT")))
        .order_by(BackupObject.object_key)
    )
    return list(rows.all())


def _plain(store: MemoryStore, key: str) -> bytes:
    out = io.BytesIO()
    crypto.decrypt_stream(io.BytesIO(store.raw(key)), out, crypto.keyring(KEY_A))
    return out.getvalue()


async def test_j21_only_br33_evidence_and_idempotent(db: AsyncSession, world: World) -> None:
    out = await jobs.enqueue_evidence(db, world.settings)
    assert out == {"clips": 2, "snapshots": 1}
    rows = await _objects(db)
    ids = {r.clip_id for r in rows} | {r.snapshot_id for r in rows}
    assert ids - {None} == {c.id for c in world.clips.values()} | {world.snapshot.id}
    assert all(r.status == "PENDING" and r.reason == "EVIDENCE" and not r.cloud_present for r in rows)
    clip_row = next(r for r in rows if r.clip_id == world.clips["CAM1"].id)
    assert clip_row.object_key == f"backup/evidence/clips/{world.clips['CAM1'].id}.enc"
    assert clip_row.sha256 == world.clips["CAM1"].sha256
    assert await jobs.enqueue_evidence(db, world.settings) == {"clips": 0, "snapshots": 0}


async def test_j21_all_pack_clips_option(db: AsyncSession, world: World) -> None:
    cfg = await settings_service.get(db)
    cfg.backup_all_pack_clips = True
    await db.flush()
    out = await jobs.enqueue_evidence(db, world.settings)
    assert out["all_pack"] == 2  # clip phiên PACK không phải bằng chứng
    loose = (await db.scalars(select(Clip.id).where(Clip.session_id == world.loose.id))).all()
    reasons = {r.clip_id: r.reason for r in await _objects(db) if r.clip_id}
    assert {reasons[c] for c in loose} == {"ALL_PACK"}
    assert reasons[world.clips["CAM1"].id] == "EVIDENCE"


async def test_j21_j22_skip_when_not_on(db: AsyncSession, world: World, memory_store: MemoryStore) -> None:
    cfg = await settings_service.get(db)
    cfg.backup_enabled = False
    await db.flush()
    assert await jobs.enqueue_evidence(db, world.settings) == {"skipped": "DISABLED"}
    assert await jobs.upload_evidence(db, world.settings, store=memory_store) == {"skipped": "DISABLED"}
    assert memory_store.keys() == []


async def test_j22_uploads_encrypted_with_metadata(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    await jobs.enqueue_evidence(db, world.settings)
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["UPLOADED"] == 3
    for row in await _objects(db):
        assert row.status == "UPLOADED"
        assert row.cloud_present is True
        assert row.cloud_key_fingerprint == crypto.fingerprint(crypto.parse_key(KEY_A))
        src = world.clips["CAM1"] if row.clip_id == world.clips["CAM1"].id else None
        if src is not None:
            assert src.path is not None
            assert _plain(memory_store, row.object_key) == world.files[src.path]
            head = memory_store.head(row.object_key)
            assert head is not None
            assert head.metadata["sha256"] == src.sha256
            assert head.metadata["relpath"] == src.path
            assert head.metadata["kind"] == "CLIP"
            assert "integrity" not in head.metadata
        assert row.encrypted_size == len(memory_store.raw(row.object_key))
    loose = (await db.scalars(select(Clip.path).where(Clip.session_id == world.loose.id))).all()
    assert not any(
        str(p) in k
        for p in loose
        for k in memory_store.keys()  # noqa: SIM118
    )  # clip ngoài BR-33 không lên cloud


async def test_j22_hash_mismatch_not_uploaded(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    clip = world.clips["CAM1"]
    assert clip.path is not None
    (world.settings.video_root / clip.path).write_bytes(b"tep-bi-sua")
    await jobs.enqueue_evidence(db, world.settings)
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["HASH_MISMATCH"] == 1
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == clip.id))
    assert row is not None
    assert row.status == "HASH_MISMATCH"
    assert row.sha256_actual == hashlib.sha256(b"tep-bi-sua").hexdigest()
    assert row.cloud_present is False
    assert memory_store.head(row.object_key) is None


async def test_j22_source_deleted_vs_source_missing(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    await jobs.enqueue_evidence(db, world.settings)
    cam1, cam2 = world.clips["CAM1"], world.clips["CAM2"]
    cam1.status, cam1.deleted_at = "DELETED", NOW  # retention xóa trước khi tải
    assert cam2.path is not None
    (world.settings.video_root / cam2.path).unlink()  # còn hạn mà mất tệp (EX-K9)
    await db.flush()
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["SOURCE_DELETED"] == 1
    assert out["SOURCE_MISSING"] == 1
    rows = {r.clip_id: r for r in await _objects(db)}
    assert rows[cam1.id].status == "SOURCE_DELETED"
    missing = rows[cam2.id]
    assert missing.status == "FAILED"
    assert missing.last_error == "SOURCE_MISSING"
    assert missing.attempts == 1
    assert missing.next_attempt_at == NOW + timedelta(minutes=5)


async def test_j22_network_down_then_recovers(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    await jobs.enqueue_evidence(db, world.settings)
    memory_store.fail = CloudError(UNREACHABLE)
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["FAILED"] == 3
    rows = await _objects(db)
    assert {r.status for r in rows} == {"FAILED"}
    assert all(r.next_attempt_at == NOW + timedelta(minutes=5) for r in rows)
    assert await jobs.upload_evidence(db, world.settings, store=memory_store) == {
        "lease_expired": 0
    }  # chưa tới hạn
    memory_store.fail = None
    clock.advance(timedelta(minutes=6))
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["UPLOADED"] == 3


async def test_j22_lease_expired_uploading_requeued(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    await jobs.enqueue_evidence(db, world.settings)
    row = (await _objects(db))[0]
    row.status, row.updated_at = (
        "UPLOADING",
        NOW - timedelta(seconds=world.settings.backup_upload_budget_s + 61),
    )
    await db.flush()
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["lease_expired"] == 1
    assert out["UPLOADED"] == 3
    await db.refresh(row)
    assert row.status == "UPLOADED"


async def test_j22_yields_to_share_job(db: AsyncSession, world: World, memory_store: MemoryStore) -> None:
    from redis import Redis

    from aicam.modules.cloud.ratelimit import SHARE_ACTIVE_KEY

    await jobs.enqueue_evidence(db, world.settings)
    r = Redis.from_url(world.settings.redis_url)
    r.set(SHARE_ACTIVE_KEY, "1", ex=30)
    try:
        out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    finally:
        r.delete(SHARE_ACTIVE_KEY)
        r.close()
    assert out.get("yielded_to_share") is True
    assert {r.status for r in await _objects(db)} == {"PENDING"}


async def _uploaded(db: AsyncSession, world: World, store: MemoryStore) -> dict[object, BackupObject]:
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=store)
    return {r.clip_id or r.snapshot_id: r for r in await _objects(db)}


async def test_j23_deletes_only_retention_deleted(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    rows = await _uploaded(db, world, memory_store)
    cam1, cam2 = world.clips["CAM1"], world.clips["CAM2"]
    # CAM1: J-02 xóa theo retention (audit RETENTION) → xóa bản cloud.
    cam1.status, cam1.deleted_at = "DELETED", NOW
    audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=cam1.id,
                 data={"reason": "RETENTION"})  # fmt: skip
    # CAM2: `DELETED` không có audit retention (vd. dữ liệu khôi phục lệch) → KHÔNG xóa.
    cam2.status = "DELETED"
    # Ảnh `MISSING` (thiếu tệp sau khôi phục) → KHÔNG xóa (DEC-499).
    world.snapshot.status = "MISSING"
    await db.flush()
    out = await jobs.prune(db, world.settings, store=memory_store)
    assert out["evidence_deleted"] == 1
    r1, r2, rs = rows[cam1.id], rows[cam2.id], rows[world.snapshot.id]
    for r in (r1, r2, rs):
        await db.refresh(r)
    assert r1.status == "CLOUD_DELETED"
    assert r1.cloud_present is False
    assert r1.cloud_key_fingerprint is None
    assert memory_store.head(r1.object_key) is None
    assert memory_store.versions(r1.object_key)[-1] is None  # delete marker, bản cũ còn ≤ 7 ngày
    assert r2.cloud_present is True
    assert memory_store.head(r2.object_key) is not None
    assert rs.cloud_present is True
    assert memory_store.head(rs.object_key) is not None


async def test_j23_snapshot_retention_and_uploading_skipped(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    rows = await _uploaded(db, world, memory_store)
    world.snapshot.status, world.snapshot.deleted_at = "DELETED", NOW
    cam1 = world.clips["CAM1"]
    cam1.status, cam1.deleted_at = "DELETED", NOW
    audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=cam1.id,
                 data={"reason": "RETENTION"})  # fmt: skip
    rows[cam1.id].status = "UPLOADING"  # đang tải lại — lượt này bỏ qua
    await db.flush()
    out = await jobs.prune(db, world.settings, store=memory_store)
    assert out["evidence_deleted"] == 1
    await db.refresh(rows[world.snapshot.id])
    assert rows[world.snapshot.id].status == "CLOUD_DELETED"
    assert memory_store.head(rows[cam1.id].object_key) is not None


async def test_j23_blocked_when_restore_pending_or_schema_mismatch(
    db: AsyncSession, world: World, memory_store: MemoryStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = await _uploaded(db, world, memory_store)
    cam1 = world.clips["CAM1"]
    cam1.status, cam1.deleted_at = "DELETED", NOW
    audit.record(db, "DELETE_CLIP", user_id=None, object_type="CLIP", object_id=cam1.id,
                 data={"reason": "RETENTION"})  # fmt: skip
    cfg = await settings_service.get(db)
    cfg.backup_restore_pending = True
    await db.flush()
    assert await jobs.prune(db, world.settings, store=memory_store) == {"skipped": "RESTORE_PENDING"}
    cfg.backup_restore_pending = False
    await db.flush()

    from aicam.core import schema_guard

    async def _mismatch(_: AsyncSession) -> bool:
        return False

    key = rows[cam1.id].object_key
    monkeypatch.setattr(schema_guard, "matches", _mismatch)
    assert await jobs.prune(db, world.settings, store=memory_store) == {"skipped_schema_mismatch": 1}
    assert memory_store.head(key) is not None


def _run(days_ago: float, status: str = "SUCCESS", at: datetime | None = None) -> BackupRun:
    return BackupRun(
        kind="DB", trigger="SCHEDULE", status=status, started_at=at or NOW - timedelta(days=days_ago)
    )


def test_db_keep_policy_always_three_latest() -> None:
    old = [_run(100 + i) for i in range(5)]  # mọi bản > 30 ngày, không ngày 1
    for r in old:
        r.id = r.id or __import__("uuid").uuid4()
    keep = jobs.db_runs_to_keep(old, NOW, "Asia/Ho_Chi_Minh")
    assert keep == {r.id for r in sorted(old, key=lambda r: r.started_at, reverse=True)[:3]}


def test_db_keep_policy_30_days_and_monthly() -> None:
    import uuid

    runs = [_run(d) for d in (1, 10, 29, 31, 45)]
    # 2026-08-01 01:00 VN = 2026-07-31 18:00 UTC (ngày 1 giờ VN) và một lượt sau đó cùng ngày.
    first = _run(0, at=datetime(2026, 7, 31, 18, 0, tzinfo=UTC))
    later = _run(0, at=datetime(2026, 8, 1, 0, 0, tzinfo=UTC))
    too_old = _run(0, at=datetime(2025, 8, 31, 18, 0, tzinfo=UTC))  # ngày 1 nhưng > 12 tháng
    failed = _run(2, status="FAILED")
    runs += [first, later, too_old, failed]
    for r in runs:
        r.id = uuid.uuid4()
    keep = jobs.db_runs_to_keep(runs, NOW, "Asia/Ho_Chi_Minh")
    assert {runs[0].id, runs[1].id, runs[2].id, first.id} <= keep
    assert runs[3].id not in keep
    assert later.id not in keep
    assert too_old.id not in keep
    assert failed.id not in keep


async def test_j23_prunes_old_db_runs_keeps_three(
    db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    fp = crypto.fingerprint(crypto.parse_key(KEY_A))
    runs = []
    for i in range(5):
        run = _run(40 + i)
        db.add(run)
        await db.flush()
        key = f"backup/db/x/aicam-{i}.dump.enc"
        memory_store.put_stream(key, io.BytesIO(b"dump"))
        db.add(BackupObject(kind="DB_DUMP", run_id=run.id, object_key=key, status="UPLOADED",
                            cloud_present=True, cloud_key_fingerprint=fp, encrypted_size=4))  # fmt: skip
        runs.append((run, key))
    await db.flush()
    out = await jobs.prune(db, world.settings, store=memory_store)
    assert out["db_runs_deleted"] == 2
    for i, (run, key) in enumerate(runs):
        await db.refresh(run)
        if i < 3:
            assert run.cloud_deleted_at is None
            assert memory_store.head(key) is not None
        else:
            assert run.cloud_deleted_at is not None
            assert memory_store.head(key) is None
