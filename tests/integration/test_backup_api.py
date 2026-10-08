"""API-180 / 181 / 182 / 184 / 185, API-81 `backup`, API-32 `BACKUP_STALE`, WS `backup.updated` (02 §6.2;
FR-02.15,
02.17, 02.18; EX-K1, K2, K6, K9; DEC-500, 517)."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.backup import service
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.cloud.store import MemoryStore
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package
from aicam.modules.settings import service as settings_service

from .backup_fixtures import enable_backup, login
from .factories import make_station_account
from .returns_helpers import pack_session_with_clips

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _clock() -> None:
    clock.freeze(NOW)


async def _audit(db: AsyncSession, action: str) -> list[AuditLog]:
    return list((await db.scalars(select(AuditLog).where(AuditLog.action == action))).all())


async def test_status_unconfirmed_then_confirm_key(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, backup_settings: Settings
) -> None:
    headers, user_id = await login(backup_api, db)
    res = await backup_api.get("/api/v1/backup", headers=headers)
    assert res.status_code == 200, res.text
    body = res.json()
    fp = service.current_fingerprint(backup_settings)
    assert body["configured"] is True
    assert body["state"] == "KEY_UNCONFIRMED"
    assert body["key"]["fingerprint"] == fp
    assert body["key"]["old_keys"] == []
    assert body["storage"] == {"endpoint_host": "memory", "bucket": "test-backup"}
    assert body["db"]["next_run_at"] is None
    assert "A" * 10 not in res.text  # không lộ khóa (KEY_A = base64 "AAAA…")

    # bật khi chưa xác nhận → 409
    res = await backup_api.put("/api/v1/backup/settings", headers=headers, json={"enabled": True})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_KEY_UNCONFIRMED"

    res = await backup_api.post(
        "/api/v1/backup/confirm-key", headers=headers, json={"fingerprint": "0000-0000"}
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_KEY_MISMATCH"

    res = await backup_api.post("/api/v1/backup/confirm-key", headers=headers, json={"fingerprint": fp})
    assert res.status_code == 200
    body = res.json()
    assert body["state"] == "ON"
    assert body["enabled"] is True
    assert body["key"]["confirmed_by"]["id"] == str(user_id)
    assert body["db"]["next_run_at"] == "2026-10-07T06:00:00Z"
    (row,) = await _audit(db, "BACKUP_KEY_CONFIRM")
    assert row.data == {"fingerprint": fp, "previous_fingerprint": None}


async def test_confirm_key_keeps_disabled_when_restore_pending(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, backup_settings: Settings
) -> None:
    headers, _ = await login(backup_api, db)
    cfg = await settings_service.get(db)
    cfg.backup_restore_pending = True
    await db.flush()
    res = await backup_api.post(
        "/api/v1/backup/confirm-key",
        headers=headers,
        json={"fingerprint": service.current_fingerprint(backup_settings)},
    )
    assert res.json()["state"] == "RESTORE_PENDING"
    assert res.json()["enabled"] is False
    res = await backup_api.put("/api/v1/backup/settings", headers=headers, json={"enabled": True})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_RESTORE_UNVERIFIED"


async def test_settings_update_validation_and_audit(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, backup_settings: Settings
) -> None:
    headers, _ = await login(backup_api, db)
    await enable_backup(db, backup_settings)
    res = await backup_api.put("/api/v1/backup/settings", headers=headers, json={"upload_mbps": 0})
    assert res.status_code == 422
    res = await backup_api.put(
        "/api/v1/backup/settings",
        headers=headers,
        json={"enabled": False, "upload_mbps": 25, "all_pack_clips": True},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["state"] == "DISABLED"
    assert body["settings"]["upload_mbps"] == 25
    assert body["settings"]["all_pack_clips"] is True
    (row,) = await _audit(db, "BACKUP_SETTINGS_UPDATE")
    assert row.data is not None
    assert row.data["after"] == {"enabled": False, "upload_mbps": 25, "all_pack_clips": True}
    res = await backup_api.put("/api/v1/backup/settings", headers=headers, json={"enabled": True})
    assert res.json()["state"] == "ON"


async def test_not_configured_503(backup_api: AsyncClient, db: AsyncSession) -> None:
    headers, _ = await login(backup_api, db)
    res = await backup_api.get("/api/v1/backup", headers=headers)
    assert res.json()["state"] == "NOT_CONFIGURED"
    assert res.json()["storage"] is None
    assert res.json()["key"]["fingerprint"] is None
    for method, path, json in (
        ("PUT", "/api/v1/backup/settings", {"enabled": True}),
        ("POST", "/api/v1/backup/confirm-key", {"fingerprint": "x"}),
        ("POST", "/api/v1/backup/run-db", None),
    ):
        res = await backup_api.request(method, path, headers=headers, json=json)
        assert res.status_code == 503, path
        assert res.json()["error"]["code"] == "BACKUP_NOT_CONFIGURED"


async def test_run_now(
    backup_api: AsyncClient,
    db: AsyncSession,
    memory_store: MemoryStore,
    backup_settings: Settings,
    sent_jobs: list[tuple[str, list[Any], str, float]],
) -> None:
    headers, _ = await login(backup_api, db)
    res = await backup_api.post("/api/v1/backup/run-db", headers=headers)
    assert res.json()["error"]["code"] == "BACKUP_KEY_UNCONFIRMED"
    await enable_backup(db, backup_settings)
    stale = BackupRun(kind="DB", trigger="SCHEDULE", status="RUNNING", started_at=NOW - timedelta(hours=3))
    db.add(stale)
    await db.flush()
    res = await backup_api.post("/api/v1/backup/run-db", headers=headers)
    assert res.status_code == 202, res.text
    run_id = res.json()["run_id"]
    assert ("backup.run_db", [run_id, "MANUAL"], "backup", 0.0) in sent_jobs
    await db.refresh(stale)
    assert stale.status == "FAILED"  # DEC-500 trước lượt mới
    res = await backup_api.post("/api/v1/backup/run-db", headers=headers)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_RUNNING"
    assert len(await _audit(db, "BACKUP_RUN_NOW")) == 1
    cfg = await settings_service.get(db)
    cfg.backup_enabled = False
    await db.flush()
    res = await backup_api.post("/api/v1/backup/run-db", headers=headers)
    assert res.json()["error"]["code"] == "BACKUP_DISABLED"


@pytest.mark.parametrize("role", ["SUPERVISOR", "CSKH"])
async def test_admin_only(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, role: str
) -> None:
    headers, _ = await login(backup_api, db, role)
    for method, path in (("GET", "/api/v1/backup"), ("GET", "/api/v1/backup/issues"),
                         ("POST", "/api/v1/backup/run-db"), ("PUT", "/api/v1/backup/settings")):  # fmt: skip
        res = await backup_api.request(method, path, headers=headers, json={})
        assert res.status_code == 403, path


async def _objects(db: AsyncSession) -> dict[str, BackupObject]:
    _, station = await make_station_account(db, username=f"tst_st_{uuid.uuid4().hex[:6]}")
    package = Package(tracking_number="SPXTSTAPI0001", warehouse_status="PACKED")
    db.add(package)
    await db.flush()
    pack = await pack_session_with_clips(db, station, package, NOW - timedelta(days=1))
    clips = (
        await db.scalars(select(Clip).where(Clip.session_id == pack.id).order_by(Clip.camera_role))
    ).all()
    rows = {
        "mismatch": BackupObject(kind="CLIP", clip_id=clips[0].id, object_key="k1", status="HASH_MISMATCH",
                                 sha256=clips[0].sha256, sha256_actual="ff" * 32, updated_at=NOW),
        "missing": BackupObject(kind="CLIP", clip_id=clips[1].id, object_key="k2", status="FAILED",
                                attempts=1, last_error="SOURCE_MISSING", updated_at=NOW,
                                created_at=NOW - timedelta(hours=30)),
    }  # fmt: skip
    for r in rows.values():
        db.add(r)
    await db.flush()
    return rows


async def test_issues_and_status_counts(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, backup_settings: Settings
) -> None:
    headers, _ = await login(backup_api, db)
    await enable_backup(db, backup_settings)
    rows = await _objects(db)
    res = await backup_api.get("/api/v1/backup/issues", headers=headers, params={"kind": "HASH_MISMATCH"})
    assert res.status_code == 200
    (item,) = res.json()["items"]
    assert item["object_id"] == str(rows["mismatch"].id)
    assert item["tracking_number"] == "SPXTSTAPI0001"
    assert item["sha256_expected"] == "ab" * 32
    assert item["sha256_actual"] == "ff" * 32
    assert item["resolution"] is None
    res = await backup_api.get("/api/v1/backup/issues", headers=headers, params={"kind": "SOURCE_MISSING"})
    (item,) = res.json()["items"]
    assert item["detail"] == "Không thấy tệp tại kho."
    res = await backup_api.get("/api/v1/backup/issues", headers=headers, params={"kind": "UPLOAD_FAILED"})
    assert res.json()["total"] == 0  # attempts < 3
    res = await backup_api.get("/api/v1/backup/issues", headers=headers)
    assert res.json()["total"] == 2
    res = await backup_api.get("/api/v1/backup/issues", headers=headers, params={"kind": "X"})
    assert res.status_code == 422

    body = (await backup_api.get("/api/v1/backup", headers=headers)).json()
    ev = body["evidence"]
    assert ev["hash_mismatch"] == 1
    assert ev["failed"] == 1
    assert ev["source_missing"] == 1
    assert ev["late_count"] == 1
    assert body["last_error"]["code"] == "SOURCE_MISSING"

    health = (await backup_api.get("/api/v1/system/health", headers=headers)).json()
    assert health["backup"]["state"] == "ON"
    assert health["backup"]["late"] is True

    daily = (await backup_api.get("/api/v1/reports/daily", headers=headers)).json()
    stale = {a["reason"]: a for a in daily["attention"] if a["kind"] == "BACKUP_STALE"}
    assert stale["HASH_MISMATCH"]["count"] == 1
    assert stale["SOURCE_MISSING"]["count"] == 1
    assert stale["EVIDENCE_LATE"]["count"] == 1
    sup, _ = await login(backup_api, db, "SUPERVISOR")
    daily = (await backup_api.get("/api/v1/reports/daily", headers=sup)).json()
    assert not [a for a in daily["attention"] if a["kind"] == "BACKUP_STALE"]


async def test_db_failed_twice_and_db_late(
    backup_api: AsyncClient, db: AsyncSession, memory_store: MemoryStore, backup_settings: Settings
) -> None:
    headers, _ = await login(backup_api, db)
    await enable_backup(db, backup_settings)
    cfg = await settings_service.get(db)
    cfg.backup_confirmed_at = NOW - timedelta(days=3)
    for h, st in ((30, "SUCCESS"), (12, "FAILED"), (6, "FAILED")):
        db.add(BackupRun(kind="DB", trigger="SCHEDULE", status=st, started_at=NOW - timedelta(hours=h),
                         finished_at=NOW - timedelta(hours=h) + timedelta(minutes=5),
                         error="CLOUD_UNREACHABLE: mất mạng" if st == "FAILED" else None))  # fmt: skip
    await db.flush()
    body = (await backup_api.get("/api/v1/backup", headers=headers)).json()
    assert body["db"]["consecutive_failures"] == 2
    assert body["db"]["late"] is True
    assert body["last_error"]["code"] == "CLOUD_UNREACHABLE"
    assert len(body["history"]) == 3
    daily = (await backup_api.get("/api/v1/reports/daily", headers=headers)).json()
    reasons = {a["reason"] for a in daily["attention"] if a["kind"] == "BACKUP_STALE"}
    assert {"DB_LATE", "DB_FAILED_TWICE"} <= reasons


async def test_ws_backup_updated_on_admin_channel(
    backup_api: AsyncClient,
    db: AsyncSession,
    memory_store: MemoryStore,
    backup_settings: Settings,
    redis_client: Any,
) -> None:
    import asyncio
    import json

    headers, _ = await login(backup_api, db)
    await enable_backup(db, backup_settings)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ws:admin")
    await pubsub.get_message(timeout=1)  # xác nhận subscribe
    res = await backup_api.put("/api/v1/backup/settings", headers=headers, json={"upload_mbps": 20})
    assert res.status_code == 200
    msg = None
    for _ in range(20):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if msg:
            break
        await asyncio.sleep(0.05)
    await pubsub.aclose()
    assert msg is not None
    event = json.loads(msg["data"])
    assert event["type"] == "backup.updated"
    assert event["data"] == {"state": "ON", "pending": 0, "last_db_success_at": None}
