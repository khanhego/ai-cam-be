"""`SOURCE_MISSING` (EX-K9, DEC-517): clip `READY` mất tệp → `FAILED SOURCE_MISSING` (không `SOURCE_DELETED`),
giãn cách 5 / 15 / 60 phút, API-185 `kind=SOURCE_MISSING` từ lần đầu, API-180 `source_missing`, API-32
`BACKUP_STALE reason=SOURCE_MISSING`; chép lại tệp + API-188 `RETRY` → `UPLOADED`; `RETRY` mà vẫn thiếu → hiện
lại; `IGNORE` → cuối; `IGNORE` khi tệp đã có lại → 409; `UPLOAD_ANYWAY` → 409. (Đặt nguồn `MISSING` —
T-291.)"""

from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.backup import jobs
from aicam.modules.backup.models import BackupObject
from aicam.modules.cloud.store import MemoryStore

from .backup_fixtures import NOW, World, login, use_settings

pytestmark = pytest.mark.integration


async def _missing(db: AsyncSession, world: World, store: MemoryStore) -> tuple[BackupObject, bytes]:
    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    path = world.settings.video_root / cam1.path
    content = path.read_bytes()
    path.unlink()  # IT xóa tay / ổ hỏng
    await jobs.enqueue_evidence(db, world.settings)
    await jobs.upload_evidence(db, world.settings, store=store)
    row = await db.scalar(select(BackupObject).where(BackupObject.clip_id == cam1.id))
    assert row is not None
    return row, content


async def test_source_missing_backoff_and_visibility(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    headers, _ = await login(backup_api, db)
    row, _ = await _missing(db, world, memory_store)
    assert row.status == "FAILED"
    assert row.last_error == "SOURCE_MISSING"
    assert world.clips["CAM1"].status == "READY"  # chưa đổi nguồn (T-291 mới đặt MISSING)
    waits = [row.next_attempt_at]
    for _ in range(3):
        clock.advance(timedelta(minutes=61))
        await jobs.upload_evidence(db, world.settings, store=memory_store)
        await db.refresh(row)
        waits.append(row.next_attempt_at)
    assert row.attempts == 4
    assert waits[0] == NOW + timedelta(minutes=5)
    deltas = [(w - (NOW + timedelta(minutes=61 * i))) for i, w in enumerate(waits) if w is not None]
    assert deltas[1] == timedelta(minutes=15)
    assert deltas[2] == timedelta(minutes=60)
    assert deltas[3] == timedelta(minutes=60)
    headers, _ = await login(backup_api, db)  # đồng hồ đã tua quá hạn access token
    issues = (await backup_api.get("/api/v1/backup/issues", headers=headers,
                                   params={"kind": "SOURCE_MISSING"})).json()  # fmt: skip
    assert [i["object_id"] for i in issues["items"]] == [str(row.id)]
    assert issues["items"][0]["detail"] == "Không thấy tệp tại kho."
    body = (await backup_api.get("/api/v1/backup", headers=headers)).json()
    assert body["evidence"]["source_missing"] == 1
    assert body["evidence"]["source_deleted"] == 0
    daily = (await backup_api.get("/api/v1/reports/daily", headers=headers)).json()
    assert {"kind": "BACKUP_STALE", "reason": "SOURCE_MISSING", "count": 1} in daily["attention"]


async def test_retry_after_copy_back_uploads(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    headers, _ = await login(backup_api, db)
    row, content = await _missing(db, world, memory_store)
    url = f"/api/v1/backup/issues/{row.id}/resolve"
    res = await backup_api.post(
        url, headers=headers, json={"action": "UPLOAD_ANYWAY", "note": "khong hop le"}
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "BACKUP_ISSUE_ACTION_INVALID"

    # Thử lại khi tệp vẫn chưa có → vẫn SOURCE_MISSING và hiện lại (không kẹt ẩn).
    res = await backup_api.post(url, headers=headers, json={"action": "RETRY", "note": "IT dang chep lai"})
    assert res.status_code == 200
    assert res.json()["resolution"]["action"] == "RETRY"
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    await db.refresh(row)
    assert row.last_error == "SOURCE_MISSING"
    assert row.resolution_action is None
    listed = (await backup_api.get("/api/v1/backup/issues", headers=headers,
                                   params={"kind": "SOURCE_MISSING"})).json()  # fmt: skip
    assert listed["total"] == 1

    cam1 = world.clips["CAM1"]
    assert cam1.path is not None
    (world.settings.video_root / cam1.path).write_bytes(content)  # IT chép lại
    res = await backup_api.post(url, headers=headers, json={"action": "IGNORE", "note": "bo qua di"})
    assert res.status_code == 409
    assert res.json()["error"]["message"] == "Tệp đã có lại tại kho — bấm Thử lại ngay."
    res = await backup_api.post(url, headers=headers, json={"action": "RETRY", "note": "Đã chép lại tệp"})
    assert res.status_code == 200
    out = await jobs.upload_evidence(db, world.settings, store=memory_store)
    assert out["UPLOADED"] == 1
    await db.refresh(row)
    assert row.status == "UPLOADED"
    assert row.cloud_present is True


async def test_ignore_source_missing_is_final(
    backup_api: AsyncClient, db: AsyncSession, world: World, memory_store: MemoryStore
) -> None:
    use_settings(backup_api, world.settings)
    headers, _ = await login(backup_api, db)
    row, _ = await _missing(db, world, memory_store)
    res = await backup_api.post(f"/api/v1/backup/issues/{row.id}/resolve", headers=headers,
                                json={"action": "IGNORE", "note": "Ổ hỏng, không còn tệp"})  # fmt: skip
    assert res.status_code == 200
    assert res.json()["status"] == "IGNORED"
    clock.advance(timedelta(hours=2))
    await jobs.upload_evidence(db, world.settings, store=memory_store)
    await db.refresh(row)
    assert row.status == "IGNORED"
    headers, _ = await login(backup_api, db)
    body = (await backup_api.get("/api/v1/backup", headers=headers)).json()
    assert body["evidence"]["source_missing"] == 0
    assert body["evidence"]["ignored"] == 1
