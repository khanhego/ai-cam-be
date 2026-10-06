"""API-80 mở rộng + API-82 (T-114; FR-02.10, BR-25, L2) — TC-02.37, 02.38 (API), 02.39, TC-P2.12.

Ngưỡng mới tùy chọn (thiếu = giữ cũ), sàn `RETENTION_CLIP_MIN_DAYS`, xác nhận khi giảm (`details.impact`),
audit `RETENTION_REDUCED`; API-82 đếm theo đúng điều kiện J-02 (trừ `held` / phiên được bảo vệ).
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.modules.media.models import Clip, VideoSegment
from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Camera

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 20, 3, 0, tzinfo=UTC)
BASE = {
    "retention_raw_days": 30,
    "retention_clip_days": 90,
    "session_warn_minutes": 15,
    "session_abandon_minutes": 30,
}


@pytest.fixture(autouse=True)
def _clock() -> None:
    clock.freeze(NOW)


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> dict[str, str]:
    user = await make_user(db, f"tst_set_{role.lower()}", role, display_name=f"QA {role}")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _put(api: AsyncClient, headers: dict[str, str], **change: Any) -> Any:
    return await api.put("/api/v1/settings", headers=headers, json={**BASE, **change})


async def _fixture_media(db: AsyncSession) -> None:
    """3 clip quá 80 ngày (1 xóa được, 1 `held`, 1 mới 10 ngày) + video thô 2 giờ cũ 25 ngày, 1 giờ mới."""
    _, station = await make_station_account(db, "tst_set_station", "TST Set Station")
    cam = Camera(station_id=station.id, role="CAM1", rtsp_url="rtsp://x", mediamtx_path="cam-set")
    db.add(cam)
    await db.flush()
    package = Package(tracking_number="SPXSET0000001")
    db.add(package)
    await db.flush()
    for ended_ago, held, size in ((80, False, 1000), (80, True, 2000), (10, False, 4000)):
        ended = NOW - timedelta(days=ended_ago)
        pack = PackSession(
            id=uuid.uuid4(), type="PACK", package_id=package.id, station_id=station.id, status="COMPLETED",
            started_at=ended - timedelta(minutes=1), ended_at=ended, flags=[], open_code="SPXSET0000001",
        )  # fmt: skip
        db.add(pack)
        await db.flush()
        db.add(
            Clip(
                session_id=pack.id,
                camera_role="CAM1",
                status="READY",
                start_at=pack.started_at,
                end_at=ended,
                size_bytes=size,
                held=held,
                path=f"clips/{pack.id}.mp4",
                flags=[],
            )
        )
    for hours_ago, start in ((25 * 24, 0), (25 * 24, 1), (2, 0)):
        at = NOW - timedelta(hours=hours_ago) + timedelta(hours=start)
        seg = VideoSegment(
            camera_id=cam.id, start_at=at, end_at=at + timedelta(hours=1), path=f"raw/{at}.mp4"
        )
        seg.size_bytes = 500
        db.add(seg)
    await db.flush()


async def test_get_includes_thresholds_and_minimum(api: AsyncClient, db: AsyncSession) -> None:
    """API-80 GET: 6 ngưỡng mới (mặc định 02a §9) + `retention_clip_min_days` (chỉ đọc, env)."""
    headers = await _login(api, db, "SUPERVISOR")
    body = (await api.get("/api/v1/settings", headers=headers)).json()
    assert {k: body[k] for k in ("return_warn_minutes", "return_abandon_minutes", "return_missing_days",
                                  "handover_warn_hours", "claim_deadline_days", "claim_due_soon_hours",
                                  "retention_clip_min_days")} == {
        "return_warn_minutes": 20, "return_abandon_minutes": 45, "return_missing_days": 7,
        "handover_warn_hours": 24, "claim_deadline_days": 7, "claim_due_soon_hours": 48,
        "retention_clip_min_days": 60,
    }  # fmt: skip


async def test_put_thresholds_optional_and_validated(api: AsyncClient, db: AsyncSession) -> None:
    """Thiếu ngưỡng = giữ cũ; có → lưu; ràng buộc chéo phiên hoàn; ngoài khoảng → 422 theo trường."""
    headers = await _login(api, db, "ADMIN")
    res = await _put(api, headers, return_missing_days=10, handover_warn_hours=12)
    assert res.status_code == 200, res.text
    assert (res.json()["return_missing_days"], res.json()["handover_warn_hours"]) == (10, 12)
    res = await _put(api, headers, claim_deadline_days=14)
    assert (res.json()["return_missing_days"], res.json()["claim_deadline_days"]) == (10, 14)
    res = await _put(api, headers, return_warn_minutes=50)  # bỏ dở 45 ≤ cảnh báo 50
    assert res.status_code == 422
    assert "return_abandon_minutes" in res.json()["error"]["details"]["fields"]
    res = await _put(api, headers, return_missing_days=61)
    assert res.status_code == 422
    assert res.json()["error"]["code"] == "VALIDATION_ERROR"


async def test_below_minimum(api: AsyncClient, db: AsyncSession) -> None:
    """TC-02.37 (BR-25, AC-28): 45 < sàn 60 → 422 `RETENTION_BELOW_MINIMUM` `details.min = 60`."""
    headers = await _login(api, db, "ADMIN")
    res = await _put(api, headers, retention_clip_days=45, confirm_reduction=True)
    assert res.status_code == 422
    err = res.json()["error"]
    assert (err["code"], err["details"]["min"]) == ("RETENTION_BELOW_MINIMUM", 60)
    assert err["message"] == "Số ngày giữ clip không được thấp hơn 60."


async def test_reduction_needs_confirmation(api: AsyncClient, db: AsyncSession) -> None:
    """TC-02.39 / 02.38 (API): giảm 90 → 70 không xác nhận → 409 + `details.impact` (như API-82), không lưu;
    `confirm_reduction` → lưu + audit `RETENTION_REDUCED` (cũ, mới, impact). Tăng không cần xác nhận."""
    await _fixture_media(db)
    headers = await _login(api, db, "ADMIN")
    res = await _put(api, headers, retention_clip_days=70)
    assert res.status_code == 409
    err = res.json()["error"]
    assert err["code"] == "RETENTION_REDUCTION_UNCONFIRMED"
    impact = err["details"]["impact"]
    assert (impact["clips"], impact["clip_bytes"], impact["protected_clips"]) == (1, 1000, 1)
    assert (await api.get("/api/v1/settings", headers=headers)).json()["retention_clip_days"] == 90

    res = await _put(api, headers, retention_clip_days=70, confirm_reduction=True)
    assert res.status_code == 200
    assert res.json()["retention_clip_days"] == 70
    entry = await db.scalar(select(AuditLog).where(AuditLog.action == "RETENTION_REDUCED"))
    assert entry is not None
    assert entry.data is not None
    assert entry.data["before"]["retention_clip_days"] == 90
    assert entry.data["after"]["retention_clip_days"] == 70
    assert entry.data["impact"]["clips"] == 1

    res = await _put(api, headers, retention_clip_days=120)
    assert res.status_code == 200
    res = await _put(api, headers, retention_clip_days=120, retention_raw_days=20)  # giảm video thô
    assert res.status_code == 409


async def test_api82_impact(api: AsyncClient, db: AsyncSession) -> None:
    """API-82: clip quá hạn mới (trừ `held`), giờ video thô quá hạn, `next_run_at` = 19:00 UTC kế tiếp;
    sàn áp cho số ngày giữ clip (50 → 60); chỉ ADMIN; tham số ngoài 1–365 → 422."""
    await _fixture_media(db)
    admin = await _login(api, db, "ADMIN")
    res = await api.get(
        "/api/v1/settings/retention-impact",
        params={"retention_raw_days": 20, "retention_clip_days": 50},
        headers=admin,
    )
    assert res.status_code == 200, res.text
    assert res.json() == {
        "clips": 1, "clip_bytes": 1000, "raw_hours": 2, "raw_bytes": 1000, "protected_clips": 1,
        "next_run_at": "2026-10-20T19:00:00Z",
    }  # fmt: skip
    sup = await _login(api, db, "SUPERVISOR")
    res = await api.get(
        "/api/v1/settings/retention-impact", params={"retention_raw_days": 20, "retention_clip_days": 70},
        headers=sup,
    )  # fmt: skip
    assert res.status_code == 403
    res = await api.get(
        "/api/v1/settings/retention-impact", params={"retention_raw_days": 0, "retention_clip_days": 70},
        headers=admin,
    )  # fmt: skip
    assert res.status_code == 422


async def test_impact_timeout_scoped_and_mapped(db: AsyncSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """G3 B-4: `statement_timeout` chỉ áp quanh truy vấn đếm (transaction PUT giữ giá trị cũ); quá giờ → 503
    `RETENTION_IMPACT_TIMEOUT`, transaction ngoài vẫn dùng được."""
    from sqlalchemy import text

    from aicam.core.errors import AppError
    from aicam.core.settings import Settings
    from aicam.modules.media import service as media

    settings = Settings(app_env="test")
    before = await db.scalar(text("SHOW statement_timeout"))
    await media.retention_impact(db, 30, 90, settings)
    assert await db.scalar(text("SHOW statement_timeout")) == before

    async def slow(*_: Any) -> dict[str, Any]:
        await db.execute(text("SELECT pg_sleep(0.3)"))
        return {}

    monkeypatch.setattr(media, "IMPACT_TIMEOUT_MS", 50)
    monkeypatch.setattr(media, "_retention_counts", slow)
    with pytest.raises(AppError) as exc:
        await media.retention_impact(db, 30, 90, settings)
    assert (exc.value.code, exc.value.status_code) == ("RETENTION_IMPACT_TIMEOUT", 503)
    assert await db.scalar(text("SELECT 1")) == 1
    assert await db.scalar(text("SHOW statement_timeout")) == before
