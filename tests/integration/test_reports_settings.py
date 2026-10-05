"""API-32 báo cáo ngày, API-80 cài đặt, API-81 sức khỏe, J-11 dọn dẹp (T-18).

TC-09.01 (API), TC-02.09 / 02.16 / 02.17 / 02.18 (API), TC-P.03, TC-P.08.
"""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.imports import service as imports
from aicam.modules.imports.models import CsvImport
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession, ScanDedup
from aicam.modules.stations.mediamtx import MediaMTXError, PathStat
from aicam.modules.stations.models import Camera
from aicam.modules.stations.router import get_mediamtx

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)  # 08:00 giờ VN


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> tuple[dict[str, str], uuid.UUID]:
    user = await make_user(db, f"tst_{role.lower()}", role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


async def _session(
    db: AsyncSession, station_id: uuid.UUID, n: int, status: str, ended: datetime | None,
    flags: list[str] | None = None, package_status: str = "HANDED_OVER",
) -> PackSession:  # fmt: skip
    package = Package(tracking_number=f"SPXTST{n:07d}", warehouse_status=package_status)
    db.add(package)
    await db.flush()
    started = (ended or clock.now()) - timedelta(minutes=2)
    pack = PackSession(package_id=package.id, station_id=station_id, status=status, started_at=started,
                       ended_at=ended, open_code=package.tracking_number, flags=flags or [],
                       package_status_before="NEW")  # fmt: skip
    db.add(pack)
    await db.flush()
    return pack


# ---------------------------------------------------------------- API-32


async def test_daily_counts(api: AsyncClient, db: AsyncSession, redis_client: object) -> None:
    """TC-09.01 / AC-18: 12 đóng, 3 lệch, 1 bỏ dở, 2 hủy, 4 PACKED, 1 hủy sau đóng."""
    clock.freeze(NOW)
    headers, _ = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    db.add(Camera(station_id=station.id, role="CAM1", rtsp_url="rtsp://x/1", mediamtx_path="cam-x1",
                  status="OFFLINE", clock_offset_ms=1400))  # fmt: skip
    ended = NOW - timedelta(minutes=30)
    for i in range(12):
        status = "PACKED" if i < 4 else "CANCELLED_AFTER_PACK" if i == 4 else "HANDED_OVER"
        flags = ["HAD_MISMATCH"] if i < 3 else ["REPACK"] if i == 3 else []
        await _session(db, station.id, i + 1, "COMPLETED", ended, flags, package_status=status)
    await _session(db, station.id, 20, "ABANDONED", ended, package_status="NEW")
    await _session(db, station.id, 21, "CANCELLED", ended, package_status="NEW")
    await _session(db, station.id, 22, "CANCELLED", ended, package_status="NEW")
    await _session(db, station.id, 23, "COMPLETED", NOW - timedelta(days=1), ["HAD_MISMATCH"])  # hôm qua
    await _session(db, station.id, 24, "OPEN", None, package_status="PACKING")

    res = await api.get("/api/v1/reports/daily", headers=headers, params={"date": "2026-10-05"})

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["counts"] == {"packed": 12, "had_mismatch": 3, "abandoned": 1, "cancelled": 2,
                              "packed_not_handed_over": 4, "cancelled_after_pack": 1}  # fmt: skip
    assert body["stations"][0]["state"] == "PACKING"
    assert body["stations"][0]["tracking_number"] == "SPXTST0000024"
    kinds = {a["kind"]: a for a in body["attention"]}
    assert kinds["CANCELLED_AFTER_PACK"]["count"] == 1
    assert kinds["CAMERA_OFFLINE"]["role"] == "CAM1"
    assert kinds["CLOCK_DRIFT"]["offset_ms"] == 1400
    yesterday = (
        await api.get("/api/v1/reports/daily", headers=headers, params={"date": "2026-10-04"})
    ).json()
    assert yesterday["counts"]["packed"] == 1
    assert yesterday["counts"]["had_mismatch"] == 1


async def test_daily_cache_dropped_on_report_updated(
    api: AsyncClient, db: AsyncSession, redis_client: object
) -> None:
    """TC-09.03 (lỗi QA M2): sau `report.updated`, API-32 trả số mới thay vì bản cache 5 giây."""
    from aicam.realtime import publish

    clock.freeze(NOW)
    headers, _ = await _login(api, db, "SUPERVISOR")
    _, station = await make_station_account(db)
    await _session(db, station.id, 1, "COMPLETED", NOW - timedelta(minutes=5))
    first = (await api.get("/api/v1/reports/daily", headers=headers)).json()["counts"]["packed"]
    await _session(db, station.id, 2, "COMPLETED", NOW - timedelta(minutes=1))

    stale = (await api.get("/api/v1/reports/daily", headers=headers)).json()["counts"]["packed"]
    await publish.to_dashboard("report.updated", {"date": "2026-10-05"})
    fresh = (await api.get("/api/v1/reports/daily", headers=headers)).json()["counts"]["packed"]

    assert (first, stale, fresh) == (1, 1, 2)


async def test_daily_rejects_future_and_station(
    api: AsyncClient, db: AsyncSession, redis_client: object
) -> None:
    clock.freeze(NOW)
    headers, _ = await _login(api, db, "SUPERVISOR")
    future = await api.get("/api/v1/reports/daily", headers=headers, params={"date": "2026-10-06"})
    assert (future.status_code, future.json()["error"]["code"]) == (422, "VALIDATION_ERROR")
    user, _ = await make_station_account(db)
    login = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    station_h = {"Authorization": f"Bearer {login.json()['access_token']}"}
    assert (await api.get("/api/v1/reports/daily", headers=station_h)).status_code == 403


# ---------------------------------------------------------------- API-80


async def test_settings_get_put_and_audit(api: AsyncClient, db: AsyncSession) -> None:
    admin, admin_id = await _login(api, db, "ADMIN")
    sup, _ = await _login(api, db, "SUPERVISOR")
    body = {"retention_raw_days": 30, "retention_clip_days": 180, "session_warn_minutes": 10,
            "session_abandon_minutes": 40}  # fmt: skip

    assert (await api.get("/api/v1/settings", headers=sup)).json()["retention_clip_days"] == 90
    assert (await api.put("/api/v1/settings", headers=sup, json=body)).status_code == 403
    res = await api.put("/api/v1/settings", headers=admin, json=body)

    assert res.status_code == 200
    assert {k: res.json()[k] for k in body} == body
    entry = await db.scalar(select(AuditLog).where(AuditLog.action == "SETTINGS_UPDATE"))
    assert entry is not None
    assert entry.user_id == admin_id
    assert entry.data is not None
    assert entry.data["before"]["retention_clip_days"] == 90


@pytest.mark.parametrize(
    ("change", "field"),
    [
        ({"retention_raw_days": 30, "retention_clip_days": 20}, "retention_clip_days"),  # TC-02.09
        ({"session_warn_minutes": 30, "session_abandon_minutes": 30}, "session_abandon_minutes"),  # TC-02.16
        ({"retention_raw_days": 0}, "retention_raw_days"),  # TC-02.17
        ({"retention_clip_days": 366}, "retention_clip_days"),  # TC-02.18
    ],
)
async def test_settings_validation(
    api: AsyncClient, db: AsyncSession, change: dict[str, int], field: str
) -> None:
    admin, _ = await _login(api, db, "ADMIN")
    body = {"retention_raw_days": 30, "retention_clip_days": 90, "session_warn_minutes": 15,
            "session_abandon_minutes": 30, **change}  # fmt: skip
    res = await api.put("/api/v1/settings", headers=admin, json=body)
    assert (res.status_code, res.json()["error"]["code"]) == (422, "VALIDATION_ERROR")
    assert field in res.json()["error"]["details"]["fields"]
    assert (await api.get("/api/v1/settings", headers=admin)).json()["retention_clip_days"] == 90


# ---------------------------------------------------------------- API-81


class _MediaMTX:
    def __init__(self, fail: bool) -> None:
        self.fail = fail

    async def list_paths(self) -> dict[str, PathStat]:
        if self.fail:
            raise MediaMTXError("down")
        return {}


@pytest.mark.parametrize("fail", [False, True])
async def test_health(api: AsyncClient, db: AsyncSession, redis_client: object, fail: bool) -> None:
    """API-81: từng thành phần OK / ERROR, không 500 khi MediaMTX chết; CSKH ⛔ (TC-P.08)."""
    api._transport.app.dependency_overrides[get_mediamtx] = lambda: _MediaMTX(fail)  # type: ignore[attr-defined]
    admin, _ = await _login(api, db, "ADMIN")
    cskh, _ = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    db.add(Camera(station_id=station.id, role="CAM2", rtsp_url="rtsp://x/2", mediamtx_path="cam-x2"))
    await db.flush()

    res = await api.get("/api/v1/system/health", headers=admin)

    assert res.status_code == 200
    body = res.json()
    assert (body["db"], body["redis"], body["mediamtx"]) == ("OK", "OK", "ERROR" if fail else "OK")
    assert body["cameras"][0]["station_name"] == "TST Station 01"
    assert (await api.get("/api/v1/system/health", headers=cskh)).status_code == 403


# ---------------------------------------------------------------- J-11


async def test_housekeeping_pieces(db: AsyncSession, tmp_path: Path, test_settings: Settings) -> None:
    clock.freeze(datetime.now(UTC))
    user, station = await make_station_account(db)
    db.add(ScanDedup(client_scan_id=uuid.uuid4(), station_id=station.id, response={},
                     created_at=clock.now() - timedelta(minutes=11)))  # fmt: skip
    fresh = ScanDedup(client_scan_id=uuid.uuid4(), station_id=station.id, response={}, created_at=clock.now())
    old_file = tmp_path / "old.csv"
    old_file.write_text("x")
    db.add_all([
        fresh,
        CsvImport(file_name="a.csv", created_by=user.id, expires_at=clock.now() - timedelta(minutes=1)),
        CsvImport(file_name="b.csv", status="COMMITTED", created_by=user.id, file_path="old.csv",
                  created_at=clock.now() - timedelta(days=91), expires_at=clock.now()),
    ])  # fmt: skip
    # Phiên kết thúc 6 phút trước chưa có clip / có clip PENDING → cần J-01; vừa đóng / đã READY → không.
    lost = await _session(db, station.id, 1, "COMPLETED", clock.now() - timedelta(minutes=6))
    stuck = await _session(db, station.id, 2, "CANCELLED", clock.now() - timedelta(minutes=6))
    done = await _session(db, station.id, 3, "COMPLETED", clock.now() - timedelta(minutes=6))
    await _session(db, station.id, 4, "COMPLETED", clock.now() - timedelta(minutes=2))
    # G3-F1: phiên đủ khi có dòng clip của mọi vai (CAM1 + CAM2) và không còn PENDING.
    for pack, role, status in ((stuck, "CAM1", "PENDING"), (done, "CAM1", "READY"), (done, "CAM2", "READY")):
        db.add(
            Clip(
                session_id=pack.id,
                camera_role=role,
                status=status,
                start_at=clock.now(),
                end_at=clock.now(),
            )
        )
    await db.flush()

    assert await sessions.purge_scan_dedup(db, timedelta(minutes=10)) == 1
    assert await imports.expire_previews(db) == 1
    assert await imports.purge_old_files(db, tmp_path) == 1
    assert not old_file.exists()
    missing = await media.sessions_missing_clips(db, timedelta(minutes=5), timedelta(days=1))
    assert set(missing) == {lost.id, stuck.id}
    remaining: Any = (await db.scalars(select(ScanDedup.client_scan_id))).all()
    assert list(remaining) == [fresh.client_scan_id]


async def test_health_db_ping_timeout_does_not_break_session(db: AsyncSession) -> None:
    """G3-N11: ping DB quá giờ bị hủy trên connection riêng — session chung của API-81 vẫn truy vấn được."""
    from sqlalchemy import text

    from aicam.modules.settings import service as settings_service

    async def slow_ping(session: AsyncSession) -> None:
        await settings_service._ping_db(session)
        await asyncio.sleep(1)

    assert await settings_service._check(slow_ping(db), limit_s=0.05) == "ERROR"
    assert await db.scalar(text("SELECT 1")) == 1
    assert await settings_service._check(settings_service._ping_db(db)) == "OK"
