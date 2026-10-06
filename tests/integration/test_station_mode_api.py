"""Loại bàn / chế độ / người kiểm (T-106; FR-01.01, FR-01.07, FR-04.10, BR-28).

API-60 `kind` (TC-01.31, TC-P2.01), API-100 (TC-04.33, TC-04.34, TC-01.32 bước 1, TC-P2.02),
API-101 (TC-04.02 phía API), API-03 / API-91 xóa tên người kiểm (TC-04.35 phía API), API-04 `station`.
"""

from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration


async def _login(api: AsyncClient, username: str, client: str) -> dict[str, str]:
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture
async def admin(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    await make_user(db, "tst_admin_mode", "ADMIN")
    return await _login(api, "tst_admin_mode", "DASHBOARD")


async def _station(
    api: AsyncClient, db: AsyncSession, kind: str = "BOTH", work_mode: str = "PACK"
) -> tuple[Station, dict[str, str]]:
    _, station = await make_station_account(db, "tst_station_mode", "TST Station Mode")
    station.kind, station.work_mode = kind, work_mode
    await db.flush()
    return station, await _login(api, "tst_station_mode", "STATION")


async def _open_session(db: AsyncSession, station: Station) -> PackSession:
    package = Package(tracking_number="SPXMODE000001", warehouse_status="PACKING")
    db.add(package)
    await db.flush()
    pack = PackSession(package_id=package.id, station_id=station.id, status="OPEN", open_code="SPXMODE000001")
    db.add(pack)
    await db.flush()
    return pack


# ---------------------------------------------------------------- API-60 kind


async def test_create_station_kind_sets_work_mode(api: AsyncClient, admin: dict[str, str]) -> None:
    ret = await api.post("/api/v1/stations", json={"name": "TST Hoàn 02", "kind": "RETURN"}, headers=admin)
    default = await api.post("/api/v1/stations", json={"name": "TST Gói 03"}, headers=admin)

    assert ret.status_code == 201
    assert (ret.json()["kind"], ret.json()["work_mode"], ret.json()["operator_name"]) == (
        "RETURN",
        "RETURN",
        None,
    )
    assert (default.json()["kind"], default.json()["work_mode"]) == ("PACK", "PACK")


async def test_patch_kind_rules(api: AsyncClient, db: AsyncSession, admin: dict[str, str]) -> None:
    """Sang BOTH giữ chế độ đang chạy; sang RETURN → work_mode = RETURN (02 API-60)."""
    station, _ = await _station(api, db, kind="PACK", work_mode="PACK")

    both = await api.patch(f"/api/v1/stations/{station.id}", json={"kind": "BOTH"}, headers=admin)
    ret = await api.patch(f"/api/v1/stations/{station.id}", json={"kind": "RETURN"}, headers=admin)

    assert (both.json()["kind"], both.json()["work_mode"]) == ("BOTH", "PACK")
    assert (ret.json()["kind"], ret.json()["work_mode"]) == ("RETURN", "RETURN")


async def test_patch_kind_busy(api: AsyncClient, db: AsyncSession, admin: dict[str, str]) -> None:
    """TC-01.31: đổi loại khi có phiên mở → 409 STATION_BUSY; tên vẫn đổi được."""
    station, _ = await _station(api, db, kind="PACK")
    await _open_session(db, station)

    res = await api.patch(f"/api/v1/stations/{station.id}", json={"kind": "RETURN"}, headers=admin)
    rename = await api.patch(f"/api/v1/stations/{station.id}", json={"name": "TST Mode mới"}, headers=admin)

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "STATION_BUSY"
    assert rename.status_code == 200
    assert rename.json()["kind"] == "PACK"


async def test_patch_kind_busy_with_pending_approval(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str]
) -> None:
    station, _ = await _station(api, db, kind="BOTH")
    db.add(ApprovalRequest(type="REPACK", station_id=station.id, tracking_number="SPXMODE000009", context={}))
    await db.flush()

    res = await api.patch(f"/api/v1/stations/{station.id}", json={"kind": "PACK"}, headers=admin)

    assert res.status_code == 409


@pytest.mark.parametrize("role", ["SUPERVISOR", "CSKH"])
async def test_kind_admin_only(api: AsyncClient, db: AsyncSession, role: str) -> None:
    """TC-P2.01."""
    station, _ = await _station(api, db)
    await make_user(db, f"tst_{role.lower()}_mode", role)
    headers = await _login(api, f"tst_{role.lower()}_mode", "DASHBOARD")

    res = await api.patch(f"/api/v1/stations/{station.id}", json={"kind": "RETURN"}, headers=headers)

    assert res.status_code == 403


# ---------------------------------------------------------------- API-100


async def test_work_mode_switch(api: AsyncClient, db: AsyncSession) -> None:
    _, headers = await _station(api, db, kind="BOTH")

    res = await api.put("/api/v1/station/work-mode", json={"work_mode": "RETURN"}, headers=headers)
    again = await api.put("/api/v1/station/work-mode", json={"work_mode": "RETURN"}, headers=headers)

    assert res.status_code == 200, res.text
    st: dict[str, Any] = res.json()["state"]["station"]
    assert (st["kind"], st["work_mode"], st["operator_name"]) == ("BOTH", "RETURN", None)
    assert res.json()["state"]["state"] == "READY"
    assert again.status_code == 200
    logs = (await db.scalars(select(AuditLog).where(AuditLog.action == "STATION_WORK_MODE"))).all()
    assert [log.data for log in logs] == [{"old": "PACK", "new": "RETURN"}]  # lần 2 không đổi → không audit
    state = await api.get("/api/v1/station/state", headers=headers)
    assert state.json()["station"]["work_mode"] == "RETURN"


@pytest.mark.parametrize(("kind", "target"), [("RETURN", "PACK"), ("PACK", "RETURN")])
async def test_work_mode_not_allowed(api: AsyncClient, db: AsyncSession, kind: str, target: str) -> None:
    """TC-04.34, TC-01.32 bước 1: station không phải "Cả hai" → 409 MODE_NOT_ALLOWED."""
    _, headers = await _station(api, db, kind=kind, work_mode=kind)

    res = await api.put("/api/v1/station/work-mode", json={"work_mode": target}, headers=headers)

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "MODE_NOT_ALLOWED"


async def test_work_mode_session_active(api: AsyncClient, db: AsyncSession) -> None:
    """TC-04.33: có phiên hoạt động → 409 SESSION_ACTIVE, chế độ giữ nguyên."""
    station, headers = await _station(api, db, kind="BOTH")
    await _open_session(db, station)

    res = await api.put("/api/v1/station/work-mode", json={"work_mode": "RETURN"}, headers=headers)

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "SESSION_ACTIVE"
    await db.refresh(station)
    assert station.work_mode == "PACK"


async def test_work_mode_station_only(api: AsyncClient, admin: dict[str, str]) -> None:
    """TC-P2.02: tài khoản dashboard → 403."""
    res = await api.put("/api/v1/station/work-mode", json={"work_mode": "RETURN"}, headers=admin)
    op = await api.put("/api/v1/station/operator", json={"name": "Lan"}, headers=admin)

    assert (res.status_code, op.status_code) == (403, 403)


# ---------------------------------------------------------------- API-101


async def test_operator_set_and_validate(api: AsyncClient, db: AsyncSession) -> None:
    _, headers = await _station(api, db, kind="RETURN", work_mode="RETURN")

    ok = await api.put("/api/v1/station/operator", json={"name": "  Lan   QA "}, headers=headers)
    short = await api.put("/api/v1/station/operator", json={"name": " L "}, headers=headers)
    long = await api.put("/api/v1/station/operator", json={"name": "x" * 41}, headers=headers)
    exact = await api.put("/api/v1/station/operator", json={"name": "y" * 40}, headers=headers)

    assert ok.status_code == 200, ok.text
    assert ok.json()["state"]["station"]["operator_name"] == "Lan QA"
    assert short.status_code == 422
    assert "name" in short.json()["error"]["details"]["fields"]
    assert long.status_code == 422
    assert exact.status_code == 200
    logs = (await db.scalars(select(AuditLog).where(AuditLog.action == "STATION_OPERATOR"))).all()
    assert [log.data for log in logs] == [{"old": None, "new": "Lan QA"}, {"old": "Lan QA", "new": "y" * 40}]


async def test_operator_session_active(api: AsyncClient, db: AsyncSession) -> None:
    station, headers = await _station(api, db, kind="RETURN", work_mode="RETURN")
    await _open_session(db, station)

    res = await api.put("/api/v1/station/operator", json={"name": "Lan"}, headers=headers)

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "SESSION_ACTIVE"


# ---------------------------------------------------------------- BR-28: API-03 / API-91 xóa tên


async def test_logout_clears_operator(api: AsyncClient, db: AsyncSession) -> None:
    """TC-04.35 (phía API): station đăng xuất → tên người kiểm bị xóa."""
    station, headers = await _station(api, db, kind="RETURN", work_mode="RETURN")
    await api.put("/api/v1/station/operator", json={"name": "Lan QA"}, headers=headers)

    res = await api.post("/api/v1/auth/logout", headers=headers)

    assert res.status_code == 204
    await db.refresh(station)
    assert station.operator_name is None


async def test_dashboard_logout_does_not_touch_station(api: AsyncClient, db: AsyncSession) -> None:
    station, headers = await _station(api, db, kind="RETURN", work_mode="RETURN")
    await api.put("/api/v1/station/operator", json={"name": "Lan QA"}, headers=headers)
    await make_user(db, "tst_admin_logout", "ADMIN")
    admin = await _login(api, "tst_admin_logout", "DASHBOARD")

    await api.post("/api/v1/auth/logout", headers=admin)

    await db.refresh(station)
    assert station.operator_name == "Lan QA"


async def test_revoke_sessions_clears_operator(
    api: AsyncClient, db: AsyncSession, admin: dict[str, str]
) -> None:
    station, headers = await _station(api, db, kind="RETURN", work_mode="RETURN")
    await api.put("/api/v1/station/operator", json={"name": "Lan QA"}, headers=headers)

    res = await api.post(f"/api/v1/users/{station.account_user_id}/revoke-sessions", headers=admin)

    assert res.status_code == 204
    await db.refresh(station)
    assert station.operator_name is None


async def test_me_station_kind(api: AsyncClient, db: AsyncSession) -> None:
    """API-04: `station.kind`, `station.work_mode`."""
    _, headers = await _station(api, db, kind="BOTH", work_mode="RETURN")

    me = (await api.get("/api/v1/me", headers=headers)).json()

    assert (me["station"]["kind"], me["station"]["work_mode"]) == ("BOTH", "RETURN")
