"""API-01..04, API-90..92 — 02 §6.2; TC-10.xx trong 04-test-cases."""

from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.modules.users.models import RefreshToken, User

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration


async def _login(api: AsyncClient, username: str, client: str = "DASHBOARD", password: str = PASSWORD):  # type: ignore[no-untyped-def]
    return await api.post(
        "/api/v1/auth/login", json={"username": username, "password": password, "client": client}
    )


async def _token(api: AsyncClient, username: str, client: str = "DASHBOARD") -> str:
    res = await _login(api, username, client)
    assert res.status_code == 200, res.text
    return str(res.json()["access_token"])


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------- API-01


async def test_station_login_returns_station_and_sets_station_cookie(
    api: AsyncClient, db: AsyncSession
) -> None:
    _, station = await make_station_account(db)

    res = await _login(api, "TST_station01", "STATION")

    assert res.status_code == 200
    body = res.json()
    assert body["user"]["role"] == "STATION"
    # Phase 2 (02 API-04): thêm `kind`, `work_mode` — chỉ thêm trường.
    assert body["user"]["station"] == {
        "id": str(station.id),
        "name": "TST Station 01",
        "kind": "PACK",
        "work_mode": "PACK",
    }
    assert body["expires_in"] == 900
    cookie = res.headers["set-cookie"]
    assert cookie.startswith("rt_station=")
    assert "HttpOnly" in cookie
    assert "Path=/api/v1/auth" in cookie
    assert "SameSite=strict" in cookie


async def test_login_audits_and_resets_fail_counter(api: AsyncClient, db: AsyncSession) -> None:
    user = await make_user(db, "tst_admin", failed_logins=3)

    assert (await _login(api, "tst_admin")).status_code == 200

    await db.refresh(user)
    assert user.failed_logins == 0
    logs = (await db.scalars(select(AuditLog).where(AuditLog.user_id == user.id))).all()
    assert [log.action for log in logs] == ["LOGIN"]


async def test_wrong_password(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")

    res = await _login(api, "tst_admin", password="sai-mat-khau")

    assert res.status_code == 401
    assert res.json()["error"]["code"] == "INVALID_CREDENTIALS"


async def test_unknown_user_same_error_as_wrong_password(api: AsyncClient) -> None:
    res = await _login(api, "khong_ton_tai")

    assert res.status_code == 401
    assert res.json()["error"]["code"] == "INVALID_CREDENTIALS"


@pytest.mark.parametrize(("username", "role", "client"), [("tst_cskh", "CSKH", "STATION")])
async def test_wrong_client(
    api: AsyncClient, db: AsyncSession, username: str, role: str, client: str
) -> None:
    await make_user(db, username, role)

    res = await _login(api, username, client)

    assert res.status_code == 403
    assert res.json()["error"]["code"] == "WRONG_CLIENT"
    assert "dashboard" in res.json()["error"]["message"]


async def test_station_account_on_dashboard_is_wrong_client(api: AsyncClient, db: AsyncSession) -> None:
    await make_station_account(db)

    res = await _login(api, "tst_station01", "DASHBOARD")

    assert res.json()["error"]["code"] == "WRONG_CLIENT"


async def test_disabled_account(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_off", "CSKH", is_active=False)

    res = await _login(api, "tst_off")

    assert res.status_code == 403
    assert res.json()["error"]["code"] == "ACCOUNT_DISABLED"


async def test_inactive_station(api: AsyncClient, db: AsyncSession) -> None:
    await make_station_account(db, active=False)

    res = await _login(api, "tst_station01", "STATION")

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "STATION_INACTIVE"


async def test_lock_after_ten_failures_then_unlock_after_15_minutes(
    api: AsyncClient, db: AsyncSession
) -> None:
    """TC-10.04."""
    await make_user(db, "tst_admin")

    codes = [(await _login(api, "tst_admin", password="sai")).status_code for _ in range(10)]
    locked = await _login(api, "tst_admin")

    assert codes == [401] * 10
    assert locked.status_code == 423
    assert locked.json()["error"]["code"] == "ACCOUNT_LOCKED"
    assert "until" in locked.json()["error"]["details"]

    clock.advance(timedelta(minutes=15, seconds=1))
    assert (await _login(api, "tst_admin")).status_code == 200


async def test_ip_rate_limit(api: AsyncClient, db: AsyncSession, test_settings: object) -> None:
    for i in range(30):
        await _login(api, f"khong_co_{i}")

    res = await _login(api, "khong_co_x")

    assert res.status_code == 429
    assert res.json()["error"]["code"] == "RATE_LIMITED"
    assert res.headers["retry-after"] == "300"


# ---------------------------------------------------------------- API-02, 03


async def test_refresh_rotates_token(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    await _login(api, "tst_admin")
    first = api.cookies.get("rt_dashboard")

    res = await api.post("/api/v1/auth/refresh", json={"client": "DASHBOARD"})

    assert res.status_code == 200
    assert res.json()["access_token"]
    assert api.cookies.get("rt_dashboard") != first
    rows = (await db.scalars(select(RefreshToken).order_by(RefreshToken.created_at))).all()
    assert rows[0].revoked_at is not None
    assert rows[0].replaced_by == rows[1].id


async def test_refresh_reuse_revokes_whole_chain(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    await _login(api, "tst_admin")
    stolen = api.cookies.get("rt_dashboard")
    await api.post("/api/v1/auth/refresh", json={"client": "DASHBOARD"})

    # Kẻ gian dùng lại cookie cũ đã bị xoay vòng.
    api.cookies.set("rt_dashboard", stolen or "", domain="testserver.local", path="/api/v1/auth")
    reuse = await api.post("/api/v1/auth/refresh", json={"client": "DASHBOARD"})

    assert reuse.status_code == 401
    active = (await db.scalars(select(RefreshToken).where(RefreshToken.revoked_at.is_(None)))).all()
    assert active == []


async def test_refresh_cookie_is_per_client(api: AsyncClient, db: AsyncSession) -> None:
    """Station và dashboard cùng trình duyệt không ghi đè nhau (review N3)."""
    await make_user(db, "tst_admin")
    await make_station_account(db)
    await _login(api, "tst_station01", "STATION")
    await _login(api, "tst_admin", "DASHBOARD")

    station = await api.post("/api/v1/auth/refresh", json={"client": "STATION"})
    dashboard = await api.post("/api/v1/auth/refresh", json={"client": "DASHBOARD"})

    assert station.status_code == dashboard.status_code == 200
    me_station = await api.get("/api/v1/me", headers=_bearer(station.json()["access_token"]))
    assert me_station.json()["role"] == "STATION"


async def test_refresh_expired(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    await _login(api, "tst_admin")

    clock.advance(timedelta(days=7, seconds=1))
    res = await api.post("/api/v1/auth/refresh", json={"client": "DASHBOARD"})

    assert res.status_code == 401


async def test_refresh_without_cookie(api: AsyncClient) -> None:
    res = await api.post("/api/v1/auth/refresh", json={"client": "STATION"})

    assert res.status_code == 401
    assert res.json()["error"]["code"] == "UNAUTHENTICATED"


async def test_logout_revokes_refresh(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    token = await _token(api, "tst_admin")

    res = await api.post("/api/v1/auth/logout", headers=_bearer(token))

    assert res.status_code == 204
    assert (await api.post("/api/v1/auth/refresh", json={"client": "DASHBOARD"})).status_code == 401


# ---------------------------------------------------------------- API-04


async def test_me_returns_permissions(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_cskh", "CSKH")
    token = await _token(api, "tst_cskh")

    res = await api.get("/api/v1/me", headers=_bearer(token))

    assert res.status_code == 200
    assert res.json()["role"] == "CSKH"
    assert "clips.export" in res.json()["permissions"]
    assert "users.manage" not in res.json()["permissions"]


async def test_access_token_expires(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_cskh", "CSKH")
    token = await _token(api, "tst_cskh")

    clock.advance(timedelta(minutes=15, seconds=1))

    assert (await api.get("/api/v1/me", headers=_bearer(token))).status_code == 401


# ---------------------------------------------------------------- API-90..92


@pytest.mark.parametrize("role", ["SUPERVISOR", "CSKH"])
async def test_users_api_is_admin_only(api: AsyncClient, db: AsyncSession, role: str) -> None:
    """TC-P.09."""
    await make_user(db, "tst_other", role)
    token = await _token(api, "tst_other")

    res = await api.get("/api/v1/users", headers=_bearer(token))

    assert res.status_code == 403
    assert res.json()["error"]["code"] == "FORBIDDEN"


async def test_station_cannot_list_users(api: AsyncClient, db: AsyncSession) -> None:
    await make_station_account(db)
    token = await _token(api, "tst_station01", "STATION")

    assert (await api.get("/api/v1/users", headers=_bearer(token))).status_code == 403


async def test_admin_creates_and_lists_users(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    token = await _token(api, "tst_admin")

    created = await api.post(
        "/api/v1/users",
        headers=_bearer(token),
        json={
            "username": "station01",
            "display_name": "Station 01",
            "role": "STATION",
            "password": "12345678",
        },
    )
    listed = await api.get("/api/v1/users", headers=_bearer(token), params={"role": "STATION"})

    assert created.status_code == 201
    assert created.json()["station"] is None
    assert listed.json()["total"] == 1
    assert listed.json()["items"][0]["username"] == "station01"


async def test_create_user_validation_and_duplicate(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    token = await _token(api, "tst_admin")

    bad = await api.post(
        "/api/v1/users",
        headers=_bearer(token),
        json={"username": "Có Dấu", "display_name": "x", "role": "CSKH", "password": "123"},
    )
    dup = await api.post(
        "/api/v1/users",
        headers=_bearer(token),
        json={"username": "tst_admin", "display_name": "x", "role": "CSKH", "password": "12345678"},
    )

    assert bad.status_code == 422
    assert set(bad.json()["error"]["details"]["fields"]) == {"username", "password"}
    assert dup.status_code == 409
    assert dup.json()["error"]["code"] == "USERNAME_TAKEN"


async def test_cannot_disable_last_admin(api: AsyncClient, db: AsyncSession) -> None:
    """TC-10.07."""
    admin = await make_user(db, "tst_admin")
    token = await _token(api, "tst_admin")

    res = await api.patch(f"/api/v1/users/{admin.id}", headers=_bearer(token), json={"is_active": False})
    demote = await api.patch(f"/api/v1/users/{admin.id}", headers=_bearer(token), json={"role": "CSKH"})

    assert res.status_code == demote.status_code == 409
    assert res.json()["error"]["code"] == "LAST_ADMIN"


async def test_disable_user_revokes_sessions_and_audits(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    cskh = await make_user(db, "tst_cskh", "CSKH")
    admin_token = await _token(api, "tst_admin")
    await _login(api, "tst_cskh")

    res = await api.patch(f"/api/v1/users/{cskh.id}", headers=_bearer(admin_token), json={"is_active": False})

    assert res.status_code == 200
    assert res.json()["is_active"] is False
    active = await db.scalars(
        select(RefreshToken).where(RefreshToken.user_id == cskh.id, RefreshToken.revoked_at.is_(None))
    )
    assert active.all() == []
    log = await db.scalar(select(AuditLog).where(AuditLog.action == "USER_UPDATE"))
    assert log is not None
    assert log.data == {"is_active": False}


async def test_password_change_masked_in_audit(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_admin")
    cskh = await make_user(db, "tst_cskh", "CSKH")
    token = await _token(api, "tst_admin")

    await api.patch(f"/api/v1/users/{cskh.id}", headers=_bearer(token), json={"password": "matkhaumoi1"})

    log = await db.scalar(select(AuditLog).where(AuditLog.action == "USER_UPDATE"))
    assert log is not None
    assert log.data == {"password": "***"}
    assert (await _login(api, "tst_cskh", password="matkhaumoi1")).status_code == 200


async def test_revoke_sessions(api: AsyncClient, db: AsyncSession) -> None:
    """TC-10.06 (phần BE: refresh bị thu hồi)."""
    await make_user(db, "tst_admin")
    station_user, _ = await make_station_account(db)
    admin_token = await _token(api, "tst_admin")
    await _login(api, "tst_station01", "STATION")

    res = await api.post(f"/api/v1/users/{station_user.id}/revoke-sessions", headers=_bearer(admin_token))

    assert res.status_code == 204
    assert (await api.post("/api/v1/auth/refresh", json={"client": "STATION"})).status_code == 401


async def test_audit_logs_filter(api: AsyncClient, db: AsyncSession) -> None:
    admin = await make_user(db, "tst_admin", display_name="Quản trị")
    await make_user(db, "tst_cskh", "CSKH")
    token = await _token(api, "tst_admin")
    await _login(api, "tst_cskh")

    res = await api.get(
        "/api/v1/audit-logs", headers=_bearer(token), params={"action": "LOGIN", "user_id": str(admin.id)}
    )

    assert res.status_code == 200
    assert res.json()["total"] == 1
    item = res.json()["items"][0]
    assert item["user"] == {"id": str(admin.id), "display_name": "Quản trị"}


async def test_users_unique_username_in_db(db: AsyncSession) -> None:
    await make_user(db, "tst_one")

    assert await db.scalar(select(User.username).where(User.username == "tst_one")) == "tst_one"
