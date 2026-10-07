"""T-228 (FR-10.02, 02 §6.1 cột Quyền, §8 AuthZ): ma trận 4 vai × mọi API Phase 3 — server chặn đúng vai,
API-04 `/me` trả đúng quyền mới (04 TC-10.44).

Vai bị chặn → 403 `FORBIDDEN` (kiểm trước khi đọc body / path). Vai được phép → không 401 / 403 (chỉ kiểm
với yêu cầu không có tác dụng phụ: GET, id ngẫu nhiên → 404, body sai → 422); nghiệp vụ kiểm ở test module.
Không token → 401.
"""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

A, S, C = "ADMIN", "SUPERVISOR", "CSKH"
ROLES = (A, S, C, "STATION")
X = str(uuid.uuid4())

# (id, method, path, vai được phép, body, kiểm vai được phép?)
MATRIX: list[tuple[str, str, str, set[str], object, bool]] = [
    ("API-150", "GET", "/reports/returns", {A, S, C}, None, True),
    ("API-151", "GET", "/reports/claims", {A, S, C}, None, True),
    ("API-152", "GET", "/reports/productivity", {A, S}, None, True),
    ("API-153 returns", "GET", "/reports/returns/export", {A, S, C}, None, True),
    ("API-153 productivity", "GET", "/reports/productivity/export", {A, S}, None, True),
    ("API-154", "POST", f"/shops/{X}/disconnect", {A}, None, True),
    ("API-156", "GET", "/shops/brief", {A, S, C}, None, True),
    ("API-71 tiktok", "POST", "/shops/tiktok/auth-url", {A}, None, False),
    ("API-160", "POST", "/shares", {A, S, C}, {"bad": 1}, True),
    ("API-161", "GET", "/shares", {A, S, C}, None, True),
    ("API-162", "GET", f"/shares/{X}", {A, S, C}, None, True),
    ("API-163", "POST", f"/shares/{X}/revoke", {A, S, C}, None, True),
    ("API-164", "GET", f"/shares/options?package_id={X}", {A, S, C}, None, True),
    ("API-170", "GET", "/notify/channels", {A}, None, True),
    ("API-171", "POST", "/notify/channels", {A}, {"bad": 1}, True),
    ("API-172", "PATCH", f"/notify/channels/{X}", {A}, {"name": "Kênh"}, True),
    ("API-173", "DELETE", f"/notify/channels/{X}", {A}, None, True),
    ("API-174", "POST", f"/notify/channels/{X}/test", {A}, None, True),
    ("API-175", "GET", "/notify/messages", {A}, None, True),
    ("API-176", "PUT", "/notify/quiet-hours", {A}, {"bad": 1}, True),
    ("API-180", "GET", "/backup", {A}, None, True),
    ("API-181", "PUT", "/backup/settings", {A}, {"bad": 1}, True),
    ("API-182", "POST", "/backup/confirm-key", {A}, {"bad": 1}, True),
    ("API-183", "POST", "/backup/test", {A}, None, False),
    ("API-184", "POST", "/backup/run-db", {A}, None, False),
    ("API-185", "GET", "/backup/issues", {A}, None, True),
    ("API-187", "POST", "/backup/reupload-old-key", {A}, None, False),
    ("API-188", "POST", f"/backup/issues/{X}/resolve", {A}, {"bad": 1}, True),
    ("API-189", "POST", f"/claims/{X}/return-sessions/{X}/review", {A, S, C}, {"bad": 1}, True),
]


async def _tokens(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    for role in (A, S, C):
        await make_user(db, f"tst_authz_{role.lower()}", role)
    await make_station_account(db, "tst_authz_station")
    out: dict[str, str] = {}
    for role, username, client in (
        (A, "tst_authz_admin", "DASHBOARD"),
        (S, "tst_authz_supervisor", "DASHBOARD"),
        (C, "tst_authz_cskh", "DASHBOARD"),
        ("STATION", "tst_authz_station", "STATION"),
    ):
        res = await api.post(
            "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
        )
        assert res.status_code == 200, res.text
        out[role] = res.json()["access_token"]
    return out


async def _call(api: AsyncClient, method: str, path: str, body: object, token: str | None) -> int:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    kwargs: dict[str, object] = {"headers": headers}
    if body is not None:
        kwargs["json"] = body
    res = await api.request(method, "/api/v1" + path, **kwargs)  # type: ignore[arg-type]
    return res.status_code


async def test_phase3_role_matrix(api: AsyncClient, db: AsyncSession) -> None:
    tokens = await _tokens(api, db)
    wrong: list[str] = []
    for api_id, method, path, allowed, body, check_allowed in MATRIX:
        assert await _call(api, method, path, body, None) == 401, f"{api_id} không token phải 401"
        for role in ROLES:
            status = await _call(api, method, path, body, tokens[role])
            if role not in allowed and status != 403:
                wrong.append(f"{api_id} {role}: {status} (cần 403)")
            if role in allowed and check_allowed and status in (401, 403):
                wrong.append(f"{api_id} {role}: {status} (được phép)")
    assert not wrong, wrong


async def test_me_phase3_permissions_four_roles(api: AsyncClient, db: AsyncSession) -> None:
    """04 TC-10.44: ADMIN đủ 9 quyền mới; SUPERVISOR không `notify.manage`, `backup.manage`; CSKH không
    `reports.productivity`, `shares.revoke_any`, `notify.*`, `backup.*`; STATION không quyền nào."""
    tokens = await _tokens(api, db)
    new = {
        "reports.returns",
        "reports.claims",
        "reports.productivity",
        "shares.create",
        "shares.read",
        "shares.revoke_any",
        "notify.manage",
        "backup.manage",
        "backup.read",
    }
    got: dict[str, set[str]] = {}
    for role, token in tokens.items():
        res = await api.get("/api/v1/me", headers={"Authorization": f"Bearer {token}"})
        assert res.status_code == 200, res.text
        got[role] = set(res.json()["permissions"]) & new
    assert got[A] == new
    assert got[S] == new - {"notify.manage", "backup.manage"}
    assert got[C] == {"reports.returns", "reports.claims", "shares.create", "shares.read"}
    assert got["STATION"] == set()
