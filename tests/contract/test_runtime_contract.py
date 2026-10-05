"""Contract test runtime (02 §6): gọi API thật (Postgres + Redis của stack dev) và kiểm response.

- Mọi mốc giờ trả ra là ISO-8601 UTC có hậu tố `Z` (02 §6 "Thời gian", RB-11).
- Khóa của các object tự do trong OpenAPI (`context` API-20, `attention[]` API-32) đúng 02 §6.
- Lỗi theo dạng `{"error": {"code", "message", "details"?}}`.
"""

import re
import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions.router import get_platform_adapter
from tests.integration.factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}")


def _datetimes(value: Any, path: str = "$") -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(value, dict):
        for k, v in value.items():
            found.extend(_datetimes(v, f"{path}.{k}"))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found.extend(_datetimes(v, f"{path}[{i}]"))
    elif isinstance(value, str) and _DATETIME.match(value):
        found.append((path, value))
    return found


def _assert_utc_z(body: Any, label: str) -> int:
    stamps = _datetimes(body)
    wrong = [(p, v) for p, v in stamps if not v.endswith("Z")]
    assert not wrong, f"{label}: giờ không theo ISO-8601 UTC 'Z': {wrong}"
    return len(stamps)


async def _login(api: AsyncClient, username: str, client: str) -> dict[str, str]:
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    assert res.status_code == 200, res.text
    _assert_utc_z(res.json(), "API-01")
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def test_main_flow_responses_follow_contract(api: AsyncClient, db: AsyncSession) -> None:
    api._transport.app.dependency_overrides[get_platform_adapter] = MockAdapter  # type: ignore[attr-defined]
    user, _ = await make_station_account(db)
    await make_user(db, "tst_admin_contract", "ADMIN")
    st = await _login(api, user.username, "STATION")
    adm = await _login(api, "tst_admin_contract", "DASHBOARD")
    checked = 0

    async def get(url: str, headers: dict[str, str], label: str) -> dict[str, Any]:
        nonlocal checked
        res = await api.get(url, headers=headers)
        assert res.status_code == 200, f"{label}: {res.status_code} {res.text}"
        checked += _assert_utc_z(res.json(), label)
        return res.json()  # type: ignore[no-any-return]

    async def scan(code: str) -> dict[str, Any]:
        nonlocal checked
        res = await api.post(
            "/api/v1/station/scan", headers=st, json={"code": code, "client_scan_id": str(uuid.uuid4())}
        )
        assert res.status_code == 200, res.text
        checked += _assert_utc_z(res.json(), "API-11")
        return res.json()  # type: ignore[no-any-return]

    await get("/api/v1/me", st, "API-04")
    opened = await scan("SPXTST0000012")
    assert opened["outcome"] == "SESSION_OPENED"
    session_id = opened["state"]["session"]["id"]
    assert opened["state"]["session"]["started_at"].endswith("Z")
    await get("/api/v1/station/state", st, "API-10")

    # API-13 ASSIST → API-20 context đủ khóa → API-21 CONTINUE.
    req = await api.post(
        "/api/v1/station/approval-requests", headers=st, json={"type": "ASSIST", "session_id": session_id}
    )
    assert req.status_code == 201, req.text
    checked += _assert_utc_z(req.json(), "API-13")
    pending = await get("/api/v1/approval-requests", adm, "API-20")
    assert set(pending["items"][0]["context"]) == {"expected", "actual", "source", "tray_match"}
    decided = await api.post(
        f"/api/v1/approval-requests/{pending['items'][0]['id']}/decision",
        headers=adm,
        json={"action": "CONTINUE", "note": None},
    )
    assert decided.status_code == 200, decided.text
    checked += _assert_utc_z(decided.json(), "API-21")

    closed = await scan("SPXTST0000012")
    assert closed["outcome"] == "SESSION_COMPLETED"
    again = await scan("SPXTST0000012")  # alert.data.packed_at cũng phải có `Z`
    assert again["alert"]["code"] == "ALREADY_PACKED"
    assert again["alert"]["data"]["packed_at"].endswith("Z")
    await get("/api/v1/station/sessions/recent", st, "API-15")

    found = await get("/api/v1/packages?q=SPXTST0000012", adm, "API-30")
    detail = await get(f"/api/v1/packages/{found['items'][0]['id']}", adm, "API-31")
    assert detail["sessions"][0]["ended_at"].endswith("Z")
    report = await get("/api/v1/reports/daily", adm, "API-32")
    assert all("kind" in a for a in report["attention"])
    await get("/api/v1/users", adm, "API-90")
    await get("/api/v1/audit-logs", adm, "API-92")
    await get("/api/v1/settings", adm, "API-80")

    assert checked >= 15, f"chỉ kiểm được {checked} mốc giờ — dữ liệu test quá ít"


async def test_error_envelope(api: AsyncClient, db: AsyncSession) -> None:
    """02 §6 "Lỗi": `{"error": {"code", "message", "details"?}}`; 401 / 403 / 404 / 422 cùng một dạng."""
    await make_user(db, "tst_cskh_contract", "CSKH")
    cskh = await _login(api, "tst_cskh_contract", "DASHBOARD")
    cases = [
        (await api.get("/api/v1/me"), 401, "UNAUTHENTICATED"),
        (await api.get("/api/v1/users", headers=cskh), 403, "FORBIDDEN"),
        (await api.get(f"/api/v1/packages/{uuid.uuid4()}", headers=cskh), 404, "NOT_FOUND"),
        (await api.post("/api/v1/auth/login", json={"username": "x"}), 422, "VALIDATION_ERROR"),
        (
            await api.post(
                "/api/v1/auth/login",
                json={"username": "khong_co", "password": "sai12345", "client": "DASHBOARD"},
            ),
            401,
            "INVALID_CREDENTIALS",
        ),
    ]
    for res, status, code in cases:
        assert res.status_code == status, (code, res.text)
        err = res.json()["error"]
        assert err["code"] == code
        assert isinstance(err["message"], str)
        assert err["message"]
    assert "fields" in cases[3][0].json()["error"]["details"]
