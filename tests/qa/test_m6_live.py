"""QA M6 (nền tảng Phase 2) trên stack dev thật — API qua HTTP (04-test-cases item 02).

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa -m qa -k m6` (tự `scripts/qa-reset.sh`).
Phủ T-101..T-106: migration 0003 (sàn retention), API-60 `kind`, API-100, API-101, API-03 xóa người kiểm,
API-04 `permissions` / `station`, API-122. Dùng `TST Station 02` (không camera) để không đụng bộ QA Phase 1.
Phần returns / đối soát / hồ sơ (T-104+) thêm ở T-118.
"""

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.qa import stack

BASE = os.environ.get("QA_BASE_URL")
pytestmark = [
    pytest.mark.qa,
    pytest.mark.skipif(not BASE, reason="đặt QA_BASE_URL để chạy QA trên stack thật"),
]

ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "matkhau123"
COMPOSE = stack.COMPOSE


def _psql(sql: str) -> str:
    out = subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "aicam", "-d", "aicam", "-tA", "-c", sql],
        capture_output=True,
        text=True,
        check=False,
    )
    return (out.stdout + out.stderr).strip()


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    subprocess.run([str(ROOT / "scripts/qa-reset.sh")], check=True, capture_output=True)  # noqa: S603


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
        yield c


def _login(client: httpx.Client, username: str, kind: str = "DASHBOARD") -> dict[str, str]:
    res = client.post("/auth/login", json={"username": username, "password": PASSWORD, "client": kind})
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    return {
        "ADMIN": _login(client, "tst_admin"),
        "SUPERVISOR": _login(client, "tst_sup"),
        "CSKH": _login(client, "tst_cskh"),
    }


def _station02(client: httpx.Client, admin: dict[str, str]) -> dict[str, Any]:
    items = client.get("/stations", headers=admin).json()["items"]
    return next(s for s in items if s["name"] == "TST Station 02")  # type: ignore[no-any-return]


def _package_id(client: httpx.Client, headers: dict[str, str], code: str) -> str:
    items = client.get("/packages", params={"q": code}, headers=headers).json()["items"]
    return next(i["id"] for i in items if i["tracking_number"] == code)  # type: ignore[no-any-return]


# ---------------------------------------------------------------- migration 0003 (T-101)


def test_mg_migration_head_and_retention_floor(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-MG.01 (một phần, trên stack): DB ở head (0005 từ M10 — T-118), cột / bảng mới có, retention ≥ sàn 60
    (DEC-257)."""
    assert _psql("SELECT version_num FROM alembic_version") == "0005"
    assert _psql("SELECT count(*) FROM package WHERE status_changed_at IS NULL OR created_at IS NULL") == "0"
    assert _psql("SELECT to_regclass('return_case') IS NOT NULL AND to_regclass('claim') IS NOT NULL") == "t"
    setting = client.get("/settings", headers=tokens["ADMIN"]).json()
    assert setting["retention_clip_days"] >= 60


# ---------------------------------------------------------------- API-60 kind, API-100, API-101 (T-106)


def test_tc_p2_01_kind_admin_only(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-P2.01 + TC-01.30 (API): Admin đặt loại "Nhận hoàn" → work_mode RETURN; vai khác 403."""
    station = _station02(client, tokens["ADMIN"])
    for role in ("SUPERVISOR", "CSKH"):
        res = client.patch(f"/stations/{station['id']}", json={"kind": "RETURN"}, headers=tokens[role])
        assert res.status_code == 403
    res = client.patch(f"/stations/{station['id']}", json={"kind": "RETURN"}, headers=tokens["ADMIN"])
    assert res.status_code == 200
    assert (res.json()["kind"], res.json()["work_mode"]) == ("RETURN", "RETURN")


def test_tc_04_34_mode_not_allowed(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-04.34 / TC-01.32 bước 1: station không phải "Cả hai" không đổi chế độ."""
    station = _station02(client, tokens["ADMIN"])
    client.patch(f"/stations/{station['id']}", json={"kind": "RETURN"}, headers=tokens["ADMIN"])
    st = _login(client, "tst_station02", "STATION")
    res = client.put("/station/work-mode", json={"work_mode": "PACK"}, headers=st)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "MODE_NOT_ALLOWED"


def test_tc_04_01_both_mode_operator_and_logout(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-04.01 + TC-04.35 (phần API) + TC-P2.02: đổi chế độ, nhập người kiểm, đăng xuất xóa tên."""
    station = _station02(client, tokens["ADMIN"])
    client.patch(f"/stations/{station['id']}", json={"kind": "BOTH"}, headers=tokens["ADMIN"])
    st = _login(client, "tst_station02", "STATION")
    assert client.get("/me", headers=st).json()["station"]["kind"] == "BOTH"
    assert (
        client.put("/station/work-mode", json={"work_mode": "RETURN"}, headers=tokens["ADMIN"]).status_code
        == 403
    )

    mode = client.put("/station/work-mode", json={"work_mode": "RETURN"}, headers=st)
    assert mode.status_code == 200, mode.text
    assert mode.json()["state"]["station"]["work_mode"] == "RETURN"
    bad = client.put("/station/operator", json={"name": "L"}, headers=st)
    assert bad.status_code == 422
    op = client.put("/station/operator", json={"name": " Lan QA "}, headers=st)
    assert op.status_code == 200
    assert op.json()["state"]["station"]["operator_name"] == "Lan QA"

    audit = client.get("/audit-logs", params={"action": "STATION_OPERATOR"}, headers=tokens["ADMIN"]).json()
    assert audit["items"][0]["data"] == {"old": None, "new": "Lan QA"}

    assert client.post("/auth/logout", headers=st).status_code == 204
    st2 = _login(client, "tst_station02", "STATION")
    state = client.get("/station/state", headers=st2).json()
    assert state["station"]["operator_name"] is None
    assert state["station"]["work_mode"] == "RETURN"


# ---------------------------------------------------------------- API-04 permissions (T-102)


def test_tc_p2_me_permissions(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    perms = {role: set(client.get("/me", headers=h).json()["permissions"]) for role, h in tokens.items()}
    assert {"returns.read", "recon.read", "claims.manage"} <= perms["CSKH"]
    assert "warehouse_status.adjust" not in perms["CSKH"]
    assert {"warehouse_status.adjust", "recon.resolve", "returns.link"} <= perms["SUPERVISOR"]


# ---------------------------------------------------------------- API-122 (T-102)


def test_tc_06_13_adjust_status(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-06.13 (API) + TC-P2.08: SUPERVISOR chỉnh NEW → HANDED_OVER; CSKH 403; lịch sử MANUAL."""
    package_id = _package_id(client, tokens["ADMIN"], "SPXTST0000012")
    body = {"to_status": "HANDED_OVER", "reason": "Đã gửi thật 05/10, phiên bỏ dở"}
    assert (
        client.post(f"/packages/{package_id}/warehouse-status", json=body, headers=tokens["CSKH"]).status_code
        == 403
    )

    res = client.post(f"/packages/{package_id}/warehouse-status", json=body, headers=tokens["SUPERVISOR"])

    assert res.status_code == 200, res.text
    assert res.json()["package"]["warehouse_status"] == "HANDED_OVER"
    assert res.json()["recon_alert"] is None
    timeline = client.get(f"/packages/{package_id}", headers=tokens["ADMIN"]).json()["timeline"]
    assert (timeline[-1]["source"], timeline[-1]["to_status"]) == ("MANUAL", "HANDED_OVER")
    audit = client.get("/audit-logs", params={"action": "WAREHOUSE_STATUS_ADJUST"}, headers=tokens["ADMIN"])
    assert audit.json()["items"][0]["data"]["reason"] == "Đã gửi thật 05/10, phiên bỏ dở"


def test_tc_06_14_transition_not_allowed(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-06.14: kiện RETURN_EXPECTED → RETURN_RECEIVED_OK bị chặn, `details.allowed` = [DELIVERED]."""
    _psql("UPDATE package SET warehouse_status = 'RETURN_EXPECTED' WHERE tracking_number = 'SPXTST0000020'")
    package_id = _package_id(client, tokens["ADMIN"], "SPXTST0000020")

    res = client.post(
        f"/packages/{package_id}/warehouse-status",
        json={"to_status": "RETURN_RECEIVED_OK", "reason": "Thử chuyển tay"},
        headers=tokens["SUPERVISOR"],
    )

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "TRANSITION_NOT_ALLOWED"
    assert res.json()["error"]["details"]["allowed"] == ["DELIVERED"]
