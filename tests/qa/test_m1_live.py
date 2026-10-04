"""QA phạm vi M1 trên stack dev thật (04-test-cases §1: API qua HTTP, server phải chặn quyền).

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa -v`
Mỗi test ghi mã TC trong docstring. Phiên chạy bắt đầu bằng `scripts/qa-reset.sh` (migrate lại + seed TST).
"""

import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

BASE = os.environ.get("QA_BASE_URL")
pytestmark = [
    pytest.mark.qa,
    pytest.mark.skipif(not BASE, reason="đặt QA_BASE_URL để chạy QA trên stack thật"),
]

ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "matkhau123"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker/compose.dev.yml")]


def _psql(sql: str) -> str:
    out = subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "aicam", "-d", "aicam", "-tA", "-c", sql],
        capture_output=True,
        text=True,
        check=False,
    )
    return (out.stdout + out.stderr).strip()


@pytest.fixture(scope="session", autouse=True)
def _reset() -> None:
    subprocess.run([str(ROOT / "scripts/qa-reset.sh")], check=True, capture_output=True)  # noqa: S603


@pytest.fixture(scope="session")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
        yield c


def _login(
    client: httpx.Client, username: str, client_kind: str = "DASHBOARD", password: str = PASSWORD
) -> httpx.Response:
    return client.post(
        "/auth/login", json={"username": username, "password": password, "client": client_kind}
    )


@pytest.fixture(scope="session")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    out = {}
    for role, username, kind in [
        ("ADMIN", "tst_admin", "DASHBOARD"),
        ("SUPERVISOR", "tst_sup", "DASHBOARD"),
        ("CSKH", "tst_cskh", "DASHBOARD"),
        ("STATION", "tst_station01", "STATION"),
    ]:
        res = _login(client, username, kind)
        assert res.status_code == 200, res.text
        out[role] = {"Authorization": f"Bearer {res.json()['access_token']}"}
    return out


def _scan(
    client: httpx.Client, headers: dict[str, str], code: str, scan_id: str | None = None
) -> dict[str, Any]:
    res = client.post(
        "/station/scan", headers=headers, json={"code": code, "client_scan_id": scan_id or str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


# ---------------------------------------------------------------- M10


def test_tc_10_01_station_login(client: httpx.Client) -> None:
    """TC-10.01 (phần API): đăng nhập station, cookie rt_station httpOnly."""
    res = _login(client, "tst_station01", "STATION")
    assert res.status_code == 200
    assert res.json()["user"]["station"]["name"] == "TST Station 01"
    cookie = res.headers["set-cookie"]
    assert cookie.startswith("rt_station=")
    assert "HttpOnly" in cookie


def test_tc_10_02_wrong_password(client: httpx.Client) -> None:
    """TC-10.02."""
    res = _login(client, "tst_cskh", password="sai")
    assert res.status_code == 401
    assert res.json()["error"]["code"] == "INVALID_CREDENTIALS"


def test_tc_10_03_wrong_client(client: httpx.Client) -> None:
    """TC-10.03."""
    res = _login(client, "tst_cskh", "STATION")
    assert res.status_code == 403
    assert res.json()["error"]["code"] == "WRONG_CLIENT"


def test_tc_10_04_lock_after_ten_failures(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-10.04: dùng tài khoản riêng để không khóa tài khoản demo."""
    created = client.post(
        "/users",
        headers=tokens["ADMIN"],
        json={"username": "qa_lock", "display_name": "QA", "role": "CSKH", "password": "12345678"},
    )
    assert created.status_code == 201
    codes = [_login(client, "qa_lock", password="sai").status_code for _ in range(10)]
    locked = _login(client, "qa_lock", password="12345678")
    assert codes == [401] * 10
    assert locked.status_code == 423
    assert locked.json()["error"]["code"] == "ACCOUNT_LOCKED"
    assert "until" in locked.json()["error"]["details"]


def test_tc_10_07_last_admin(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-10.07."""
    admin_id = client.get("/me", headers=tokens["ADMIN"]).json()["id"]
    res = client.patch(f"/users/{admin_id}", headers=tokens["ADMIN"], json={"is_active": False})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "LAST_ADMIN"


def test_tc_10_08_username_taken(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-10.08."""
    res = client.post(
        "/users",
        headers=tokens["ADMIN"],
        json={"username": "tst_admin", "display_name": "x", "role": "CSKH", "password": "12345678"},
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "USERNAME_TAKEN"


def test_tc_10_09_audit_m1_actions(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-10.09 (phần M1: LOGIN, USER_UPDATE, STATION_UPDATE, CAMERA_UPDATE — VIEW/EXPORT_CLIP ở M2)."""
    admin = tokens["ADMIN"]
    user = client.post(
        "/users",
        headers=admin,
        json={"username": "qa_audit", "display_name": "QA", "role": "CSKH", "password": "12345678"},
    )
    station = client.post("/stations", headers=admin, json={"name": "QA Audit"}).json()
    client.put(
        f"/stations/{station['id']}/cameras/CAM1", headers=admin, json={"rtsp_url": "rtsp://10.255.255.1/x"}
    )
    res = client.get("/audit-logs", headers=admin, params={"page_size": 100})
    items = res.json()["items"]
    by_action: dict[str, Any] = {}
    for item in items:  # mới nhất trước → giữ lần đầu tiên gặp
        by_action.setdefault(item["action"], item)
    assert {"LOGIN", "USER_UPDATE", "STATION_UPDATE", "CAMERA_UPDATE"} <= set(by_action)
    assert by_action["USER_UPDATE"]["object_id"] == user.json()["id"]
    assert by_action["STATION_UPDATE"]["user"]["display_name"] == "Quản trị"


def test_tc_10_10_audit_log_cannot_be_modified() -> None:
    """TC-10.10."""
    assert "chỉ cho phép INSERT" in _psql("UPDATE audit_log SET action = 'X'")
    assert "chỉ cho phép INSERT" in _psql("DELETE FROM audit_log")


# ---------------------------------------------------------------- M01


def test_tc_01_01_api_create_station_camera_online(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-01.01 (phần API + J-08): tạo station, gắn Cam 2 vào camera giả, ONLINE ≤ 10 giây."""
    admin = tokens["ADMIN"]
    station = client.post("/stations", headers=admin, json={"name": "QA Station"}).json()
    test = client.post("/cameras/test", headers=admin, json={"rtsp_url": "rtsp://mediamtx:8554/cam-fake2"})
    assert test.status_code == 200, test.text
    assert test.json()["snapshot"].startswith("data:image/jpeg;base64,")
    cam = client.put(
        f"/stations/{station['id']}/cameras/CAM2",
        headers=admin,
        json={"rtsp_url": "rtsp://localhost:8554/cam-fake2"},
    ).json()
    started = time.monotonic()
    status = "OFFLINE"
    while time.monotonic() - started < 15:
        listed = client.get("/stations", headers=admin).json()["items"]
        status = next(c["status"] for s in listed for c in s["cameras"] if c["id"] == cam["id"])
        if status == "ONLINE":
            break
        time.sleep(0.5)
    assert status == "ONLINE"
    assert time.monotonic() - started <= 10


def test_tc_01_02_name_taken(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-01.02."""
    res = client.post("/stations", headers=tokens["ADMIN"], json={"name": "tst station 01"})
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "NAME_TAKEN"


def test_tc_01_03_account_in_use(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-01.03."""
    users = client.get("/users", headers=tokens["ADMIN"], params={"role": "STATION"}).json()["items"]
    st1 = next(u for u in users if u["username"] == "tst_station01")
    res = client.post(
        "/stations", headers=tokens["ADMIN"], json={"name": "QA S3", "account_user_id": st1["id"]}
    )
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "ACCOUNT_IN_USE"


def test_tc_01_04_camera_unreachable(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-01.04 (IP không tồn tại)."""
    res = client.post(
        "/cameras/test", headers=tokens["ADMIN"], json={"rtsp_url": "rtsp://10.255.255.1:554/x"}
    )
    assert res.status_code == 422
    assert res.json()["error"]["code"] == "CAMERA_UNREACHABLE"
    assert res.json()["error"]["details"]["reason"] in {"TIMEOUT", "STREAM"}


def test_tc_01_05_06_roi(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-01.05, TC-01.06."""
    admin = tokens["ADMIN"]
    station = next(
        s for s in client.get("/stations", headers=admin).json()["items"] if s["name"] == "TST Station 01"
    )
    cams = {c["role"]: c for c in station["cameras"]}
    roi = {"x": 0.2, "y": 0.2, "w": 0.6, "h": 0.6}
    assert client.put(f"/cameras/{cams['CAM2']['id']}/roi", headers=admin, json=roi).json()["roi"] == roi
    assert (
        client.put(f"/cameras/{cams['CAM1']['id']}/roi", headers=admin, json=roi).json()["error"]["code"]
        == "ROI_ONLY_CAM2"
    )
    assert (
        client.put(f"/cameras/{cams['CAM2']['id']}/roi", headers=admin, json={**roi, "w": 0.04}).status_code
        == 422
    )


def test_tc_01_07_simulated_camera_loss(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-01.07 / TC-01.08 GIẢ LẬP bằng camera giả (dừng container fake-cam2) — HW thật vẫn chờ T-4."""
    admin = tokens["ADMIN"]

    def cam2_status() -> str:
        st = next(
            s for s in client.get("/stations", headers=admin).json()["items"] if s["name"] == "TST Station 01"
        )
        return str(next(c["status"] for c in st["cameras"] if c["role"] == "CAM2"))

    for _ in range(30):
        if cam2_status() == "ONLINE":
            break
        time.sleep(0.5)
    subprocess.run([*COMPOSE, "stop", "fake-cam2"], check=True, capture_output=True)  # noqa: S603
    try:
        started = time.monotonic()
        while cam2_status() != "OFFLINE" and time.monotonic() - started < 15:
            time.sleep(0.25)
        offline_after = time.monotonic() - started
        assert cam2_status() == "OFFLINE"
        assert offline_after <= 10
    finally:
        subprocess.run([*COMPOSE, "start", "fake-cam2"], check=True, capture_output=True)  # noqa: S603
    started = time.monotonic()
    while cam2_status() != "ONLINE" and time.monotonic() - started < 20:
        time.sleep(0.5)
    assert cam2_status() == "ONLINE"


# ---------------------------------------------------------------- M03 (quét)


def test_tc_03_01_02_open_close(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.01, TC-03.02 (API)."""
    st = tokens["STATION"]
    opened = _scan(client, st, "SPXTST0000012")
    assert opened["outcome"] == "SESSION_OPENED"
    assert len(opened["state"]["session"]["package"]["items"]) == 3
    closed = _scan(client, st, "SPXTST0000012")
    assert closed["outcome"] == "SESSION_COMPLETED"
    assert closed["state"]["state"] == "READY"


def test_tc_03_03_fifty_scans_p95(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.03, AC-01, NFR-01: 50 lần quét liên tiếp (25 đơn mở + đóng), p95 ≤ 1 giây."""
    st = tokens["STATION"]
    durations = []
    for n in range(1, 26):
        code = f"SPXTST999{n:04d}"
        for _ in range(2):
            t0 = time.perf_counter()
            _scan(client, st, code)
            durations.append(time.perf_counter() - t0)
    durations.sort()
    p95 = durations[int(len(durations) * 0.95) - 1]
    print(
        f"\n[NFR-01] 50 lần quét: p50={durations[24] * 1000:.0f} ms "
        f"p95={p95 * 1000:.0f} ms max={durations[-1] * 1000:.0f} ms"
    )
    assert p95 <= 1.0


def test_tc_03_04_05_06_mismatch(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.04, TC-03.05, TC-03.06 (AC-03: 20 lần quét đóng sai → 20/20 MISMATCH)."""
    st = tokens["STATION"]
    assert _scan(client, st, "SPXTST0000001")["outcome"] == "SESSION_OPENED"
    outcomes = [_scan(client, st, f"SPXTST00000{n:02d}")["outcome"] for n in range(13, 33) if n != 9]
    assert outcomes == ["MISMATCH"] * len(outcomes)
    fixed = _scan(client, st, "SPXTST0000001")
    assert fixed["outcome"] == "SESSION_COMPLETED"


def test_tc_03_07_no_new_session_while_mismatch(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-03.07."""
    st = tokens["STATION"]
    _scan(client, st, "SPXTST0000002")
    _scan(client, st, "SPXTST0000003")
    again = _scan(client, st, "SPXTST0000004")
    assert again["outcome"] == "MISMATCH"
    assert again["state"]["session"]["package"]["tracking_number"] == "SPXTST0000002"
    assert _scan(client, st, "SPXTST0000002")["outcome"] == "SESSION_COMPLETED"


@pytest.mark.parametrize(
    ("code", "alert"),
    [
        ("SPXTST0000009", "ORDER_CANCELLED"),  # TC-03.08, AC-05
        ("SPXTST0000010", "ALREADY_PACKED"),  # TC-03.09
        ("SPXTST0000011", "ALREADY_HANDED_OVER"),  # TC-03.10
        ("abc!!12345", "INVALID_CODE"),  # TC-03.14
    ],
)
def test_tc_03_alerts(client: httpx.Client, tokens: dict[str, dict[str, str]], code: str, alert: str) -> None:
    """TC-03.08, 03.09, 03.10, 03.14."""
    res = _scan(client, tokens["STATION"], code)
    assert res["outcome"] == "ALERT"
    assert res["alert"]["code"] == alert
    assert res["state"]["session"] is None


def test_tc_03_12_unknown_code_unverified(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.12/13 (sàn mock không có mã → mở phiên chưa xác minh)."""
    st = tokens["STATION"]
    res = _scan(client, st, "SPXQA00000001")
    assert res["outcome"] == "SESSION_OPENED"
    assert res["state"]["session"]["flags"] == ["UNVERIFIED"]
    _scan(client, st, "SPXQA00000001")


def test_tc_03_15_open_at_other_station(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.15."""
    st2 = {"Authorization": f"Bearer {_login(client, 'tst_station02', 'STATION').json()['access_token']}"}
    _scan(client, tokens["STATION"], "SPXTST0000005")
    res = _scan(client, st2, "SPXTST0000005")
    assert res["alert"]["code"] == "PACKED_ELSEWHERE_IN_PROGRESS"
    _scan(client, tokens["STATION"], "SPXTST0000005")


def test_tc_03_17_retry_same_scan_id(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.17."""
    st = tokens["STATION"]
    scan_id = str(uuid.uuid4())
    first = _scan(client, st, "SPXTST0000006", scan_id)
    retry = _scan(client, st, "SPXTST0000006", scan_id)
    assert first["outcome"] == retry["outcome"] == "SESSION_OPENED"
    assert retry["state"]["state"] == "PACKING"
    _scan(client, st, "SPXTST0000006")


def test_tc_03_18_19_cancel(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.18, TC-03.19."""
    st = tokens["STATION"]
    session_id = _scan(client, st, "SPXTST0000007")["state"]["session"]["id"]
    bad = client.post(
        f"/station/sessions/{session_id}/cancel", headers=st, json={"reason": "OTHER", "note": " "}
    )
    assert bad.status_code == 422
    ok = client.post(f"/station/sessions/{session_id}/cancel", headers=st, json={"reason": "OUT_OF_STOCK"})
    assert ok.status_code == 200
    assert ok.json()["state"]["state"] == "READY"
    assert _scan(client, st, "SPXTST0000007")["outcome"] == "SESSION_OPENED"  # kiện về NEW, mở lại được
    _scan(client, st, "SPXTST0000007")


def test_tc_03_33_recent(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.33 (phần danh sách; xem clip ở M2)."""
    items = client.get("/station/sessions/recent", headers=tokens["STATION"]).json()["items"]
    assert 1 <= len(items) <= 5
    assert items == sorted(items, key=lambda i: i["started_at"], reverse=True)


# ---------------------------------------------------------------- §3 phân quyền (API M1)

MATRIX = [
    # (method, path, body, quyền được phép)
    ("GET", "/station/state", None, {"STATION"}),  # TC-P.01
    ("GET", "/station/sessions/recent", None, {"STATION"}),
    ("GET", "/stations", None, {"ADMIN"}),  # TC-P.07
    ("POST", "/stations", {"name": "P"}, {"ADMIN"}),
    (
        "PUT",
        "/cameras/00000000-0000-0000-0000-000000000000/roi",
        {"x": 0, "y": 0, "w": 0.5, "h": 0.5},
        {"ADMIN"},
    ),
    ("GET", "/live", None, {"ADMIN", "SUPERVISOR"}),
    ("GET", "/cameras/00000000-0000-0000-0000-000000000000/snapshot", None, {"ADMIN", "SUPERVISOR"}),
    ("POST", "/cameras/test", {"rtsp_url": "rtsp://127.0.0.1:9/x"}, {"ADMIN"}),
    ("GET", "/users", None, {"ADMIN"}),  # TC-P.09
    ("POST", "/users/00000000-0000-0000-0000-000000000000/revoke-sessions", None, {"ADMIN"}),
    ("GET", "/audit-logs", None, {"ADMIN"}),
]


@pytest.mark.parametrize(("method", "path", "body", "allowed"), MATRIX)
@pytest.mark.parametrize("role", ["ADMIN", "SUPERVISOR", "CSKH", "STATION"])
def test_tc_p_matrix(
    client: httpx.Client,
    tokens: dict[str, dict[str, str]],
    role: str,
    method: str,
    path: str,
    body: dict[str, Any] | None,
    allowed: set[str],
) -> None:
    """TC-P.01, P.07, P.09 (endpoint đã có ở M1): server chặn 403, không chỉ UI ẩn."""
    res = client.request(method, path, headers=tokens[role], json=body)
    if role in allowed:
        assert res.status_code != 403, res.text
    else:
        assert res.status_code == 403, f"{role} {method} {path} → {res.status_code}"
        assert res.json()["error"]["code"] == "FORBIDDEN"


def test_no_token_is_401(client: httpx.Client) -> None:
    for path in ("/station/state", "/stations", "/users", "/me"):
        assert client.get(path).status_code == 401
