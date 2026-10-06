"""QA M9 (đồng bộ hoàn + đối soát) trên stack dev thật — API qua HTTP, job chạy trong container.

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa -m qa -k m9` (tự `qa-reset.sh`). Cần stack dev
với `SHOPEE_ENABLED=true` (adapter mock), worker queue `default` (J-14 qua API-123).
Phủ T-105, T-113, T-114, T-115: kết nối shop mock → J-13 (yêu cầu trả mock `2410RTTST041` → hồ sơ
"Đang về", chỉ hoàn tiền `2410RTTST044` → `NO_PARCEL`) → API-110 / API-30 / API-31 / API-32; J-14 tua giờ
(`return_missing_days` = 1, đẩy lùi `status_changed_at`) → `RETURN_MISSING` + cảnh báo Cao; BR-14 → API-121
xử lý, chạy lại không tạo lại; API-123 409 khi đang chạy; API-80 sàn 60 + xác nhận hạ; API-82.
Shopee returns thật: **chưa test — thiếu partner T-3**.
"""

import os
import subprocess
import time
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
SETTINGS = {"retention_raw_days": 30, "retention_clip_days": 90, "session_warn_minutes": 15,
            "session_abandon_minutes": 30}  # fmt: skip


def _psql(sql: str) -> str:
    out = subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "aicam", "-d", "aicam", "-tA", "-c", sql],
        capture_output=True,
        text=True,
        check=False,
    )
    return (out.stdout + out.stderr).strip()


def _job(expr: str) -> str:
    """Chạy một task Celery ngay trong container api (cùng code + env với worker), trả kết quả."""
    code = f"from aicam.workers import tasks; print({expr})"
    out = subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "api", "python", "-c", code], capture_output=True, text=True, check=False
    )
    assert out.returncode == 0, out.stderr[-2000:]
    return out.stdout.strip().splitlines()[-1]


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    subprocess.run([str(ROOT / "scripts/qa-reset.sh")], check=True, capture_output=True)  # noqa: S603


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
        yield c


def _login(client: httpx.Client, username: str) -> dict[str, str]:
    res = client.post("/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"})
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    return {"ADMIN": _login(client, "tst_admin"), "SUPERVISOR": _login(client, "tst_sup"),
            "CSKH": _login(client, "tst_cskh")}  # fmt: skip


@pytest.fixture(scope="module")
def synced(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> str:
    """Kết nối shop mock (API-71 → callback) rồi chạy J-13 một lần — trả kết quả J-13."""
    res = client.post("/shops/shopee/auth-url", headers=tokens["ADMIN"])
    if res.status_code == 503:
        pytest.skip("Cần stack dev SHOPEE_ENABLED=true (adapter mock) cho J-13")
    assert res.status_code == 200, res.text
    url = httpx.URL(res.json()["url"])
    # Cookie state `Secure`: httpx không gửi qua http://localhost → gửi tay
    # (trình duyệt coi localhost an toàn).
    cookie = {"Cookie": f"aicam_shopee_state={res.cookies['aicam_shopee_state']}"}
    redirect = client.get(f"{BASE}{url.raw_path.decode()}", headers=cookie, follow_redirects=False)
    assert redirect.headers["location"] == "/admin/settings/shopee?result=connected", redirect.headers
    return _job("tasks.sync_returns()")


def _case(client: httpx.Client, headers: dict[str, str], tab: str, code: str) -> dict[str, Any]:
    items = client.get("/returns", params={"tab": tab, "q": code}, headers=headers).json()["items"]
    assert len(items) == 1, items
    return items[0]  # type: ignore[no-any-return]


def _package_id(client: httpx.Client, headers: dict[str, str], code: str) -> str:
    items = client.get("/packages", params={"q": code}, headers=headers).json()["items"]
    return next(i["id"] for i in items if i["tracking_number"] == code)  # type: ignore[no-any-return]


def _alerts(client: httpx.Client, headers: dict[str, str], package_id: str) -> list[dict[str, Any]]:
    return client.get("/recon-alerts", params={"package_id": package_id}, headers=headers).json()["items"]  # type: ignore[no-any-return]


def _run_recon(client: httpx.Client, headers: dict[str, str]) -> None:
    """J-14 chạy đồng bộ trong container (cùng code worker) — kết quả xác định cho kiểm tra."""
    out = _job("tasks.run_recon_rules()")
    assert "'created'" in out, out


def _redis(*args: str) -> str:
    return subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "redis", "redis-cli", *args], capture_output=True, text=True, check=False
    ).stdout.strip()


# ---------------------------------------------------------------- J-13 (T-105)


def test_tc_05_30_31_j13_mock_creates_cases(
    client: httpx.Client, tokens: dict[str, dict[str, str]], synced: str
) -> None:
    """TC-05.30 / 05.31 / 05.39: J-13 mock → hồ sơ `BUYER_RETURN` "Đang về" (mã chiều về, lý do, hạn) + kiện
    `RETURN_EXPECTED`; chỉ hoàn tiền → `NO_PARCEL`, kiện không đổi; yêu cầu đã hủy chưa từng thấy → bỏ qua."""
    assert "'status': 'OK'" in synced, synced
    staff = tokens["CSKH"]
    case = _case(client, staff, "EXPECTED", "SPXRTTST000041")
    assert (case["kind"], case["status"], case["reason_label"]) == ("BUYER_RETURN", "EXPECTED", "Hàng bị hư")
    assert case["packages"][0]["warehouse_status"] == "RETURN_EXPECTED"
    refund = _case(client, staff, "NO_PARCEL", "2410RTTST044")
    assert refund["kind"] == "REFUND_ONLY"
    assert refund["packages"][0]["warehouse_status"] != "RETURN_EXPECTED"
    assert (
        client.get("/returns", params={"tab": "ALL", "q": "2410RTTST045"}, headers=staff).json()["items"]
        == []
    )
    # API-30 tra mã chiều về → kiện, có `return_case` brief (TC-07.30 phần API).
    items = client.get("/packages", params={"q": "SPXRTTST000041"}, headers=staff).json()["items"]
    assert items[0]["tracking_number"] == "SPXTST0000041"
    assert items[0]["return_case"]["code"] == case["code"]
    # Chạy lại: idempotent theo `platform_return_sn` (mock nạp lại fixture trong tiến trình mới → hạn người
    # bán tương đối đổi, nên chỉ so số hồ sơ / trạng thái).
    before = _psql("SELECT count(*), string_agg(status, ',' ORDER BY code) FROM return_case")
    assert "'status': 'OK'" in _job("tasks.sync_returns()")
    assert _psql("SELECT count(*), string_agg(status, ',' ORDER BY code) FROM return_case") == before


# ---------------------------------------------------------------- J-14, API-120..123 (T-113)


def test_tc_06_01_j14_overdue_after_time_travel(
    client: httpx.Client, tokens: dict[str, dict[str, str]], synced: str
) -> None:
    """TC-06.01 (tua giờ bằng setting + đẩy lùi mốc): `return_missing_days = 1`, kiện 41 đang về 2 ngày → J-14
    (API-123, worker thật) → `RETURN_MISSING`, hồ sơ "Quá hạn", cảnh báo `RETURN_OVERDUE` Cao; D2 đếm."""
    sup = tokens["SUPERVISOR"]
    res = client.put("/settings", json={**SETTINGS, "return_missing_days": 1}, headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    assert res.json()["return_missing_days"] == 1
    assert _psql(
        "UPDATE package SET status_changed_at = now() - interval '2 days' "
        "WHERE tracking_number = 'SPXTST0000041'"
    ) == "UPDATE 1"  # fmt: skip
    _run_recon(client, sup)
    package_id = _package_id(client, sup, "SPXTST0000041")
    alerts = _alerts(client, sup, package_id)
    assert [(a["rule"], a["severity"], a["status"]) for a in alerts] == [("RETURN_OVERDUE", "HIGH", "OPEN")]
    assert alerts[0]["allowed_status_targets"] == ["RETURN_EXPECTED", "DELIVERED"]
    assert _case(client, sup, "MISSING", "SPXRTTST000041")["status"] == "MISSING"
    detail = client.get(f"/packages/{package_id}", headers=sup).json()
    assert detail["warehouse_status"] == "RETURN_MISSING"
    assert detail["recon_alerts"][0]["br"] == "BR-12"
    report = client.get("/reports/daily", headers=sup).json()
    assert report["counts"]["returns_missing"] == 2  # 41 + kiện mẫu SPXTST0000049 của seed-demo (T-116)
    assert report["counts"]["recon_open"]["HIGH"] >= 1
    assert {a["kind"] for a in report["attention"]} >= {"RETURN_MISSING", "RECON_HIGH"}


def test_tc_06_08_12_resolve_and_not_recreated(
    client: httpx.Client, tokens: dict[str, dict[str, str]], synced: str
) -> None:
    """BR-14 (TC-06.08) + API-121 (TC-06.12, 06.17): kiện `PACKED` 25 giờ → cảnh báo TB → "Đã kiểm kệ" →
    người thứ hai `409 ALREADY_RESOLVED`; J-14 chạy lại không tạo lại (cùng `context_key`); CSKH không
    xử lý."""
    sup, admin = tokens["SUPERVISOR"], tokens["ADMIN"]
    assert _psql(
        "UPDATE package SET status_changed_at = now() - interval '25 hours' "
        "WHERE tracking_number = 'SPXTST0000010'"
    ) == "UPDATE 1"  # fmt: skip
    _run_recon(client, sup)
    package_id = _package_id(client, sup, "SPXTST0000010")
    (alert,) = [a for a in _alerts(client, sup, package_id) if a["rule"] == "PACKED_NOT_HANDED_OVER"]
    assert (alert["severity"], alert["status"]) == ("MEDIUM", "OPEN")
    assert client.post(f"/recon-alerts/{alert['id']}/resolve", json={"note": "x"},
                       headers=tokens["CSKH"]).status_code == 403  # fmt: skip
    res = client.post(f"/recon-alerts/{alert['id']}/resolve", json={"note": "Đã kiểm kệ"}, headers=sup)
    assert res.status_code == 200, res.text
    assert res.json()["resolution"]["action"] == "RESOLVE"
    res = client.post(f"/recon-alerts/{alert['id']}/resolve", json={"note": "Tôi cũng xử lý"}, headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (409, "ALREADY_RESOLVED")
    assert res.json()["error"]["details"]["resolved_by"]["display_name"] == "Nguyễn B"
    _run_recon(client, sup)
    statuses = [
        a["status"] for a in _alerts(client, sup, package_id) if a["rule"] == "PACKED_NOT_HANDED_OVER"
    ]
    assert statuses == ["RESOLVED"]
    assert _psql("SELECT count(*) FROM audit_log WHERE action = 'RECON_RESOLVE'") == "1"


# ---------------------------------------------------------------- API-80, API-82 (T-114)


def test_tc_02_37_39_retention_floor_and_confirm(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-02.37: 45 < sàn 60 → 422 `RETENTION_BELOW_MINIMUM`; TC-02.39: giảm 90 → 70 chưa xác nhận → 409 +
    `impact`; xác nhận → lưu + audit `RETENTION_REDUCED`; API-82 chỉ ADMIN; TC-P2.12."""
    admin = tokens["ADMIN"]
    got = client.get("/settings", headers=tokens["SUPERVISOR"]).json()
    assert got["retention_clip_min_days"] == 60
    res = client.put("/settings", json={**SETTINGS, "retention_clip_days": 45}, headers=admin)
    assert (res.status_code, res.json()["error"]["code"], res.json()["error"]["details"]["min"]) == (
        422, "RETENTION_BELOW_MINIMUM", 60,
    )  # fmt: skip
    res = client.put("/settings", json={**SETTINGS, "retention_clip_days": 70}, headers=admin)
    assert (res.status_code, res.json()["error"]["code"]) == (409, "RETENTION_REDUCTION_UNCONFIRMED")
    assert set(res.json()["error"]["details"]["impact"]) == {
        "clips", "clip_bytes", "raw_hours", "raw_bytes", "protected_clips", "next_run_at",
    }  # fmt: skip
    res = client.put("/settings", json={**SETTINGS, "retention_clip_days": 70, "confirm_reduction": True},
                     headers=admin)  # fmt: skip
    assert (res.status_code, res.json()["retention_clip_days"]) == (200, 70)
    assert _psql("SELECT count(*) FROM audit_log WHERE action = 'RETENTION_REDUCED'") == "1"
    params = {"retention_raw_days": 20, "retention_clip_days": 60}
    impact = client.get("/settings/retention-impact", params=params, headers=admin)
    assert impact.status_code == 200, impact.text
    assert impact.json()["next_run_at"].endswith("T19:00:00Z")
    assert (
        client.get("/settings/retention-impact", params=params, headers=tokens["SUPERVISOR"]).status_code
        == 403
    )
    assert (
        client.put("/settings", json={**SETTINGS, "confirm_reduction": True}, headers=admin).status_code
        == 200
    )


def test_tc_06_18_run_now(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-06.18: API-123 → 202 `{queued: true}` (worker queue `default` chạy J-14); J-14 đang giữ
    `recon:run` →
    `409 RECON_IN_PROGRESS`; CSKH → 403."""
    sup = tokens["SUPERVISOR"]
    assert client.post("/recon/run", headers=tokens["CSKH"]).status_code == 403
    deadline = time.monotonic() + 30
    res = client.post("/recon/run", headers=sup)
    while res.status_code == 409 and time.monotonic() < deadline:  # lượt J-14 trước còn chạy
        time.sleep(1)
        res = client.post("/recon/run", headers=sup)
    assert (res.status_code, res.json()) == (202, {"queued": True}), res.text
    time.sleep(3)
    assert _redis("SET", "recon:run", "qa", "EX", "30") == "OK"
    try:
        res = client.post("/recon/run", headers=sup)
        assert (res.status_code, res.json()["error"]["code"]) == (409, "RECON_IN_PROGRESS")
    finally:
        _redis("DEL", "recon:run")
