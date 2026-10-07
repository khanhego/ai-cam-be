"""QA M7 (nhận hàng hoàn tại station + hardening) trên stack dev thật — UC-02 qua HTTP với camera giả.

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa -m qa -k m7` (tự `qa-reset.sh --mute-cam2`).
Cần stack dev đầy đủ (mediamtx, fake-cam1/2, vision, worker, beat — J-01, J-07, J-17 chạy thật).
Phủ T-104, T-107, T-108, T-117, T-109: mở phiên RETURN (mã gốc / mã đơn / tra sàn mock), ảnh API-103 + API-106
từ fake-cam1, API-102, đóng bằng mã cùng hồ sơ, API-110 / 111, tự hoàn tất quá giờ (J-07, ngưỡng hạ qua psql),
ảnh lúc đóng gói (J-17), PACK chặn kiện hoàn. Shopee thật / camera thật: chưa test (T-3, T-4).
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
    subprocess.run([str(ROOT / "scripts/qa-reset.sh"), "--mute-cam2"], check=True, capture_output=True)  # noqa: S603
    time.sleep(10)  # MediaMTX bắt đầu ghi path camera vừa seed (J-01 cần video)


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
    return {"ADMIN": _login(client, "tst_admin"), "SUPERVISOR": _login(client, "tst_sup")}


def _scan(client: httpx.Client, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = client.post(
        "/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def _package_id(client: httpx.Client, headers: dict[str, str], code: str) -> str:
    items = client.get("/packages", params={"q": code}, headers=headers).json()["items"]
    return next(i["id"] for i in items if i["tracking_number"] == code)  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def desk(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    """TST Station 01 (Cam 1 / Cam 2 giả): đóng gói SPXTST0000012 ở chế độ PACK, bàn giao tay (API-122), rồi
    chuyển "Cả hai" → chế độ nhận hoàn (PRE-8; người kiểm nhập ở TC-04.03)."""
    stations = client.get("/stations", headers=tokens["ADMIN"]).json()["items"]
    station = next(s for s in stations if s["name"] == "TST Station 01")
    res = client.patch(f"/stations/{station['id']}", json={"kind": "BOTH"}, headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    st = _login(client, "tst_station01", "STATION")
    assert _scan(client, st, "SPXTST0000012")["outcome"] == "SESSION_OPENED"
    time.sleep(8)
    closed = _scan(client, st, "SPXTST0000012")
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    package_id = _package_id(client, tokens["ADMIN"], "SPXTST0000012")
    deadline = time.time() + 90  # J-01 cắt clip phiên PACK (NFR-03 ≤ 60 giây) → `pack_reference.clips`
    while time.time() < deadline:
        clips = client.get(f"/packages/{package_id}", headers=tokens["ADMIN"]).json()["sessions"][0]["clips"]
        if len(clips) == 2 and all(c["status"] == "READY" for c in clips):
            break
        time.sleep(3)
    adjust = client.post(
        f"/packages/{package_id}/warehouse-status",
        json={"to_status": "HANDED_OVER", "reason": "QA M7 bàn giao tay"},
        headers=tokens["SUPERVISOR"],
    )
    assert adjust.status_code == 200, adjust.text
    assert client.put("/station/work-mode", json={"work_mode": "RETURN"}, headers=st).status_code == 200
    return {"headers": st, "station": station, "pack_closed": closed}


def test_pack_close_returns_closed_session(desk: dict[str, Any]) -> None:
    """TC-03.72 / FR-03.14 (phần API): `closed_session` PACK, Cam 2 tắt ROI → `CAM2_UNVERIFIED`."""
    closed = desk["pack_closed"]["closed_session"]
    assert (closed["type"], closed["tracking_number"], closed["package_status"]) == (
        "PACK",
        "SPXTST0000012",
        "PACKED",
    )
    assert "CAM2_UNVERIFIED" in closed["flags"]


def test_tc_04_03_operator_required(client: httpx.Client, desk: dict[str, Any]) -> None:
    """TC-04.03, BR-28: chưa nhập người kiểm → `OPERATOR_REQUIRED`."""
    body = _scan(client, desk["headers"], "SPXTST0000012")
    assert body["alert"]["code"] == "OPERATOR_REQUIRED"
    res = client.put("/station/operator", json={"name": "Lan QA"}, headers=desk["headers"])
    assert res.status_code == 200


def test_tc_04_08_10_alerts(client: httpx.Client, desk: dict[str, Any]) -> None:
    """TC-04.08 (mã lạ → `RETURN_NOT_FOUND` sau tra sàn mock), TC-04.10 (kiện `PACKED` → `NOT_SHIPPED`)."""
    started = time.monotonic()
    unknown = _scan(client, desk["headers"], "SPXVN0000000000")
    assert time.monotonic() - started < 3
    assert unknown["alert"]["code"] == "RETURN_NOT_FOUND"
    assert unknown["alert"]["data"]["can_open_unidentified"] is True
    assert _scan(client, desk["headers"], "SPXTST0000010")["alert"]["code"] == "NOT_SHIPPED"


def test_tc_04_45_lookup(client: httpx.Client, desk: dict[str, Any]) -> None:
    """TC-04.45 / 04.46 (API-104)."""
    res = client.get("/station/return-lookup", params={"q": "2410TST0001"}, headers=desk["headers"])
    assert res.status_code == 200, res.text
    by_code = {i["tracking_number"]: i for i in res.json()["items"]}
    assert by_code["SPXTST0000012"]["can_open"] is True
    short = client.get("/station/return-lookup", params={"q": "241"}, headers=desk["headers"])
    assert short.status_code == 422


@pytest.fixture(scope="module")
def inspecting(client: httpx.Client, desk: dict[str, Any]) -> dict[str, Any]:
    """UC-02: quét mã đơn → R2 (≤ 1 giây), hồ sơ "Về trước khi sàn báo", có tham chiếu phiên đóng gói."""
    started = time.monotonic()
    body = _scan(client, desk["headers"], "2410TST00012")
    elapsed = time.monotonic() - started
    assert body["outcome"] == "SESSION_OPENED", body
    return {"session": body["state"]["session"], "state": body["state"], "elapsed": elapsed}


def test_tc_04_06_open_by_order_code(inspecting: dict[str, Any]) -> None:
    """TC-04.06, AC-22, NFR-01 (một lần đo trên máy dev): R2 + hồ sơ + `pack_reference` có clip."""
    session = inspecting["session"]
    assert inspecting["state"]["state"] == "INSPECTING"
    assert inspecting["elapsed"] < 1.0
    assert session["type"] == "RETURN"
    assert session["operator_name"] == "Lan QA"
    assert session["return_case"]["kind"] == "UNANNOUNCED"
    assert session["return_case"]["code"].startswith("HH-")
    assert session["inspection"]["lines_mode"] == "FULL"
    assert [line["quantity_requested"] for line in session["inspection"]["lines"]] == [2, 1, 1]
    assert session["pack_reference"] is not None
    assert {c["camera_role"] for c in session["pack_reference"]["clips"]} == {"CAM1", "CAM2"}
    assert "NO_PACK_CLIP" not in session["flags"]


def test_tc_04_40_snapshot_from_fake_cam(
    client: httpx.Client, desk: dict[str, Any], inspecting: dict[str, Any]
) -> None:
    """TC-04.40, NFR-32 (đo trên fake-cam1): nhấn F2 3 lần — mỗi ảnh 201 < 2 giây (T-121: khung mới nhất
    vision giữ trong Redis, không mở RTSP mỗi lần), JPEG thật, file 0444."""
    session_id = inspecting["session"]["id"]
    timings = []
    for _ in range(3):
        started = time.monotonic()
        res = client.post(f"/station/sessions/{session_id}/snapshots", headers=desk["headers"])
        timings.append(round(time.monotonic() - started, 3))
        assert res.status_code == 201, res.text
    print(f"TC-04.40 API-103 (giây): {timings}")
    assert max(timings) < 2.0, timings
    shot = res.json()["snapshot"]
    assert len(shot["sha256"]) == 64
    image = client.get(shot["url"].removeprefix("/api/v1"))
    assert image.status_code == 200
    assert image.content[:2] == b"\xff\xd8"
    path = _psql(f"SELECT path FROM snapshot WHERE id = '{shot['id']}'")  # noqa: S608
    mode = subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "api", "stat", "-c", "%a", f"/data/video/{path}"],
        capture_output=True, text=True, check=False,
    ).stdout.strip()  # fmt: skip
    assert mode == "444"


def test_tc_04_16_19_conclude_and_close(
    client: httpx.Client, desk: dict[str, Any], inspecting: dict[str, Any], tokens: dict[str, dict[str, str]]
) -> None:
    """TC-04.16 (BR-22 chặn), TC-04.18 (BR-07), TC-04.19 / 04.21 (đóng bằng mã cùng hồ sơ, kết luận có vấn
    đề), API-110 / 111."""
    session = inspecting["session"]
    lines = [
        {
            "order_item_id": line["order_item_id"],
            "quantity_received": line["quantity_requested"],
            "condition": "OK",
        }
        for line in session["inspection"]["lines"]
    ]
    assert _scan(client, desk["headers"], "SPXTST0000012")["alert"]["code"] == "INSPECTION_REQUIRED"
    lines[0]["quantity_received"] = 1
    bad = client.put(f"/station/sessions/{session['id']}/inspection", headers=desk["headers"],
                     json={"conclusion": "OK", "lines": lines})  # fmt: skip
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "CONCLUSION_INCONSISTENT"
    lines[0]["condition"] = "MISSING_ITEM"
    ok = client.put(f"/station/sessions/{session['id']}/inspection", headers=desk["headers"],
                    json={"conclusion": "MISSING_ITEM", "note": "QA thiếu 1 áo", "lines": lines})  # fmt: skip
    assert ok.status_code == 200, ok.text

    started = time.monotonic()
    closed = _scan(client, desk["headers"], "SPXTST0000012")
    assert time.monotonic() - started < 1.0
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    info = closed["closed_session"]
    assert (info["type"], info["conclusion"], info["package_status"]) == (
        "RETURN", "MISSING_ITEM", "RETURN_RECEIVED_ISSUE",
    )  # fmt: skip
    assert info["return_case_status"] == "RECEIVED_ISSUE"
    assert closed["state"]["today_return_issue_count"] == 1

    listing = client.get("/returns", params={"tab": "RECEIVED"}, headers=tokens["SUPERVISOR"]).json()
    case = next(i for i in listing["items"] if i["code"] == session["return_case"]["code"])
    detail = client.get(f"/returns/{case['id']}", headers=tokens["SUPERVISOR"]).json()
    assert detail["sessions"][0]["conclusion"] == "MISSING_ITEM"
    assert detail["sessions"][0]["operator_name"] == "Lan QA"
    recent = client.get("/station/sessions/recent", headers=desk["headers"]).json()["items"][0]
    assert (recent["type"], recent["conclusion"]) == ("RETURN", "MISSING_ITEM")


def test_tc_04_26_auto_close_overdue(client: httpx.Client, desk: dict[str, Any]) -> None:
    """TC-04.26 (EX-R15, DEC-253) trên stack thật: ngưỡng 1 / 2 phút qua psql, J-07 (30 giây) tự hoàn tất
    phiên có kết luận đã lưu; kiện `NEW` của đơn sàn đã giao mở được (EX-R3, cờ `NO_PACK_CLIP`)."""
    assert _psql("UPDATE setting SET return_warn_minutes = 1, return_abandon_minutes = 2") == "UPDATE 1"
    try:
        opened = _scan(client, desk["headers"], "SPXTST0000041")
        assert opened["outcome"] == "SESSION_OPENED", opened
        session = opened["state"]["session"]
        assert "NO_PACK_CLIP" in session["flags"]
        lines = [{"order_item_id": li["order_item_id"], "quantity_received": li["quantity_requested"],
                  "condition": "OK"} for li in session["inspection"]["lines"]]  # fmt: skip
        saved = client.put(f"/station/sessions/{session['id']}/inspection", headers=desk["headers"],
                           json={"conclusion": "OK", "lines": lines})  # fmt: skip
        assert saved.status_code == 200, saved.text
        deadline = time.time() + 210  # quá 2 phút + nhịp J-07 30 giây
        status = ""
        while time.time() < deadline:
            sql = "SELECT status || ',' || array_to_string(flags, '|') FROM session WHERE id = '{}'"
            status = _psql(sql.format(session["id"]))
            if status.startswith("COMPLETED"):
                break
            time.sleep(5)
        assert status.startswith("COMPLETED"), status
        assert "AUTO_CLOSED" in status
        state = client.get("/station/state", headers=desk["headers"]).json()
        assert state["state"] == "READY"
        assert (
            _psql("SELECT warehouse_status FROM package WHERE tracking_number = 'SPXTST0000041'")
            == "RETURN_RECEIVED_OK"
        )
    finally:
        _psql("UPDATE setting SET return_warn_minutes = 20, return_abandon_minutes = 45")


def test_tc_02_42_pack_snapshot(desk: dict[str, Any]) -> None:
    """TC-02.42, AC-31, L8: J-01 → J-17 trích ảnh Cam 1 lúc đóng gói (`PACK_CLOSE`) cho phiên PACK của 012."""
    deadline = time.time() + 120
    found = ""
    while time.time() < deadline:
        found = _psql(
            "SELECT s.status FROM snapshot s JOIN session p ON p.id = s.session_id "
            "WHERE s.kind = 'PACK_CLOSE' AND p.open_code = 'SPXTST0000012' AND p.type = 'PACK'"
        )
        if found == "READY":
            break
        time.sleep(5)
    assert found == "READY"


def test_tc_04_53_return_package_at_pack_desk(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-04.53, EX-R16: kiện đã nhận hoàn quét ở bàn đóng gói (TST Station 02 chế độ PACK) → không 500."""
    st2 = _login(client, "tst_station02", "STATION")
    body = _scan(client, st2, "SPXTST0000012")
    assert body["outcome"] == "ALERT"
    assert body["alert"]["code"] == "ALREADY_HANDED_OVER"
    assert body["alert"]["data"]["is_return"] is True
