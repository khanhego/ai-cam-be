"""QA phạm vi M3 (Cam 2 + duyệt) trên stack dev thật: fake-cam2 → MediaMTX → vision → Redis → api → WS.

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa/test_m3_live.py -v`
Cần stack dev đầy đủ (mediamtx, fake-cam2, vision). Bắt đầu bằng `scripts/qa-reset.sh`.
fake-cam2 phát vòng 60 giây: 0–20 giây phiếu SPXTST0000001 · 20–25 trống · 25–45 SPXTST0000002 ·
45–50 hai phiếu …002 + …003 · 50–60 trống. Tỉ lệ đọc / độ trễ với phiếu thật (AC-04) cần camera thật (T-4).
Mỗi test ghi mã TC trong docstring.
"""

import json
import os
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

BASE = os.environ.get("QA_BASE_URL")
pytestmark = [
    pytest.mark.qa,
    pytest.mark.skipif(not BASE, reason="đặt QA_BASE_URL để chạy QA trên stack thật"),
]

ROOT = Path(__file__).resolve().parents[2]
PASSWORD = "matkhau123"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker/compose.dev.yml")]
CYCLE_S = 60


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
    time.sleep(15)  # MediaMTX kéo path camera vừa seed, vision nạp lại danh sách camera (≤ 10 giây)


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, str]:
    out = {}
    for role, username, kind in [
        ("ADMIN", "tst_admin", "DASHBOARD"),
        ("SUPERVISOR", "tst_sup", "DASHBOARD"),
        ("CSKH", "tst_cskh", "DASHBOARD"),
        ("STATION", "tst_station01", "STATION"),
        ("STATION2", "tst_station02", "STATION"),
    ]:
        res = client.post("/auth/login", json={"username": username, "password": PASSWORD, "client": kind})
        assert res.status_code == 200, res.text
        out[role] = res.json()["access_token"]
    return out


def _h(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _scan(client: httpx.Client, token: str, code: str) -> dict[str, Any]:
    res = client.post(
        "/station/scan", headers=_h(token), json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def _state(client: httpx.Client, token: str) -> dict[str, Any]:
    res = client.get("/station/state", headers=_h(token))
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def _wait(predicate: Callable[[], Any], timeout: float, step: float = 0.2) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(step)
    raise AssertionError(f"Hết {timeout} giây vẫn chưa đạt điều kiện")


def _wait_tray(client: httpx.Client, token: str, codes: list[str], timeout: float = CYCLE_S + 15) -> float:
    """Chờ khay chuyển SANG `codes` (bỏ qua nếu đang sẵn ở đó giữa chừng) — trả thời điểm thấy."""
    _wait(lambda: _state(client, token)["tray"]["codes"] != codes, timeout)
    _wait(lambda: _state(client, token)["tray"]["codes"] == codes, timeout)
    return time.monotonic()


class WsRecorder:
    """Ghi mọi sự kiện WS (kèm giờ nhận) ở thread nền."""

    def __init__(self, path: str, token: str) -> None:
        url = f"{BASE.replace('http', 'ws', 1)}/ws/{path}?token={token}"  # type: ignore[union-attr]
        self.conn = connect(url, open_timeout=10, legacy=True)
        self.events: list[tuple[float, dict[str, Any]]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            for raw in self.conn:
                self.events.append((time.monotonic(), json.loads(raw)))
        except ConnectionClosed:
            pass

    def of_type(self, kind: str) -> list[tuple[float, dict[str, Any]]]:
        return [(t, e) for t, e in list(self.events) if e["type"] == kind]

    def close(self) -> None:
        self.conn.close()
        self._thread.join(5)


# ---------------------------------------------------------------- Cam 2 (T-12)


def test_tray_follows_fake_cam2(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.20 phần đọc mã trên camera giả: API-10 `tray` đi đúng vòng fake-cam2 (S1, không phiên)."""
    seen: list[list[str]] = []
    deadline = time.monotonic() + CYCLE_S + 10
    while time.monotonic() < deadline and len(seen) < 6:
        codes = _state(client, tokens["STATION"])["tray"]["codes"]
        if not seen or seen[-1] != codes:
            seen.append(codes)
        time.sleep(0.2)
    assert ["SPXTST0000001"] in seen
    assert ["SPXTST0000002"] in seen
    assert ["SPXTST0000002", "SPXTST0000003"] in seen
    assert [] in seen


def test_open_while_tray_shows_other_label(client: httpx.Client, tokens: dict[str, str]) -> None:
    """BR-06, DEC-111: khay đang có phiếu …002 → quét mở …001 → MISMATCH nguồn CAM2 ngay; hủy phiên."""
    st = tokens["STATION"]
    _wait_tray(client, st, ["SPXTST0000002"])

    res = _scan(client, st, "SPXTST0000001")

    assert res["outcome"] == "MISMATCH", res
    assert res["state"]["tray"]["match"] == "DIFFERENT"
    assert res["state"]["session"]["mismatch"] == {
        "source": "CAM2", "expected": "SPXTST0000001", "actual": "SPXTST0000002",
    }  # fmt: skip
    cancel = client.post(
        f"/station/sessions/{res['state']['session']['id']}/cancel",
        headers=_h(st),
        json={"reason": "WRONG_SCAN"},
    )
    assert cancel.status_code == 200, cancel.text
    assert cancel.json()["state"]["state"] == "READY"


def test_cam2_cycle_mismatch_then_clear_then_close(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.21..03.23 trên camera giả (BR-06, FR-03.06, 03.07, BR-18).

    Mở …001 khi khay thấy …001 (MATCH) → khay đổi …002: WS station.state MISMATCH CAM2 (trong cùng sự kiện
    khay đổi) → quét đúng …001 hai lần vẫn MISMATCH → khay trống: phiên về OPEN → quét …001: COMPLETED,
    không cờ Cam 2.
    """
    st = tokens["STATION"]
    ws = WsRecorder("station", st)
    try:
        _wait_tray(client, st, ["SPXTST0000001"])
        opened = _scan(client, st, "SPXTST0000001")
        assert opened["outcome"] == "SESSION_OPENED", opened
        assert opened["state"]["tray"]["match"] == "MATCH"

        def mismatch_event() -> tuple[float, dict[str, Any]] | None:
            for t, e in ws.of_type("station.state"):
                if e["data"]["state"] == "MISMATCH":
                    return t, e["data"]
            return None

        t_mismatch, data = _wait(mismatch_event, 40)
        assert data["session"]["mismatch"] == {
            "source": "CAM2", "expected": "SPXTST0000001", "actual": "SPXTST0000002",
        }  # fmt: skip
        assert data["tray"]["match"] == "DIFFERENT"
        # Khay trống trước đó (giây 20 → 25 của vòng): thời điểm WS NOT_SEEN → MISMATCH ≈ 5 giây − khử nhiễu.
        t_empty = max(t for t, e in ws.of_type("station.state") if e["data"]["tray"]["match"] == "NOT_SEEN")
        gap = t_mismatch - t_empty
        assert 3.0 <= gap <= 5.5, gap

        for _ in range(2):  # TC-03.22
            again = _scan(client, st, "SPXTST0000001")
            assert (again["outcome"], again["state"]["session"]["mismatch"]["source"]) == ("MISMATCH", "CAM2")

        # Giây 45–50: 2 phiếu → vẫn MISMATCH; giây 50: khay trống → OPEN (TC-03.23)
        def reopened() -> float | None:
            for t, e in ws.of_type("station.state"):
                if t > t_mismatch and e["data"]["state"] == "PACKING":
                    return t
            return None

        _wait(reopened, 35)
        assert any(
            e["data"]["tray"]["match"] == "MULTIPLE" and e["data"]["state"] == "MISMATCH"
            for t, e in ws.of_type("station.state")
            if t > t_mismatch
        )
        done = _scan(client, st, "SPXTST0000001")
        assert done["outcome"] == "SESSION_COMPLETED", done
        flags = _psql("SELECT flags FROM session WHERE open_code = 'SPXTST0000001' AND status = 'COMPLETED'")
        assert "HAD_MISMATCH" in flags
        assert "CAM2_UNVERIFIED" not in flags
    finally:
        ws.close()


def test_vision_stopped_marks_unavailable(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.25: dừng vision → `tray.match` UNAVAILABLE (≤ TTL 5 giây); đóng vẫn được, cờ CAM2_UNVERIFIED."""
    st = tokens["STATION"]
    subprocess.run([*COMPOSE, "stop", "vision"], check=True, capture_output=True)  # noqa: S603
    try:
        _wait(lambda: _state(client, st)["tray"]["match"] == "UNAVAILABLE", 10)
        assert _scan(client, st, "SPXTST0000017")["outcome"] == "SESSION_OPENED"
        done = _scan(client, st, "SPXTST0000017")
        assert done["outcome"] == "SESSION_COMPLETED"
        assert "CAM2_UNVERIFIED" in _psql("SELECT flags FROM session WHERE open_code = 'SPXTST0000017'")
    finally:
        subprocess.run([*COMPOSE, "start", "vision"], check=True, capture_output=True)  # noqa: S603
    _wait(lambda: _state(client, st)["tray"]["match"] != "UNAVAILABLE", 30)


# ---------------------------------------------------------------- Duyệt (T-13), TST Station 02 (không camera)


def _request(client: httpx.Client, token: str, **body: Any) -> httpx.Response:
    return client.post("/station/approval-requests", headers=_h(token), json=body)


def _decide(
    client: httpx.Client, token: str, approval_id: str, action: str, note: str | None = None
) -> httpx.Response:
    return client.post(
        f"/approval-requests/{approval_id}/decision", headers=_h(token), json={"action": action, "note": note}
    )


def _wait_event(
    ws: WsRecorder, kind: str, match: Callable[[dict[str, Any]], bool], timeout: float = 2.0
) -> float:
    def found() -> float | None:
        return next((t for t, e in ws.of_type(kind) if match(e["data"])), None)

    return float(_wait(found, timeout, step=0.02))


def test_mismatch_request_continue_within_2s(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.40 (API + WS), AC-19: yêu cầu lên dashboard ≤ 2 giây; duyệt → PACKING ≤ 2 giây; audit tên."""
    st, sup = tokens["STATION2"], tokens["SUPERVISOR"]
    dash = WsRecorder("dashboard", sup)
    station_ws = WsRecorder("station", st)
    cskh = WsRecorder("dashboard", tokens["CSKH"])
    try:
        opened = _scan(client, st, "SPXTST0000021")
        assert _scan(client, st, "SPXTST0000022")["outcome"] == "MISMATCH"
        t0 = time.monotonic()
        res = _request(client, st, type="MISMATCH", session_id=opened["state"]["session"]["id"])
        assert res.status_code == 201, res.text
        apr = res.json()["approval_request"]
        assert res.json()["state"]["state"] == "WAITING_APPROVAL"
        t_created = _wait_event(dash, "approval.created", lambda d: d["id"] == apr["id"])
        assert t_created - t0 <= 2.0

        items = client.get("/approval-requests", headers=_h(sup), params={"status": "PENDING"}).json()[
            "items"
        ]
        item = next(i for i in items if i["id"] == apr["id"])
        assert item["station"]["name"] == "TST Station 02"
        assert (item["context"]["expected"], item["context"]["actual"]) == ("SPXTST0000021", "SPXTST0000022")

        t1 = time.monotonic()
        decided = _decide(client, sup, apr["id"], "CONTINUE")
        assert decided.status_code == 200, decided.text
        assert decided.json()["approval_request"]["decided_by"]["display_name"] == "Nguyễn B"
        t_state = _wait_event(station_ws, "station.state", lambda d: d["state"] == "PACKING")
        assert t_state - t1 <= 2.0
        _wait_event(dash, "approval.resolved", lambda d: d["id"] == apr["id"])
        time.sleep(1)
        assert not cskh.of_type("approval.created")  # TC-03.56: CSKH không nhận sự kiện duyệt
        assert cskh.of_type("report.updated")
    finally:
        for ws in (dash, station_ws, cskh):
            ws.close()
    logs = client.get(
        "/audit-logs", headers=_h(tokens["ADMIN"]), params={"action": "APPROVAL_DECISION"}
    ).json()["items"]
    assert any(log["object_id"] == apr["id"] and log["user"]["display_name"] == "Nguyễn B" for log in logs)
    _scan(client, st, "SPXTST0000021")  # đóng phiên


def test_assist_withdraw_then_conflicts(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.41, 03.46, 03.48, 03.49 (API): ASSIST → gửi lại 409 → rút → duyệt sau khi rút 409 WITHDRAWN."""
    st, sup = tokens["STATION2"], tokens["SUPERVISOR"]
    sid = _scan(client, st, "SPXTST0000023")["state"]["session"]["id"]
    res = _request(client, st, type="ASSIST", session_id=sid)
    assert res.status_code == 201, res.text
    apr_id = res.json()["approval_request"]["id"]
    again = _request(client, st, type="ASSIST", session_id=sid)
    assert (again.status_code, again.json()["error"]["code"]) == (409, "APPROVAL_ALREADY_PENDING")
    assert _scan(client, st, "SPXTST0000024")["outcome"] == "IGNORED"  # TC-03.50

    back = client.post(f"/station/approval-requests/{apr_id}/withdraw", headers=_h(st))
    assert back.status_code == 200, back.text
    assert back.json()["state"]["state"] == "PACKING"
    late = _decide(client, sup, apr_id, "CONTINUE")
    assert late.status_code == 409
    details = late.json()["error"]["details"]
    # DEC-60: rút yêu cầu cũng ghi `decided_at` (đồng hồ quá giờ tính lại từ mốc này), `decided_by` null.
    assert (details["status"], details["decided_by"]) == ("WITHDRAWN", None)
    assert details["decided_at"] is not None
    _scan(client, st, "SPXTST0000023")


def test_two_approvers_at_once(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.47: tst_sup CONTINUE và tst_admin CANCEL_SESSION song song → 200 + 409 ALREADY_RESOLVED."""
    st = tokens["STATION2"]
    sid = _scan(client, st, "SPXTST0000025")["state"]["session"]["id"]
    apr_id = _request(client, st, type="ASSIST", session_id=sid).json()["approval_request"]["id"]
    results: dict[str, httpx.Response] = {}
    barrier = threading.Barrier(2)

    def fire(role: str, action: str) -> None:
        with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
            barrier.wait()
            results[role] = _decide(c, tokens[role], apr_id, action)

    threads = [
        threading.Thread(target=fire, args=("SUPERVISOR", "CONTINUE")),
        threading.Thread(target=fire, args=("ADMIN", "CANCEL_SESSION")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)

    assert sorted(r.status_code for r in results.values()) == [200, 409]
    winner = next(r for r in results.values() if r.status_code == 200).json()["approval_request"]
    loser = next(r for r in results.values() if r.status_code == 409).json()["error"]
    assert loser["code"] == "ALREADY_RESOLVED"
    assert loser["details"]["decided_by"]["display_name"] == winner["decided_by"]["display_name"]
    status = _psql(f"SELECT status FROM session WHERE id = '{uuid.UUID(sid)}'")  # noqa: S608
    assert status == ("OPEN" if winner["decision"] == "CONTINUE" else "CANCELLED")
    if status == "OPEN":
        _scan(client, st, "SPXTST0000025")


def test_repack_approve_complete_supersedes(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.51 (API), AC-14, BR-03: …010 (PACKED) → REPACK → duyệt → hoàn tất → phiên cũ SUPERSEDED."""
    st, sup = tokens["STATION2"], tokens["SUPERVISOR"]
    alert = _scan(client, st, "SPXTST0000010")
    assert (alert["outcome"], alert["alert"]["code"]) == ("ALERT", "ALREADY_PACKED")
    res = _request(client, st, type="REPACK", tracking_number="SPXTST0000010")
    assert res.status_code == 201, res.text
    assert res.json()["state"]["session"] is None
    decided = _decide(client, sup, res.json()["approval_request"]["id"], "APPROVE_REPACK")
    assert decided.status_code == 200, decided.text
    state = _state(client, st)
    assert state["state"] == "PACKING"
    assert "REPACK" in state["session"]["flags"]
    sql = "SELECT string_agg(status, ',' ORDER BY started_at) FROM session WHERE open_code = 'SPXTST0000010'"
    assert _psql(sql) == "COMPLETED,OPEN"

    assert _scan(client, st, "SPXTST0000010")["outcome"] == "SESSION_COMPLETED"
    assert _psql(sql) == "SUPERSEDED,COMPLETED"
    assert _psql("SELECT warehouse_status FROM package WHERE tracking_number = 'SPXTST0000010'") == "PACKED"


def test_repack_handed_over_not_eligible(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.55: REPACK kiện …011 đã bàn giao → 409 NOT_ELIGIBLE, không tạo yêu cầu."""
    res = _request(client, tokens["STATION2"], type="REPACK", tracking_number="SPXTST0000011")
    assert (res.status_code, res.json()["error"]["code"]) == (409, "NOT_ELIGIBLE")
    assert _psql("SELECT count(*) FROM approval_request WHERE tracking_number = 'SPXTST0000011'") == "0"


def test_close_with_note_blocked_by_cam2_then_continue(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-03.43, 03.44 trên fake-cam2: khay thấy …002 khi mở phiên …026 → MISMATCH CAM2 → gửi duyệt →
    Đóng có ghi chú 409 TRAY_STILL_DIFFERENT → Cho tiếp tục → về MISMATCH CAM2 ngay → hủy phiên.
    """
    st, sup = tokens["STATION"], tokens["SUPERVISOR"]
    _wait_tray(client, st, ["SPXTST0000002"])
    opened = _scan(client, st, "SPXTST0000026")
    assert opened["outcome"] == "MISMATCH", opened
    res = _request(client, st, type="MISMATCH", session_id=opened["state"]["session"]["id"])
    assert res.status_code == 201, res.text
    apr_id = res.json()["approval_request"]["id"]
    items = client.get("/approval-requests", headers=_h(sup)).json()["items"]
    assert next(i for i in items if i["id"] == apr_id)["context"]["tray_match"] == "DIFFERENT"

    blocked = _decide(client, sup, apr_id, "CLOSE_WITH_NOTE", "QA")
    assert (blocked.status_code, blocked.json()["error"]["code"]) == (409, "TRAY_STILL_DIFFERENT")
    cont = _decide(client, sup, apr_id, "CONTINUE")
    assert cont.status_code == 200, cont.text
    state = _state(client, st)
    assert (state["state"], state["session"]["mismatch"]["source"]) == ("MISMATCH", "CAM2")
    cancel = client.post(
        f"/station/sessions/{state['session']['id']}/cancel", headers=_h(st), json={"reason": "WRONG_SCAN"}
    )
    assert cancel.status_code == 200, cancel.text
