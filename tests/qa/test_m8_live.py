"""QA M8 (hồ sơ khiếu nại + bảo vệ bằng chứng) trên stack dev thật — camera giả, J-01 / J-16 chạy thật.

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa -m qa -k m8` (tự `qa-reset.sh --mute-cam2`).
Cần stack dev đầy đủ (mediamtx, fake-cam1/2, vision, worker, worker-export, beat).
Phủ T-110, T-111, T-119, T-112: phiên hoàn "Hộp rỗng" → hồ sơ khiếu nại tự tạo (phiên PACK + RETURN + ảnh) →
API-133 đổi trạng thái → API-136 gói zip encode thật trong `worker-export` (SHA-256 clip gốc khớp DB,
`info.json` L5, `ket-luan.json`) → API-31 `protection`; API-105 mở phiên chưa xác định (kiện tạm `TAM-`) →
API-112 gắn đơn kiện `SPXTST0000011`. Shopee thật / camera thật: chưa test (T-3, T-4).
"""

import hashlib
import io
import json
import os
import subprocess
import time
import uuid
import zipfile
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
UNKNOWN = "SPXVN0000000888"


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
    return {
        "ADMIN": _login(client, "tst_admin"),
        "SUPERVISOR": _login(client, "tst_sup"),
        "CSKH": _login(client, "tst_cskh"),
    }


def _scan(client: httpx.Client, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = client.post(
        "/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def _package_id(client: httpx.Client, headers: dict[str, str], code: str) -> str:
    items = client.get("/packages", params={"q": code}, headers=headers).json()["items"]
    return next(i["id"] for i in items if i["tracking_number"] == code)  # type: ignore[no-any-return]


def _wait_clips_ready(client: httpx.Client, headers: dict[str, str], package_id: str, n_sessions: int) -> Any:
    deadline = time.time() + 120  # J-01 (NFR-03 ≤ 60 giây)
    detail: Any = None
    while time.time() < deadline:
        detail = client.get(f"/packages/{package_id}", headers=headers).json()
        clips = [c for s in detail["sessions"][:n_sessions] for c in s["clips"]]
        if len(clips) == 2 * n_sessions and all(c["status"] == "READY" for c in clips):
            return detail
        time.sleep(3)
    raise AssertionError(f"clip chưa READY: {detail}")


def _inspect(client: httpx.Client, headers: dict[str, str], session: dict[str, Any], conclusion: str) -> None:
    lines = [
        {"order_item_id": line["order_item_id"], "quantity_received": 0, "condition": "MISSING_ITEM"}
        for line in session["inspection"]["lines"]
    ]
    res = client.put(
        f"/station/sessions/{session['id']}/inspection",
        headers=headers,
        json={"conclusion": conclusion, "note": "QA M8", "lines": lines},
    )
    assert res.status_code == 200, res.text


@pytest.fixture(scope="module")
def desk(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    """TST Station 01: đóng gói SPXTST0000012 (clip thật từ camera giả), bàn giao tay, chuyển nhận hoàn."""
    stations = client.get("/stations", headers=tokens["ADMIN"]).json()["items"]
    station = next(s for s in stations if s["name"] == "TST Station 01")
    assert (
        client.patch(f"/stations/{station['id']}", json={"kind": "BOTH"}, headers=tokens["ADMIN"]).status_code
        == 200
    )
    st = _login(client, "tst_station01", "STATION")
    assert _scan(client, st, "SPXTST0000012")["outcome"] == "SESSION_OPENED"
    time.sleep(8)
    assert _scan(client, st, "SPXTST0000012")["outcome"] == "SESSION_COMPLETED"
    package_id = _package_id(client, tokens["ADMIN"], "SPXTST0000012")
    _wait_clips_ready(client, tokens["ADMIN"], package_id, 1)
    adjust = client.post(
        f"/packages/{package_id}/warehouse-status",
        json={"to_status": "HANDED_OVER", "reason": "QA M8 bàn giao tay"},
        headers=tokens["SUPERVISOR"],
    )
    assert adjust.status_code == 200, adjust.text
    assert client.put("/station/work-mode", json={"work_mode": "RETURN"}, headers=st).status_code == 200
    assert client.put("/station/operator", json={"name": "Lan QA"}, headers=st).status_code == 200
    return {"headers": st, "package_id": package_id}


@pytest.fixture(scope="module")
def claim(client: httpx.Client, desk: dict[str, Any], tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    """UC-02 + BR-08: quét mã đơn → chụp 2 ảnh → "Hộp rỗng" → quét mã gốc đóng → hồ sơ khiếu nại tự tạo."""
    opened = _scan(client, desk["headers"], "2410TST00012")
    assert opened["outcome"] == "SESSION_OPENED", opened
    session = opened["state"]["session"]
    for _ in range(2):
        shot = client.post(f"/station/sessions/{session['id']}/snapshots", headers=desk["headers"])
        assert shot.status_code == 201, shot.text
    _inspect(client, desk["headers"], session, "EMPTY_BOX")
    closed = _scan(client, desk["headers"], "SPXTST0000012")
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    code = closed["closed_session"]["claim_code"]
    assert code is not None
    assert code.startswith("KN-")
    listing = client.get("/claims", params={"q": code}, headers=tokens["CSKH"]).json()
    assert listing["total"] == 1
    _wait_clips_ready(client, tokens["ADMIN"], desk["package_id"], 2)  # clip phiên RETURN
    return {"id": listing["items"][0]["id"], "code": code, "return_session_id": session["id"]}


def test_auto_claim_has_pack_and_return_evidence(
    client: httpx.Client, claim: dict[str, Any], desk: dict[str, Any], tokens: dict[str, dict[str, str]]
) -> None:
    """TC-04.21, AC-06, FR-08.06: loại Hộp rỗng, Sàn, tự tạo; bằng chứng = phiên PACK + RETURN + ảnh; không
    thiếu; API-15 `claim_code`."""
    detail = client.get(f"/claims/{claim['id']}", headers=tokens["CSKH"]).json()
    assert (detail["type"], detail["counterparty"], detail["source"], detail["status"]) == (
        "EMPTY_BOX", "PLATFORM", "AUTO_RETURN", "NEW"
    )  # fmt: skip
    sessions = [e["session"] for e in detail["evidence"] if e["kind"] == "SESSION"]
    assert [s["type"] for s in sessions] == ["PACK", "RETURN"]
    assert all(c["status"] == "READY" for s in sessions for c in s["clips"])
    kinds = sorted(e["snapshot"]["kind"] for e in detail["evidence"] if e["kind"] == "SNAPSHOT")
    assert kinds in (["MANUAL", "MANUAL", "PACK_CLOSE"], ["MANUAL", "MANUAL"])  # J-17 có thể chưa xong
    assert detail["missing"] == []
    recent = client.get("/station/sessions/recent", headers=desk["headers"]).json()["items"][0]
    assert recent["claim_code"] == claim["code"]


def test_claim_status_flow(
    client: httpx.Client, claim: dict[str, Any], tokens: dict[str, dict[str, str]]
) -> None:
    """TC-08.01 (một phần), D17: nhận phụ trách → Đã gửi (mã sàn); version tăng; WON từ NEW bị chặn."""
    detail = client.get(f"/claims/{claim['id']}", headers=tokens["CSKH"]).json()
    me = client.get("/me", headers=tokens["CSKH"]).json()["id"]
    bad = client.patch(
        f"/claims/{claim['id']}", headers=tokens["CSKH"], json={"version": detail["version"], "status": "WON"}
    )
    assert (bad.status_code, bad.json()["error"]["code"]) == (409, "INVALID_TRANSITION")
    res = client.patch(
        f"/claims/{claim['id']}",
        headers=tokens["CSKH"],
        json={
            "version": detail["version"],
            "owner_user_id": me,
            "status": "SUBMITTED",
            "platform_claim_ref": "SPE-QA8",
        },
    )
    assert res.status_code == 200, res.text
    out = res.json()
    assert (out["status"], out["owner"]["id"], out["version"]) == ("SUBMITTED", me, detail["version"] + 1)


def test_evidence_pack_zip(
    client: httpx.Client, claim: dict[str, Any], tokens: dict[str, dict[str, str]]
) -> None:
    """TC-08.16, AC-25, NFR-34 (máy dev): gói zip encode thật ≤ 3 phút; clip gốc SHA-256 = DB; video ghép;
    `info.json` L5; `ket-luan.json`; SHA-256 zip = API-137."""
    created = client.post(f"/claims/{claim['id']}/evidence-packs", headers=tokens["CSKH"])
    assert created.status_code == 202, created.text
    pack_id = created.json()["id"]
    started = time.monotonic()
    status: Any = None
    while time.monotonic() - started < 240:
        status = client.get(f"/evidence-packs/{pack_id}", headers=tokens["CSKH"]).json()
        if status["status"] in ("READY", "FAILED"):
            break
        time.sleep(2)
    assert status["status"] == "READY", status
    elapsed = time.monotonic() - started
    assert elapsed < 180, elapsed
    zipped = httpx.get(f"{BASE}{status['files']['zip']}", timeout=60)
    assert zipped.status_code == 200
    assert hashlib.sha256(zipped.content).hexdigest() == status["sha256"]
    zf = zipfile.ZipFile(io.BytesIO(zipped.content))
    names = zf.namelist()
    pack_dir = next(n.rsplit("/", 1)[0] for n in names if "/01-dong-goi-" in n)
    ret_dir = next(n.rsplit("/", 1)[0] for n in names if "/02-mo-hoan-" in n)
    detail = client.get(f"/claims/{claim['id']}", headers=tokens["CSKH"]).json()
    db_sha = {
        (e["session"]["type"], c["camera_role"]): c["sha256"]
        for e in detail["evidence"]
        if e["kind"] == "SESSION"
        for c in e["session"]["clips"]
    }
    for folder, kind in ((pack_dir, "PACK"), (ret_dir, "RETURN")):
        for role in ("CAM1", "CAM2"):
            assert hashlib.sha256(zf.read(f"{folder}/goc-{role}.mp4")).hexdigest() == db_sha[(kind, role)]
        video = zf.read(f"{folder}/video-ghep-co-chu.mp4")
        assert len(video) > 10_000
        assert video[4:8] == b"ftyp"
    info = json.loads(zf.read(f"{ret_dir}/info.json"))
    assert (info["session_type"], info["session_status"], info["operator_name"]) == (
        "RETURN",
        "COMPLETED",
        "Lan QA",
    )
    assert [c["camera_role"] for c in info["cameras"]] == ["CAM1", "CAM2"]
    assert json.loads(zf.read(f"{ret_dir}/ket-luan.json"))["conclusion"] == "EMPTY_BOX"
    assert {f"{ret_dir}/anh-01.jpg", f"{ret_dir}/anh-02.jpg"} <= set(names)
    summary = json.loads(zf.read(f"{claim['code']}/ho-so.json"))
    assert (summary["code"], summary["missing"]) == (claim["code"], [])


def test_protection_on_package_detail(
    client: httpx.Client, desk: dict[str, Any], claim: dict[str, Any], tokens: dict[str, dict[str, str]]
) -> None:
    """TC-02.30 (phần API-31), FR-02.09: clip phiên PACK + RETURN được bảo vệ bởi hồ sơ (`CLAIM`) và hồ sơ
    hàng hoàn vừa nhận (`RETURN_CASE`, +7 ngày); `retention_until` null; API-42 CSKH 403 (DEC-209)."""
    detail = client.get(f"/packages/{desk['package_id']}", headers=tokens["CSKH"]).json()
    for session in detail["sessions"][:2]:
        assert session["protected_by_claims"][0]["code"] == claim["code"]
        for clip in session["clips"]:
            assert clip["protected_by_claim"] is True
            assert set(clip["protection"]["reasons"]) == {"CLAIM", "RETURN_CASE"}
            assert clip["retention_until"] is None
    clip_id = detail["sessions"][0]["clips"][0]["id"]
    assert (
        client.put(f"/clips/{clip_id}/hold", json={"held": True}, headers=tokens["CSKH"]).status_code == 403
    )


def test_unidentified_then_link_order(
    client: httpx.Client, desk: dict[str, Any], claim: dict[str, Any], tokens: dict[str, dict[str, str]]
) -> None:
    """TC-04.08 / 04.09 (API-105 chưa xác định → kiện tạm `TAM-`), TC-07.33 (API-112 gắn đơn kiện
    SPXTST0000011): hồ sơ gắn đơn `UNANNOUNCED`, kiện 011 `RETURN_RECEIVED_ISSUE`, hồ sơ khiếu nại sang kiện
    011."""
    assert _scan(client, desk["headers"], UNKNOWN)["alert"]["code"] == "RETURN_NOT_FOUND"
    opened = client.post(
        "/station/return-sessions",
        headers=desk["headers"],
        json={"unidentified_code": UNKNOWN, "client_scan_id": str(uuid.uuid4())},
    )
    assert opened.status_code == 200, opened.text
    session = opened.json()["state"]["session"]
    assert session["return_case"]["kind"] == "UNIDENTIFIED"
    assert "UNIDENTIFIED" in session["flags"]
    _inspect(client, desk["headers"], session, "DAMAGED")
    closed = _scan(client, desk["headers"], UNKNOWN)
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    claim_code = closed["closed_session"]["claim_code"]
    assert claim_code is not None
    cases = client.get("/returns", params={"tab": "UNIDENTIFIED"}, headers=tokens["SUPERVISOR"]).json()[
        "items"
    ]
    case = next(c for c in cases if c["code"] == session["return_case"]["code"])
    placeholder = case["packages"][0]["tracking_number"]
    assert placeholder.startswith("TAM-")
    found = client.get("/packages", params={"q": placeholder}, headers=tokens["SUPERVISOR"]).json()["items"]
    assert found[0]["is_placeholder"] is True
    target = _package_id(client, tokens["SUPERVISOR"], "SPXTST0000011")
    assert (
        client.post(
            f"/returns/{case['id']}/link-order", headers=tokens["CSKH"], json={"package_id": target}
        ).status_code
        == 403
    )

    res = client.post(
        f"/returns/{case['id']}/link-order", headers=tokens["SUPERVISOR"], json={"package_id": target}
    )

    assert res.status_code == 200, res.text
    body = res.json()
    assert (body["kind"], body["order"]["platform_order_sn"], body["merged_into"]) == (
        "UNANNOUNCED",
        "2410TST00011",
        None,
    )
    detail = client.get(f"/packages/{target}", headers=tokens["SUPERVISOR"]).json()
    assert detail["warehouse_status"] == "RETURN_RECEIVED_ISSUE"
    assert detail["timeline"][-1]["source"] == "MANUAL"
    moved = client.get("/claims", params={"q": "SPXTST0000011"}, headers=tokens["CSKH"]).json()["items"]
    assert [c["code"] for c in moved] == [claim_code]
    assert (
        client.get("/packages", params={"q": placeholder}, headers=tokens["SUPERVISOR"]).json()["total"] == 0
    )
