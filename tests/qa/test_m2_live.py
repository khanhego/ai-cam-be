"""QA phạm vi M2 (video bằng chứng) trên stack dev thật: camera giả → MediaMTX → J-01 → API-30..46, 80, 81.

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa/test_m2_live.py -v`
Cần stack dev đầy đủ (mediamtx, fake-cam1/2, worker, worker-export, beat). Bắt đầu bằng `scripts/qa-reset.sh`.
Mỗi test ghi mã TC trong docstring.
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


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    subprocess.run([str(ROOT / "scripts/qa-reset.sh")], check=True, capture_output=True)  # noqa: S603
    time.sleep(15)  # MediaMTX bắt đầu ghi path camera vừa seed


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=30) as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    out = {}
    for role, username, kind in [
        ("ADMIN", "tst_admin", "DASHBOARD"),
        ("SUPERVISOR", "tst_sup", "DASHBOARD"),
        ("CSKH", "tst_cskh", "DASHBOARD"),
        ("STATION", "tst_station01", "STATION"),
    ]:
        res = client.post("/auth/login", json={"username": username, "password": PASSWORD, "client": kind})
        assert res.status_code == 200, res.text
        out[role] = {"Authorization": f"Bearer {res.json()['access_token']}"}
    return out


def _scan(client: httpx.Client, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = client.post(
        "/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def packed(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    """Phiên SPXTST0000001 dài 20 giây ở TST Station 01, chờ clip (TC-02.01 phần API)."""
    assert _scan(client, tokens["STATION"], "SPXTST0000001")["outcome"] == "SESSION_OPENED"
    time.sleep(20)
    assert _scan(client, tokens["STATION"], "SPXTST0000001")["outcome"] == "SESSION_COMPLETED"
    closed = time.time()
    found = client.get("/packages", headers=tokens["CSKH"], params={"q": "SPXTST0000001"}).json()
    package_id = found["items"][0]["id"]
    while time.time() - closed < 60:
        detail = client.get(f"/packages/{package_id}", headers=tokens["CSKH"]).json()
        clips = detail["sessions"][0]["clips"]
        if len(clips) == 2 and all(c["status"] == "READY" for c in clips):
            return {"detail": detail, "latency_s": time.time() - closed}
        time.sleep(2)
    pytest.fail("Clip không READY trong 60 giây (NFR-03)")


def test_clips_ready_within_60s(packed: dict[str, Any]) -> None:
    """TC-02.01 (API), NFR-03: 2 clip READY ≤ 60 giây, dài ≥ phiên + 10 giây đệm."""
    assert packed["latency_s"] <= 60
    for clip in packed["detail"]["sessions"][0]["clips"]:
        assert clip["duration_s"] >= 29.5
        assert len(clip["sha256"]) == 64


def test_clip_hash_and_readonly_on_disk(packed: dict[str, Any]) -> None:
    """TC-02.04, TC-02.15: SHA-256 file trên đĩa = giá trị API-31; quyền 444."""
    for clip in packed["detail"]["sessions"][0]["clips"]:
        path = subprocess.run(  # noqa: S603
            [*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "aicam", "-d", "aicam", "-tA", "-c",
             f"select path from clip where id = '{uuid.UUID(clip['id'])}'"],  # noqa: S608 — id là UUID
            capture_output=True, text=True, check=True,
        ).stdout.strip()  # fmt: skip
        out = subprocess.run(  # noqa: S603
            [*COMPOSE, "exec", "-T", "api", "sh", "-c",
             f"sha256sum /data/video/{path}; stat -c %a /data/video/{path}"],
            capture_output=True, text=True, check=True,
        ).stdout.split()  # fmt: skip
        assert out[0] == clip["sha256"]
        assert out[-1] == "444"


def test_play_url_and_range(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, Any]
) -> None:
    """TC-P.04 (station mình trong ngày ✅), API-41 Range 206, chữ ký sai 403."""
    clip_id = packed["detail"]["sessions"][0]["clips"][0]["id"]
    for role in ("CSKH", "STATION"):
        assert client.get(f"/clips/{clip_id}/play-url", headers=tokens[role]).status_code == 200
    url = client.get(f"/clips/{clip_id}/play-url", headers=tokens["CSKH"]).json()["url"]
    media = httpx.get(f"{BASE}{url}", headers={"Range": "bytes=0-1023"})
    assert (media.status_code, media.headers["content-type"]) == (206, "video/mp4")
    bad = httpx.get(f"{BASE}{url.replace('sig=', 'sig=00')}")
    assert (bad.status_code, bad.json()["error"]["code"]) == (403, "SIGNATURE_INVALID")


def test_hold_and_rebuild_permissions(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, Any]
) -> None:
    """TC-P.05: giữ clip CSKH ✅, Station ⛔; cắt lại CSKH ⛔; không có clip FAILED → 409 CLIP_NOT_FAILED."""
    session = packed["detail"]["sessions"][0]
    clip_id = session["clips"][0]["id"]
    assert (
        client.put(f"/clips/{clip_id}/hold", headers=tokens["STATION"], json={"held": True}).status_code
        == 403
    )
    held = client.put(f"/clips/{clip_id}/hold", headers=tokens["CSKH"], json={"held": True}).json()
    assert held["retention_until"] is None
    client.put(f"/clips/{clip_id}/hold", headers=tokens["CSKH"], json={"held": False})
    url = f"/sessions/{session['id']}/clips/rebuild"
    assert client.post(url, headers=tokens["CSKH"]).status_code == 403
    res = client.post(url, headers=tokens["SUPERVISOR"])
    assert (res.status_code, res.json()["error"]["code"]) == (409, "CLIP_NOT_FAILED")


def test_export_side_by_side(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, Any]
) -> None:
    """TC-07.07 (API), TC-02.14: xuất ghép READY, tải video + info.json, hash clip gốc khớp."""
    session = packed["detail"]["sessions"][0]
    res = client.post(
        f"/sessions/{session['id']}/exports", headers=tokens["CSKH"], json={"layout": "SIDE_BY_SIDE"}
    )
    assert res.status_code == 202
    export_id = res.json()["id"]
    started = time.time()
    while time.time() - started < 60:
        body = client.get(f"/exports/{export_id}", headers=tokens["CSKH"]).json()
        if body["status"] in ("READY", "FAILED"):
            break
        time.sleep(1)
    assert body["status"] == "READY"
    assert client.get(f"/exports/{export_id}", headers=tokens["SUPERVISOR"]).status_code == 404
    info = httpx.get(f"{BASE}{body['files']['info']}").json()
    assert info["source_clip_sha256"] == {c["camera_role"]: c["sha256"] for c in session["clips"]}
    video = httpx.get(f"{BASE}{body['files']['video']}")
    assert video.status_code == 200
    assert info["sha256"] == body["sha256"]


def test_daily_settings_health(
    client: httpx.Client, tokens: dict[str, dict[str, str]], packed: dict[str, Any]
) -> None:
    """API-32 (đếm phiên vừa đóng), API-80 (Supervisor chỉ đọc), API-81 (mọi thành phần OK) — TC-P.08."""
    daily = client.get("/reports/daily", headers=tokens["CSKH"]).json()
    assert daily["counts"]["packed"] >= 1
    assert client.get("/settings", headers=tokens["SUPERVISOR"]).status_code == 200
    body = client.get("/settings", headers=tokens["ADMIN"]).json()
    payload = {k: body[k] for k in ("retention_raw_days", "retention_clip_days", "session_warn_minutes",
                                     "session_abandon_minutes")}  # fmt: skip
    assert client.put("/settings", headers=tokens["SUPERVISOR"], json=payload).status_code == 403
    health = client.get("/system/health", headers=tokens["ADMIN"]).json()
    assert (health["db"], health["redis"], health["mediamtx"]) == ("OK", "OK", "OK")
    assert client.get("/system/health", headers=tokens["CSKH"]).status_code == 403
