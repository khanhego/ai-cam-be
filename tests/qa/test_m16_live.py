"""QA live item 03 — M16 (link chia sẻ bằng chứng: J-24 ffmpeg thật → MinIO → W1, thu hồi J-25) trên stack
thật.

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m16` (tự `qa-reset.sh --mute-cam2`; Phase 3 dọn 2
bucket).
Stack: `worker-export` (J-24: ffmpeg ghép 2 camera + `drawtext`), bucket link `aicam-dev-share`,
`S3_PUBLIC_ENDPOINT`
= MinIO của stack (máy chạy test mở được như trình duyệt người nhận), beat J-25. Phủ: TC-07.46, 07.48, 07.51
(API tạm
`S3_ENDPOINT` rỗng), 07.62, MS.07, W1 (AC-52: trang tĩnh + video + ảnh từ MinIO không cookie), thu hồi → link
chết
≤ 60 giây (FR-07.08, AC-55), audit `SHARE_CREATE` / `SHARE_REVOKE` (T-229).
"""

import hashlib
import html
import re
import subprocess
import time
import uuid
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    p3.reset("--mute-cam2")


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with p3.api_client() as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    out = p3.tokens_for(client)
    res = client.post("/users", json={"username": "tst_cskh2", "display_name": "Mai", "role": "CSKH",
                                      "password": p3.PASSWORD}, headers=out["ADMIN"])  # fmt: skip
    assert res.status_code == 201, res.text
    out["CSKH2"] = p3.login(client, "tst_cskh2")
    return out


@pytest.fixture(scope="module")
def sessions(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, str]:
    """Phiên PACK thật có clip READY: 01 (link chính), 02 (clip Cam 1 MISSING), 07 (31 phút)."""
    out = {}
    for code in ("SPXTST0000001", "SPXTST0000002", "SPXTST0000007"):
        detail = p3.pack_with_clips(client, tokens, code, hold_s=5)
        out[code] = next(s["id"] for s in detail["sessions"] if s["type"] == "PACK")
    return out


def _body(session_id: str, **kw: Any) -> dict[str, Any]:
    return {"source_type": "SESSION", "session_id": session_id, "session_ids": [session_id],
            "recipient": "ĐVVC SPX – phiếu QA 98765", "expires_days": 1, **kw}  # fmt: skip


def _wait_active(client: httpx.Client, headers: dict[str, str], share_id: str) -> dict[str, Any]:
    def done() -> dict[str, Any] | None:
        share: dict[str, Any] = client.get(f"/shares/{share_id}", headers=headers).json()
        return share if share["status"] != "CREATING" else None

    return p3.wait_for(done, 180, 2, f"J-24 link {share_id}")


@pytest.fixture(scope="module")
def share(
    client: httpx.Client, tokens: dict[str, dict[str, str]], sessions: dict[str, str]
) -> dict[str, Any]:
    sid = sessions["SPXTST0000001"]
    options = client.get("/shares/options", params={"session_id": sid}, headers=tokens["CSKH"]).json()
    assert options["storage_configured"] is True
    (opt,) = options["sessions"]
    assert (opt["id"], opt["selectable"], opt["default_selected"]) == (sid, True, True)
    res = client.post("/shares", json=_body(sid), headers=tokens["CSKH"])
    assert res.status_code == 202, res.text
    assert res.json()["status"] == "CREATING"
    return _wait_active(client, tokens["CSKH"], res.json()["id"])


def test_tc_07_46_create_from_session_active(client: httpx.Client, share: dict[str, Any]) -> None:
    """TC-07.46: API-164 phiên → API-160 nguồn `SESSION` → J-24 (worker-export) → `ACTIVE`, `session_count =
    1`,
    `url` trỏ MinIO công khai (`S3_PUBLIC_ENDPOINT`), hash video + nguồn Cam 1 / Cam 2."""
    assert share["status"] == "ACTIVE", share
    assert (share["source"]["type"], share["session_count"], share["progress"]) == ("SESSION", 1, 100)
    assert share["source"]["tracking_number"] == "SPXTST0000001"
    assert share["url"].startswith(f"{p3.MINIO_URL}/aicam-dev-share/share/")
    (item,) = share["items"]
    assert item["video_sha256"]
    assert item["source_sha256"]["CAM1"]
    assert item["source_sha256"]["CAM2"]
    assert "shares.build" in p3.logs("worker-export")


def test_w1_opens_from_minio_without_login(share: dict[str, Any]) -> None:
    """AC-52 / W1: trang tĩnh mở thẳng từ MinIO (không cookie / Bearer) → 200 HTML có CSP; video (ffmpeg J-24:
    ghép 2 camera + chữ `drawtext`) tải được, SHA-256 = `video_sha256`, ffprobe đọc được (H.264, có thời
    lượng);
    ảnh JPEG tải được."""
    w1 = httpx.get(share["url"], timeout=30)
    assert w1.status_code == 200
    assert w1.headers["content-type"].startswith("text/html")
    assert "Content-Security-Policy" in w1.text
    assert "SPXTST0000001" in w1.text
    links = [html.unescape(u) for u in re.findall(r'(?:src|href)="([^"]+)"', w1.text)]
    video = next(u for u in links if "/v1.mp4" in u and "attachment" not in u)
    res = httpx.get(video, timeout=60)
    assert res.status_code == 200
    assert hashlib.sha256(res.content).hexdigest() == share["items"][0]["video_sha256"]
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "stream=codec_name,width,height:format=duration",  # noqa: S607
         "-of", "default=nw=1", "-i", "pipe:0"],
        input=res.content, capture_output=True, check=False, timeout=60,
    )  # fmt: skip
    info = out.stdout.decode()
    assert out.returncode == 0, out.stderr.decode()[-1000:]
    assert "codec_name=h264" in info, info
    duration = float(re.search(r"duration=([\d.]+)", info).group(1))  # type: ignore[union-attr]
    assert duration >= 3, info
    jpg = next(u for u in links if ".jpg" in u)
    image = httpx.get(jpg, timeout=30)
    assert (image.status_code, image.content[:2]) == (200, b"\xff\xd8")


def test_tc_07_48_validation(
    client: httpx.Client, tokens: dict[str, dict[str, str]], sessions: dict[str, str]
) -> None:
    """TC-07.48: 0 phiên; 5 phiên; phiên không thuộc nguồn; tổng > 30 phút; `recipient` "AB"; `expires_days`
    5."""
    cskh = tokens["CSKH"]
    sid = sessions["SPXTST0000001"]
    five = [sid, *(str(uuid.uuid4()) for _ in range(4))]
    cases = [
        (_body(sid, session_ids=[]), {"session_ids": "Chọn ít nhất 1 phiên."}),
        (
            _body(sid, session_ids=five),
            {"session_ids": "Chọn tối đa 4 phiên."},
        ),
        (
            _body(sid, session_ids=[sessions["SPXTST0000002"]]),
            {"session_ids": "Phiên không thuộc hồ sơ này."},
        ),
        (_body(sid, recipient="AB"), {"recipient": "Ghi rõ gửi cho ai (3–100 ký tự)."}),
    ]
    for body, fields in cases:
        res = client.post("/shares", json=body, headers=cskh)
        assert res.status_code == 422, (body, res.text)
        assert res.json()["error"]["details"]["fields"] == fields
    res = client.post("/shares", json=_body(sid, expires_days=5), headers=cskh)
    assert res.status_code == 422, res.text
    assert "expires_days" in res.json()["error"]["details"]["fields"]
    long_sid = sessions["SPXTST0000007"]
    # Thời lượng phiên = `duration_s` clip Cam 1 → đặt 31 phút (1.860 giây) bằng psql.
    sql = "UPDATE clip SET duration_s = 1860 WHERE session_id = '%s' AND camera_role = 'CAM1'"
    assert p3.psql(sql % long_sid) == "UPDATE 1"
    res = client.post("/shares", json=_body(long_sid), headers=cskh)
    assert res.status_code == 422, res.text
    assert res.json()["error"]["details"]["fields"] == {"session_ids": "Tổng thời lượng tối đa 30 phút."}


def test_tc_ms_07_missing_clip_not_shareable(
    client: httpx.Client, tokens: dict[str, dict[str, str]], sessions: dict[str, str]
) -> None:
    """TC-MS.07: Cam 1 `MISSING` → API-164 `selectable = false`, `unavailable_reason = CLIP_MISSING`; API-160
    →
    409 `SESSION_CLIP_UNAVAILABLE` `reason = CLIP_MISSING`."""
    sid = sessions["SPXTST0000002"]
    p3.psql(f"UPDATE clip SET status = 'MISSING' WHERE session_id = '{sid}' AND camera_role = 'CAM1'")  # noqa: S608
    (opt,) = client.get("/shares/options", params={"session_id": sid}, headers=tokens["CSKH"]).json()[
        "sessions"
    ]
    assert (opt["selectable"], opt["unavailable_reason"]) == (False, "CLIP_MISSING")
    res = client.post("/shares", json=_body(sid), headers=tokens["CSKH"])
    assert p3.err(res) == (409, "SESSION_CLIP_UNAVAILABLE"), res.text
    assert res.json()["error"]["details"]["reason"] == "CLIP_MISSING"


def test_tc_07_51_cloud_not_configured_temp_api(sessions: dict[str, str]) -> None:
    """TC-07.51 (API): `S3_ENDPOINT` rỗng (API tạm cùng DB) → API-164 `storage_configured = false`; API-160 →
    503 `CLOUD_NOT_CONFIGURED` "Chưa cấu hình kho lưu cloud. Admin: Cài đặt → Sao lưu."."""
    sid = sessions["SPXTST0000001"]
    with p3.temp_api({"S3_ENDPOINT": ""}) as api:
        cskh = p3.login(api, "tst_cskh")
        assert (
            api.get("/shares/options", params={"session_id": sid}, headers=cskh).json()["storage_configured"]
            is False
        )
        res = api.post("/shares", json=_body(sid), headers=cskh)
        assert p3.err(res) == (503, "CLOUD_NOT_CONFIGURED"), res.text
        assert res.json()["error"]["message"] == "Chưa cấu hình kho lưu cloud. Admin: Cài đặt → Sao lưu."


def test_tc_07_62_cskh_revokes_only_own(
    client: httpx.Client, tokens: dict[str, dict[str, str]], sessions: dict[str, str], share: dict[str, Any]
) -> None:
    """TC-07.62: L1 của `tst_cskh` (fixture `share`), L2 của `tst_cskh2` → `tst_cskh` thu hồi L2 → 403, L2
    `can_revoke = false` với `tst_cskh`; thu hồi L1 → 200 (`REVOKED`); audit `SHARE_CREATE` × 2,
    `SHARE_REVOKE`;
    link L1 chết (≥ 400) ≤ 60 giây (J-25 xóa thư mục trên MinIO — FR-07.08)."""
    res = client.post("/shares", json=_body(sessions["SPXTST0000001"], recipient="Shopee CSKH"),
                      headers=tokens["CSKH2"])  # fmt: skip
    assert res.status_code == 202, res.text
    l2 = _wait_active(client, tokens["CSKH2"], res.json()["id"])
    assert l2["status"] == "ACTIVE"
    cskh = tokens["CSKH"]
    assert client.get(f"/shares/{l2['id']}", headers=cskh).json()["can_revoke"] is False
    assert p3.err(client.post(f"/shares/{l2['id']}/revoke", headers=cskh)) == (403, "FORBIDDEN")
    res = client.post(f"/shares/{share['id']}/revoke", headers=cskh)
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "REVOKED"
    assert res.json()["revoked_by"]["display_name"] == "Lan"
    started = time.monotonic()

    def dead() -> int | None:
        code = httpx.get(share["url"], timeout=10).status_code
        return code if code >= 400 else None

    p3.wait_for(dead, 70, 2, "link L1 chết sau thu hồi")
    assert time.monotonic() - started <= 70
    assert client.get(f"/shares/{share['id']}", headers=cskh).json()["revoke_pending"] is False
    listing = client.get("/shares", params={"status": "ALL"}, headers=tokens["ADMIN"]).json()
    assert (listing["counts"]["ACTIVE"], listing["counts"]["REVOKED"]) == (1, 1)
    # Admin thu hồi được link người khác (`shares.revoke_any`).
    assert client.post(f"/shares/{l2['id']}/revoke", headers=tokens["ADMIN"]).status_code == 200
    admin = tokens["ADMIN"]
    assert len(p3.audit_rows(client, admin, "SHARE_CREATE")) == 2
    assert len(p3.audit_rows(client, admin, "SHARE_REVOKE")) == 2
