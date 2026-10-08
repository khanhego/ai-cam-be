"""QA live item 03 — bản sửa review G3 trên stack thật (lượt QA sau G3, DEC-930).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k g3` (tự `qa-reset.sh --mute-cam2`; Phase 3 dọn 2
bucket). Các test chạy **theo thứ tự** trong module (trạng thái nối tiếp: sao lưu, Station 01 PACK → RETURN).
Phủ: hai lệnh vận hành mới `aicam backup-restore --list` (G3-BK-3, 02 API-186 — bản DB chưa hoàn tất bị
`--db latest` bỏ qua) và `aicam notify-reset-zalo-token` (G3-NT-1, ops §10); API-132 / API-164
`primary_unavailable*` khi Cam 1 phiên chính `MISSING` (G3-EV-4, DEC-862); API-160 409 `SESSION_EXCLUDED` cho
phiên mở hoàn quét nhầm làm nguồn phiên (G3-FE-1, DEC-864); API-32 `CANCEL_REVERT_PENDING` chỉ ADMIN + `aicam
fix-cancel-requests` (G3-EV-3, DEC-861). Kiện hủy oan tạo bằng psql (seed không có kiện Phase 2 bị hủy oan).
"""

import time
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark

BUCKET = "aicam-dev-backup"


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    p3.reset("--mute-cam2")


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with p3.api_client() as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    return p3.tokens_for(client)


def _aicam(*args: str, env: dict[str, str] | None = None) -> tuple[int, str]:
    env_args = [a for k, v in (env or {}).items() for a in ("-e", f"{k}={v}")]
    out = p3.compose("exec", "-T", *env_args, "api", "aicam", *args, timeout=900)
    return out.returncode, out.stdout + out.stderr


# ---------------------------------------------------------------- aicam backup-restore --list


def test_backup_restore_list_empty() -> None:
    """`--list` khi kho chưa có bản DB nào → mã 0, "Không có bản sao DB nào", không khôi phục gì."""
    code, out = _aicam("backup-restore", "--list")
    assert code == 0, out
    assert "Không có bản sao DB nào dưới backup/db/ trên kho lưu." in out


def test_backup_restore_list_marks_latest_and_incomplete(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """G3-BK-3: J-20 thật (bản DB + bản file nhập cùng lượt) → `--list` đánh dấu "hoàn tất ← --db latest";
    thêm bản DB mồ côi mới hơn (không có bản file nhập — như lượt chết giữa chừng) → liệt kê trước (mới nhất
    trước), "CHƯA HOÀN TẤT", `latest` vẫn trỏ bản hoàn tất. Không đổi DB."""
    admin = tokens["ADMIN"]
    fingerprint = client.get("/backup", headers=admin).json()["key"]["fingerprint"]
    assert (
        client.post("/backup/confirm-key", json={"fingerprint": fingerprint}, headers=admin).status_code
        == 200
    )
    assert client.put("/backup/settings", json={"enabled": True}, headers=admin).status_code == 200
    res = client.post("/backup/run-db", headers=admin)
    assert res.status_code == 202, res.text
    run_id = res.json()["run_id"]

    def ok() -> bool:
        history = client.get("/backup", headers=admin).json()["history"]
        return any(h["id"] == run_id and h["status"] == "SUCCESS" for h in history)

    p3.wait_for(ok, 180, 2, "J-20 SUCCESS")
    listing = p3.mc("ls", "--recursive", f"local/{BUCKET}/backup/db/")
    assert listing.returncode == 0, listing.stderr
    keys = ["backup/db/" + line.split()[-1] for line in listing.stdout.splitlines() if line.strip()]
    (real,) = [k for k in keys if k.endswith(".dump.enc")]
    folder = real.rsplit("/", 1)[0]
    orphan = f"{folder}/aicam-99991231T000000Z.dump.enc"
    cp = p3.mc("cp", f"local/{BUCKET}/{real}", f"local/{BUCKET}/{orphan}")
    assert cp.returncode == 0, cp.stderr
    before = p3.psql("SELECT count(*) FROM package")

    code, out = _aicam("backup-restore", "--list")
    assert code == 0, out
    lines = [line.strip() for line in out.splitlines() if line.strip().startswith("backup/db/")]
    assert [line.split()[0] for line in lines] == [orphan, real], out
    assert "CHƯA HOÀN TẤT" in lines[0], lines[0]
    assert "← --db latest" not in lines[0], lines[0]
    assert "hoàn tất" in lines[1], lines[1]
    assert lines[1].endswith("← --db latest"), lines[1]
    assert "aicam backup-restore --db <khóa đối tượng>" in out
    assert p3.psql("SELECT count(*) FROM package") == before  # chỉ liệt kê
    p3.mc("rm", f"local/{BUCKET}/{orphan}")


# ---------------------------------------------------------------- aicam notify-reset-zalo-token


def test_notify_reset_zalo_token() -> None:
    """G3-NT-1 (ops §10): `ZALO_OA_REFRESH_TOKEN` trống → mã 2 + hướng dẫn, không xóa; có biến → DB chưa có
    token → mã 0 "DB chưa có token"; có dòng `notify_provider_token` ZALO_OA → mã 0 "Đã xóa", dòng mất."""
    sql_count = "SELECT count(*) FROM notify_provider_token WHERE provider = 'ZALO_OA'"
    p3.psql(
        "INSERT INTO notify_provider_token (provider, access_token_enc, refresh_token_enc, expires_at, "
        "updated_at) VALUES ('ZALO_OA', 'x'::bytea, 'y'::bytea, now(), now())"
    )
    assert p3.psql(sql_count) == "1"
    code, out = _aicam("notify-reset-zalo-token", env={"ZALO_OA_REFRESH_TOKEN": ""})
    assert code == 2, out
    assert "ZALO_OA_REFRESH_TOKEN trống" in out
    assert p3.psql(sql_count) == "1"  # không xóa khi chưa có token thay thế
    env = {"ZALO_OA_REFRESH_TOKEN": "qa-refresh-token-moi"}
    code, out = _aicam("notify-reset-zalo-token", env=env)
    assert code == 0, out
    assert "Đã xóa token Zalo OA lưu trong DB" in out
    assert p3.psql(sql_count) == "0"
    code, out = _aicam("notify-reset-zalo-token", env=env)
    assert code == 0, out
    assert "DB chưa có token Zalo OA" in out


# ---------------------------------------------------------------- API-132 / API-164 primary_unavailable


def test_primary_unavailable_when_cam1_missing(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """G3-EV-4 (DEC-862): hồ sơ tạo tay từ kiện đóng gói thật (phiên chính = PACK hiệu lực) → API-132 /
    API-164 `primary_unavailable = false`; Cam 1 phiên chính `MISSING` → `true` + `CLIP_MISSING` ở cả hai
    (phiên chính không đổi — BR-39); nguồn phiên (API-164 `session_id`) luôn `false` / null."""
    detail = p3.pack_with_clips(client, tokens, "SPXTST0000003", hold_s=5)
    pack = next(s for s in detail["sessions"] if s["type"] == "PACK")
    cskh = tokens["CSKH"]
    body = {"package_id": detail["id"], "type": "MISSING_ITEM", "counterparty": "PLATFORM",
            "note": "QA G3 phiên chính"}  # fmt: skip
    res = client.post("/claims", json=body, headers=cskh)
    assert res.status_code == 201, res.text
    claim = res.json()
    assert any(e["session"]["id"] == pack["id"] for e in claim["evidence"] if e.get("session")), claim[
        "evidence"
    ]
    assert (claim["primary_unavailable"], claim["primary_unavailable_reason"]) == (False, None)
    opts = client.get("/shares/options", params={"claim_id": claim["id"]}, headers=cskh).json()
    assert (opts["primary_unavailable"], opts["primary_unavailable_reason"]) == (False, None)

    cam1 = next(c for c in pack["clips"] if c["camera_role"] == "CAM1")
    assert p3.psql(f"UPDATE clip SET status = 'MISSING' WHERE id = '{cam1['id']}'") == "UPDATE 1"  # noqa: S608
    claim = client.get(f"/claims/{claim['id']}", headers=cskh).json()
    assert (claim["primary_unavailable"], claim["primary_unavailable_reason"]) == (True, "CLIP_MISSING")
    primary = [e["session"]["id"] for e in claim["evidence"] if e.get("session") and e.get("primary")]
    assert primary == [pack["id"]]  # BR-39 không đổi: vẫn là phiên chính
    opts = client.get("/shares/options", params={"claim_id": claim["id"]}, headers=cskh).json()
    assert (opts["primary_unavailable"], opts["primary_unavailable_reason"]) == (True, "CLIP_MISSING")
    by_session = client.get("/shares/options", params={"session_id": pack["id"]}, headers=cskh).json()
    assert (by_session["primary_unavailable"], by_session["primary_unavailable_reason"]) == (False, None)


# ---------------------------------------------------------------- API-160 SESSION_EXCLUDED


def test_api160_session_excluded_for_wrong_scan_return(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """G3-FE-1 (DEC-864): Station 01 "Cả hai" — đóng gói SPXTST0000012 (clip thật), bàn giao tay, chế độ nhận
    hoàn → quét mã đơn mở phiên RETURN → station tự hủy lý do "Quét nhầm" (≤ 60 giây) → API-164 nguồn phiên đó
    `evidence_exclusion = STATION_CANCEL`; API-160 nguồn `SESSION` = phiên đó → 409 `SESSION_EXCLUDED`
    `details.session_id`, không tạo link; phiên PACK cùng kiện vẫn tạo được (202)."""
    admin, st, cskh = tokens["ADMIN"], tokens["STATION"], tokens["CSKH"]
    stations = client.get("/stations", headers=admin).json()["items"]
    station = next(s for s in stations if s["name"] == "TST Station 01")
    res = client.patch(f"/stations/{station['id']}", json={"kind": "BOTH"}, headers=admin)
    assert res.status_code == 200, res.text
    detail = p3.pack_with_clips(client, tokens, "SPXTST0000012", hold_s=5)
    pack_id = next(s["id"] for s in detail["sessions"] if s["type"] == "PACK")
    res = client.post(f"/packages/{detail['id']}/warehouse-status",
                      json={"to_status": "HANDED_OVER", "reason": "QA G3 bàn giao tay"},
                      headers=tokens["SUPERVISOR"])  # fmt: skip
    assert res.status_code == 200, res.text
    assert client.put("/station/work-mode", json={"work_mode": "RETURN"}, headers=st).status_code == 200
    assert client.put("/station/operator", json={"name": "Lan QA"}, headers=st).status_code == 200
    opened = p3.scan(client, st, "2410TST00012")
    assert opened["outcome"] == "SESSION_OPENED", opened
    ret = opened["state"]["session"]
    assert ret["type"] == "RETURN"
    time.sleep(5)
    res = client.post(f"/station/sessions/{ret['id']}/cancel", json={"reason": "WRONG_SCAN"}, headers=st)
    assert res.status_code == 200, res.text
    assert client.put("/station/work-mode", json={"work_mode": "PACK"}, headers=st).status_code == 200

    opts = client.get("/shares/options", params={"session_id": ret["id"]}, headers=cskh)
    assert opts.status_code == 200, opts.text
    (opt,) = opts.json()["sessions"]
    assert (opt["id"], opt["evidence_exclusion"]) == (ret["id"], "STATION_CANCEL")
    count_before = p3.psql("SELECT count(*) FROM share_link")
    body: dict[str, Any] = {"source_type": "SESSION", "session_id": ret["id"], "session_ids": [ret["id"]],
                            "recipient": "ĐVVC SPX – phiếu QA G3", "expires_days": 1}  # fmt: skip
    res = client.post("/shares", json=body, headers=cskh)
    assert p3.err(res) == (409, "SESSION_EXCLUDED"), res.text
    assert res.json()["error"]["details"] == {"session_id": ret["id"]}
    assert p3.psql("SELECT count(*) FROM share_link") == count_before
    ok = client.post("/shares", json={**body, "session_id": pack_id, "session_ids": [pack_id]}, headers=cskh)
    assert ok.status_code == 202, ok.text


# ---------------------------------------------------------------- API-32 CANCEL_REVERT_PENDING (ADMIN)


def _attention(client: httpx.Client, headers: dict[str, str]) -> dict[str, Any]:
    res = client.get("/reports/daily", headers=headers)
    assert res.status_code == 200, res.text
    return {a["kind"]: a for a in res.json()["attention"]}


def test_d2_cancel_revert_pending_admin_only(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """G3-EV-3 (DEC-861, 02 §6.2 API-32): kiện của đơn đang yêu cầu hủy (`SPXTSTB000000015`, nhóm
    `CANCEL_REQUESTED`) bị đưa về `CANCELLED` như Phase 2 (psql, lịch sử nguồn PLATFORM) → ADMIN thấy
    `CANCEL_REVERT_PENDING count = 1`; SUPERVISOR / CSKH không thấy. `aicam fix-cancel-requests` chạy thử liệt
    kê "SẼ TRẢ LẠI", không ghi; `--apply` → kiện về `NEW`, audit `PACKAGE_CANCEL_REVERT`, mục D2 biến mất."""
    code = "SPXTSTB000000015"
    assert "CANCEL_REVERT_PENDING" not in _attention(client, tokens["ADMIN"])
    pkg = p3.package_by_code(client, tokens["ADMIN"], code)
    assert pkg["warehouse_status"] == "NEW"
    p3.psql(f"UPDATE package SET warehouse_status = 'CANCELLED' WHERE id = '{pkg['id']}'")  # noqa: S608
    time.sleep(6)  # cache API-32 5 giây
    admin_items = _attention(client, tokens["ADMIN"])
    assert admin_items.get("CANCEL_REVERT_PENDING", {}).get("count") == 1, admin_items
    for role in ("SUPERVISOR", "CSKH"):
        assert "CANCEL_REVERT_PENDING" not in _attention(client, tokens[role]), role

    rc, out = _aicam("fix-cancel-requests")
    assert rc == 0, out
    assert code in out, out
    assert "SẼ TRẢ LẠI" in out, out
    assert p3.package_by_code(client, tokens["ADMIN"], code)["warehouse_status"] == "CANCELLED"
    rc, out = _aicam("fix-cancel-requests", "--apply")
    assert rc == 0, out
    assert "ĐÃ TRẢ LẠI" in out, out
    assert p3.package_by_code(client, tokens["ADMIN"], code)["warehouse_status"] == "NEW"
    assert len(p3.audit_rows(client, tokens["ADMIN"], "PACKAGE_CANCEL_REVERT")) == 1
    time.sleep(6)
    assert "CANCEL_REVERT_PENDING" not in _attention(client, tokens["ADMIN"])
