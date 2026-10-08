"""QA live item 03 — M12 (đa shop + hardening L11 / L13 / L14 / L15) trên stack thật (T-229).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m12` (tự `qa-reset.sh --mute-cam2`: seed Phase 3 —
4 shop mock `CONNECTED`, J-04 + J-13 thật qua adapter mock). Phủ: TC-05.73, 05.93, 03.86 (+ TikTok
`TTTST0000000050`), 03.92, 07.40, 07.41, MS.01 / 03 / 08, 08.53 (một phần), 08.59, 08.64, 09.45, 10.44.
TC-08.55 (403 CSKH gỡ lý do hủy) cần phiên RETURN bị hủy — seed không có: INT + E2E (DEC-823).
"""

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
    return p3.tokens_for(client)


@pytest.fixture(scope="module")
def shops(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, dict[str, Any]]:
    return p3.shops(client, tokens["ADMIN"])


# ---------------------------------------------------------------- M05 đa shop


def test_tc_05_73_same_order_sn_two_shops(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-05.73 (BR-29): `2410DUP00001` ở Shopee `TST B` và TikTok `TST TikTok A (mock)` → 2 đơn, 2 shop."""
    items = client.get("/packages", params={"q": "2410DUP00001"}, headers=tokens["CSKH"]).json()["items"]
    got = {(i["platform"], i["shop"]["name"], i["tracking_number"]) for i in items}
    assert got == {
        ("SHOPEE", "TST B", "SPXTSTB000000021"),
        ("TIKTOK", "TST TikTok A (mock)", "TTTST0000000021"),
    }
    assert len({i["shop"]["id"] for i in items}) == 2


def test_tc_05_93_code_in_two_shops_ambiguous(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-05.93 (EX-P14): mã chưa có trong DB, tra sàn thấy ở 2 shop → phiên mở "chưa xác minh" + cờ
    `AMBIGUOUS_SHOP`; dòng thời gian D4 có 2 shop. Seed J-04 đã nhận `SPXTSTX0000001` về `TST B` (EX-T2 bỏ ở
    `TTMOCKB`) → xóa kiện / đơn cục bộ trước để buộc tra sàn (lệch dữ liệu test — DEC-823)."""
    assert p3.psql(
        "DELETE FROM package WHERE tracking_number = 'SPXTSTX0000001'; "
        "DELETE FROM \"order\" WHERE platform_order_sn = '2410TSTBX001'"
    ).endswith("DELETE 1")
    st = tokens["STATION"]
    res = p3.scan(client, st, "SPXTSTX0000001")
    assert res["outcome"] == "SESSION_OPENED", res
    assert set(res["state"]["session"]["flags"]) >= {"UNVERIFIED", "AMBIGUOUS_SHOP"}
    assert p3.scan(client, st, "SPXTSTX0000001")["outcome"] == "SESSION_COMPLETED"
    package = p3.package_by_code(client, tokens["CSKH"], "SPXTSTX0000001")
    detail = client.get(f"/packages/{package['id']}", headers=tokens["CSKH"]).json()
    shops = [e["shops"] for e in detail["timeline"] if e.get("shops")]
    assert shops == [
        [{"platform": "SHOPEE", "name": "TST B"}, {"platform": "TIKTOK", "name": "TST TikTok B (mock)"}]
    ]


# ---------------------------------------------------------------- M03 station đóng gói


@pytest.mark.parametrize(
    ("code", "platform"), [("SPXTSTB000000015", "SHOPEE"), ("TTTST0000000050", "TIKTOK")]
)
def test_tc_03_86_cancel_requested_alert(
    client: httpx.Client, tokens: dict[str, dict[str, str]], code: str, platform: str
) -> None:
    """TC-03.86 (BR-01 / BR-21 v0.4): đơn đang yêu cầu hủy (Shopee `IN_CANCEL` lượt 1, TikTok `PENDING`) →
    `ALERT ORDER_CANCEL_REQUESTED`, không mở phiên; kiện vẫn `NEW` (không bị hủy)."""
    res = p3.scan(client, tokens["STATION"], code)
    assert (res["outcome"], res["alert"]["code"]) == ("ALERT", "ORDER_CANCEL_REQUESTED")
    assert res["alert"]["data"] == {"platform": platform}
    assert res["state"]["session"] is None
    assert p3.package_by_code(client, tokens["CSKH"], code)["warehouse_status"] == "NEW"


def test_tc_03_92_packer_name_optional(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-03.92: `packer_name_required = false` (mặc định) → quét mở phiên được, `operator_name = null`."""
    st = tokens["STATION"]
    res = p3.scan(client, st, "SPXTSTB000000002")
    assert res["outcome"] == "SESSION_OPENED", res
    assert res["state"]["session"]["operator_name"] is None
    assert res["state"]["session"]["package"]["order"]["shop_name"] == "TST B"
    assert p3.scan(client, st, "SPXTSTB000000002")["outcome"] == "SESSION_COMPLETED"


# ---------------------------------------------------------------- M07 lọc sàn / shop


def test_tc_07_40_packages_platform_shop_filter(
    client: httpx.Client, tokens: dict[str, dict[str, str]], shops: dict[str, dict[str, Any]]
) -> None:
    """TC-07.40 (API phần): API-30 `platform=TIKTOK&shop_id=<A>` → chỉ kiện shop A, có `platform` + `shop`."""
    shop_a = shops["TST TikTok A (mock)"]["id"]
    res = client.get("/packages", params={"platform": "TIKTOK", "shop_id": shop_a, "page_size": 100},
                     headers=tokens["CSKH"]).json()  # fmt: skip
    assert res["total"] >= 15
    assert {(i["platform"], i["shop"]["id"]) for i in res["items"]} == {("TIKTOK", shop_a)}
    # TikTok kho sàn xử lý (FBT) bị bỏ qua (AC-41); kiện gộp 2 đơn có 1 dòng.
    codes = [i["tracking_number"] for i in res["items"]]
    assert "TTTST0000000098" not in codes
    assert codes.count("TTTST0000000077") == 1


def test_tc_07_41_returns_recon_claims_filter(
    client: httpx.Client, tokens: dict[str, dict[str, str]], shops: dict[str, dict[str, Any]]
) -> None:
    """TC-07.41: API-110 / 120 / 130 `platform=SHOPEE&shop_id=<990002>` → chỉ mục shop đó, item có
    `platform` + `shop {id, name}`."""
    shop_b = shops["TST B"]["id"]
    params = {"platform": "SHOPEE", "shop_id": shop_b}
    returns = client.get("/returns", params={**params, "tab": "ALL"}, headers=tokens["CSKH"]).json()["items"]
    assert returns
    assert {(r["platform"], r["shop"]["id"], r["shop"]["name"]) for r in returns} == {
        ("SHOPEE", shop_b, "TST B")
    }
    tiktok = client.get(
        "/returns", params={"tab": "ALL", "platform": "TIKTOK"}, headers=tokens["CSKH"]
    ).json()
    assert tiktok["items"]
    assert {r["platform"] for r in tiktok["items"]} == {"TIKTOK"}
    for path in ("/recon-alerts", "/claims"):
        res = client.get(path, params=params, headers=tokens["SUPERVISOR"])
        assert res.status_code == 200, (path, res.text)
        for item in res.json()["items"]:
            assert item["shop"]["id"] == shop_b, (path, item)
    # Lọc sai giá trị → 422.
    assert client.get("/returns", params={"platform": "LAZADA"}, headers=tokens["CSKH"]).status_code == 422


# ---------------------------------------------------------------- MS clip / ảnh MISSING


@pytest.fixture(scope="module")
def missing_clip(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    """Phiên PACK thật SPXTST0000003 → clip READY (camera giả) → đặt clip Cam 1 `MISSING` (như J-23 / TC-02.80
    sau khi IT bỏ qua tệp không thấy) bằng psql."""
    detail = p3.pack_with_clips(client, tokens, "SPXTST0000003")
    session = next(s for s in detail["sessions"] if s["type"] == "PACK")
    cam1 = next(c for c in session["clips"] if c["camera_role"] == "CAM1")
    assert p3.psql(f"UPDATE clip SET status = 'MISSING' WHERE id = '{cam1['id']}'") == "UPDATE 1"  # noqa: S608
    return {"package_id": detail["id"], "session_id": session["id"], "clip_id": cam1["id"]}


def test_tc_ms_01_03_08_missing_clip_readers(
    client: httpx.Client, tokens: dict[str, dict[str, str]], missing_clip: dict[str, Any]
) -> None:
    """TC-MS.01 (API-40 / 41 → 409 `CLIP_NOT_READY` `details.status = MISSING`), TC-MS.03 (API-46 → 409
    `CLIP_NOT_FAILED`), TC-MS.08 (API-31 200, clip `MISSING` không 500)."""
    admin = tokens["ADMIN"]
    clip_id = missing_clip["clip_id"]
    # API-41 chỉ mở bằng URL ký từ API-40 → clip MISSING không có URL ký (kiểm API-40).
    for path in (f"/clips/{clip_id}/play-url",):
        res = client.get(path, headers=admin)
        assert res.status_code == 409, (path, res.status_code, res.text)
        body = res.json()["error"]
        assert (body["code"], body["details"]["status"]) == ("CLIP_NOT_READY", "MISSING")
        assert body["message"] == "Thiếu tệp clip trên máy chủ — không phát được."
    res = client.post(f"/sessions/{missing_clip['session_id']}/clips/rebuild", headers=admin)
    assert res.status_code == 409, res.text
    body = res.json()["error"]
    assert (body["code"], body["details"]["status"]) == ("CLIP_NOT_FAILED", "MISSING")
    assert body["message"] == "Clip thiếu tệp trên máy chủ — không cắt lại được."
    detail = client.get(f"/packages/{missing_clip['package_id']}", headers=tokens["CSKH"])
    assert detail.status_code == 200
    clips = {c["id"]: c for s in detail.json()["sessions"] for c in s["clips"]}
    assert clips[clip_id]["status"] == "MISSING"


# ---------------------------------------------------------------- M08 API-189 / API-134 hardening


@pytest.fixture(scope="module")
def claim(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, Any]:
    items = client.get("/claims", headers=tokens["CSKH"]).json()["items"]
    assert items, "seed-demo cần ≥ 1 hồ sơ khiếu nại (KN-000001)"
    return client.get(f"/claims/{items[0]['id']}", headers=tokens["CSKH"]).json()  # type: ignore[no-any-return]


def test_tc_08_53_review_errors(
    client: httpx.Client, tokens: dict[str, dict[str, str]], claim: dict[str, Any]
) -> None:
    """TC-08.53 (phần chạy được với seed — seed không có phiên RETURN, DEC-823): thiếu `reason_code` → 422
    "Chọn lý do."; `note` "abc" → 422; phiên không phải phiên hoàn của hồ sơ (PACK) / không có → 404; action
    lạ → 422. Các nhánh `SESSION_NOT_ELIGIBLE` / `VERSION_CONFLICT` / `CLAIM_CLOSED`: INT
    `test_return_session_review.py`."""
    sup = tokens["SUPERVISOR"]
    session_id = claim["evidence"][0]["session"]["id"]
    url = f"/claims/{claim['id']}/return-sessions/{session_id}/review"
    version = claim["version"]
    no_reason = {"version": version, "action": "MARK_WRONG_SCAN", "note": "Kiện của đơn khác"}
    res = client.post(url, json=no_reason, headers=sup)
    assert res.status_code == 422, res.text
    assert res.json()["error"]["details"]["fields"] == {"reason_code": "Chọn lý do."}
    bad_note = {"version": version, "action": "MARK_WRONG_SCAN", "reason_code": "WRONG_SCAN", "note": "abc"}
    res = client.post(url, json=bad_note, headers=sup)
    assert res.status_code == 422, res.text
    assert res.json()["error"]["details"]["fields"] == {"note": "Nhập ghi chú (5–500 ký tự)."}
    ok_body = {"version": version, "action": "MARK_WRONG_SCAN", "reason_code": "WRONG_SCAN",
               "note": "Kiện của đơn khác"}  # fmt: skip
    for sid in (session_id, uuid.uuid4()):
        res = client.post(f"/claims/{claim['id']}/return-sessions/{sid}/review", json=ok_body, headers=sup)
        assert p3.err(res) == (404, "NOT_FOUND"), res.text
    res = client.post(url, json={**ok_body, "action": "XYZ"}, headers=sup)
    assert res.status_code == 422, res.text
    assert "action" in res.json()["error"]["details"]["fields"]


def test_tc_08_59_remove_evidence_needs_reason(
    client: httpx.Client, tokens: dict[str, dict[str, str]], claim: dict[str, Any]
) -> None:
    """TC-08.59 / 08.64: API-134 bỏ phiên PACK không `note` → 422 "Nhập lý do bỏ bằng chứng (5–500 ký tự)."
    (không đổi); có lý do → bỏ được + audit `CLAIM_EVIDENCE_REMOVE` `data.reason`; thêm lại."""
    cskh = tokens["CSKH"]
    session_id = claim["evidence"][0]["session"]["id"]
    url = f"/claims/{claim['id']}/evidence"
    for note in (None, "", "abc"):
        res = client.put(
            url, json={"version": claim["version"], "session_ids": [], "note": note}, headers=cskh
        )
        assert res.status_code == 422, res.text
        assert res.json()["error"]["details"]["fields"]["note"] == "Nhập lý do bỏ bằng chứng (5–500 ký tự)."
    current = client.get(f"/claims/{claim['id']}", headers=cskh).json()
    assert current["version"] == claim["version"]
    body = {"version": claim["version"], "session_ids": [], "note": "Phiên không liên quan"}
    res = client.put(url, json=body, headers=cskh)
    assert res.status_code == 200, res.text
    rows = p3.audit_rows(client, tokens["ADMIN"], "CLAIM_EVIDENCE_REMOVE")
    assert len(rows) == 1
    assert rows[0]["data"]["reason"] == "Phiên không liên quan"
    assert rows[0]["user"]["display_name"]
    after = res.json()
    res = client.put(url, json={"version": after["version"], "session_ids": [session_id]}, headers=cskh)
    assert res.status_code == 200, res.text


# ---------------------------------------------------------------- M09 D2, M10 /me


def test_tc_09_45_daily_new_counts_and_role_filter(
    client: httpx.Client, tokens: dict[str, dict[str, str]], shops: dict[str, dict[str, Any]]
) -> None:
    """TC-09.45: API-32 có `returns_dropped_7d`, `refund_only_pending`, `claims_overdue_unsent`; shop lỗi →
    ADMIN thấy `SYNC_ERROR` (`shop_name`, `platform`, `code`), SUPERVISOR / CSKH không."""
    shop = shops["TST TikTok B (mock)"]
    error = '{"code": "SYNC_FAILED", "message": "HTTP 503", "at": "2026-10-07T00:00:00Z"}'
    since = "now() - interval '2 hours'"
    p3.psql(f"UPDATE shop SET last_error = '{error}', error_since = {since} WHERE id = '{shop['id']}'")  # noqa: S608
    p3.redis("DEL", f"report:daily:{time.strftime('%Y-%m-%d', time.gmtime(time.time() + 7 * 3600))}")
    try:
        admin = client.get("/reports/daily", headers=tokens["ADMIN"]).json()
        for key in ("returns_dropped_7d", "refund_only_pending", "claims_overdue_unsent"):
            assert isinstance(admin["counts"][key], int), key
        sync = [a for a in admin["attention"] if a["kind"] == "SYNC_ERROR"]
        assert sync == [
            {"kind": "SYNC_ERROR", "shop_id": shop["id"], "at": "2026-10-07T00:00:00Z",
             "shop_name": "TST TikTok B (mock)", "platform": "TIKTOK", "code": "SYNC_FAILED"},
        ] or [{k: a.get(k) for k in ("shop_name", "platform", "code")} for a in sync] == [
            {"shop_name": "TST TikTok B (mock)", "platform": "TIKTOK", "code": "SYNC_FAILED"}
        ]  # fmt: skip
        for role in ("SUPERVISOR", "CSKH"):
            kinds = {
                a["kind"] for a in client.get("/reports/daily", headers=tokens[role]).json()["attention"]
            }
            assert not kinds & {"SYNC_ERROR", "BACKUP_STALE"}, (role, kinds)
    finally:
        p3.psql(f"UPDATE shop SET last_error = NULL, error_since = NULL WHERE id = '{shop['id']}'")  # noqa: S608


P3_PERMS = {"reports.returns", "reports.claims", "reports.productivity", "shares.create", "shares.read",
            "shares.revoke_any", "notify.manage", "backup.manage", "backup.read"}  # fmt: skip


def test_tc_10_44_me_permissions(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-10.44 (API-04): quyền Phase 3 theo vai (DEC-780: SUPERVISOR có `backup.read` chỉ cho FE)."""
    got = {
        role: set(client.get("/me", headers=tokens[role]).json()["permissions"]) & P3_PERMS
        for role in ("ADMIN", "SUPERVISOR", "CSKH", "STATION")
    }
    assert got["ADMIN"] == P3_PERMS
    assert not got["SUPERVISOR"] & {"notify.manage", "backup.manage"}
    assert got["SUPERVISOR"] >= {"reports.returns", "reports.claims", "reports.productivity", "shares.create"}
    assert not got["CSKH"] & {"reports.productivity", "shares.revoke_any", "notify.manage", "backup.manage",
                               "backup.read"}  # fmt: skip
    assert got["CSKH"] >= {"reports.returns", "reports.claims", "shares.create", "shares.read"}
    assert got["STATION"] == set()
