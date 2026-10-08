"""QA live item 03 — M13 (TikTok Shop mock 2 shop, J-04 / J-13 trên worker thật) trên stack thật (T-229).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m13` (tự `qa-reset.sh`). Stack:
`TIKTOK_ENABLED=true`,
`TIKTOK_ADAPTER=mock` (adapter TikTok thật trên transport giả), worker `worker-sync` (`sync_fast`) +
`worker-sync-long` (`sync`). Phủ: AC-40 / 41 / 42 (phần trạng thái ban đầu), TC-05.55 (API), TC-08.72, kết nối
TikTok qua API-155 mock → callback, API-73 → J-04 trên `sync_fast`, J-13 phân phối → `sync`.
TikTok Shop partner thật: **chưa test — thiếu tài nguyên** (X3, Q18).
"""

import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    p3.reset()


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with p3.api_client() as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    return p3.tokens_for(client)


def _shops(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> dict[str, dict[str, Any]]:
    return p3.shops(client, tokens["ADMIN"])


def test_ac41_tiktok_status_groups_on_stack() -> None:
    """AC-41: 9 trạng thái TikTok → nhóm (seed J-04 thật), `XYZ` → `UNKNOWN`; đơn kho TikTok (FBT) bỏ qua;
    chưa thanh toán chưa có kiện; kiện gộp `TTTST0000000077` thuộc 2 đơn."""
    rows = dict(
        line.split("|")
        for line in p3.psql(
            'SELECT platform_order_sn, platform_status_group FROM "order" o JOIN shop s ON s.id = o.shop_id '
            "WHERE s.platform_shop_id = 'TTMOCKA' AND platform_order_sn LIKE '5761TT00000000%'"
        ).splitlines()
    )
    assert [rows[f"5761TT00000000{i}"] for i in range(11, 20)] == [
        "UNPAID", "UNPAID", "AWAITING_SHIPMENT", "AWAITING_SHIPMENT", "AWAITING_SHIPMENT", "SHIPPED",
        "DELIVERED", "DELIVERED", "CANCELLED",
    ]  # fmt: skip
    assert rows["5761TT0000000099"] == "UNKNOWN"
    assert "5761TT0000000098" not in rows
    assert (
        p3.psql(
            "SELECT count(*) FROM package WHERE tracking_number IN ('TTTST0000000098', 'TTTST0000000011')"
        )
        == "0"
    )
    assert (
        p3.psql("SELECT warehouse_status FROM package WHERE tracking_number = 'TTTST0000000019'")
        == "CANCELLED"
    )
    merged = p3.psql(
        "SELECT count(*) FROM package_order po JOIN package p ON p.id = po.package_id "
        "WHERE p.tracking_number = 'TTTST0000000077'"
    )
    assert merged == "1"  # + đơn chính `package.order_id` = 2 đơn (FR-05.22)


def test_ac42_tiktok_returns_initial_state(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """AC-42 (trạng thái lúc nạp — đổi theo giờ: INT `clock.advance`) + TC-08.72: J-13 thật tạo hồ sơ cho 6
    kịch
    bản + mã chiều về trùng; Chỉ hoàn tiền không hạn sàn → `response_due_source = DEFAULT`, hạn = báo + 48
    giờ."""
    items = client.get("/returns", params={"tab": "ALL", "platform": "TIKTOK", "page_size": 100},
                       headers=tokens["CSKH"]).json()["items"]  # fmt: skip
    by_order = {i["order"]["platform_order_sn"]: i for i in items}
    assert {f"5761TT00000000{n}" for n in range(61, 68)} <= set(by_order)
    assert {i["shop"]["name"] for i in items} == {"TST TikTok A (mock)"}
    refund = by_order["5761TT0000000061"]
    assert (refund["kind"], refund["status"]) == ("REFUND_ONLY", "NO_PARCEL")
    assert refund["response_due_source"] == "DEFAULT"
    due = datetime.fromisoformat(refund["response_due_at"]) - datetime.fromisoformat(refund["reported_at"])
    assert due == timedelta(hours=48), (refund["reported_at"], refund["response_due_at"])
    ret = by_order["5761TT0000000062"]
    assert (ret["kind"], ret["status"], ret["return_tracking_number"]) == (
        "BUYER_RETURN", "EXPECTED", "TTRTTST000062",
    )  # fmt: skip
    pending = by_order["5761TT0000000064"]
    assert (pending["platform_status_group"], pending["return_tracking_number"]) == ("REQUESTED", None)
    assert by_order["5761TT0000000067"]["return_tracking_number"] == "RTTST-DUP-1"


def test_tiktok_connect_mock_callback(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """API-155 TikTok (mock) → callback API-156: một lần ủy quyền trả 2 shop (`count=2`), shop Shopee không
    bị ngắt (AC-40); audit `SHOP_CONNECT` có `platform`."""
    admin = tokens["ADMIN"]
    res = client.post("/shops/tiktok/auth-url", headers=admin)
    assert res.status_code == 200, res.text
    url = httpx.URL(res.json()["url"])
    cookie = {"Cookie": f"aicam_tiktok_state={res.cookies['aicam_tiktok_state']}"}
    cb = client.get(f"{p3.BASE}{url.raw_path.decode()}", headers=cookie, follow_redirects=False)
    assert cb.status_code == 302
    assert cb.headers["location"] == "/admin/settings/platforms?platform=tiktok&result=connected&count=2"
    states = {name: s["auth_status"] for name, s in _shops(client, tokens).items()}
    assert states == dict.fromkeys(["TST Shop (mock)", "TST B", "TST TikTok A (mock)", "TST TikTok B (mock)"],
                                   "CONNECTED")  # fmt: skip
    rows = p3.audit_rows(client, admin, "SHOP_CONNECT")
    assert [r["data"].get("platform") for r in rows] == ["TIKTOK", "TIKTOK"]
    # Callback dùng lại state → từ chối (không kết nối lại).
    again = client.get(f"{p3.BASE}{url.raw_path.decode()}", headers=cookie, follow_redirects=False)
    assert "result=connected" not in again.headers.get("location", "")


def test_api73_sync_now_runs_on_sync_fast_worker(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """API-73 → J-04 một shop chạy trên `worker-sync` (queue `sync_fast`, 02a §7 / DEC-503): 202 →
    `last_synced_at`
    mới, lock nhả, log worker có task của shop."""
    admin = tokens["ADMIN"]
    shop = _shops(client, tokens)["TST TikTok A (mock)"]
    before = shop["last_synced_at"]
    res = client.post(f"/shops/{shop['id']}/sync", headers=admin)
    deadline = time.monotonic() + 60
    while res.status_code == 409 and time.monotonic() < deadline:
        time.sleep(1)
        res = client.post(f"/shops/{shop['id']}/sync", headers=admin)
    assert (res.status_code, res.json()) == (202, {"queued": True}), res.text

    def synced() -> dict[str, Any] | None:
        now = _shops(client, tokens)["TST TikTok A (mock)"]
        done = now["last_synced_at"] != before and not now["sync_in_progress"]
        return now if done else None

    now = p3.wait_for(synced, 60, 2, "J-04 TikTok A trên worker-sync")
    assert (now["auth_status"], now["last_error"]) == ("CONNECTED", None)
    log = p3.logs("worker-sync")
    assert "platforms.sync_shop_orders" in log or "platforms.sync_orders" in log, log[-2000:]
    assert "succeeded" in log


def test_j13_dispatch_runs_on_sync_worker(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """J-13 không shop (beat) → phân phối mỗi shop một task `platforms.sync_shop_returns` trên
    `worker-sync-long`
    (queue `sync`, NFR-39); chạy lại idempotent (không nhân đôi hồ sơ)."""
    before = p3.psql("SELECT count(*) FROM return_case")
    p3.send_task("platforms.sync_returns")

    def done() -> bool:
        return p3.logs("worker-sync-long", "3m").count("platforms.sync_shop_returns[") >= 8  # nhận + xong × 4

    p3.wait_for(done, 120, 3, "4 task J-13 trên worker-sync-long")
    log = p3.logs("worker-sync-long", "3m")
    assert log.count("succeeded") >= 4, log[-3000:]
    assert p3.psql("SELECT count(*) FROM return_case") == before


def test_tc_05_55_disconnect_one_shop(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-05.55 (API phần): API-154 ngắt `TST TikTok B (mock)` → `DISCONNECTED`, token xóa, `disconnected_at`;
    audit `SHOP_DISCONNECT`; J-04 sau đó không gọi shop B (không đổi `last_synced_at`), shop khác vẫn chạy;
    đơn
    của B giữ nguyên (EX-T7); quét kiện B vẫn được (dữ liệu giữ)."""
    admin = tokens["ADMIN"]
    shops = _shops(client, tokens)
    shop_b, shop_a = shops["TST TikTok B (mock)"], shops["TST TikTok A (mock)"]
    orders_before = p3.psql(f"SELECT count(*) FROM \"order\" WHERE shop_id = '{shop_b['id']}'")  # noqa: S608
    assert client.post(f"/shops/{shop_b['id']}/disconnect", headers=tokens["SUPERVISOR"]).status_code == 403
    res = client.post(f"/shops/{shop_b['id']}/disconnect", headers=admin)
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["auth_status"] == "DISCONNECTED"
    assert body["disconnected_at"] is not None
    tokens_left = p3.psql(
        f"SELECT access_token_enc IS NULL AND refresh_token_enc IS NULL FROM shop WHERE id = '{shop_b['id']}'"  # noqa: S608
    )
    assert tokens_left == "t"
    rows = p3.audit_rows(client, admin, "SHOP_DISCONNECT")
    assert len(rows) == 1
    assert rows[0]["object_id"] == shop_b["id"]
    # Idempotent.
    assert client.post(f"/shops/{shop_b['id']}/disconnect", headers=admin).status_code == 200
    a_before = _shops(client, tokens)["TST TikTok A (mock)"]["last_synced_at"]
    b_before = _shops(client, tokens)["TST TikTok B (mock)"]["last_synced_at"]
    p3.send_task("platforms.sync_orders")

    def a_synced() -> bool:
        return bool(_shops(client, tokens)["TST TikTok A (mock)"]["last_synced_at"] != a_before)

    p3.wait_for(a_synced, 90, 2, "J-04 phân phối chạy shop A")
    time.sleep(3)
    assert _shops(client, tokens)["TST TikTok B (mock)"]["last_synced_at"] == b_before
    assert p3.psql(f"SELECT count(*) FROM \"order\" WHERE shop_id = '{shop_b['id']}'") == orders_before  # noqa: S608
    assert shop_a["id"] != shop_b["id"]
    # API-73 shop đã ngắt → không đẩy job.
    res = client.post(f"/shops/{shop_b['id']}/sync", headers=admin)
    assert res.status_code in {409, 422}, res.text
