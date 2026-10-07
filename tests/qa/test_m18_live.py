"""QA live item 03 — M18 (hoàn thiện: `seed-demo` Phase 3, hồi quy station sau nâng cấp) trên stack thật
(T-229).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m18` (tự `qa-reset.sh --mute-cam2`). Phủ: `aicam
seed-demo`
Phase 3 (4 shop `CONNECTED`, J-04 + J-13 thật qua adapter mock, đơn Phase 1 về shop `990001` — DEC-821; chạy
lại
idempotent), TC-R3.08 (hủy phiên PACK tại station không bị BR-37), TC-R3.03 phần API (đường cũ
`/shops/shopee/auth-url` → callback D7 mới), migration head 0007. Audit TC-10.40 / 10.41 kiểm theo từng module
(m13 SHOP_*, m14 REPORT_EXPORT, m15 BACKUP_* + MEDIA_MARK_MISSING + BACKUP_VERIFY_ACCEPT, m16 SHARE_*,
m17 NOTIFY_*, m12 CLAIM_EVIDENCE_REMOVE); contract / OpenAPI (TC-R3.07) chạy `tests/contract` riêng.
"""

from collections.abc import Iterator

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark

COUNTS = (
    "SELECT (SELECT count(*) FROM shop) || ',' || (SELECT count(*) FROM \"order\") || ',' || "
    "(SELECT count(*) FROM package) || ',' || (SELECT count(*) FROM return_case) || ',' || "
    "(SELECT count(*) FROM claim) || ',' || (SELECT count(*) FROM \"user\")"
)


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


def test_seed_demo_phase3_state(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """`seed-demo` Phase 3: head 0007; 4 shop mock `CONNECTED` (2 Shopee + 2 TikTok), không lỗi đồng bộ;
    đơn Phase 1 `2410TST000xx` thuộc `990001` với nhóm trạng thái (DEC-821); hồ sơ hàng hoàn cả 2 sàn."""
    assert p3.psql("SELECT version_num FROM alembic_version") == "0007"
    shops = p3.shops(client, tokens["ADMIN"])
    assert {(s["platform"], name) for name, s in shops.items()} == {
        ("SHOPEE", "TST Shop (mock)"), ("SHOPEE", "TST B"),
        ("TIKTOK", "TST TikTok A (mock)"), ("TIKTOK", "TST TikTok B (mock)"),
    }  # fmt: skip
    assert {s["auth_status"] for s in shops.values()} == {"CONNECTED"}
    assert all(s["last_error"] is None and s["last_synced_at"] for s in shops.values())
    unclaimed = p3.psql(
        "SELECT count(*) FROM \"order\" WHERE platform_order_sn ~ '^2410TST000[0-3][0-9]$' AND "
        "(shop_id IS NULL OR platform_status_group = 'UNKNOWN')"
    )
    assert unclaimed == "0"
    cancelled = p3.package_by_code(client, tokens["CSKH"], "SPXTST0000009")
    assert (cancelled["warehouse_status"], cancelled["shop"]["name"]) == ("CANCELLED", "TST Shop (mock)")
    platforms = {r["platform"] for r in client.get("/returns", params={"tab": "ALL", "page_size": 100},
                                                   headers=tokens["CSKH"]).json()["items"]}  # fmt: skip
    assert {"SHOPEE", "TIKTOK"} <= platforms


def test_seed_demo_rerun_idempotent() -> None:
    """Chạy lại `aicam seed-demo` trên DB đã seed → không nhân đôi shop / đơn / kiện / hồ sơ / tài khoản."""
    before = p3.psql(COUNTS)
    out = p3.compose("exec", "-T", "api", "aicam", "seed-demo", timeout=300)
    assert out.returncode == 0, out.stdout[-2000:] + out.stderr[-2000:]
    assert "= shop TikTok TTMOCKB" in out.stdout
    assert p3.psql(COUNTS) == before


def test_tc_r3_08_station_cancel_pack_not_br37(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-R3.08: phiên PACK mở 5 phút → station tự hủy (API-12) → 200 như Phase 1 (luật 60 giây BR-37 chỉ áp
    phiên RETURN)."""
    st = tokens["STATION"]
    opened = p3.scan(client, st, "SPXTST0000008")
    assert opened["outcome"] == "SESSION_OPENED", opened
    sid = opened["state"]["session"]["id"]
    p3.psql(f"UPDATE session SET started_at = now() - interval '5 minutes' WHERE id = '{sid}'")  # noqa: S608
    res = client.post(f"/station/sessions/{sid}/cancel", json={"reason": "OUT_OF_STOCK"}, headers=st)
    assert res.status_code == 200, res.text
    assert res.json()["state"]["session"] is None
    sql = "SELECT status, cancel_reason FROM session WHERE id = '%s'"
    assert p3.psql(sql % sid) == "CANCELLED|OUT_OF_STOCK"


def test_tc_r3_03_old_shopee_route(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-R3.03 (API): đường cũ `POST /shops/shopee/auth-url` vẫn chạy → callback về D7
    `/admin/settings/platforms?platform=shopee&result=connected&count=1`; shop Shopee khác không bị ngắt."""
    res = client.post("/shops/shopee/auth-url", headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    url = httpx.URL(res.json()["url"])
    cookie = {"Cookie": f"aicam_shopee_state={res.cookies['aicam_shopee_state']}"}
    cb = client.get(f"{p3.BASE}{url.raw_path.decode()}", headers=cookie, follow_redirects=False)
    assert (cb.status_code, cb.headers["location"]) == (
        302, "/admin/settings/platforms?platform=shopee&result=connected&count=1",
    )  # fmt: skip
    assert {s["auth_status"] for s in p3.shops(client, tokens["ADMIN"]).values()} == {"CONNECTED"}
