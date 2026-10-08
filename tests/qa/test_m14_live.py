"""QA live item 03 — M14 (báo cáo D20: API-150..153) trên stack thật (T-229).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m14` (tự `qa-reset.sh`). Dữ liệu = seed-demo Phase 3
(không phải `report_fixture` PRE-20 — fixture đó chỉ có ở INT `tests/integration/test_reports_api.py`, nơi
kiểm
đúng từng con số TC-09.30 / 31 / 34 / 35 — DEC-823). Ở đây kiểm trên stack: công thức tỷ lệ = tử / mẫu, cộng
dồn
theo shop / loại khớp SQL, lọc sàn / shop (TC-09.32), kỳ trống (TC-09.33), quyền (TC-09.36), kỳ sai
(TC-09.37), CSV
(TC-09.40 phần API) + audit `REPORT_EXPORT`.
"""

from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark

TODAY = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()  # "hôm nay" của báo cáo = giờ VN
PERIOD = {"from": (TODAY - timedelta(days=29)).isoformat(), "to": TODAY.isoformat()}


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


def _get(client: httpx.Client, headers: dict[str, str], report: str, **params: Any) -> dict[str, Any]:
    res = client.get(f"/reports/{report}", params={**PERIOD, **params}, headers=headers)
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def _rate(card: dict[str, Any]) -> None:
    if card["denominator"]:
        assert card["value"] == pytest.approx(card["numerator"] / card["denominator"], abs=1e-4), card
    else:
        assert card["value"] is None, card


def test_tc_09_30_31_returns_report_consistent(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-09.30 / 31 (trên seed): tỷ lệ = tử / mẫu; Chỉ hoàn tiền tách riêng (không vào tử tỷ lệ hoàn);
    `by_shop` cộng = tử; số hồ sơ TikTok theo shop khớp SQL."""
    rep = _get(client, tokens["SUPERVISOR"], "returns")
    assert rep["period"] == PERIOD
    cards = rep["cards"]
    _rate(cards["return_rate"])
    _rate(cards["issue_rate"])
    kinds = {k["kind"]: k["count"] for k in rep["by_kind"]}
    assert cards["refund_only"]["count"] == kinds.get("REFUND_ONLY", 0)
    assert sum(s["return_cases"] for s in rep["by_shop"]) == cards["return_rate"]["numerator"]
    assert sum(s["handed_over"] for s in rep["by_shop"]) == cards["return_rate"]["denominator"]
    tiktok = [s for s in rep["by_shop"] if s["platform"] == "TIKTOK"]
    sql = p3.psql(
        "SELECT count(*) FROM return_case rc JOIN shop s ON s.id = rc.shop_id "
        "WHERE s.platform = 'TIKTOK' AND rc.kind NOT IN ('REFUND_ONLY', 'UNIDENTIFIED')"
    )
    assert sum(s["return_cases"] for s in tiktok) == int(sql)
    assert set(rep["reason_by_conclusion"]["conclusions"]) >= {"OK", "DAMAGED", "MISSING_ITEM"}
    assert all({"sku", "return_requests"} <= set(p) for p in rep["top_products"])
    assert len(rep["top_products"]) <= 20


def test_tc_09_32_filter_platform_shop(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-09.32: `platform=TIKTOK` → chỉ số chỉ gồm TikTok; `shop_id=<990002>` → chỉ shop đó."""
    sup = tokens["SUPERVISOR"]
    every = _get(client, sup, "returns")
    tiktok = _get(client, sup, "returns", platform="TIKTOK")
    assert tiktok["filters"]["platform"] == "TIKTOK"
    assert {s["platform"] for s in tiktok["by_shop"]} <= {"TIKTOK"}
    assert tiktok["cards"]["return_rate"]["numerator"] <= every["cards"]["return_rate"]["numerator"]
    shop_b = p3.shops(client, tokens["ADMIN"])["TST B"]["id"]
    only_b = _get(client, sup, "returns", shop_id=shop_b)
    assert {s["shop_id"] for s in only_b["by_shop"]} <= {shop_b}
    for report in ("claims", "productivity"):
        assert _get(client, sup, report, platform="TIKTOK")["filters"]["platform"] == "TIKTOK"


def test_tc_09_33_empty_period(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-09.33: kỳ không có dữ liệu → tỷ lệ `null`, bảng trống."""
    rep = client.get("/reports/returns", params={"from": "2026-01-01", "to": "2026-01-31"},
                     headers=tokens["SUPERVISOR"]).json()  # fmt: skip
    assert rep["cards"]["return_rate"] == {"numerator": 0, "denominator": 0, "value": None}
    assert rep["cards"]["issue_rate"]["value"] is None
    assert rep["reason_by_conclusion"]["rows"] == []
    assert rep["top_products"] == []


def test_tc_09_34_35_claims_productivity_shape(
    client: httpx.Client, tokens: dict[str, dict[str, str]]
) -> None:
    """TC-09.34 / 35 (trên seed): báo cáo khiếu nại / năng suất trả đủ khối; tỷ lệ = tử / mẫu."""
    claims = _get(client, tokens["CSKH"], "claims")
    assert {"cards", "by_status", "by_type_result", "by_counterparty", "by_shop", "series"} <= set(claims)
    for card in claims["cards"].values():
        if isinstance(card, dict) and {"numerator", "denominator", "value"} <= set(card):
            _rate(card)
    prod = _get(client, tokens["SUPERVISOR"], "productivity")
    assert {"cards", "by_station", "by_operator", "return_by_operator"} <= set(prod)


def test_tc_09_36_cskh_no_productivity(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-09.36: CSKH API-152 / API-153 `productivity` → 403 `FORBIDDEN`; báo cáo khác được; STATION 403."""
    cskh = tokens["CSKH"]
    assert p3.err(client.get("/reports/productivity", params=PERIOD, headers=cskh)) == (403, "FORBIDDEN")
    res = client.get("/reports/productivity/export", params=PERIOD, headers=cskh)
    assert p3.err(res) == (403, "FORBIDDEN")
    assert client.get("/reports/returns", params=PERIOD, headers=cskh).status_code == 200
    assert client.get("/reports/returns", params=PERIOD, headers=tokens["STATION"]).status_code == 403


def test_tc_09_37_bad_period(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-09.37: `from > to`; 367 ngày; 366 ngày (200); `to` = ngày mai."""
    sup = tokens["SUPERVISOR"]
    end = TODAY - timedelta(days=1)
    cases = [
        ({"from": "2026-09-30", "to": "2026-09-01"}, {"to": "Ngày đến phải sau ngày từ."}),
        ({"from": (end - timedelta(days=366)).isoformat(), "to": end.isoformat()},
         {"from": "Chọn tối đa 366 ngày."}),
        ({"from": TODAY.isoformat(), "to": (TODAY + timedelta(days=1)).isoformat()},
         {"to": "Không chọn ngày trong tương lai."}),
    ]  # fmt: skip
    for params, fields in cases:
        res = client.get("/reports/returns", params=params, headers=sup)
        assert res.status_code == 422, (params, res.text)
        assert res.json()["error"]["details"]["fields"] == fields
    params = {"from": (end - timedelta(days=365)).isoformat(), "to": end.isoformat()}
    assert client.get("/reports/returns", params=params, headers=sup).status_code == 200


def test_tc_09_40_csv_export_and_audit(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-09.40 (phần API): API-153 `returns` → `text/csv; charset=utf-8` có BOM, tên tệp theo kỳ,
    dấu `;` (BUG-G4-1), tiêu đề tiếng Việt, tỷ lệ dạng "x,y%"; audit `REPORT_EXPORT {report, from, to}`.
    Mở Excel: MAN."""
    res = client.get("/reports/returns/export", params=PERIOD, headers=tokens["SUPERVISOR"])
    assert res.status_code == 200, res.text
    assert res.headers["content-type"] == "text/csv; charset=utf-8"
    name = f"bao-cao-hang-hoan-{PERIOD['from']}_{PERIOD['to']}.csv"
    assert res.headers["content-disposition"] == f'attachment; filename="{name}"'
    assert res.content.startswith("﻿".encode())
    text = res.content.decode("utf-8-sig")
    assert text.startswith("Báo cáo hàng hoàn\r\n")
    assert "Chỉ số;Giá trị;Tử số;Mẫu số" in text
    rep = _get(client, tokens["SUPERVISOR"], "returns")
    card = rep["cards"]["return_rate"]
    if card["value"] is not None:
        shown = f"{card['value'] * 100:.1f}".replace(".", ",")
        assert f"Tỷ lệ hoàn;{shown}%;{card['numerator']};{card['denominator']}" in text
    rows = p3.audit_rows(client, tokens["ADMIN"], "REPORT_EXPORT")
    assert len(rows) == 1
    assert {"report": "returns", "from": PERIOD["from"], "to": PERIOD["to"]}.items() <= rows[0][
        "data"
    ].items()
    for report in ("claims", "productivity"):
        out = client.get(f"/reports/{report}/export", params=PERIOD, headers=tokens["ADMIN"])
        assert out.status_code == 200, (report, out.text)
        assert out.content.startswith("﻿".encode())
