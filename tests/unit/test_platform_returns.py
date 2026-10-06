"""Adapter yêu cầu trả hàng (T-103; FR-05.05, 05.07, 05.11, 05.12; 02a §7, DEC-259, DEC-262).

Shopee: HTTP giả (respx) theo tài liệu công khai v2 — **chưa test với Shopee thật, thiếu partner (T-3)**;
TC-05.44, TC-05.45 vẫn "chưa test — thiếu tài nguyên". Mock: 4 fixture (02a §7).
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from aicam.core import clock
from aicam.modules.platforms.base import ShipmentRef, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee import mapping, returns_mapping
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient

BASE = "https://partner.test-stable.shopeemobile.com"
NOW = datetime(2026, 10, 6, 1, 0, tzinfo=UTC)
CREDS = ShopCredentials("990001", "acc", "ref", NOW + timedelta(hours=4))


@pytest.fixture(autouse=True)
def _clock() -> None:
    clock.freeze(NOW)


def ok(response: Any) -> httpx.Response:
    return httpx.Response(200, json={"error": "", "message": "", "request_id": "r", "response": response})


def _adapter(page_size: int = 2) -> ShopeeAdapter:
    async def _sleep(_: float) -> None:
        return None

    client = ShopeeClient(2001234, "k", BASE, max_attempts=2, backoff_s=0.0, sleep=_sleep)
    return ShopeeAdapter(client, returns_page_size=page_size, returns_window=timedelta(days=15))


def _detail(sn: str, status: str = "ACCEPTED", **kw: Any) -> dict[str, Any]:
    return {
        "return_sn": sn,
        "order_sn": f"ORD{sn}",
        "status": status,
        "reason": "ITEM_DAMAGED",
        "text_reason": "Áo bị rách",
        "tracking_number": "spxrt0001",
        "needs_logistics": True,
        "return_seller_due_date": int((NOW + timedelta(days=3)).timestamp()),
        "create_time": int(NOW.timestamp()) - 3600,
        "update_time": int(NOW.timestamp()) - 60,
        "item": [
            {
                "item_id": 1,
                "model_id": 11,
                "name": "Áo",
                "model_name": "Đen / L",
                "item_sku": "AT",
                "amount": 2,
            }
        ],
        **kw,
    }


# ---------------------------------------------------------------- mapping (02a §7)


@pytest.mark.parametrize(
    ("status", "group"),
    [
        ("REQUESTED", "OPEN"),
        ("PROCESSING", "OPEN"),
        ("ACCEPTED", "OPEN"),
        ("JUDGING", "OPEN"),
        ("SELLER_DISPUTE", "OPEN"),
        ("CANCELLED", "CANCELLED"),
        ("REFUND_PAID", "DONE"),
        ("CLOSED", "CLOSED"),
        ("SOMETHING_NEW", "OPEN"),
    ],
)
def test_status_group(status: str, group: str) -> None:
    """DEC-262: DONE chỉ REFUND_PAID; CLOSED tách riêng; trạng thái lạ → OPEN (không tự hủy hồ sơ)."""
    assert returns_mapping.status_group(status) == group


def test_reason_normalized_and_labelled() -> None:
    assert returns_mapping.normalize_reason("item_damaged") == "ITEM_DAMAGED"
    assert returns_mapping.normalize_reason("NEW_REASON_X") == "OTHER"
    assert returns_mapping.normalize_reason(None) is None
    assert set(returns_mapping.REASON_LABELS) == returns_mapping.REASONS


def test_to_platform_return_fields() -> None:
    ret = returns_mapping.to_platform_return(_detail("RT1", needs_logistics=False))

    assert (ret.return_sn, ret.order_sn, ret.status_group) == ("RT1", "ORDRT1", "OPEN")
    assert ret.needs_parcel is False
    assert ret.return_tracking_number == "SPXRT0001"
    assert ret.seller_due_at == NOW + timedelta(days=3)
    assert ret.items[0].quantity == 2
    assert (ret.items[0].sku, ret.items[0].variation) == ("AT", "Đen / L")
    assert ret.raw["text_reason"] == "Áo bị rách"


def test_missing_needs_logistics_means_parcel() -> None:
    detail = _detail("RT2")
    del detail["needs_logistics"]

    assert returns_mapping.to_platform_return(detail).needs_parcel is True


# ---------------------------------------------------------------- hint RETURN_EXPECTED (DEC-259)


@pytest.mark.parametrize(
    ("order_status", "logistics", "hint"),
    [
        ("TO_RETURN", "", "RETURN_EXPECTED"),
        ("SHIPPED", "LOGISTICS_DELIVERY_FAILED", "RETURN_EXPECTED"),
        ("SHIPPED", "LOGISTICS_COD_REJECTED", "RETURN_EXPECTED"),
        ("COMPLETED", "LOGISTICS_DELIVERY_FAILED", "RETURN_EXPECTED"),  # tín hiệu hoàn thắng DELIVERED
        ("SHIPPED", "LOGISTICS_PICKUP_DONE", "HANDED_OVER"),
        ("SHIPPED", "LOGISTICS_LOST", "HANDED_OVER"),
        ("TO_CONFIRM_RECEIVE", "", "DELIVERED"),
        ("READY_TO_SHIP", "LOGISTICS_READY", None),
    ],
)
def test_warehouse_hint_rank(order_status: str, logistics: str, hint: str | None) -> None:
    assert mapping.warehouse_hint(order_status, logistics) == hint


# ---------------------------------------------------------------- Shopee HTTP giả


@respx.mock
async def test_list_returns_pages_and_windows() -> None:
    """get_return_list: page_no / page_size, cửa sổ update_time ≤ 15 ngày (chưa xác nhận ở T-3)."""
    calls: list[dict[str, list[str]]] = []

    def _list(request: httpx.Request) -> httpx.Response:
        q = parse_qs(urlparse(str(request.url)).query)
        calls.append(q)
        page = int(q["page_no"][0])
        if page == 1:
            return ok({"return": [_detail("RT1"), _detail("RT2")], "more": True})
        return ok({"return": [_detail("RT3", status="REFUND_PAID")], "more": False})

    respx.get(f"{BASE}/api/v2/returns/get_return_list").mock(side_effect=_list)

    since = NOW - timedelta(days=20)
    out = [r async for r in _adapter().list_returns(CREDS, since)]

    # 20 ngày → 2 cửa sổ (15 + 5); cửa sổ 1 hai trang, cửa sổ 2 hai trang (mock trả cùng dữ liệu).
    assert [r.return_sn for r in out][:3] == ["RT1", "RT2", "RT3"]
    assert out[2].status_group == "DONE"
    windows = {(c["update_time_from"][0], c["update_time_to"][0]) for c in calls}
    assert len(windows) == 2
    for start, end in windows:
        assert int(end) - int(start) <= 15 * 86400
    assert calls[0]["page_size"] == ["2"]


@respx.mock
async def test_get_return_detail() -> None:
    route = respx.get(f"{BASE}/api/v2/returns/get_return_detail").mock(return_value=ok(_detail("RT9")))

    ret = await _adapter().get_return(CREDS, "RT9")

    assert ret is not None
    assert ret.return_sn == "RT9"
    assert parse_qs(urlparse(str(route.calls[0].request.url)).query)["return_sn"] == ["RT9"]


async def test_shopee_returns_without_creds() -> None:
    adapter = _adapter()

    assert [r async for r in adapter.list_returns(None, NOW)] == []
    assert await adapter.get_return(None, "RT1") is None


# ---------------------------------------------------------------- mock (4 fixture)


async def test_mock_has_four_fixture_kinds() -> None:
    mock = MockAdapter()

    out = {r.return_sn: r async for r in mock.list_returns(None, NOW - timedelta(days=1))}

    assert set(out) == {"2410RTTST041", "2410RTTST044", "2410RTTST045"}
    buyer = out["2410RTTST041"]
    assert (buyer.order_sn, buyer.status_group, buyer.needs_parcel) == ("2410TST00041", "OPEN", True)
    assert buyer.return_tracking_number == "SPXRTTST000041"
    assert buyer.reason == "ITEM_DAMAGED"
    assert buyer.seller_due_at == NOW + timedelta(hours=72)
    assert out["2410RTTST044"].needs_parcel is False
    assert out["2410RTTST045"].status_group == "CANCELLED"
    assert await mock.get_return(None, "2410RTTST041") == buyer
    assert await mock.get_return(None, "KHONGCO") is None


async def test_mock_failed_delivery_order_and_hint() -> None:
    """failed_delivery_order.json: đơn TO_RETURN 2 kiện → hint RETURN_EXPECTED cho cả hai kiện."""
    mock = MockAdapter()
    order = await mock.get_order(None, "2410TST00043")

    assert order is not None
    assert order.status == "TO_RETURN"
    assert order.tracking_numbers == ("SPXTST0000043-1", "SPXTST0000043-2")
    refs = [ShipmentRef("2410TST00043", c) for c in order.tracking_numbers]
    hints = {s.tracking_number: s.warehouse_hint for s in await mock.get_shipping_statuses(None, refs)}
    assert hints == {"SPXTST0000043-1": "RETURN_EXPECTED", "SPXTST0000043-2": "RETURN_EXPECTED"}


async def test_mock_return_controls() -> None:
    mock = MockAdapter()
    later = NOW + timedelta(minutes=5)
    mock.set_return_status("2410RTTST045", "ACCEPTED", later)

    ret = await mock.get_return(None, "2410RTTST045")
    assert ret is not None
    assert ret.status_group == "OPEN"
    # Chỉ yêu cầu đổi sau mốc `since` (cursor J-13) được trả lại.
    assert [r.return_sn async for r in mock.list_returns(None, NOW + timedelta(minutes=1))] == [
        "2410RTTST045"
    ]
