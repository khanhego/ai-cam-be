"""T-210: yêu cầu trả TikTok → `PlatformReturn` (BR-31, 02 §5.3 nhóm yêu cầu trả 5 giá trị) + adapter
`list_returns` / `get_return` trên HTTP giả theo **định dạng giả định** (02a §7.1). Chưa test với TikTok thật.
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx

from aicam.core import clock
from aicam.modules.platforms.base import RETURN_STATUS_GROUPS, ShopCredentials
from aicam.modules.platforms.tiktok import returns_mapping
from aicam.modules.platforms.tiktok.adapter import TikTokAdapter
from aicam.modules.platforms.tiktok.client import TikTokClient
from aicam.modules.returns.views import reason_label

API = "https://open-api.tiktok.test"
NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)
CREDS = ShopCredentials("7001", "tt-acc", "r", NOW + timedelta(days=1), shop_cipher="C1")


def _ret(**over: Any) -> dict[str, Any]:
    data = {
        "return_id": "RT-1", "order_id": "5761061", "return_type": "RETURN_AND_REFUND",
        "return_status": "AWAITING_BUYER_SHIP",
        "return_reason": "ecom_order_delivered_refund_and_return_reason_wrong_item",
        "return_reason_text": "Giao sai màu", "return_tracking_number": "ttrt0001",
        "return_line_items": [
            {"seller_sku": "AO", "product_name": "Áo", "sku_name": "Đen", "sku_id": "S1"},
            {"seller_sku": "AO", "product_name": "Áo", "sku_name": "Đen", "sku_id": "S1"},
        ],
        "create_time": 1790000000, "update_time": 1790000100,
    }  # fmt: skip
    data.update(over)
    return data


@pytest.mark.parametrize(
    ("status", "group"),
    [
        ("RETURN_OR_REFUND_REQUEST_PENDING", "REQUESTED"),
        ("AWAITING_BUYER_SHIP", "ACCEPTED"),
        ("BUYER_SHIPPED_ITEM", "ACCEPTED"),
        ("RECEIVE_REJECTED", "ACCEPTED"),
        ("REQUEST_REJECTED", "CANCELLED"),
        ("RETURN_OR_REFUND_REQUEST_CANCEL", "CANCELLED"),
        ("RETURN_OR_REFUND_REQUEST_COMPLETE", "DONE"),
        ("RETURN_OR_REFUND_REQUEST_CLOSED", "CLOSED"),
        ("XYZ", None),
    ],
)
def test_status_groups(status: str, group: str | None) -> None:
    assert returns_mapping.status_group(status) == group
    assert group is None or group in RETURN_STATUS_GROUPS


def test_types_br31() -> None:
    """BR-31: chỉ hoàn tiền → không cần kiện; trả + hoàn → cần kiện; đổi hàng → cần kiện, lý do "Đổi hàng"."""
    refund = returns_mapping.to_platform_return(_ret(return_type="REFUND_ONLY", return_tracking_number=None))
    assert (refund.needs_parcel, refund.is_exchange, refund.return_tracking_number) == (False, False, None)
    both = returns_mapping.to_platform_return(_ret())
    assert (both.needs_parcel, both.is_exchange, both.reason) == (True, False, "WRONG_ITEM")
    assert both.return_tracking_number == "TTRT0001"
    assert [(i.sku, i.quantity, i.variation) for i in both.items] == [("AO", 2, "Đen")]
    assert (both.return_sn, both.order_sn, both.status_group) == ("RT-1", "5761061", "ACCEPTED")
    swap = returns_mapping.to_platform_return(_ret(return_type="REPLACEMENT"))
    assert (swap.needs_parcel, swap.is_exchange, swap.reason) == (True, True, "EXCHANGE")
    assert reason_label("EXCHANGE") == "Đổi hàng"
    assert reason_label("WRONG_ITEM") == "Sai sản phẩm"
    odd = returns_mapping.to_platform_return(_ret(return_type="ABC", return_reason="xx"))
    assert (odd.needs_parcel, odd.reason) == (True, "OTHER")  # loại lạ → chờ kiện (an toàn)
    assert odd.seller_due_at is None  # tên trường hạn người bán chưa rõ (L14)
    due = returns_mapping.to_platform_return(_ret(seller_response_deadline=1790086400))
    assert due.seller_due_at == datetime.fromtimestamp(1790086400, tz=UTC)


@pytest.fixture(autouse=True)
def _clock() -> Any:
    clock.freeze(NOW)
    yield
    clock.reset()


@respx.mock
async def test_list_and_get_return() -> None:
    async def _no_sleep(_: float) -> None:
        return None

    adapter = TikTokAdapter(
        TikTokClient("k", "s", API, "https://auth.test", max_attempts=2, sleep=_no_sleep),
        authorize_url="a", service_id="1",
    )  # fmt: skip
    route = respx.post(f"{API}/return_refund/202309/returns/search").mock(
        side_effect=[
            httpx.Response(
                200, json={"code": 0, "data": {"return_orders": [_ret()], "next_page_token": "p2"}}
            ),
            httpx.Response(
                200, json={"code": 0, "data": {"return_orders": [_ret(return_id="RT-2", order_id="")]}}
            ),
            httpx.Response(200, json={"code": 0, "data": {"return_orders": [_ret(return_id="RT-9")]}}),
        ]
    )
    since = NOW - timedelta(minutes=15)
    rows = [r async for r in adapter.list_returns(CREDS, since)]
    assert [r.return_sn for r in rows] == ["RT-1"]  # thiếu order_id → bỏ
    assert json.loads(route.calls[0].request.content) == {
        "update_time_ge": int(since.timestamp()), "update_time_lt": int(NOW.timestamp())
    }  # fmt: skip
    got = await adapter.get_return(CREDS, "RT-9")
    assert got is not None
    assert got.return_sn == "RT-9"
    assert json.loads(route.calls[2].request.content) == {"return_ids": ["RT-9"]}
