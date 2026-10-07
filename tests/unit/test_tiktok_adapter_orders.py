"""T-209: TikTok adapter đơn / kiện / vận chuyển / yêu cầu hủy / tra khi quét + kiện gộp + EX-T5 trên HTTP giả
(respx) theo **định dạng giả định** 02a §7.1. **Chưa test với TikTok thật — thiếu tài khoản đối tác.**
"""

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
import respx

from aicam.core import clock
from aicam.modules.platforms.base import ShipmentRef, ShopCredentials
from aicam.modules.platforms.tiktok.adapter import TikTokAdapter
from aicam.modules.platforms.tiktok.client import TikTokClient

API = "https://open-api.tiktok.test"
NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)
CREDS = ShopCredentials(
    "7001", "tt-acc", "tt-ref", NOW + timedelta(days=1), shop_cipher="C1", grant_ref="OPEN-1"
)


@pytest.fixture(autouse=True)
def _clock() -> Any:
    clock.freeze(NOW)
    yield
    clock.reset()


@pytest.fixture
def adapter() -> TikTokAdapter:
    async def _no_sleep(_: float) -> None:
        return None

    client = TikTokClient("k", "s", API, "https://auth.test", max_attempts=2, sleep=_no_sleep)
    return TikTokAdapter(client, authorize_url="a", service_id="1")


def ok(data: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "message": "Success", "request_id": "r", "data": data})


def line(
    tracking: str | None, *, sku: str = "AT-DEN-L", name: str = "Áo thun", pkg: str = "P1", **kw: Any
) -> dict[str, Any]:
    return {
        "id": f"L{tracking}{sku}", "product_name": name, "sku_name": "Đen / L", "seller_sku": sku,
        "sku_image": "https://img/x.jpg", "package_id": pkg, "tracking_number": tracking, **kw,
    }  # fmt: skip


def detail(
    oid: str, status: str = "AWAITING_SHIPMENT", lines: list[dict[str, Any]] | None = None, **kw: Any
) -> dict[str, Any]:
    return {
        "id": oid, "status": status, "line_items": lines if lines is not None else [line(f"TT{oid}")],
        "buyer_message": "", "fulfillment_type": "FULFILLMENT_BY_SELLER", "create_time": 1790000000,
        "update_time": 1790000100, **kw,
    }  # fmt: skip


def _details_route(orders: list[dict[str, Any]]) -> respx.Route:
    by_id = {o["id"]: o for o in orders}

    def _side(request: httpx.Request) -> httpx.Response:
        ids = parse_qs(request.url.query.decode())["ids"][0].split(",")
        return ok({"orders": [by_id[i] for i in ids if i in by_id]})

    return respx.get(f"{API}/order/202309/orders").mock(side_effect=_side)


@respx.mock
async def test_list_updated_orders_union_with_cancellations_always_reads_detail(
    adapter: TikTokAdapter,
) -> None:
    """J-04 (DEC-502): đơn đổi + đơn có yêu cầu hủy đổi trong cửa sổ (kể cả khi `orders/search` không trả đơn
    đó) → luôn đọc chi tiết; nhóm theo yêu cầu hủy mới nhất; khử trùng; phân trang `next_page_token`."""
    search = respx.post(f"{API}/order/202309/orders/search").mock(
        side_effect=[
            ok({"orders": [{"id": "5761001"}, {"id": "5761002"}], "next_page_token": "p2"}),
            ok({"orders": [{"id": "5761001"}], "next_page_token": ""}),
        ]
    )
    respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok(
            {
                "cancellations": [
                    {"order_id": "5761003", "cancel_status": "PENDING", "update_time": 1790000050},
                    {"order_id": "5761002", "cancel_status": "PENDING", "update_time": 1790000010},
                    {"order_id": "5761002", "cancel_status": "REJECTED", "update_time": 1790000060},
                ]
            }
        )
    )
    details = _details_route([detail("5761001"), detail("5761002"), detail("5761003")])
    since = NOW - timedelta(minutes=10)
    orders = [o async for o in adapter.list_updated_orders(CREDS, since)]
    assert [(o.platform_order_sn, o.status_group) for o in orders] == [
        ("5761001", "AWAITING_SHIPMENT"),
        ("5761002", "AWAITING_SHIPMENT"),  # yêu cầu mới nhất bị từ chối
        ("5761003", "CANCEL_REQUESTED"),  # chỉ có ở cancellations/search
    ]
    body = json.loads(search.calls[0].request.content)
    assert body == {"update_time_ge": int(since.timestamp()), "update_time_lt": int(NOW.timestamp())}
    q = parse_qs(search.calls[1].request.url.query.decode())
    assert (q["page_token"], q["page_size"]) == (["p2"], ["50"])
    assert details.call_count == 1
    assert parse_qs(details.calls[0].request.url.query.decode())["ids"] == ["5761001,5761002,5761003"]
    assert orders[2].raw["latest_cancel"]["cancel_status"] == "PENDING"
    assert details.calls[0].request.headers["x-tts-access-token"] == "tt-acc"
    assert parse_qs(details.calls[0].request.url.query.decode())["shop_cipher"] == ["C1"]


@respx.mock
async def test_order_fields_items_tracking_and_fulfillment(adapter: TikTokAdapter) -> None:
    """Dòng hàng TikTok mỗi đơn vị một `line_item` → gộp số lượng; mỗi kiện một mã vận đơn; EX-T5 đơn kho
    TikTok gắn `fulfilled_by_platform`."""
    _details_route(
        [
            detail(
                "5761010",
                lines=[
                    line("TTA1", sku="AO"),
                    line("TTA1", sku="AO"),
                    line("TTA2", sku="TAT", name="Tất", pkg="P2"),
                    line("TTA2", sku="X", display_status="CANCELLED", pkg="P2"),
                ],
                buyer_message="Gói kỹ",
            ),
            detail("5761011", fulfillment_type="FULFILLMENT_BY_TIKTOK"),
        ]
    )
    respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok({"cancellations": []})
    )
    order = await adapter.get_order(CREDS, "5761010")
    assert order is not None
    assert order.tracking_numbers == ("TTA1", "TTA2")
    assert [(i.product_name, i.quantity, i.sku) for i in order.items] == [
        ("Áo thun", 2, "AO"),
        ("Tất", 1, "TAT"),
    ]
    assert (order.buyer_note, order.fulfilled_by_platform) == ("Gói kỹ", False)
    assert order.created_at == datetime.fromtimestamp(1790000000, tz=UTC)
    fbt = await adapter.get_order(CREDS, "5761011")
    assert fbt is not None
    assert fbt.fulfilled_by_platform is True


@respx.mock
async def test_merged_package_same_batch_and_combined_tag(adapter: TikTokAdapter) -> None:
    """FR-05.22 kiện gộp: hai đơn chung mã vận đơn trong lô → mỗi đơn có `merged_order_sns` là đơn kia; đơn
    `COMBINED` một mình → hỏi kiện lấy đơn còn lại."""
    respx.post(f"{API}/order/202309/orders/search").mock(
        return_value=ok({"orders": [{"id": "5761077"}, {"id": "5761078"}]})
    )
    respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok({"cancellations": []})
    )
    _details_route(
        [
            detail("5761077", lines=[line("TTTST0000000077", pkg="PK77")]),
            detail("5761078", lines=[line("TTTST0000000077", pkg="PK77")]),
            detail("5761079", lines=[line("TTTST0000000079", pkg="PK79")], split_or_combine_tag="COMBINED"),
        ]
    )
    pkg = respx.get(f"{API}/fulfillment/202309/packages/PK79").mock(
        return_value=ok({"id": "PK79", "orders": [{"id": "5761079"}, {"id": "5761080"}]})
    )
    orders = [o async for o in adapter.list_updated_orders(CREDS, NOW - timedelta(minutes=5))]
    assert {o.platform_order_sn: o.merged_order_sns for o in orders} == {
        "5761077": ("5761078",),
        "5761078": ("5761077",),
    }
    single = await adapter.get_order(CREDS, "5761079")
    assert single is not None
    assert single.merged_order_sns == ("5761080",)
    await adapter.get_order(CREDS, "5761079")
    assert pkg.call_count == 1  # bộ nhớ kiện


@respx.mock
async def test_shipping_statuses_from_detail(adapter: TikTokAdapter) -> None:
    """J-06: đọc lại chi tiết theo lô → nhóm + `package_status` → gợi ý kho; kiện giao thất bại →
    RETURNING."""
    _details_route(
        [
            detail("5761020", "IN_TRANSIT", [line("TTS1")]),
            detail("5761021", "IN_TRANSIT", [line("TTS2", package_status="DELIVERY_FAILED")]),
            detail("5761022", "AWAITING_SHIPMENT", [line("TTS3")], is_buyer_request_cancel=True),
            detail("5761023", "AWAITING_COLLECTION", [line("TTS4")]),  # cờ vắng, yêu cầu hủy PENDING
        ]
    )
    cancels = respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok(
            {
                "cancellations": [
                    {"cancel_id": "C23", "order_id": "5761023", "cancel_status": "PENDING", "update_time": 1}
                ]
            }
        )
    )
    refs = [
        ShipmentRef("5761020", "tts1"), ShipmentRef("5761021", "TTS2"), ShipmentRef("5761022", "TTS3"),
        ShipmentRef("5761023", "TTS4"),
    ]  # fmt: skip
    out = await adapter.get_shipping_statuses(CREDS, refs)
    assert [(s.tracking_number, s.warehouse_hint, s.order_status_group) for s in out] == [
        ("TTS1", "HANDED_OVER", "SHIPPED"),
        ("TTS2", "RETURN_EXPECTED", "RETURNING"),
        ("TTS3", None, "CANCEL_REQUESTED"),
        ("TTS4", None, "CANCEL_REQUESTED"),  # G3-MS-2: tra yêu cầu hủy theo lô
    ]
    assert out[0].order_status == "IN_TRANSIT"
    assert cancels.call_count == 1  # một lời gọi cho cả lô, chỉ đơn còn hủy được
    assert json.loads(cancels.calls[0].request.content)["order_ids"] == ["5761022", "5761023"]


@respx.mock
async def test_find_by_tracking_lookback_one_page_then_cache(adapter: TikTokAdapter) -> None:
    """AS-12: không có API tra theo mã vận đơn → 1 trang đơn cập nhật 60 phút gần nhất; lần sau dùng bộ
    nhớ."""
    search = respx.post(f"{API}/order/202309/orders/search").mock(
        return_value=ok({"orders": [{"id": "5761030"}, {"id": "5761031"}], "next_page_token": "more"})
    )
    respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok({"cancellations": []})
    )
    _details_route([detail("5761030", lines=[line("TTF1")]), detail("5761031", lines=[line("TTF2")])])
    found = await adapter.find_by_tracking(CREDS, "ttf2")
    assert found is not None
    assert found.platform_order_sn == "5761031"
    assert search.call_count == 1  # chỉ 1 trang
    body = json.loads(search.calls[0].request.content)
    assert body["update_time_ge"] == int((NOW - timedelta(minutes=60)).timestamp())
    assert await adapter.find_by_tracking(CREDS, "TTF1") is not None
    assert search.call_count == 1  # TTF1 đã nhớ từ lần dò trước
    assert await adapter.find_by_tracking(CREDS, "KHONGCO") is None


@respx.mock
async def test_search_max_pages_raises_instead_of_dropping_orders(
    adapter: TikTokAdapter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3-MS-4: chạm `SEARCH_MAX_PAGES` mà sàn vẫn trả `next_page_token` → `PlatformError` (J-04 FAILED,
    cursor không tiến) — không trả nửa danh sách để cursor tiến qua đơn chưa đọc."""
    from aicam.modules.platforms import base
    from aicam.modules.platforms.tiktok import adapter as tiktok_adapter

    monkeypatch.setattr(tiktok_adapter, "SEARCH_MAX_PAGES", 3)
    search = respx.post(f"{API}/order/202309/orders/search").mock(
        return_value=ok({"orders": [{"id": "5761090"}], "next_page_token": "more"})
    )
    respx.post(f"{API}/return_refund/202309/cancellations/search").mock(
        return_value=ok({"cancellations": []})
    )
    _details_route([detail("5761090")])
    with pytest.raises(base.PlatformError, match="trang"):
        _ = [o async for o in adapter.list_updated_orders(CREDS, NOW - timedelta(hours=1))]
    assert search.call_count == 3
