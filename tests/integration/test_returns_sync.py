"""J-13 `sync_returns`, J-06 / J-04 mở rộng (T-105) — FR-05.05, 05.11, 05.12, 03.15;
DEC-248, 254, 258, 259, 267.

TC-05.30..05.43 (trừ 05.44 / 05.45: **chưa test với Shopee thật — thiếu partner T-3**). Adapter mock (PRE-9) +
adapter Shopee trên HTTP giả (respx) cho TC-05.42.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, Shop, StatusHistory
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MOCK_SHOP_ID, MockAdapter
from aicam.modules.platforms.shopee.adapter import ShopeeAdapter
from aicam.modules.platforms.shopee.client import ShopeeClient
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.realtime import publish

from .factories import make_station_account
from .returns_helpers import make_order, platform_return, return_session

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 1, 0, tzinfo=UTC)
BASE = "https://partner.test-stable.shopeemobile.com"


@pytest.fixture(autouse=True)
async def _env(db: AsyncSession, test_settings: Settings, redis_client: object) -> None:
    from aicam.modules.settings.models import Setting

    clock.freeze(NOW)
    test_settings.shopee_enabled = True
    row = await db.get(Setting, 1)
    assert row is not None
    row.recon_start_at = NOW - timedelta(days=30)  # nâng cấp Phase 2 từ 30 ngày trước
    await db.flush()


@pytest.fixture
def mock() -> MockAdapter:
    return MockAdapter()


@pytest.fixture
def ws(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    sent: list[tuple[str, dict[str, Any]]] = []

    async def _capture(event: str, data: dict[str, Any]) -> None:
        sent.append((event, data))

    monkeypatch.setattr(publish, "to_dashboard", _capture)
    return sent


async def _shop(db: AsyncSession, settings: Settings) -> Shop:
    shop = Shop(platform="SHOPEE", platform_shop_id=MOCK_SHOP_ID, name="TST Shop")
    platforms.store_credentials(
        shop,
        ShopCredentials(MOCK_SHOP_ID, "acc-1", "ref-1", NOW + timedelta(hours=4)),
        Cipher(settings.fernet_key),
    )
    db.add(shop)
    await db.flush()
    return shop


async def _case_of(db: AsyncSession, order: Order) -> list[ReturnCase]:
    return list(
        (
            await db.scalars(
                select(ReturnCase)
                .where(ReturnCase.order_id == order.id)
                .order_by(ReturnCase.created_at)
                .execution_options(populate_existing=True)
            )
        ).all()
    )


async def _history(db: AsyncSession, package: Package) -> list[tuple[str | None, str, str]]:
    rows = (
        await db.scalars(
            select(StatusHistory)
            .where(StatusHistory.package_id == package.id)
            .order_by(StatusHistory.at, StatusHistory.id)
        )
    ).all()
    return [(h.from_status, h.to_status, h.source) for h in rows]


def _platform_order(n: int, status: str, quantity: int) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=f"2410TST{n:05d}",
        status=status,
        tracking_numbers=(f"SPXTST{n:07d}",),
        items=(PlatformItem("Áo thun basic", quantity, "AT-DEN-L", "Đen / L"),),
        created_at=NOW,
        updated_at=NOW,
        status_group=shopee_order_group(status),
    )


def _only(mock: MockAdapter, *return_sns: str) -> None:
    mock.returns = {sn: r for sn, r in mock.returns.items() if sn in return_sns}


# ---------------------------------------------------------------- J-13


async def test_j13_buyer_return_expected(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings, ws: list[tuple[str, dict[str, Any]]]
) -> None:
    """TC-05.30: yêu cầu trả có kiện về → hồ sơ `BUYER_RETURN` `EXPECTED`, mã chiều về, lý do, hạn người bán;
    kiện `DELIVERED → RETURN_EXPECTED` (PLATFORM); WS `return.updated`; cursor tiến."""
    shop = await _shop(db, test_settings)
    order, (package,) = await make_order(db, 41)
    _only(mock, "2410RTTST041")

    out = await sync.sync_returns(db, mock, test_settings)

    assert out[str(shop.id)]["status"] == "OK"
    assert out[str(shop.id)]["changed"] == 1
    (case,) = await _case_of(db, order)
    assert (case.kind, case.status, case.source) == ("BUYER_RETURN", "EXPECTED", "PLATFORM")
    assert (case.platform_return_sn, case.return_tracking_number) == ("2410RTTST041", "SPXRTTST000041")
    assert (case.reason, case.reason_text) == ("ITEM_DAMAGED", "Áo bị rách ở tay")
    assert case.seller_due_at is not None
    assert case.requested_items[0]["quantity"] == 2
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    assert (await _history(db, package))[-1] == ("DELIVERED", "RETURN_EXPECTED", "PLATFORM")
    assert ("return.updated", {"return_case_id": str(case.id), "status": "EXPECTED"}) in ws
    await db.refresh(shop)
    assert shop.last_return_cursor == NOW

    # Chạy lại: idempotent theo `platform_return_sn`, không đổi gì.
    out = await sync.sync_returns(db, mock, test_settings)
    assert out[str(shop.id)]["changed"] == 0
    assert len(await _case_of(db, order)) == 1


async def test_j13_refund_only_no_parcel(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.31: chỉ hoàn tiền → hồ sơ `REFUND_ONLY` / `NO_PARCEL`; kiện vẫn `DELIVERED`."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 44)
    _only(mock, "2410RTTST044")
    await sync.sync_returns(db, mock, test_settings)
    (case,) = await _case_of(db, order)
    assert (case.kind, case.status, case.needs_parcel) == ("REFUND_ONLY", "NO_PARCEL", False)
    await db.refresh(package)
    assert package.warehouse_status == "DELIVERED"


async def test_j13_cancelled_before_arrival(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.32 (EX-R7): lần 1 `ACCEPTED` → `EXPECTED`; sàn hủy → hồ sơ `CANCELLED`, kiện `RETURN_EXPECTED →
    DELIVERED`. Yêu cầu đã hủy mà chưa từng thấy → không tạo hồ sơ."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 45)
    _only(mock, "2410RTTST045")
    mock.set_return_status("2410RTTST045", "ACCEPTED", NOW)
    await sync.sync_returns(db, mock, test_settings)
    (case,) = await _case_of(db, order)
    assert case.status == "EXPECTED"

    clock.advance(timedelta(minutes=20))
    mock.set_return_status("2410RTTST045", "CANCELLED", clock.now())
    await sync.sync_returns(db, mock, test_settings)
    (case,) = await _case_of(db, order)
    assert (case.status, case.platform_status) == ("CANCELLED", "CANCELLED")
    await db.refresh(package)
    assert package.warehouse_status == "DELIVERED"
    assert (await _history(db, package))[-1] == ("RETURN_EXPECTED", "DELIVERED", "PLATFORM")

    other, _ = await make_order(db, 46)
    mock.put_return(
        replace(platform_return(46), status="CANCELLED", status_group="CANCELLED", updated_at=clock.now())
    )
    await sync.sync_returns(db, mock, test_settings)
    assert await _case_of(db, other) == []


async def test_j13_new_package_and_unknown_order(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.39 (DEC-254): đơn chưa có trong hệ thống → `get_order` + upsert; kiện `NEW → RETURN_EXPECTED`."""
    await _shop(db, test_settings)
    _only(mock, "2410RTTST041")
    await sync.sync_returns(db, mock, test_settings)
    package = await orders.find_package(db, "SPXTST0000041")
    assert package is not None
    assert package.warehouse_status == "RETURN_EXPECTED"
    assert (await _history(db, package))[-1] == ("NEW", "RETURN_EXPECTED", "PLATFORM")


async def test_j13_done_status_kept_for_br19(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """Nhóm `DONE` (`REFUND_PAID`) khi kiện chưa về: hồ sơ vẫn chờ kiện, lưu `platform_status` (BR-19 xét)."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 51)
    mock.put_return(replace(platform_return(51), status="REFUND_PAID", status_group="DONE", updated_at=NOW))
    await sync.sync_returns(db, mock, test_settings)
    (case,) = await _case_of(db, order)
    assert (case.status, case.platform_status) == ("EXPECTED", "REFUND_PAID")
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"


async def test_j13_platform_reports_after_received(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.38 (EX-R1, DEC-267 b): hồ sơ `UNANNOUNCED` đã nhận → sàn báo yêu cầu trả → gắn vào **chính**
    hồ sơ đó (`kind = BUYER_RETURN`, mã sàn), không hồ sơ mới, kiện đã nhận giữ nguyên."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 46)
    scan = await returns.attach_or_create(db, order, returns.Signal(returns.SIGNAL_WAREHOUSE_SCAN,
                                                                    package_ids=(package.id,)))  # fmt: skip
    case = scan.case
    assert case is not None
    package.warehouse_status = "RETURN_RECEIVED_OK"
    case.status, case.received_at, case.conclusion = "RECEIVED_OK", NOW, "OK"
    await db.flush()
    _only(mock)
    mock.put_return(replace(platform_return(46), updated_at=NOW))

    await sync.sync_returns(db, mock, test_settings)

    (only,) = await _case_of(db, order)
    assert only.id == case.id
    assert (only.kind, only.platform_return_sn, only.status) == (
        "BUYER_RETURN",
        "2410RTTST046",
        "RECEIVED_OK",
    )
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_RECEIVED_OK"


async def test_j13_merges_unidentified_by_return_tracking(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """02 §6.3 #6 / DEC-316: hồ sơ chưa xác định mở bằng mã chiều về (phiên đã đóng) → J-13 thấy yêu cầu có mã
    đó → gộp vào hồ sơ của đơn; kiện tạm xóa; kiện thật `RETURN_RECEIVED_*` theo kết luận."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 41)
    _, station = await make_station_account(db)
    case, placeholder = await returns.create_unidentified(db)
    db.add(return_session(station, placeholder, case, conclusion="DAMAGED", open_code="SPXRTTST000041"))
    case.status = "RECEIVED_ISSUE"
    placeholder.warehouse_status = "RETURN_RECEIVED_ISSUE"
    await db.flush()
    _only(mock, "2410RTTST041")

    await sync.sync_returns(db, mock, test_settings)

    merged = await db.get(ReturnCase, case.id, populate_existing=True)
    assert merged is not None
    (destination,) = [c for c in await _case_of(db, order) if c.id != case.id]
    assert (merged.status, merged.merged_into_id) == ("CANCELLED", destination.id)
    assert destination.status == "RECEIVED_ISSUE"
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_RECEIVED_ISSUE"
    assert await db.get(Package, placeholder.id, populate_existing=True) is None


async def test_j13_failure_sets_last_error_and_lock(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.43: lỗi cuối → `shop.last_error.code = SYNC_FAILED` (`job = returns`), cursor không tiến;
    lượt sau thành công xóa lỗi của J-13. J-13 chạy chồng → bỏ lượt (lock `sync_returns:{shop}`)."""
    shop = await _shop(db, test_settings)
    mock.fail_returns_times = 5
    out = await sync.sync_returns(db, mock, test_settings)
    assert out[str(shop.id)]["status"] == "FAILED"
    await db.refresh(shop)
    assert shop.last_error is not None
    assert (shop.last_error["code"], shop.last_error["job"]) == ("SYNC_FAILED", "returns")
    assert shop.last_return_cursor is None

    mock.fail_returns_times = 0
    token = await sync._acquire_returns_lock(shop.id)
    assert token is not None
    out = await sync.sync_returns(db, mock, test_settings)
    assert out[str(shop.id)] == {"status": "SKIPPED", "reason": "locked"}
    await sync._release_returns_lock(shop.id, token)

    out = await sync.sync_returns(db, mock, test_settings)
    assert out[str(shop.id)]["status"] == "OK"
    await db.refresh(shop)
    assert shop.last_error is None


async def test_j13_disabled(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    test_settings.shopee_enabled = False
    assert await sync.sync_returns(db, mock, test_settings) == {"skipped": "not_configured"}


@respx.mock
async def test_j13_shopee_503_twice_then_ok(db: AsyncSession, test_settings: Settings) -> None:
    """TC-05.42 (adapter Shopee, HTTP giả): `get_return_list` 503 hai lần rồi OK → hồ sơ tạo, không
    `last_error`.     Định dạng theo tài liệu công khai — **chưa test với Shopee thật (T-3)**."""

    async def _no_sleep(_: float) -> None:
        return None

    adapter = ShopeeAdapter(ShopeeClient(2001234, "k", BASE, sleep=_no_sleep))
    order, _ = await make_order(db, 41)
    ok = httpx.Response(200, json={"error": "", "response": {"more": False, "return": [
        {"return_sn": "2410RTSPE041", "order_sn": order.platform_order_sn, "status": "ACCEPTED",
         "needs_logistics": True, "tracking_number": "SPXRTSPE000041", "reason": "ITEM_DAMAGED",
         "update_time": int(NOW.timestamp()), "create_time": int(NOW.timestamp()),
         "item": [{"item_sku": "AT-DEN-L", "name": "Áo thun basic", "amount": 1}]}]}})  # fmt: skip
    replies = iter([httpx.Response(503), httpx.Response(503)])  # rồi luôn OK (lượt đầu: 2 cửa sổ)
    respx.get(f"{BASE}/api/v2/returns/get_return_list").mock(side_effect=lambda _: next(replies, ok))
    shop = await _shop(db, test_settings)
    out = await sync.sync_returns(db, adapter, test_settings)
    assert out[str(shop.id)]["status"] == "OK"
    (case,) = await _case_of(db, order)
    assert (case.kind, case.return_tracking_number) == ("BUYER_RETURN", "SPXRTSPE000041")
    await db.refresh(shop)
    assert shop.last_error is None


# ---------------------------------------------------------------- J-06 / J-04 giao thất bại


async def _handed_over(db: AsyncSession, mock: MockAdapter, n: int) -> Package:
    result = await orders.upsert_platform_order(db, mock.orders[f"2410TST{n:05d}"])
    package = result.packages[0]
    package.warehouse_status = "HANDED_OVER"
    await db.flush()
    return package


async def test_j06_delivery_failed_creates_failed_case(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.33: kiện `HANDED_OVER` giao thất bại → hồ sơ `FAILED_DELIVERY` `EXPECTED`, kiện
    `RETURN_EXPECTED`; J-06 chạy lại không tạo thêm."""
    await _shop(db, test_settings)
    package = await _handed_over(db, mock, 12)
    mock.shipping["SPXTST0000012"] = "DELIVERY_FAILED"
    await sync.sync_shipping_status(db, mock, test_settings)
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    order = await db.get(Order, package.order_id)
    assert order is not None
    (case,) = await _case_of(db, order)
    assert (case.kind, case.status, case.source) == ("FAILED_DELIVERY", "EXPECTED", "PLATFORM")
    assert case.signal_keys[0].startswith("FAILED:2410TST00012:")
    await sync.sync_shipping_status(db, mock, test_settings)
    assert len(await _case_of(db, order)) == 1


@pytest.mark.parametrize("how", ["cod_rejected", "cancelled_after_pickup"])
async def test_j06_boom_cod_and_cancel_after_pickup(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings, how: str
) -> None:
    """TC-05.35 (EX-R14, DEC-258): boom COD hoặc đơn hủy khi kiện đã giao ĐVVC → giao thất bại."""
    await _shop(db, test_settings)
    package = await _handed_over(db, mock, 13)
    if how == "cod_rejected":
        mock.shipping["SPXTST0000013"] = "COD_REJECTED"
    else:
        mock.set_status("2410TST00013", "CANCELLED", NOW)
    await sync.sync_shipping_status(db, mock, test_settings)
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    order = await db.get(Order, package.order_id)
    assert order is not None
    (case,) = await _case_of(db, order)
    assert case.kind == "FAILED_DELIVERY"


async def test_j04_cancel_after_pickup_and_to_return(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.35 qua J-04: đơn hủy, kiện `HANDED_OVER` → giao thất bại (không `return` sớm — DEC-258); đơn
    `TO_RETURN` 2 kiện (`PACKED` + `HANDED_OVER`) → một hồ sơ, cả hai `RETURN_EXPECTED`."""
    await _shop(db, test_settings)
    package = await _handed_over(db, mock, 14)
    mock.set_status("2410TST00014", "CANCELLED", NOW)
    two = PlatformOrder(
        platform_order_sn="2410TST00043", status="TO_RETURN",
        tracking_numbers=("SPXTST0000043-1", "SPXTST0000043-2"),
        items=(PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"),), created_at=NOW, updated_at=NOW,
    status_group=shopee_order_group("TO_RETURN"))  # fmt: skip
    first = await orders.upsert_platform_order(db, replace(two, status="READY_TO_SHIP"))
    first.packages[0].warehouse_status = "PACKED"
    first.packages[1].warehouse_status = "HANDED_OVER"
    await db.flush()
    mock.put(two)

    await sync.sync_orders(db, mock, test_settings)

    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    order43 = first.order
    (case,) = await _case_of(db, order43)
    assert case.kind == "FAILED_DELIVERY"
    linked = set(
        (
            await db.scalars(
                select(ReturnCasePackage.package_id).where(ReturnCasePackage.return_case_id == case.id)
            )
        ).all()
    )
    assert linked == {p.id for p in first.packages}
    for p in first.packages:
        await db.refresh(p)
        assert p.warehouse_status == "RETURN_EXPECTED"
    assert (await _history(db, first.packages[0]))[-2:] == [
        ("PACKED", "HANDED_OVER", "PLATFORM"),
        ("HANDED_OVER", "RETURN_EXPECTED", "PLATFORM"),
    ]


async def test_to_return_then_buyer_return_one_case(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.36 (DEC-248): `TO_RETURN` (J-06) rồi yêu cầu trả (J-13) cùng đơn → đúng 1 hồ sơ mở,
    `BUYER_RETURN`."""
    await _shop(db, test_settings)
    package = await _handed_over(db, mock, 15)
    mock.set_status("2410TST00015", "TO_RETURN", NOW)
    await sync.sync_shipping_status(db, mock, test_settings)
    _only(mock)
    mock.put_return(replace(platform_return(15), updated_at=NOW))
    await sync.sync_returns(db, mock, test_settings)
    order = await db.get(Order, package.order_id)
    assert order is not None
    (case,) = await _case_of(db, order)
    assert (case.kind, case.status, case.platform_return_sn) == ("BUYER_RETURN", "EXPECTED", "2410RTTST015")


async def test_buyer_return_then_to_return_one_case(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.36 chiều ngược: J-13 trước rồi J-04 thấy `TO_RETURN` → vẫn một hồ sơ `BUYER_RETURN`."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 16, warehouse_status="HANDED_OVER", status="SHIPPED")
    _only(mock)
    mock.put_return(replace(platform_return(16), updated_at=NOW))
    mock.put(_platform_order(16, "TO_RETURN", 2))
    await sync.sync_returns(db, mock, test_settings)
    await sync.sync_orders(db, mock, test_settings)
    (case,) = await _case_of(db, order)
    assert case.kind == "BUYER_RETURN"
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"


async def test_to_return_repeated_after_received_no_new_case(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.37 (DEC-267): hồ sơ giao thất bại đã `RECEIVED_OK` — J-04 / J-06 chạy 3 lần cùng tín hiệu
    `TO_RETURN` → không tạo hồ sơ mới."""
    await _shop(db, test_settings)
    package = await _handed_over(db, mock, 17)
    mock.set_status("2410TST00017", "TO_RETURN", NOW)
    await sync.sync_shipping_status(db, mock, test_settings)
    order = await db.get(Order, package.order_id)
    assert order is not None
    (case,) = await _case_of(db, order)
    await db.refresh(package)
    package.warehouse_status = "RETURN_RECEIVED_OK"
    case.status, case.received_at = "RECEIVED_OK", NOW
    await db.flush()
    for _ in range(3):
        clock.advance(timedelta(minutes=5))
        mock.set_status("2410TST00017", "TO_RETURN", clock.now())  # mốc cập nhật mới mỗi lần
        await sync.sync_orders(db, mock, test_settings)
        await sync.sync_shipping_status(db, mock, test_settings)
    assert len(await _case_of(db, order)) == 1


async def test_j06_redelivery_cancels_failed_case(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """02 §5.3 / 02a J-06: kiện của hồ sơ giao thất bại được giao lại thành công → `RETURN_EXPECTED →
    DELIVERED`, kiện rời hồ sơ, hồ sơ `CANCELLED`."""
    await _shop(db, test_settings)
    package = await _handed_over(db, mock, 18)
    mock.shipping["SPXTST0000018"] = "DELIVERY_FAILED"
    await sync.sync_shipping_status(db, mock, test_settings)
    mock.shipping["SPXTST0000018"] = "DELIVERED"
    mock.set_status("2410TST00018", "COMPLETED", NOW)
    out = await sync.sync_shipping_status(db, mock, test_settings)
    assert out["changed"] == 1
    await db.refresh(package)
    assert package.warehouse_status == "DELIVERED"
    order = await db.get(Order, package.order_id)
    assert order is not None
    (case,) = await _case_of(db, order)
    assert case.status == "CANCELLED"
    count = await db.scalar(
        select(func.count()).select_from(ReturnCasePackage).where(ReturnCasePackage.return_case_id == case.id)
    )
    assert count == 0


async def test_j04_merges_unidentified_after_order_sync(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """TC-05.40 (DEC-269): hồ sơ chưa xác định `open_code = SPXTST0000099` (phiên đã đóng); J-04 thấy đơn có
    kiện đó → gộp, phiên sang kiện thật, kiện tạm xóa."""
    await _shop(db, test_settings)
    _, station = await make_station_account(db)
    case, placeholder = await returns.create_unidentified(db)
    session_row = return_session(station, placeholder, case, conclusion="OK", open_code="SPXTST0000099")
    db.add(session_row)
    case.status = "RECEIVED_OK"
    placeholder.warehouse_status = "RETURN_RECEIVED_OK"
    await db.flush()
    mock.put(_platform_order(99, "COMPLETED", 1))

    await sync.sync_orders(db, mock, test_settings)

    real = await orders.find_package(db, "SPXTST0000099")
    assert real is not None
    await db.refresh(session_row)
    assert session_row.package_id == real.id
    merged = await db.get(ReturnCase, case.id, populate_existing=True)
    assert merged is not None
    assert (merged.kind, merged.order_id) == ("UNANNOUNCED", real.order_id)
    assert await db.get(Package, placeholder.id, populate_existing=True) is None


# ---------------------------------------------------------------- G3 F-6, F-7, F-11, R10, SM-F7


async def test_j13_record_errors_skipped_and_retried(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F-6 / R10: một yêu cầu lỗi (dữ liệu lạ) hoặc sàn không trả đơn → bỏ qua + đếm + thử lại sau; yêu cầu
    khác vẫn xử lý, cursor vẫn tiến. Lượt sau đơn đã có → xử lý xong, rời danh sách."""
    shop = await _shop(db, test_settings)
    order, _ = await make_order(db, 61)
    order_id = order.id
    mock.returns = {}
    mock.put_return(replace(platform_return(61), updated_at=NOW))
    mock.put_return(replace(platform_return(62), updated_at=NOW))  # đơn 62 chưa có, sàn không trả
    mock.orders.pop("2410TST00062", None)
    original = returns.upsert_from_platform
    boom = {"on": True}

    async def flaky(session: AsyncSession, o: Order, ret: Any, **kw: Any) -> Any:
        if boom["on"] and ret.return_sn == "2410RTTST061":
            raise ValueError("dữ liệu lạ")
        return await original(session, o, ret, **kw)

    monkeypatch.setattr(returns, "upsert_from_platform", flaky)
    shop_id = shop.id
    out = await sync.sync_returns(db, mock, test_settings)
    assert out[str(shop_id)]["status"] == "OK"
    assert out[str(shop_id)]["skipped"] == 2
    shop = await db.get(Shop, shop_id, populate_existing=True)
    assert shop is not None
    assert shop.last_return_cursor == NOW
    pending = await sync.get_redis().hgetall(sync.RETRY_KEY.format(shop=shop.id))
    assert {k.decode() if isinstance(k, bytes) else k for k in pending} == {"2410RTTST061", "2410RTTST062"}

    boom["on"] = False
    clock.freeze(NOW + timedelta(minutes=15))
    out = await sync.sync_returns(db, mock, test_settings)
    assert await db.scalar(select(func.count()).where(ReturnCase.order_id == order_id)) == 1
    pending = await sync.get_redis().hgetall(sync.RETRY_KEY.format(shop=shop.id))
    assert {k.decode() if isinstance(k, bytes) else k for k in pending} == {"2410RTTST062"}
    for _ in range(sync.RETRY_MAX_ATTEMPTS):
        await sync.sync_returns(db, mock, test_settings)
    assert await sync.get_redis().hgetall(sync.RETRY_KEY.format(shop=shop.id)) == {}  # bỏ sau N lượt


async def _second_shop(db: AsyncSession, settings: Settings) -> Shop:
    shop = Shop(platform="SHOPEE", platform_shop_id="TST-SHOP-2", name="TST Shop 2")
    platforms.store_credentials(
        shop,
        ShopCredentials("TST-SHOP-2", "acc-2", "ref-2", NOW + timedelta(hours=4)),
        Cipher(settings.fernet_key),
    )
    db.add(shop)
    await db.flush()
    return shop


async def test_j13_multi_shop_record_error_and_crash(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3 V2-1: ≥ 2 shop — bản ghi lỗi (rollback) ở shop này không làm shop sau `MissingGreenlet`; shop crash
    → shop khác vẫn chạy và lock `sync_returns:{shop}` được nhả."""
    ids = [(await _shop(db, test_settings)).id, (await _second_shop(db, test_settings)).id]
    await make_order(db, 63)
    mock.returns = {}
    mock.put_return(replace(platform_return(63), updated_at=NOW))
    original = returns.upsert_from_platform

    async def bad(session: AsyncSession, o: Order, ret: Any, **kw: Any) -> Any:
        raise ValueError("dữ liệu lạ")

    monkeypatch.setattr(returns, "upsert_from_platform", bad)
    out = await sync.sync_returns(db, mock, test_settings)
    assert {k: v["status"] for k, v in out.items()} == {str(i): "OK" for i in ids}
    assert all(v["skipped"] == 1 for v in out.values())

    monkeypatch.setattr(returns, "upsert_from_platform", original)
    real = sync.sync_shop_returns
    crashed = ids[0]

    async def crash_first(session: AsyncSession, shop: Shop, *a: Any) -> Any:
        if shop.id == crashed:
            raise RuntimeError("boom")
        return await real(session, shop, *a)

    monkeypatch.setattr(sync, "sync_shop_returns", crash_first)
    clock.freeze(NOW + timedelta(minutes=15))
    out = await sync.sync_returns(db, mock, test_settings)
    assert out[str(ids[0])]["status"] == "FAILED"
    assert out[str(ids[1])]["status"] == "OK"
    for i in ids:  # lock đã nhả
        assert await sync._acquire_returns_lock(i) is not None


async def test_j04_success_keeps_returns_error(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """F-7: J-04 thành công không xóa lỗi của J-13 (`job = returns`)."""
    shop = await _shop(db, test_settings)
    mock.fail_returns_times = 5
    await sync.sync_returns(db, mock, test_settings)
    await sync.sync_shop_orders(db, shop, mock, test_settings)
    await db.refresh(shop)
    assert shop.last_error is not None
    assert shop.last_error["job"] == "returns"


async def test_j13_shopee_returns_flag(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """F-11: adapter Shopee thật + `SHOPEE_RETURNS_ENABLED=false` → J-13 bỏ lượt; mock vẫn chạy."""
    test_settings.platform_adapter = "shopee"
    test_settings.shopee_partner_id, test_settings.shopee_partner_key = "1", "k"
    test_settings.shopee_redirect_url = "https://x/cb"
    assert await sync.sync_returns(db, mock, test_settings) == {"skipped": "returns_disabled"}
    test_settings.shopee_returns_enabled = True
    assert await sync.sync_returns(db, mock, test_settings) == {}


async def test_j05_skips_placeholder(db: AsyncSession, mock: MockAdapter, test_settings: Settings) -> None:
    """SM-F7: J-05 không tra sàn cho kiện tạm `TAM-`."""
    await _shop(db, test_settings)
    db.add(Package(tracking_number="TAM-000901", verified=False, is_placeholder=True, warehouse_status="NEW"))
    await db.flush()
    out = await sync.verify_unverified(db, mock, test_settings)
    assert out["checked"] == 0


async def test_j13_done_before_upgrade_not_pulled(
    db: AsyncSession, mock: MockAdapter, test_settings: Settings
) -> None:
    """G3 C2: lượt J-13 đầu thấy yêu cầu đã hoàn tiền (DONE) từ trước lúc nâng cấp → không tạo hồ sơ, kiện giữ
    nguyên (không loạt BR-19 / BR-12 ngày go-live); DONE sau nâng cấp → vẫn xét như cũ."""
    await _shop(db, test_settings)
    order, (package,) = await make_order(db, 52)
    old = NOW - timedelta(days=40)
    mock.put_return(replace(platform_return(52), status="REFUND_PAID", status_group="DONE", created_at=old,
                            updated_at=NOW))  # fmt: skip
    await sync.sync_returns(db, mock, test_settings)
    assert await _case_of(db, order) == []
    await db.refresh(package)
    assert package.warehouse_status == "DELIVERED"
