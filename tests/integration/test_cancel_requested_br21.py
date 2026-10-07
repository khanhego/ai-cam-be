"""T-278 — BR-21 làm rõ (DEC-494, 508): nhóm `CANCEL_REQUESTED` (Shopee `IN_CANCEL`) không hủy kiện; chỉ chặn
mở phiên (BR-01) và gắn cờ phiên PACK đang mở; yêu cầu bị từ chối → đóng gói, bàn giao bình thường; được
chấp nhận (nhóm `CANCELLED`) → luật hủy như Phase 2. 02a §5 BR-21 kịch bản (1)–(4); AC-41.

TikTok (`REJECTED` / yêu cầu hủy mới nhất) chạy cùng đường `set_platform_status` — fixture TikTok:
`test_tiktok_cancellations.py` (T-277).
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import commit
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Package
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShipmentRef, ShippingStatus
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_station_account

pytestmark = pytest.mark.integration

ITEM = PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L")
SN, CODE = "2410BR2100001", "SPXBR2100001"


def _order(status: str) -> PlatformOrder:
    return PlatformOrder(SN, status, (CODE,), (ITEM,), status_group=order_group(status))


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def station(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> dict[str, str]:
    user, _ = await make_station_account(db, "tst_st_br21", "TST BR21")
    res = await api.post("/api/v1/auth/login", json={"username": user.username, "password": PASSWORD,
                                                      "client": "STATION"})  # fmt: skip
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _scan(api: AsyncClient, headers: dict[str, str]) -> dict[str, Any]:
    res = await api.post("/api/v1/station/scan", headers=headers,
                         json={"code": CODE, "client_scan_id": str(uuid.uuid4())})  # fmt: skip
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


async def _package(db: AsyncSession) -> Package:
    package = await orders.find_package(db, CODE)
    assert package is not None
    await db.refresh(package)
    return package


async def test_cancel_requested_when_new_blocks_scan_but_keeps_package(
    api: AsyncClient, db: AsyncSession, station: dict[str, str]
) -> None:
    """(1): yêu cầu hủy khi `NEW` → kiện vẫn `NEW` (Phase 2 từng hủy oan), quét → S4
    `ORDER_CANCEL_REQUESTED`."""
    await orders.upsert_platform_order(db, _order("READY_TO_SHIP"))
    await orders.upsert_platform_order(db, _order("IN_CANCEL"))

    assert (await _package(db)).warehouse_status == "NEW"
    body = await _scan(api, station)
    assert (body["outcome"], body["alert"]["code"]) == ("ALERT", "ORDER_CANCEL_REQUESTED")
    # Thân S4 đúng chữ 01 §10.4 / 02b-station §9 (T-229, DEC-825) — station hiện `message` của server.
    assert body["alert"]["message"].endswith(
        ": người mua đang xin hủy đơn này. Chờ xử lý trên sàn, chưa đóng gói."
    )

    # (3) sàn từ chối yêu cầu hủy → đóng gói, bàn giao như thường
    await orders.upsert_platform_order(db, _order("READY_TO_SHIP"))
    opened = await _scan(api, station)
    assert opened["outcome"] == "SESSION_OPENED"
    closed = await _scan(api, station)
    assert closed["closed_session"]["package_status"] == "PACKED"


async def test_cancel_requested_during_packing_flags_session_then_packs(
    api: AsyncClient, db: AsyncSession, station: dict[str, str], test_settings: Settings, sent_jobs: list[Any]
) -> None:
    """(2): yêu cầu hủy khi `PACKING` → cờ `ORDER_CANCEL_REQUESTED` (task riêng), đóng xong `PACKED`."""
    await orders.upsert_platform_order(db, _order("READY_TO_SHIP"))
    assert (await _scan(api, station))["outcome"] == "SESSION_OPENED"
    package = await _package(db)
    sent_jobs.clear()

    await sync._upsert(db, _order("IN_CANCEL"), None)
    await commit(db)

    assert (await _package(db)).warehouse_status == "PACKING"
    assert sent_jobs == [
        ("sessions.flag_order_cancelled", [str(package.id), "CANCEL_REQUESTED"], "default", 0.0)
    ]
    assert (
        await sessions.flag_order_cancelled(db, package.id, test_settings, kind="CANCEL_REQUESTED")
        == "flagged"
    )
    assert (
        await sessions.flag_order_cancelled(db, package.id, test_settings, kind="CANCEL_REQUESTED") == "noop"
    )
    state = (await api.get("/api/v1/station/state", headers=station)).json()
    assert "ORDER_CANCEL_REQUESTED" in state["session"]["flags"]
    assert "ORDER_CANCELLED" not in state["session"]["flags"]

    closed = await _scan(api, station)
    assert closed["closed_session"]["package_status"] == "PACKED"


async def test_flag_task_noop_when_request_already_withdrawn(
    api: AsyncClient, db: AsyncSession, station: dict[str, str], test_settings: Settings
) -> None:
    await orders.upsert_platform_order(db, _order("READY_TO_SHIP"))
    await _scan(api, station)
    package = await _package(db)
    await orders.upsert_platform_order(db, _order("IN_CANCEL"))
    await orders.upsert_platform_order(db, _order("READY_TO_SHIP"))  # rút trước khi task chạy
    assert (
        await sessions.flag_order_cancelled(db, package.id, test_settings, kind="CANCEL_REQUESTED") == "noop"
    )


class _Shipping(MockAdapter):
    def __init__(self, order_status: str, hint: str | None) -> None:
        super().__init__()
        self.order_status, self.hint = order_status, hint

    async def get_shipping_statuses(self, creds: Any, refs: Any) -> list[ShippingStatus]:
        return [
            ShippingStatus(r.tracking_number, "PICKED_UP", self.hint, self.order_status, None,
                           order_group(self.order_status))
            for r in refs if isinstance(r, ShipmentRef)
        ]  # fmt: skip


async def _packed(api: AsyncClient, db: AsyncSession, station: dict[str, str]) -> Package:
    await orders.upsert_platform_order(db, _order("READY_TO_SHIP"))
    await _scan(api, station)
    await _scan(api, station)
    package = await _package(db)
    assert package.warehouse_status == "PACKED"
    return package


async def test_j06_cancel_requested_packed_keeps_status_and_hands_over(
    api: AsyncClient, db: AsyncSession, station: dict[str, str], test_settings: Settings
) -> None:
    """(3) J-06: đơn `IN_CANCEL` + ĐVVC đã lấy → `HANDED_OVER` (không `CANCELLED_AFTER_PACK`)."""
    test_settings.shopee_enabled = True
    await _packed(api, db, station)
    await sync.sync_shipping_status(db, _Shipping("IN_CANCEL", None), test_settings)
    assert (await _package(db)).warehouse_status == "PACKED"  # yêu cầu hủy: kho giữ nguyên

    await sync.sync_shipping_status(db, _Shipping("IN_CANCEL", "HANDED_OVER"), test_settings)
    assert (await _package(db)).warehouse_status == "HANDED_OVER"


async def test_cancel_accepted_after_packed_is_cancelled_after_pack(
    api: AsyncClient, db: AsyncSession, station: dict[str, str], test_settings: Settings
) -> None:
    """(4) AC-41: yêu cầu được chấp nhận (nhóm `CANCELLED`) khi `PACKED` → `CANCELLED_AFTER_PACK` (BR-11 như
    Phase 2)."""
    test_settings.shopee_enabled = True
    await _packed(api, db, station)
    await orders.upsert_platform_order(db, _order("IN_CANCEL"))
    assert (await _package(db)).warehouse_status == "PACKED"

    await sync.sync_shipping_status(db, _Shipping("CANCELLED", None), test_settings)

    assert (await _package(db)).warehouse_status == "CANCELLED_AFTER_PACK"
