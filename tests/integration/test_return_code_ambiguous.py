"""T-288: 02a §5.1 #15 (v0.3 — DEC-523; BR-29, EX-R20): mã chiều về `return_case.return_tracking_number` không
unique — ≥ 2 hồ sơ **chưa kết thúc** thuộc ≥ 2 đơn khác nhau → `resolve_code` trả `MULTIPLE_ORDERS` (API-11
ALERT `RETURN_MULTIPLE_ORDERS`); 1 mở + 1 đã nhận → mở phiên hồ sơ mở (thứ tự Phase 2); hồ sơ chưa xác định
quét mã đó → **không** tự gộp, log `unidentified_merge_ambiguous`.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.router import get_platform_adapter

from .factories import make_station_account
from .returns_helpers import make_desk, platform_return, return_session

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 6, 0, tzinfo=UTC)
DUP = "RTTST-DUP-1"


@pytest.fixture(autouse=True)
def _env(api: AsyncClient, test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    api._transport.app.dependency_overrides[get_platform_adapter] = MockAdapter  # type: ignore[attr-defined]


async def _order(db: AsyncSession, settings: Settings, ext: str, name: str, sn: str, code: str) -> Order:
    shop = Shop(platform="SHOPEE" if ext.isdigit() else "TIKTOK", platform_shop_id=ext, name=name)
    platforms.store_credentials(
        shop, ShopCredentials(ext, "a", "r", NOW + timedelta(hours=4)), Cipher(settings.fernet_key)
    )
    db.add(shop)
    await db.flush()
    data = PlatformOrder(
        sn,
        "COMPLETED",
        (code,),
        (PlatformItem("Áo thun basic", 1, "AT-DEN-L"),),
        status_group=order_group("COMPLETED"),
    )
    order = (await orders.upsert_platform_order(db, data, shop_id=shop.id)).order
    package = await orders.find_package(db, code)
    assert package is not None
    package.warehouse_status = "DELIVERED"
    await db.flush()
    return order


async def _case(db: AsyncSession, order: Order, rsn: str) -> ReturnCase:
    ret = replace(platform_return(1, tracking=DUP), return_sn=rsn, order_sn=order.platform_order_sn)
    result = await returns.upsert_from_platform(db, order, ret)
    assert result.case is not None
    return result.case


async def _two_open(db: AsyncSession, settings: Settings) -> tuple[ReturnCase, ReturnCase]:
    oa = await _order(db, settings, "990002", "TST B", "2410TSTB0020", "SPXTSTB000000020")
    ob = await _order(db, settings, "TTMOCKA", "TST TikTok A (mock)", "5761TT0000000067", "TTTST0000000067")
    return await _case(db, oa, "RSDUP0000001"), await _case(db, ob, "RSDUP0000001")


async def test_two_open_cases_same_return_code_ask_to_choose(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    ca, cb = await _two_open(db, test_settings)
    assert ca.id != cb.id
    res = await returns.resolve_code(db, DUP.lower())
    assert res.status == "MULTIPLE_ORDERS"
    assert {o.id for o in res.orders} == {ca.order_id, cb.order_id}
    desk = await make_desk(api, db, 3)
    body = (await desk.scan(DUP)).json()
    assert body["outcome"] == "ALERT"
    assert body["alert"]["code"] == "RETURN_MULTIPLE_ORDERS"
    assert [o["shop_name"] for o in body["alert"]["data"]["orders"]] == ["TST B", "TST TikTok A (mock)"]
    assert body["state"]["session"] is None


async def test_one_open_one_received_opens_the_open_case(db: AsyncSession, test_settings: Settings) -> None:
    """1 hồ sơ mở + 1 đã nhận cùng mã chiều về → không mơ hồ: mở phiên trên hồ sơ mở (thứ tự Phase 2)."""
    ca, cb = await _two_open(db, test_settings)
    cb.status = "RECEIVED_OK"
    await db.flush()
    res = await returns.resolve_code(db, DUP)
    assert (res.status, res.case) == ("FOUND", ca)


class _Logs:
    """Ghi lại log structlog của `returns.service` (logger đã cache — `capture_logs` không bắt được)."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def __getattr__(self, level: str) -> Any:
        def _log(event: str, **kw: Any) -> None:
            self.events.append({"event": event, "level": level, **kw})

        return _log


async def test_unidentified_case_with_ambiguous_code_not_auto_merged(
    db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hồ sơ chưa xác định quét `RTTST-DUP-1` + J-13 đơn A có hồ sơ mở mang mã đó → không tự gộp (≥ 2 hồ sơ
    mở thuộc đơn khác nhau), log `unidentified_merge_ambiguous`; hồ sơ giữ `UNIDENTIFIED` (gộp tay
    API-112)."""
    ca, cb = await _two_open(db, test_settings)
    case, package = await returns.create_unidentified(db)
    _, station = await make_station_account(db, "tst_st288", "TST Station 288")
    db.add(return_session(station, package, case, open_code=DUP))
    await db.flush()
    order_a = await db.get(Order, ca.order_id)
    assert order_a is not None
    logs = _Logs()
    monkeypatch.setattr(returns, "log", logs)
    merged = await returns.merge_unidentified_by_code(db, order_a)
    assert merged == []
    await db.refresh(case)
    assert (case.kind, case.order_id) == ("UNIDENTIFIED", None)
    events = [e for e in logs.events if e["event"] == "unidentified_merge_ambiguous"]
    assert len(events) == 1
    assert set(events[0]["case_ids"]) == {str(ca.id), str(cb.id)}
    # Hết mơ hồ (hồ sơ B đã nhận) → gộp như Phase 2.
    cb.status = "RECEIVED_OK"
    await db.flush()
    assert await returns.merge_unidentified_by_code(db, order_a) == [case.id]
