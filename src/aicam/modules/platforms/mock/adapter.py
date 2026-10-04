"""Adapter sàn giả (02a §7): dev, test, và khi chưa có quyền Shopee (`PLATFORM_ADAPTER=mock`).

Dữ liệu khớp seed `--prefix TST` (04-test-cases §1): SPXTST0000001..30, `…09` hủy, `…12` có 3 sản phẩm.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime

from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShippingStatus, ShopCredentials

_BASE_TIME = datetime(2026, 10, 1, tzinfo=UTC)


def _order(n: int) -> PlatformOrder:
    code = f"SPXTST{n:07d}"
    items: tuple[PlatformItem, ...] = (PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"),)
    if n == 12:
        items = (
            PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"),
            PlatformItem("Tất cổ ngắn", 1, "TAT-TRANG", "Trắng"),
            PlatformItem("Túi vải", 1, "TUI-01", None),
        )
    return PlatformOrder(
        platform_order_sn=f"2410TST{n:05d}",
        status="CANCELLED" if n == 9 else "READY_TO_SHIP",
        tracking_numbers=(code,),
        items=items,
        buyer_note="Gói kỹ giúp em" if n % 5 == 0 else None,
        created_at=_BASE_TIME,
        updated_at=_BASE_TIME,
        raw={"mock": True, "n": n},
    )


class MockAdapter:
    code = "SHOPEE"

    def __init__(self, delay_s: float = 0.0) -> None:
        self.delay_s = delay_s
        self.orders: dict[str, PlatformOrder] = {o.platform_order_sn: o for o in map(_order, range(1, 31))}
        self.shipping: dict[str, str] = {}

    # ----- điều khiển trong test
    def put(self, order: PlatformOrder) -> None:
        self.orders[order.platform_order_sn] = order

    def set_status(self, order_sn: str, status: str, at: datetime) -> None:
        self.orders[order_sn] = replace(self.orders[order_sn], status=status, updated_at=at)

    # ----- PlatformAdapter
    async def _wait(self) -> None:
        if self.delay_s:
            await asyncio.sleep(self.delay_s)

    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None:
        await self._wait()
        return self.orders.get(order_sn)

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None:
        await self._wait()
        code = tracking_number.upper()
        return next((o for o in self.orders.values() if code in o.tracking_numbers), None)

    async def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]:
        for order in sorted(self.orders.values(), key=lambda o: o.updated_at or _BASE_TIME):
            if (order.updated_at or _BASE_TIME) >= since:
                yield order

    async def get_shipping_status(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> ShippingStatus | None:
        raw = self.shipping.get(tracking_number.upper())
        if raw is None:
            return None
        hint = {"PICKED_UP": "HANDED_OVER", "IN_TRANSIT": "HANDED_OVER", "DELIVERED": "DELIVERED"}.get(raw)
        return ShippingStatus(tracking_number, raw, hint)
