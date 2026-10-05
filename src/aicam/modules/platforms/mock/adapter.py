"""Adapter sàn giả (02a §7): dev, test, và khi chưa có quyền Shopee (`PLATFORM_ADAPTER=mock`).

Dữ liệu khớp seed `--prefix TST` (04-test-cases §1): SPXTST0000001..30, `…09` hủy, `…12` có 3 sản phẩm.
"""

import asyncio
import secrets
from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

from aicam.core import clock
from aicam.modules.platforms.base import (
    PlatformAuthError,
    PlatformError,
    PlatformItem,
    PlatformOrder,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)

MOCK_SHOP_ID = "990001"
MOCK_SHOP_NAME = "TST Shop (mock)"

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
        # Điều khiển lỗi trong test (TC-05.08..05.10).
        self.fail_list_times = 0  # số lần list_updated_orders ném PlatformError trước khi chạy được
        self.fail_list_auth = False  # list_updated_orders ném PlatformAuthError (token bị thu hồi)
        self.fail_refresh = False  # refresh ném PlatformAuthError
        self.calls: list[str] = []

    # ----- điều khiển trong test
    def put(self, order: PlatformOrder) -> None:
        self.orders[order.platform_order_sn] = order

    def set_status(self, order_sn: str, status: str, at: datetime) -> None:
        self.orders[order_sn] = replace(self.orders[order_sn], status=status, updated_at=at)

    # ----- PlatformAdapter: ủy quyền (luồng giả — chuyển thẳng về callback với code giả)
    def build_auth_url(self, redirect_url: str) -> str:
        sep = "&" if "?" in redirect_url else "?"
        return f"{redirect_url}{sep}{urlencode({'code': 'MOCK-CODE', 'shop_id': MOCK_SHOP_ID})}"

    def _creds(self, shop_id: str) -> ShopCredentials:
        return ShopCredentials(
            shop_id=shop_id,
            access_token=f"mock-access-{secrets.token_hex(4)}",
            refresh_token=f"mock-refresh-{secrets.token_hex(4)}",
            expires_at=clock.now() + timedelta(hours=4),
        )

    async def exchange_code(self, code: str, shop_id: str) -> ShopCredentials:
        self.calls.append("exchange_code")
        if code != "MOCK-CODE":
            raise PlatformAuthError("error_auth: code không hợp lệ")
        return self._creds(shop_id)

    async def refresh(self, creds: ShopCredentials) -> ShopCredentials:
        self.calls.append("refresh")
        if self.fail_refresh:
            raise PlatformAuthError("error_auth: refresh token không hợp lệ")
        return self._creds(creds.shop_id)

    async def shop_name(self, creds: ShopCredentials) -> str | None:
        return MOCK_SHOP_NAME

    # ----- PlatformAdapter: đơn
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
        self.calls.append("list_updated_orders")
        if self.fail_list_auth:
            raise PlatformAuthError("invalid_access_token")
        if self.fail_list_times > 0:
            self.fail_list_times -= 1
            raise PlatformError("HTTP 503")
        for order in sorted(self.orders.values(), key=lambda o: o.updated_at or _BASE_TIME):
            if (order.updated_at or _BASE_TIME) >= since:
                yield order

    async def get_shipping_statuses(
        self, creds: ShopCredentials | None, refs: Sequence[ShipmentRef]
    ) -> list[ShippingStatus]:
        out = []
        for ref in refs:
            order = self.orders.get(ref.platform_order_sn)
            raw = self.shipping.get(ref.tracking_number.upper())
            if raw is None and order is None:
                continue
            hint = {"PICKED_UP": "HANDED_OVER", "IN_TRANSIT": "HANDED_OVER", "DELIVERED": "DELIVERED"}.get(
                raw or ""
            )
            out.append(ShippingStatus(ref.tracking_number, raw or "", hint, order.status if order else None))
        return out
