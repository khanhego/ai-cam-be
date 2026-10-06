"""Adapter sàn giả (02a §7): dev, test, và khi chưa có quyền Shopee (`PLATFORM_ADAPTER=mock`).

Dữ liệu khớp seed `--prefix TST` (04-test-cases §1): SPXTST0000001..30, `…09` hủy, `…12` có 3 sản phẩm.
Phase 2 (02a §7): 4 fixture `fixtures/returns/*.json` — yêu cầu trả 41 (khách trả), 44 (chỉ hoàn tiền),
45 (hủy) theo định dạng Shopee v2 (qua cùng `returns_mapping` như adapter thật); đơn 43 giao thất bại.
"""

import asyncio
import json
import secrets
from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from aicam.core import clock
from aicam.modules.platforms.base import (
    PlatformAuthError,
    PlatformError,
    PlatformItem,
    PlatformOrder,
    PlatformReturn,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.shopee import returns_mapping

MOCK_SHOP_ID = "990001"
MOCK_SHOP_NAME = "TST Shop (mock)"

_BASE_TIME = datetime(2026, 10, 1, tzinfo=UTC)
RETURN_FIXTURES = Path(__file__).parent / "fixtures" / "returns"
_ITEM = PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L")
# Hint vận chuyển giả (khóa = `self.shipping[mã]`), gồm tín hiệu hoàn (DEC-259).
_SHIPPING_HINTS = {
    "PICKED_UP": "HANDED_OVER",
    "IN_TRANSIT": "HANDED_OVER",
    "DELIVERED": "DELIVERED",
    "DELIVERY_FAILED": "RETURN_EXPECTED",
    "COD_REJECTED": "RETURN_EXPECTED",
}


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((RETURN_FIXTURES / name).read_text(encoding="utf-8"))
    return data


def _fixture_return(name: str, loaded_at: datetime) -> PlatformReturn:
    """Fixture → payload Shopee (hạn người bán tương đối lúc nạp) → `returns_mapping` như adapter thật."""
    data = {k: v for k, v in _load(name).items() if not k.startswith("_")}
    offset = data.pop("return_seller_due_date_offset_hours", None)
    if offset is not None:
        data["return_seller_due_date"] = int((loaded_at + timedelta(hours=offset)).timestamp())
    data.setdefault("create_time", int(loaded_at.timestamp()))
    data.setdefault("update_time", int(loaded_at.timestamp()))
    return returns_mapping.to_platform_return(data)


def _return_orders() -> list[PlatformOrder]:
    """Đơn gốc của các fixture hàng hoàn (04 §1): 41, 44, 45 một kiện đã giao; 43 hai kiện giao thất bại."""
    out = [
        PlatformOrder(
            platform_order_sn=f"2410TST{n:05d}", status="COMPLETED", tracking_numbers=(f"SPXTST{n:07d}",),
            items=(_ITEM,), created_at=_BASE_TIME, updated_at=_BASE_TIME, raw={"mock": True, "n": n},
        )
        for n in (41, 44, 45)
    ]  # fmt: skip
    failed = _load("failed_delivery_order.json")
    out.append(
        PlatformOrder(
            platform_order_sn=failed["order_sn"],
            status=failed["order_status"],
            tracking_numbers=tuple(failed["tracking_numbers"]),
            items=tuple(
                PlatformItem(i["name"], int(i["amount"]), i.get("item_sku"), i.get("model_name"))
                for i in failed["items"]
            ),
            created_at=_BASE_TIME,
            updated_at=_BASE_TIME,
            raw={"mock": True, "fixture": "failed_delivery_order"},
        )
    )
    return out


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
        for order in _return_orders():
            self.orders[order.platform_order_sn] = order
        failed = _load("failed_delivery_order.json")
        for code in failed["tracking_numbers"]:
            self.shipping[code.upper()] = "DELIVERY_FAILED"
        loaded_at = clock.now()
        self.returns: dict[str, PlatformReturn] = {
            r.return_sn: r
            for r in (
                _fixture_return(name, loaded_at)
                for name in ("buyer_return.json", "refund_only.json", "cancelled.json")
            )
        }
        self.fail_returns_times = 0  # số lần list_returns ném PlatformError (TC-05.43)
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

    def put_return(self, ret: PlatformReturn) -> None:
        self.returns[ret.return_sn] = ret

    def set_return_status(self, return_sn: str, status: str, at: datetime) -> None:
        current = self.returns[return_sn]
        self.returns[return_sn] = replace(
            current, status=status, status_group=returns_mapping.status_group(status), updated_at=at
        )

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
            hint = _SHIPPING_HINTS.get(raw or "")
            if order is not None and order.status == "TO_RETURN":
                hint = "RETURN_EXPECTED"
            out.append(ShippingStatus(ref.tracking_number, raw or "", hint, order.status if order else None))
        return out

    # ----- PlatformAdapter: yêu cầu trả (Phase 2)
    async def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]:
        self.calls.append("list_returns")
        if self.fail_returns_times > 0:
            self.fail_returns_times -= 1
            raise PlatformError("HTTP 503")
        for ret in sorted(self.returns.values(), key=lambda r: r.updated_at or _BASE_TIME):
            if (ret.updated_at or _BASE_TIME) >= since:
                yield ret

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None:
        await self._wait()
        return self.returns.get(return_sn)
