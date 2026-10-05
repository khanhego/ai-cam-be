"""Interface adapter sàn (architecture §10.1, ADR-007). Lõi nghiệp vụ chỉ dùng model chung ở đây."""

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

# Trạng thái đơn chung (map từ trạng thái riêng của từng sàn trong adapter).
CANCELLED_STATUSES = frozenset({"CANCELLED", "IN_CANCEL"})


@dataclass(frozen=True)
class PlatformItem:
    product_name: str
    quantity: int
    sku: str | None = None
    variation: str | None = None
    image_url: str | None = None


@dataclass(frozen=True)
class PlatformOrder:
    platform_order_sn: str
    status: str  # trạng thái sàn, giữ nguyên chữ của sàn (vd READY_TO_SHIP)
    tracking_numbers: tuple[str, ...]
    items: tuple[PlatformItem, ...]
    buyer_note: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def is_cancelled(self) -> bool:
        return self.status in CANCELLED_STATUSES


@dataclass(frozen=True)
class ShippingStatus:
    """Gợi ý trạng thái kho từ trạng thái vận chuyển sàn: HANDED_OVER | DELIVERED | None.

    `order_status`: trạng thái đơn trên sàn lúc tra (để J-06 bắt đơn hủy sau khi đóng — EX-P10).
    """

    tracking_number: str
    raw_status: str
    warehouse_hint: str | None
    order_status: str | None = None


@dataclass(frozen=True)
class ShipmentRef:
    """Một kiện cần tra vận chuyển (J-06)."""

    platform_order_sn: str
    tracking_number: str


@dataclass(frozen=True)
class ShopCredentials:
    shop_id: str
    access_token: str
    refresh_token: str
    expires_at: datetime


class PlatformError(Exception):
    """Lỗi gọi sàn (mạng, 5xx, rate limit đã hết lượt thử lại, lỗi tham số) — job ghi `shop.last_error`."""


class PlatformAuthError(PlatformError):
    """Token không hợp lệ / hết hạn / bị thu hồi: refresh một lần, hỏng nữa → shop `EXPIRED`."""


class PlatformAdapter(Protocol):
    code: str

    def build_auth_url(self, redirect_url: str) -> str: ...

    async def exchange_code(self, code: str, shop_id: str) -> ShopCredentials: ...

    async def refresh(self, creds: ShopCredentials) -> ShopCredentials: ...

    async def shop_name(self, creds: ShopCredentials) -> str | None: ...

    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None: ...

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None: ...

    def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]: ...

    async def get_shipping_statuses(
        self, creds: ShopCredentials | None, refs: Sequence[ShipmentRef]
    ) -> list[ShippingStatus]: ...
