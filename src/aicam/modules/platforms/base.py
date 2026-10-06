"""Interface adapter sàn (architecture §10.1, ADR-007). Lõi nghiệp vụ chỉ dùng model chung ở đây."""

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

# Nhóm trạng thái đơn chung mọi sàn (02 §5.2, ADR-011, BR-30): adapter ánh xạ chữ của sàn → nhóm
# (`platforms/<sàn>/mapping.py`); lõi chỉ đọc nhóm, không đọc chữ trạng thái của sàn (NFR-28).
ORDER_STATUS_GROUPS = (
    "UNPAID",
    "AWAITING_SHIPMENT",
    "SHIPPED",
    "DELIVERED",
    "CANCEL_REQUESTED",
    "CANCELLED",
    "RETURNING",
    "UNKNOWN",
)
# Nhóm chặn mở phiên đóng gói (BR-01 — giữ hành vi Phase 2: Shopee `IN_CANCEL` cũng chặn).
CANCEL_GROUPS = ("CANCEL_REQUESTED", "CANCELLED")
# Đơn đã rời kho trên sàn (đã giao ĐVVC / đã giao / đang hoàn về) — kiện `NEW` mở phiên hoàn được (EX-R3).
SHIPPED_GROUPS = ("SHIPPED", "DELIVERED", "RETURNING")


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
    status: str  # trạng thái sàn, giữ nguyên chữ của sàn (vd READY_TO_SHIP) — chỉ lưu / hiển thị
    tracking_numbers: tuple[str, ...]
    items: tuple[PlatformItem, ...]
    buyer_note: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    raw: dict[str, Any] = field(default_factory=dict)
    # Nhóm chung ∈ ORDER_STATUS_GROUPS — adapter đặt (TikTok cần cả yêu cầu hủy, không suy được từ `status`).
    status_group: str = "UNKNOWN"

    @property
    def is_cancelled(self) -> bool:
        return self.status_group in CANCEL_GROUPS


# Nhóm trạng thái yêu cầu trả chung (02 §5.2, BR-31): chữ lạ → None (không chờ duyệt, không kết thúc).
RETURN_STATUS_GROUPS = ("REQUESTED", "ACCEPTED", "CANCELLED", "DONE", "CLOSED")


@dataclass(frozen=True)
class ReturnItem:
    """Dòng sản phẩm khách yêu cầu trả; ghép `order_item` theo sku / tên + phân loại ở `returns` (T-104)."""

    quantity: int
    item_id: str | None = None
    model_id: str | None = None
    sku: str | None = None
    product_name: str | None = None
    variation: str | None = None


@dataclass(frozen=True)
class PlatformReturn:
    """Yêu cầu trả / hoàn tiền (02a §7). `status` = chữ sàn; `status_group` ∈ RETURN_STATUS_GROUPS / None."""

    return_sn: str
    order_sn: str
    status: str
    status_group: str | None
    needs_parcel: bool
    return_tracking_number: str | None = None
    reason: str | None = None
    # Có thể chứa thông tin người mua → không log (02a §3).
    reason_text: str | None = None
    items: tuple[ReturnItem, ...] = ()
    seller_due_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ShippingStatus:
    """Gợi ý trạng thái kho từ trạng thái vận chuyển sàn: HANDED_OVER | DELIVERED | RETURN_EXPECTED | None.

    RETURN_EXPECTED: tín hiệu hoàn (`TO_RETURN`, giao thất bại, boom COD — DEC-259).

    `order_status`: trạng thái đơn trên sàn lúc tra (để J-06 bắt đơn hủy sau khi đóng — EX-P10).
    `updated_at`: mốc cập nhật đơn trên sàn — khóa đợt giao thất bại `FAILED:{order_sn}:{mốc}` (R3-7).
    """

    tracking_number: str
    raw_status: str
    warehouse_hint: str | None
    order_status: str | None = None
    updated_at: datetime | None = None
    order_status_group: str | None = None  # nhóm của `order_status` (adapter đặt cùng lúc)


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

    def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]: ...

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None: ...
