"""Ánh xạ trạng thái TikTok Shop → nhóm chung + gợi ý kho (02 §5.3, ADR-011, BR-30; DEC-502).

**Giả định theo tài liệu công khai — chưa xác minh (Q19).** Chữ trạng thái đơn (`status`): UNPAID, ON_HOLD,
AWAITING_SHIPMENT, PARTIALLY_SHIPPING, AWAITING_COLLECTION, IN_TRANSIT, DELIVERED, COMPLETED, CANCELLED.
Yêu cầu hủy (`cancellations/search` — `cancel_status`): PENDING, APPROVED, REJECTED, CANCELLED_BY_BUYER (người
mua rút). Trạng thái kiện (`line_items[].package_status`, giả định) cho J-06: giao thất bại / trả về người bán
→ RETURN_EXPECTED.
"""

from collections.abc import Iterable
from typing import Any

# Trạng thái đơn → nhóm (02 §5.3). Chữ lạ → UNKNOWN (lõi không đổi trạng thái kho, log).
ORDER_GROUPS: dict[str, str] = {
    "UNPAID": "UNPAID",
    "ON_HOLD": "UNPAID",
    "AWAITING_SHIPMENT": "AWAITING_SHIPMENT",
    "PARTIALLY_SHIPPING": "AWAITING_SHIPMENT",
    "AWAITING_COLLECTION": "AWAITING_SHIPMENT",
    "IN_TRANSIT": "SHIPPED",
    "DELIVERED": "DELIVERED",
    "COMPLETED": "DELIVERED",
    "CANCELLED": "CANCELLED",
}
# Đơn chưa giao (còn hủy được): yêu cầu hủy `PENDING` → CANCEL_REQUESTED (DEC-502).
NOT_SHIPPED = frozenset(
    {"UNPAID", "ON_HOLD", "AWAITING_SHIPMENT", "PARTIALLY_SHIPPING", "AWAITING_COLLECTION"}
)
CANCEL_PENDING = "PENDING"
# Đơn chưa có / không còn mã vận đơn (tra khi quét bỏ qua).
NO_TRACKING_STATUSES = frozenset({"UNPAID", "ON_HOLD", "CANCELLED", ""})
# Đơn do kho TikTok giao (AS-13, EX-T5): kho không đóng gói → J-04 bỏ qua + đếm.
PLATFORM_FULFILLMENT = frozenset({"FULFILLMENT_BY_TIKTOK", "FULFILLED_BY_TIKTOK", "FBT"})

# Trạng thái kiện (giả định) báo giao thất bại / đang trả về người bán (02 §5.3 nhóm RETURNING).
PACKAGE_RETURNING = frozenset({"DELIVERY_FAILED", "RETURN_TO_SENDER", "RETURNING", "RETURNED_TO_SELLER"})
_ORDER_HINT = {"IN_TRANSIT": "HANDED_OVER", "DELIVERED": "DELIVERED", "COMPLETED": "DELIVERED"}
_PACKAGE_HINT = {
    "IN_TRANSIT": "HANDED_OVER",
    "PICKED_UP": "HANDED_OVER",
    "DELIVERED": "DELIVERED",
    **dict.fromkeys(PACKAGE_RETURNING, "RETURN_EXPECTED"),
}
_RANK = {None: 0, "HANDED_OVER": 1, "DELIVERED": 2, "RETURN_EXPECTED": 3}


def latest_cancel(cancellations: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    """Yêu cầu hủy **mới nhất** theo `update_time` (rồi `create_time`) của một đơn (DEC-502)."""
    rows = [c for c in cancellations if isinstance(c, dict)]
    if not rows:
        return None
    return max(rows, key=lambda c: (_num(c.get("update_time")), _num(c.get("create_time"))))


def cancel_pending(latest: dict[str, Any] | None, buyer_request_flag: bool = False) -> bool:
    """Có yêu cầu hủy đang chờ: theo yêu cầu mới nhất nếu biết; không biết (ngoài cửa sổ) → cờ
    `is_buyer_request_cancel` của chi tiết đơn (DEC-564)."""
    if latest is not None:
        return str(latest.get("cancel_status") or latest.get("status") or "").upper() == CANCEL_PENDING
    return buyer_request_flag


def order_group(
    status: str | None,
    latest: dict[str, Any] | None = None,
    *,
    buyer_request_flag: bool = False,
    package_statuses: Iterable[str] = (),
) -> str:
    """Nhóm chung của đơn TikTok (02a §7.1 bảng "Yêu cầu hủy"):

    1. `status` thuộc nhóm CANCELLED → CANCELLED;
    2. yêu cầu hủy mới nhất `PENDING` và đơn chưa giao → CANCEL_REQUESTED;
    3. đơn đang chuyển mà kiện giao thất bại / trả về người bán → RETURNING;
    4. còn lại (yêu cầu bị từ chối / người mua rút / không có) → nhóm theo `status` (lạ → UNKNOWN)."""
    raw = (status or "").upper()
    group = ORDER_GROUPS.get(raw, "UNKNOWN")
    if group == "CANCELLED":
        return group
    if raw in NOT_SHIPPED and cancel_pending(latest, buyer_request_flag):
        return "CANCEL_REQUESTED"
    if group == "SHIPPED" and any((p or "").upper() in PACKAGE_RETURNING for p in package_statuses):
        return "RETURNING"
    return group


def warehouse_hint(order_status: str, package_status: str) -> str | None:
    """Mốc xa nhất giữa trạng thái đơn và kiện: HANDED_OVER | DELIVERED | RETURN_EXPECTED | None (như
    Shopee)."""
    a = _ORDER_HINT.get((order_status or "").upper())
    b = _PACKAGE_HINT.get((package_status or "").upper())
    return a if _RANK[a] >= _RANK[b] else b


def fulfilled_by_platform(detail: dict[str, Any]) -> bool:
    return str(detail.get("fulfillment_type") or "").upper() in PLATFORM_FULFILLMENT


def _num(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
