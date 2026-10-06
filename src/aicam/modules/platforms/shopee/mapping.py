"""Ánh xạ trạng thái Shopee → gợi ý trạng thái kho (FR-05.04, 01 §7). Cần xác nhận với dữ liệu thật ở T-3.

Trạng thái đơn (order_status): UNPAID, READY_TO_SHIP, PROCESSED, RETRY_SHIP, SHIPPED, TO_CONFIRM_RECEIVE,
IN_CANCEL, CANCELLED, TO_RETURN, COMPLETED. Trạng thái kiện (package_list[].logistics_status):
LOGISTICS_NOT_START, LOGISTICS_PENDING_ARRANGE, LOGISTICS_READY, LOGISTICS_REQUEST_CREATED,
LOGISTICS_PICKUP_DONE,
LOGISTICS_PICKUP_RETRY, LOGISTICS_PICKUP_FAILED, LOGISTICS_DELIVERY_DONE, LOGISTICS_DELIVERY_FAILED,
LOGISTICS_REQUEST_CANCELED, LOGISTICS_COD_REJECTED, LOGISTICS_INVALID, LOGISTICS_LOST.
"""

# Đơn ở các trạng thái này chưa / không còn mã vận đơn → không gọi get_tracking_number (tiết kiệm quota).
NO_TRACKING_STATUSES = frozenset({"UNPAID", "CANCELLED", "IN_CANCEL", ""})

_ORDER_HINT = {
    "SHIPPED": "HANDED_OVER",
    "TO_RETURN": "RETURN_EXPECTED",  # Phase 2 (DEC-259)
    "TO_CONFIRM_RECEIVE": "DELIVERED",
    "COMPLETED": "DELIVERED",
}
_LOGISTICS_HINT = {
    "LOGISTICS_PICKUP_DONE": "HANDED_OVER",
    "LOGISTICS_DELIVERY_FAILED": "RETURN_EXPECTED",  # giao thất bại → hoàn về (DEC-259)
    "LOGISTICS_COD_REJECTED": "RETURN_EXPECTED",  # boom COD
    "LOGISTICS_LOST": "HANDED_OVER",
    "LOGISTICS_DELIVERY_DONE": "DELIVERED",
}
# DEC-259: tín hiệu hoàn xếp trên DELIVERED (giao thành công rồi mới bị trả vẫn là hoàn).
_RANK = {None: 0, "HANDED_OVER": 1, "DELIVERED": 2, "RETURN_EXPECTED": 3}


def warehouse_hint(order_status: str, logistics_status: str) -> str | None:
    """Mốc xa nhất giữa trạng thái đơn và kiện: HANDED_OVER | DELIVERED | RETURN_EXPECTED | None."""
    a, b = _ORDER_HINT.get(order_status), _LOGISTICS_HINT.get(logistics_status)
    return a if _RANK[a] >= _RANK[b] else b
