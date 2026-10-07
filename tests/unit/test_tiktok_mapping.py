"""T-209: ánh xạ TikTok → nhóm chung (02 §5.3; BR-30; AC-41) + 4 kịch bản yêu cầu hủy (DEC-502) + gợi ý kho.

Chữ trạng thái TikTok là **giả định theo tài liệu công khai** (Q19)."""

import pytest

from aicam.modules.platforms.base import ORDER_STATUS_GROUPS
from aicam.modules.platforms.tiktok import mapping


@pytest.mark.parametrize(
    ("status", "group"),
    [
        ("UNPAID", "UNPAID"),
        ("ON_HOLD", "UNPAID"),
        ("AWAITING_SHIPMENT", "AWAITING_SHIPMENT"),
        ("PARTIALLY_SHIPPING", "AWAITING_SHIPMENT"),
        ("AWAITING_COLLECTION", "AWAITING_SHIPMENT"),
        ("IN_TRANSIT", "SHIPPED"),
        ("DELIVERED", "DELIVERED"),
        ("COMPLETED", "DELIVERED"),
        ("CANCELLED", "CANCELLED"),
        ("XYZ", "UNKNOWN"),
        ("", "UNKNOWN"),
        (None, "UNKNOWN"),
    ],
)
def test_order_status_groups(status: str | None, group: str) -> None:
    """9 trạng thái §5.3 + chữ lạ → UNKNOWN (lõi không đổi kho)."""
    assert mapping.order_group(status) == group
    assert group in ORDER_STATUS_GROUPS


def _cancel(status: str, t: int) -> dict[str, object]:
    return {"cancel_id": f"C{t}", "order_id": "5761", "cancel_status": status, "update_time": t}


@pytest.mark.parametrize(
    ("cancels", "expected"),
    [
        ([_cancel("PENDING", 10)], "CANCEL_REQUESTED"),  # pending
        ([_cancel("PENDING", 10), _cancel("REJECTED", 20)], "AWAITING_SHIPMENT"),  # rejected
        ([_cancel("PENDING", 10), _cancel("CANCELLED_BY_BUYER", 20)], "AWAITING_SHIPMENT"),  # withdrawn
        ([_cancel("REJECTED", 10), _cancel("PENDING", 20)], "CANCEL_REQUESTED"),  # yêu cầu mới sau từ chối
    ],
)
def test_cancel_request_uses_latest_cancel(cancels: list[dict[str, object]], expected: str) -> None:
    """DEC-502: nhóm từ (đơn, yêu cầu hủy **mới nhất** theo `update_time`)."""
    latest = mapping.latest_cancel(cancels)
    assert mapping.order_group("AWAITING_SHIPMENT", latest) == expected


def test_cancel_approved_order_cancelled_wins() -> None:
    """Được chấp nhận: TikTok đổi đơn sang CANCELLED → nhóm CANCELLED (kể cả yêu cầu cũ còn PENDING)."""
    assert mapping.order_group("CANCELLED", _cancel("APPROVED", 30)) == "CANCELLED"
    assert mapping.order_group("CANCELLED", _cancel("PENDING", 30)) == "CANCELLED"


def test_cancel_pending_after_shipped_is_not_cancel_requested() -> None:
    """Đã giao ĐVVC (IN_TRANSIT) → yêu cầu hủy không còn chặn đóng gói (nhóm theo `status`)."""
    assert mapping.order_group("IN_TRANSIT", _cancel("PENDING", 10)) == "SHIPPED"


def test_buyer_request_flag_used_when_cancel_outside_window() -> None:
    """DEC-564: không thấy yêu cầu hủy trong cửa sổ → dùng cờ `is_buyer_request_cancel` của chi tiết đơn."""
    assert mapping.order_group("AWAITING_SHIPMENT", None, buyer_request_flag=True) == "CANCEL_REQUESTED"
    assert mapping.order_group("AWAITING_SHIPMENT", _cancel("REJECTED", 5), buyer_request_flag=True) == (
        "AWAITING_SHIPMENT"
    )  # yêu cầu mới nhất biết rõ thắng cờ


def test_returning_from_package_status() -> None:
    assert mapping.order_group("IN_TRANSIT", package_statuses=["DELIVERY_FAILED"]) == "RETURNING"
    assert (
        mapping.order_group("AWAITING_SHIPMENT", package_statuses=["DELIVERY_FAILED"]) == "AWAITING_SHIPMENT"
    )


@pytest.mark.parametrize(
    ("order_status", "package_status", "hint"),
    [
        ("AWAITING_COLLECTION", "", None),
        ("IN_TRANSIT", "", "HANDED_OVER"),
        ("IN_TRANSIT", "DELIVERED", "DELIVERED"),
        ("DELIVERED", "", "DELIVERED"),
        ("COMPLETED", "", "DELIVERED"),
        ("IN_TRANSIT", "DELIVERY_FAILED", "RETURN_EXPECTED"),
        ("DELIVERED", "RETURN_TO_SENDER", "RETURN_EXPECTED"),
    ],
)
def test_warehouse_hint(order_status: str, package_status: str, hint: str | None) -> None:
    assert mapping.warehouse_hint(order_status, package_status) == hint


def test_fulfilled_by_platform() -> None:
    """AS-13 / EX-T5: đơn kho TikTok nhận biết qua `fulfillment_type`."""
    assert mapping.fulfilled_by_platform({"fulfillment_type": "FULFILLMENT_BY_TIKTOK"})
    assert not mapping.fulfilled_by_platform({"fulfillment_type": "FULFILLMENT_BY_SELLER"})
    assert not mapping.fulfilled_by_platform({})
