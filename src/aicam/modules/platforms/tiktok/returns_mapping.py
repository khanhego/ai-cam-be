"""Ánh xạ yêu cầu trả / hoàn tiền TikTok Shop → `PlatformReturn` (02 §5.3, BR-31; 02a §7.1 "Yêu cầu trả").

**Giả định theo tài liệu công khai `return_refund/202309/returns/search` — chưa test với TikTok thật (Q19).**
Lưu nguyên payload (`raw`) để ánh xạ lại khi T-3 TikTok có dữ liệu thật.

- Loại (`return_type`): `REFUND_ONLY` → không cần kiện về (hồ sơ "Chỉ hoàn tiền"); `RETURN_AND_REFUND` →
  "Khách trả hàng"; `REPLACEMENT` → "Khách trả hàng", `is_exchange`, lý do `EXCHANGE` ("Đổi hàng"). Loại lạ →
  coi như có kiện về (an toàn: hồ sơ chờ kiện, không bỏ sót kiện thật — như Shopee).
- Hạn người bán: tên trường chưa rõ (L14) → thử `seller_response_deadline` / `seller_deadline`, không có →
None.
"""

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from aicam.modules.platforms.base import PlatformReturn, ReturnItem

# Trạng thái yêu cầu → nhóm chung (02 §5.3). Chữ lạ → None (không chờ duyệt, không kết thúc — BR-12 chạy).
STATUS_GROUPS: dict[str, str] = {
    "RETURN_OR_REFUND_REQUEST_PENDING": "REQUESTED",
    "REPLACEMENT_REQUEST_PENDING": "REQUESTED",
    "AWAITING_BUYER_SHIP": "ACCEPTED",
    "BUYER_SHIPPED_ITEM": "ACCEPTED",
    "RECEIVE_REJECTED": "ACCEPTED",  # đang tranh chấp việc nhận hàng
    "REQUEST_REJECTED": "CANCELLED",
    "RETURN_OR_REFUND_REQUEST_CANCEL": "CANCELLED",
    "REPLACEMENT_REQUEST_CANCEL": "CANCELLED",
    "RETURN_OR_REFUND_REQUEST_COMPLETE": "DONE",
    "REPLACEMENT_REQUEST_COMPLETE": "DONE",
    "RETURN_OR_REFUND_REQUEST_CLOSED": "CLOSED",  # đóng không hoàn tiền (giả định tên)
}
REFUND_ONLY = "REFUND_ONLY"
REPLACEMENT = "REPLACEMENT"
EXCHANGE_REASON = "EXCHANGE"

# Lý do TikTok (chuỗi khóa dài, giả định) → mã chung (FE nhãn ở `returns.views.reason_label`).
_REASON_KEYWORDS = (
    ("WRONG", "WRONG_ITEM"),
    ("DAMAGE", "ITEM_DAMAGED"),
    ("DEFECT", "ITEM_DAMAGED"),
    ("MISSING", "ITEM_MISSING"),
    ("NOT_RECEIVED", "NON_RECEIPT"),
    ("NON_RECEIPT", "NON_RECEIPT"),
    ("DESCRIPTION", "DIFFERENT_DESCRIPTION"),
    ("DESCRIBED", "DIFFERENT_DESCRIPTION"),
    ("CHANGE", "CHANGE_MIND"),
    ("NO_LONGER", "CHANGE_MIND"),
)
REASON_LABELS: dict[str, str] = {EXCHANGE_REASON: "Đổi hàng"}


def status_group(status: str) -> str | None:
    return STATUS_GROUPS.get(status.upper())


def normalize_reason(reason: str | None, return_type: str) -> str | None:
    if return_type == REPLACEMENT:
        return EXCHANGE_REASON
    if not reason:
        return None
    code = reason.strip().upper()
    for key, value in _REASON_KEYWORDS:
        if key in code:
            return value
    return "OTHER"


def _ts(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=UTC) if value else None
    except (TypeError, ValueError):
        return None


def _str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _items(lines: Iterable[Any]) -> tuple[ReturnItem, ...]:
    """Mỗi đơn vị một dòng (giả định như đơn) → gộp theo (sku, tên, phân loại)."""
    grouped: dict[tuple[str | None, str | None, str | None], int] = {}
    ids: dict[tuple[str | None, str | None, str | None], str | None] = {}
    for li in lines:
        if not isinstance(li, dict):
            continue
        key = (
            _str(li.get("seller_sku") or li.get("sku_id")),
            _str(li.get("product_name")),
            _str(li.get("sku_name")),
        )
        try:
            qty = max(0, int(li.get("quantity") or 1))
        except (TypeError, ValueError):
            qty = 0
        grouped[key] = grouped.get(key, 0) + qty
        ids.setdefault(key, _str(li.get("sku_id")))
    return tuple(
        ReturnItem(quantity=qty, model_id=ids[key], sku=key[0], product_name=key[1], variation=key[2])
        for key, qty in grouped.items()
    )


def to_platform_return(detail: dict[str, Any]) -> PlatformReturn:
    status = str(detail.get("return_status") or "").upper()
    return_type = str(detail.get("return_type") or "").upper()
    tracking = _str(detail.get("return_tracking_number"))
    return PlatformReturn(
        return_sn=str(detail["return_id"]),
        order_sn=str(detail["order_id"]),
        status=status,
        status_group=status_group(status),
        needs_parcel=return_type != REFUND_ONLY,
        return_tracking_number=tracking.upper() if tracking else None,
        reason=normalize_reason(_str(detail.get("return_reason")), return_type),
        reason_text=_str(detail.get("return_reason_text")),
        items=_items(detail.get("return_line_items") or []),
        seller_due_at=_ts(detail.get("seller_response_deadline") or detail.get("seller_deadline")),
        created_at=_ts(detail.get("create_time")),
        updated_at=_ts(detail.get("update_time")),
        raw=detail,
        is_exchange=return_type == REPLACEMENT,
    )
