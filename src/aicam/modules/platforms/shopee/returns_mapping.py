"""Ánh xạ yêu cầu trả hàng Shopee v2 → `PlatformReturn` (02a §7 "Shopee returns", DEC-262).

Theo tài liệu công khai Shopee Open Platform v2 (`returns.get_return_list`, `returns.get_return_detail`).
**Chưa test với Shopee thật — thiếu tài khoản partner (T-3).** Trường cần xác nhận: mã vận đơn chiều về
(`tracking_number`), loại "chỉ hoàn tiền" (`needs_logistics`), `return_seller_due_date`, `item[]`.
Lưu nguyên payload (`raw`) để ánh xạ lại khi T-3 có dữ liệu thật (RB-22).
"""

from datetime import UTC, datetime
from typing import Any

from aicam.modules.platforms.base import PlatformReturn, ReturnItem

# Trạng thái yêu cầu trả → nhóm chung (02a §7, DEC-262: DONE chỉ REFUND_PAID; CLOSED tách riêng).
STATUS_GROUPS: dict[str, str] = {
    "REQUESTED": "OPEN",
    "PROCESSING": "OPEN",
    "ACCEPTED": "OPEN",
    "JUDGING": "OPEN",
    "SELLER_DISPUTE": "OPEN",
    "CANCELLED": "CANCELLED",
    "REFUND_PAID": "DONE",
    "CLOSED": "CLOSED",
}
# Lý do sàn → mã chung (FE map nhãn: 01 §10). Mã lạ → OTHER, giữ chữ gốc trong `raw`.
REASONS = frozenset(
    {
        "NON_RECEIPT",
        "WRONG_ITEM",
        "ITEM_DAMAGED",
        "DIFFERENT_DESCRIPTION",
        "MUTUAL_AGREE",
        "OTHER",
        "ITEM_MISSING",
        "CHANGE_MIND",
    }
)
REASON_LABELS: dict[str, str] = {
    "NON_RECEIPT": "Không nhận được hàng",
    "WRONG_ITEM": "Sai sản phẩm",
    "ITEM_DAMAGED": "Hàng bị hư",
    "DIFFERENT_DESCRIPTION": "Khác mô tả",
    "MUTUAL_AGREE": "Thỏa thuận",
    "OTHER": "Khác",
    "ITEM_MISSING": "Thiếu hàng",
    "CHANGE_MIND": "Đổi ý",
}


def status_group(status: str) -> str:
    """Trạng thái lạ → OPEN (an toàn: hồ sơ vẫn chờ kiện, không tự hủy / đóng)."""
    return STATUS_GROUPS.get(status.upper(), "OPEN")


def normalize_reason(reason: str | None) -> str | None:
    if not reason:
        return None
    code = reason.strip().upper()
    return code if code in REASONS else "OTHER"


def _ts(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=UTC) if value else None
    except (TypeError, ValueError):
        return None


def _str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _quantity(value: Any) -> int:
    """Số lượng sàn trả (G3 F-6): chuỗi lạ / số thực / None → 0 (người kiểm sửa), không hỏng cả lượt J-13."""
    try:
        return max(0, int(float(str(value).strip()))) if value not in (None, "") else 0
    except (TypeError, ValueError):
        return 0


def to_item(raw: dict[str, Any]) -> ReturnItem:
    return ReturnItem(
        item_id=_str(raw.get("item_id")),
        model_id=_str(raw.get("model_id")),
        sku=_str(raw.get("variation_sku") or raw.get("model_sku") or raw.get("item_sku")),
        product_name=_str(raw.get("name") or raw.get("item_name")),
        variation=_str(raw.get("model_name") or raw.get("variation")),
        quantity=_quantity(raw.get("amount") or raw.get("quantity")),
    )


def to_platform_return(detail: dict[str, Any]) -> PlatformReturn:
    status = str(detail.get("status") or "").upper()
    needs = detail.get("needs_logistics")
    tracking = _str(detail.get("tracking_number"))
    return PlatformReturn(
        return_sn=str(detail["return_sn"]),
        order_sn=str(detail["order_sn"]),
        status=status,
        status_group=status_group(status),
        # Thiếu trường → coi như có kiện về (an toàn: hồ sơ chờ kiện, không bỏ sót kiện thật).
        needs_parcel=bool(needs) if needs is not None else True,
        return_tracking_number=tracking.upper() if tracking else None,
        reason=normalize_reason(_str(detail.get("reason"))),
        reason_text=_str(detail.get("text_reason")),
        items=tuple(to_item(i) for i in detail.get("item") or [] if isinstance(i, dict)),
        seller_due_at=_ts(detail.get("return_seller_due_date")),
        created_at=_ts(detail.get("create_time")),
        updated_at=_ts(detail.get("update_time")),
        raw=detail,
    )
