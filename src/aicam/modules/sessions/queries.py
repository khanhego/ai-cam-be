"""Vị từ dùng chung trên phiên (BR-39 v0.3–v0.5, 02a §5): **một** luật SQL + bản Python cùng nghĩa.

- Phiên RETURN **bị loại** (`excluded`): lý do hủy hiệu lực (`cancel_cause` của Supervisor, không có thì
  `cancel_reason` của station) ∈ `EXCLUDED_CANCEL_REASONS` và chưa được xác nhận "Là phiên hoàn thật"
  (`review_confirmed_at`), **hoặc** đã đánh dấu quét nhầm (`wrong_scan_at` — xác nhận không gỡ được, phải bỏ
  đánh dấu trước). Không tự vào bằng chứng, không bao giờ là phiên chính, không tính N03 / D2 / D3.
- Phiên **cần soát** (`review_needed`): Supervisor hủy trước Phase 3 (`cancel_reason = SUPERVISOR`, chưa có mã
  lý do), chưa xác nhận / đánh dấu — vào bằng chứng nhưng không làm phiên chính.
- `dropped_return_filter()`: phiên mở hoàn bị hủy / bỏ dở **tính** cho API-32 `returns_dropped_7d`, API-30
  `return_dropped`, J-26 N03 (= RETURN `CANCELLED` / `ABANDONED` ∧ không bị loại).
"""

from typing import Any, Protocol

from sqlalchemy import ColumnElement, and_, func, not_, or_

from aicam.modules.sessions.models import PackSession

EXCLUDED_CANCEL_REASONS = ("WRONG_SCAN", "NOT_A_RETURN")
DROPPED_STATUSES = ("CANCELLED", "ABANDONED")


def excluded_return_sql(s: Any = PackSession) -> ColumnElement[bool]:
    """`s` = `PackSession` hoặc alias của nó."""
    return and_(
        s.type == "RETURN",
        or_(
            and_(
                # `''` thay NULL: phiên không có lý do (bỏ dở) → FALSE, không NULL (NOT NULL sẽ loại nhầm).
                func.coalesce(s.cancel_cause, s.cancel_reason, "").in_(EXCLUDED_CANCEL_REASONS),
                s.review_confirmed_at.is_(None),
            ),
            s.wrong_scan_at.is_not(None),
        ),
    )


def review_needed_sql(s: Any = PackSession) -> ColumnElement[bool]:
    return and_(
        s.type == "RETURN",
        s.status == "CANCELLED",
        func.coalesce(s.cancel_reason, "") == "SUPERVISOR",  # luôn TRUE / FALSE (dùng được với NOT)
        s.cancel_cause.is_(None),
        s.review_confirmed_at.is_(None),
        s.wrong_scan_at.is_(None),
    )


def dropped_return_filter(s: Any = PackSession) -> ColumnElement[bool]:
    return and_(s.type == "RETURN", s.status.in_(DROPPED_STATUSES), not_(excluded_return_sql(s)))


class _SessionLike(Protocol):
    type: str
    status: str
    cancel_reason: str | None
    cancel_cause: str | None
    wrong_scan_at: Any
    review_confirmed_at: Any


def effective_cancel_reason(s: _SessionLike) -> str | None:
    """Lý do hủy hiệu lực: mã Supervisor chọn (API-21) nếu có, không thì lý do station."""
    return s.cancel_cause or s.cancel_reason


def excluded(s: _SessionLike) -> bool:
    """Bản Python của `excluded_return_sql` (test so khớp hai bản)."""
    if s.type != "RETURN":
        return False
    if s.wrong_scan_at is not None:
        return True
    return effective_cancel_reason(s) in EXCLUDED_CANCEL_REASONS and s.review_confirmed_at is None


def review_needed(s: _SessionLike) -> bool:
    return (
        s.type == "RETURN"
        and s.status == "CANCELLED"
        and s.cancel_reason == "SUPERVISOR"
        and s.cancel_cause is None
        and s.review_confirmed_at is None
        and s.wrong_scan_at is None
    )


def dropped(s: _SessionLike) -> bool:
    return s.type == "RETURN" and s.status in DROPPED_STATUSES and not excluded(s)
