"""SQL dùng chung về hồ sơ hàng hoàn (02a §5 BR-40): API-110 `pending_only` / `response_due_at`, API-32
`refund_only_pending` / `REFUND_ONLY_PENDING`, J-26 N04."""

from typing import Any

from sqlalchemy import ColumnElement, and_, case, func, literal, not_, or_, select

from aicam.modules.claims.models import Claim
from aicam.modules.orders.models import Package
from aicam.modules.returns.models import ReturnCase

# Yêu cầu sàn còn mở (02 §6.2 API-110 `pending_only`).
OPEN_REQUEST_GROUPS = ("REQUESTED", "ACCEPTED")


def handling_claim_condition(case_: Any = ReturnCase) -> ColumnElement[bool]:
    """Hồ sơ khiếu nại (join `Package` theo `Claim.package_id`) **chưa đóng** đang xử lý yêu cầu
    `case_` (BR-40, L26 — DEC-1001): không tính `LEGACY_HOLD`; gắn đúng hồ sơ hàng hoàn này, hoặc
    chưa gắn hồ sơ hàng hoàn nào, thuộc đơn (theo `claim.order_id` hoặc kiện của đơn) và tạo từ lúc
    sàn báo yêu cầu trở đi. Hồ sơ của yêu cầu trả khác / tạo trước yêu cầu này không tính."""
    reported = func.coalesce(case_.reported_at, case_.created_at)
    return and_(
        Claim.status != "CLOSED",
        Claim.source != "LEGACY_HOLD",
        or_(
            Claim.return_case_id == case_.id,
            and_(
                Claim.return_case_id.is_(None),
                Claim.created_at >= reported,
                or_(Claim.order_id == case_.order_id, Package.order_id == case_.order_id),
            ),
        ),
    )


def handling_claim_exists(case_: Any = ReturnCase) -> ColumnElement[bool]:
    """Yêu cầu `case_` đã có hồ sơ khiếu nại chưa đóng xử lý nó (`handling_claim_condition`)."""
    return (
        select(literal(1))
        .select_from(Claim)
        .join(Package, Package.id == Claim.package_id)
        .where(handling_claim_condition(case_))
        .exists()
    )


def refund_pending_filter(case_: Any = ReturnCase) -> ColumnElement[bool]:
    """BR-40 "Chỉ hoàn tiền chưa xử lý": hồ sơ `REFUND_ONLY` chưa hủy, yêu cầu sàn còn mở (nhóm
    `REQUESTED` / `ACCEPTED`), yêu cầu chưa có hồ sơ khiếu nại chưa đóng xử lý nó
    (`handling_claim_exists`)."""
    return and_(
        case_.kind == "REFUND_ONLY",
        case_.status != "CANCELLED",
        case_.platform_status_group.in_(OPEN_REQUEST_GROUPS),
        case_.order_id.is_not(None),
        not_(handling_claim_exists(case_)),
    )


def response_due_sql(hours: int, case_: Any = ReturnCase) -> ColumnElement[Any]:
    """Hạn phản hồi (BR-40, DEC-451): `seller_due_at`, không có → `reported_at + hours`; chỉ `REFUND_ONLY`."""
    return case(
        (
            case_.kind == "REFUND_ONLY",
            func.coalesce(case_.seller_due_at, case_.reported_at + func.make_interval(0, 0, 0, 0, hours)),
        ),
        else_=None,
    )
