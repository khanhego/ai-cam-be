"""NFR-28 / BR-30 (T-203): lõi không đọc chữ trạng thái riêng của sàn.

Chỉ `platforms/{shopee,tiktok,mock}` (và dữ liệu mẫu `entrypoints/seed_*`, định dạng sàn như fixture mock)
biết chữ trạng thái. Quét mọi chuỗi hằng (AST) trong `src/aicam` còn lại: không được có chữ trạng thái đơn /
yêu cầu trả của Shopee hay TikTok (02 §5.3), trừ chữ trùng tên một giá trị enum nội bộ (nhóm chung, trạng
thái kho, phiên, hồ sơ… — vd `CANCELLED`, `SHIPPED`, `DELIVERED`, `COMPLETED`, `CLOSED`). Danh sách điểm đã
biết (02a §5 BR-30) phải sạch: `reconciliation/rules.py`, `returns/service.py`, `sessions/return_scan.py`,
`platforms/sync.py`, `platforms/base.py`. Gán `platform_status` ngoài helper (test AST đủ) — T-278.
"""

import ast
from pathlib import Path

from aicam.modules.claims.models import CLAIM_STATUSES
from aicam.modules.media.models import CLIP_STATUSES
from aicam.modules.orders.models import WAREHOUSE_STATUSES
from aicam.modules.platforms.base import ORDER_STATUS_GROUPS, RETURN_STATUS_GROUPS
from aicam.modules.returns.models import RETURN_CASE_STATUSES
from aicam.modules.sessions.models import SESSION_STATUSES

SRC = Path(__file__).resolve().parents[2] / "src" / "aicam"
ADAPTER_DIRS = (
    "modules/platforms/shopee",
    "modules/platforms/tiktok",
    "modules/platforms/mock",
    "entrypoints/seed_",
)

SHOPEE_ORDER = (
    "UNPAID", "READY_TO_SHIP", "PROCESSED", "RETRY_SHIP", "SHIPPED", "TO_CONFIRM_RECEIVE", "COMPLETED",
    "IN_CANCEL", "CANCELLED", "TO_RETURN",
)  # fmt: skip
SHOPEE_RETURN = (
    "REQUESTED", "PROCESSING", "ACCEPTED", "JUDGING", "SELLER_DISPUTE", "CANCELLED", "REFUND_PAID", "CLOSED",
)  # fmt: skip
TIKTOK_ORDER = (
    "UNPAID", "ON_HOLD", "AWAITING_SHIPMENT", "PARTIALLY_SHIPPING", "AWAITING_COLLECTION", "IN_TRANSIT",
    "DELIVERED", "COMPLETED", "CANCELLED",
)  # fmt: skip
TIKTOK_RETURN = (
    "RETURN_OR_REFUND_REQUEST_PENDING", "AWAITING_BUYER_SHIP", "BUYER_SHIPPED_ITEM", "RECEIVE_REJECTED",
    "REQUEST_REJECTED", "RETURN_OR_REFUND_REQUEST_CANCEL", "RETURN_OR_REFUND_REQUEST_COMPLETE",
)  # fmt: skip
INTERNAL = set(
    ORDER_STATUS_GROUPS + RETURN_STATUS_GROUPS + WAREHOUSE_STATUSES + SESSION_STATUSES + CLAIM_STATUSES
    + RETURN_CASE_STATUSES + CLIP_STATUSES
)  # fmt: skip
PLATFORM_ONLY = set(SHOPEE_ORDER + SHOPEE_RETURN + TIKTOK_ORDER + TIKTOK_RETURN) - INTERNAL


def _core_files() -> list[Path]:
    return [
        p for p in SRC.rglob("*.py") if not any(str(p.relative_to(SRC)).startswith(d) for d in ADAPTER_DIRS)
    ]


def test_platform_only_statuses_are_meaningful() -> None:
    """Tập quét không rỗng và có các chữ Phase 2 từng nằm trong lõi."""
    assert {"READY_TO_SHIP", "IN_CANCEL", "TO_RETURN", "TO_CONFIRM_RECEIVE", "JUDGING", "REFUND_PAID"} <= (
        PLATFORM_ONLY
    )
    assert {"IN_TRANSIT", "ON_HOLD", "RETURN_OR_REFUND_REQUEST_PENDING"} <= PLATFORM_ONLY


def test_no_platform_status_strings_in_core() -> None:
    hits: list[str] = []
    for path in _core_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in PLATFORM_ONLY:
                hits.append(f"{path.relative_to(SRC)}:{node.lineno} {node.value!r}")
    assert hits == []


def test_no_platform_status_collections_in_core() -> None:
    """Hằng Phase 2 đọc chữ sàn trong lõi đã bỏ (nhóm thay)."""
    banned = (
        "CANCELLED_STATUSES",
        "SHIPPED_PLATFORM_STATUSES",
        "AWAITING_ACCEPT_STATUSES",
        "DONE_PLATFORM_STATUSES",
    )
    hits = [
        f"{path.relative_to(SRC)}: {name}"
        for path in _core_files()
        for name in banned
        if name in path.read_text(encoding="utf-8")
    ]
    assert hits == []
