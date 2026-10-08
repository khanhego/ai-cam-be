"""NFR-28 / BR-30 (T-203): lõi không đọc chữ trạng thái riêng của sàn.

Chỉ `platforms/{shopee,tiktok,mock}` (và dữ liệu mẫu `entrypoints/seed_*`, định dạng sàn như fixture mock)
biết chữ trạng thái. Quét mọi chuỗi hằng (AST) trong `src/aicam` còn lại: không được có chữ trạng thái đơn /
yêu cầu trả của Shopee hay TikTok (02 §5.3), trừ chữ trùng tên một giá trị enum nội bộ (nhóm chung, trạng
thái kho, phiên, hồ sơ… — vd `CANCELLED`, `SHIPPED`, `DELIVERED`, `COMPLETED`, `CLOSED`). Danh sách điểm đã
biết (02a §5 BR-30) phải sạch: `reconciliation/rules.py`, `returns/service.py`, `sessions/return_scan.py`,
`platforms/sync.py`, `platforms/base.py`. Gán `platform_status`
ngoài helper: test AST `test_platform_status_written_only_by_helpers` (T-278).
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


# ---------------------------------------------------------------- DEC-508 (T-278): một chỗ ghi


FIELDS = {"platform_status", "platform_status_group"}
HELPERS = {
    ("modules/orders/service.py", "set_platform_status"),
    ("modules/returns/service.py", "set_platform_status"),
}


def _writes(path: Path, rel: str | None = None) -> list[str]:
    """Gán `x.platform_status(_group) = …` / `Order(…, platform_status=…)` / `.values(platform_status=…)`
    ngoài hai helper."""
    rel = rel or str(path.relative_to(SRC))
    tree = ast.parse(path.read_text(encoding="utf-8"))
    hits: list[str] = []

    def visit(node: ast.AST, func: str | None) -> None:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            func = node.name
        allowed = (rel, func) in HELPERS
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AugAssign | ast.AnnAssign):
            targets = [node.target]
        for target in targets:
            for t in ast.walk(target):
                if isinstance(t, ast.Attribute) and t.attr in FIELDS and not allowed:
                    hits.append(f"{rel}:{t.lineno} gán .{t.attr} trong {func}")
        if isinstance(node, ast.Call):
            name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
            if name in ("Order", "ReturnCase", "values"):
                hits.extend(
                    f"{rel}:{node.lineno} {name}({k.arg}=…)" for k in node.keywords if k.arg in FIELDS
                )
            if name == "setattr" and len(node.args) > 1:
                arg = node.args[1]
                if isinstance(arg, ast.Constant) and arg.value in FIELDS:
                    hits.append(f"{rel}:{node.lineno} setattr {arg.value}")
        for child in ast.iter_child_nodes(node):
            visit(child, func)

    visit(tree, None)
    return hits


def test_platform_status_written_only_by_helpers() -> None:
    hits = [h for path in SRC.rglob("*.py") for h in _writes(path)]
    assert hits == []


def test_ast_check_catches_a_write(tmp_path: Path) -> None:
    """Bộ quét bắt được cả 3 dạng (không thì test trên xanh giả)."""
    bad = tmp_path / "bad.py"
    bad.write_text(
        "def f(o, s):\n    o.platform_status = 'x'\n    Order(platform_status_group='y')\n"
        "    s.execute(update(Order).values(platform_status='z'))\n",
        encoding="utf-8",
    )
    assert len(_writes(bad, "modules/orders/service.py")) == 3
