"""return_lookup_indexes — index `text_pattern_ops` cho API-104 tìm tiền tố (T-118, DEC-334)

DB collation `en_US.utf8` → `LIKE 'Q%'` (và `= OR LIKE` của API-104) không dùng được btree mặc định: quét cả bảng.
Đo 1 triệu kiện (máy dev): API-104 ~460 ms → ~8–11 ms; tạo 3 index ~1,3 giây (khóa ghi bảng trong lúc tạo).
+ index một phần `package(created_at) WHERE verified IS false AND is_placeholder IS false` cho J-14 BR-20 (G3 R7,
DEC-345 — gộp vào 0005 vì Phase 2 chưa phát hành).

Không `CREATE INDEX CONCURRENTLY` (G3 M-F3, DEC-337): nâng cấp chạy khi mọi service đã dừng (docs/ops.md §7.1) nên
khóa ghi vài giây không chặn ai; CONCURRENTLY phải ra ngoài transaction Alembic (lỗi giữa chừng để index INVALID,
nâng cấp không còn nguyên tử). `lock_timeout` 5 giây như 0003.

Revision ID: 0005
Revises: 0004
"""

from typing import Sequence, Union

from alembic import op

revision: str = "0005"
down_revision: Union[str, Sequence[str], None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

INDEXES = (
    ("ix_package_tracking_upper_pattern", "package", "upper(tracking_number) text_pattern_ops"),
    ("ix_order_platform_order_sn_upper_pattern", '"order"', "upper(platform_order_sn) text_pattern_ops"),
    (
        "ix_return_case_return_tracking_upper_pattern",
        "return_case",
        "upper(return_tracking_number) text_pattern_ops",
    ),
)
PARTIAL = (
    (
        "ix_package_unverified_created_at",
        "package",
        "created_at",
        "verified IS false AND is_placeholder IS false",
    ),
)


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    for name, table, expr in INDEXES:
        op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({expr})")
    for name, table, expr, where in PARTIAL:
        op.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({expr}) WHERE {where}")


def downgrade() -> None:
    for name, *_ in (*INDEXES, *PARTIAL):
        op.execute(f"DROP INDEX IF EXISTS {name}")
