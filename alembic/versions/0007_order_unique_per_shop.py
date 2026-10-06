"""order_unique_per_shop — mã đơn / mã yêu cầu trả unique theo shop (02a §3 Migration 0007, BR-29, T-202)

Upgrade (một transaction, `lock_timeout` 5 giây):
1. Kiểm không có (`shop_id`, `platform_order_sn`) trùng và mã trùng trong đơn file (`shop_id IS NULL`) — dữ liệu
   Phase 2 luôn đạt (đang unique toàn cục); trùng → `raise` (không đổi gì).
2. Drop unique toàn cục `order.platform_order_sn` — tên lấy từ `pg_constraint` lúc chạy (không đoán tên).
3. `uq_order_shop_sn (shop_id, platform_order_sn) WHERE shop_id IS NOT NULL` + `uq_order_noshop_sn
   (platform_order_sn) WHERE shop_id IS NULL` (vị từ literal — DEC-362; code không `ON CONFLICT` trên các index này,
   upsert dưới khóa `order:{sn}` — DEC-493).
4. `uq_return_case_platform_return_sn` → `uq_return_case_shop_return_sn (shop_id, platform_return_sn) WHERE
   platform_return_sn IS NOT NULL`.

Downgrade: còn mã đơn / mã yêu cầu trả trùng giữa shop → `raise` kèm 20 mã đầu ("sửa tiến"); không có → tạo lại
unique toàn cục cũ. Tra theo mã dùng `ix_order_platform_order_sn` (0006).

Revision ID: 0007
Revises: 0006
"""

import logging
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: Union[str, Sequence[str], None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

LOCK_TIMEOUT = "5s"
OLD_ORDER_UNIQUE = "uq_order_platform_order_sn"  # tên 0001 (naming convention) — chỉ dùng khi tạo lại
OLD_RETURN_UNIQUE = "uq_return_case_platform_return_sn"


def _codes(bind: sa.Connection, sql: str) -> list[str]:
    return [str(r[0]) for r in bind.execute(sa.text(sql)).all()]


def _order_unique_constraints(bind: sa.Connection) -> list[str]:
    """Unique một cột `platform_order_sn` trên `order` (constraint, không phải index) — tên thật từ catalog."""
    return _codes(
        bind,
        "SELECT c.conname FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid "
        "AND a.attnum = ANY(c.conkey) WHERE c.conrelid = '\"order\"'::regclass AND c.contype = 'u' "
        "AND array_length(c.conkey, 1) = 1 AND a.attname = 'platform_order_sn'",
    )


def upgrade() -> None:
    bind = op.get_bind()
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    dup = _codes(
        bind,
        'SELECT platform_order_sn FROM "order" GROUP BY shop_id, platform_order_sn HAVING count(*) > 1 '
        "ORDER BY 1 LIMIT 20",
    ) + _codes(
        bind,
        "SELECT platform_return_sn FROM return_case WHERE platform_return_sn IS NOT NULL "
        "GROUP BY shop_id, platform_return_sn HAVING count(*) > 1 ORDER BY 1 LIMIT 20",
    )
    if dup:
        raise RuntimeError(
            f"0007: mã trùng trong cùng shop — không tạo được unique theo shop: {', '.join(dup)}"
        )
    for name in _order_unique_constraints(bind):
        op.execute(f'ALTER TABLE "order" DROP CONSTRAINT {name}')
    op.create_index(
        "uq_order_shop_sn",
        "order",
        ["shop_id", "platform_order_sn"],
        unique=True,
        postgresql_where=sa.text("shop_id IS NOT NULL"),
    )
    op.create_index(
        "uq_order_noshop_sn",
        "order",
        ["platform_order_sn"],
        unique=True,
        postgresql_where=sa.text("shop_id IS NULL"),
    )
    op.execute(f"DROP INDEX IF EXISTS {OLD_RETURN_UNIQUE}")
    op.create_index(
        "uq_return_case_shop_return_sn",
        "return_case",
        ["shop_id", "platform_return_sn"],
        unique=True,
        postgresql_where=sa.text("platform_return_sn IS NOT NULL"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    orders = _codes(
        bind,
        'SELECT platform_order_sn FROM "order" GROUP BY platform_order_sn HAVING count(*) > 1 ORDER BY 1 LIMIT 20',
    )
    returns = _codes(
        bind,
        "SELECT platform_return_sn FROM return_case WHERE platform_return_sn IS NOT NULL "
        "GROUP BY platform_return_sn HAVING count(*) > 1 ORDER BY 1 LIMIT 20",
    )
    if orders or returns:
        raise RuntimeError(
            "Không downgrade 0007: có mã đơn / mã yêu cầu trả trùng giữa các shop — không lùi được về Phase 2 "
            f"(Phase 2 cần mã unique toàn hệ thống), sửa tiến. Mã đơn: {', '.join(orders) or '-'}; "
            f"mã yêu cầu trả: {', '.join(returns) or '-'}. Không có gì bị thay đổi."
        )
    op.drop_index(
        "uq_return_case_shop_return_sn",
        table_name="return_case",
        postgresql_where=sa.text("platform_return_sn IS NOT NULL"),
    )
    op.create_index(
        OLD_RETURN_UNIQUE,
        "return_case",
        ["platform_return_sn"],
        unique=True,
        postgresql_where=sa.text("platform_return_sn IS NOT NULL"),
    )
    op.drop_index("uq_order_noshop_sn", table_name="order", postgresql_where=sa.text("shop_id IS NULL"))
    op.drop_index("uq_order_shop_sn", table_name="order", postgresql_where=sa.text("shop_id IS NOT NULL"))
    op.create_unique_constraint(OLD_ORDER_UNIQUE, "order", ["platform_order_sn"])
    log.info("0007 downgrade: tạo lại unique toàn cục mã đơn / mã yêu cầu trả")
