# ruff: noqa: S608 — SQL ghép từ hằng của migration (tên bảng / cột), không có input ngoài
"""returns_recon_claims — schema Phase 2 (02a §3, T-101)

Chỉ thêm (tương thích ngược về schema): 3 sequence, 9 bảng mới, cột mới nullable / có default, CHECK mở rộng
(tập cũ ⊂ tập mới) bằng `ADD CONSTRAINT … CHECK` thường — cả migration là MỘT transaction giữ ACCESS EXCLUSIVE
trên `package` tới commit nên `NOT VALID` + `VALIDATE` không rút ngắn khóa (G3 M-F2, DEC-337). `lock_timeout` 5
giây: còn tiến trình khác giữ khóa bảng → lỗi ngay thay vì chặn hàng đợi khóa (ops: dừng mọi service trước khi
migrate — docs/ops.md §7.1). Backfill `package.created_at` / `status_changed_at` theo lô 5.000 dòng (DEC-225),
index `(warehouse_status, status_changed_at)` tạo SAU backfill (không cập nhật index từng dòng, ít bloat — ops chạy
`VACUUM ANALYZE package` sau nâng cấp); `setting.recon_start_at = now()` (DEC-228); `retention_clip_days` <
`RETENTION_CLIP_MIN_DAYS` → nâng lên sàn + audit (DEC-257).

Downgrade (T-120, DEC-252, DEC-270, DEC-331): **không xóa dữ liệu Phase 2** — chép sang schema `phase2_archive`
(9 bảng mới; phiên RETURN + `session_event` / `clip` / `approval_request` / `export` của chúng; kiện tạm; dòng
`status_history` có trạng thái `RETURN_*`; cột Phase 2 của `package` / `session` / `station` / `setting` / `shop`;
giá trị sequence) rồi mới gỡ cấu trúc, kiện `RETURN_*` về trạng thái cuối không phải hoàn trong `status_history`
(không có → `DELIVERED`), CHECK về tập cũ. File video / ảnh giữ nguyên trên đĩa. Chặn khi còn phiên RETURN đang
mở (đóng / hủy trước — docs/ops.md). Chạy sau downgrade 0004 (cờ giữ cho clip được bảo vệ, `LEGACY_HOLD`).

Upgrade lại (bước 5 §3): có `phase2_archive.meta` → khôi phục toàn bộ vào bảng mới / cột mới, kiện về trạng thái
hoàn nếu code cũ chưa đổi trạng thái, `setval` sequence; **không** drop schema (0004 còn đọc
`downgrade_held_clips` / `legacy_claims*` rồi drop ở cuối — R3-5).

Revision ID: 0003
Revises: 0002
"""

import json
import logging
import os
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, Sequence[str], None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

SEQUENCES = ("return_case_code_seq", "claim_code_seq", "placeholder_code_seq")
BACKFILL_BATCH = 5000

_OLD_WAREHOUSE = "'NEW', 'PACKING', 'PACKED', 'HANDED_OVER', 'DELIVERED', 'CANCELLED', 'CANCELLED_AFTER_PACK'"
_NEW_WAREHOUSE = (
    _OLD_WAREHOUSE
    + ", 'RETURN_EXPECTED', 'RETURN_INSPECTING', 'RETURN_RECEIVED_OK', 'RETURN_RECEIVED_ISSUE', 'RETURN_MISSING'"
)
_OLD_CANCEL = "'OUT_OF_STOCK', 'WRONG_SCAN', 'OTHER', 'SUPERVISOR'"
_CONCLUSIONS = "'OK', 'DAMAGED', 'MISSING_ITEM', 'WRONG_ITEM', 'EMPTY_BOX', 'OTHER'"

# (bảng, tên constraint, biểu thức) — CHECK trên bảng đã có dữ liệu Phase 1.
NEW_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("package", "ck_package_warehouse_status_enum", f"warehouse_status IN ({_NEW_WAREHOUSE})"),
    ("session", "ck_session_type_enum", "type IN ('PACK', 'RETURN')"),
    ("session", "ck_session_cancel_reason_enum", f"cancel_reason IN ({_OLD_CANCEL}, 'NOT_A_RETURN')"),
    ("session", "ck_session_inspection_conclusion_enum", f"inspection_conclusion IN ({_CONCLUSIONS})"),
    ("session", "ck_session_inspection_lines_mode_enum", "inspection_lines_mode IN ('FULL', 'REFERENCE')"),
    ("session", "ck_session_return_case_only_return", "type = 'RETURN' OR return_case_id IS NULL"),
    ("station", "ck_station_kind_enum", "kind IN ('PACK', 'RETURN', 'BOTH')"),
    ("station", "ck_station_work_mode_enum", "work_mode IN ('PACK', 'RETURN')"),
    ("station", "ck_station_work_mode_matches_kind", "kind = 'BOTH' OR work_mode = kind"),
    ("station", "ck_station_operator_name_length", "char_length(operator_name) BETWEEN 2 AND 40"),
    ("setting", "ck_setting_return_warn_range", "return_warn_minutes BETWEEN 1 AND 1440"),
    ("setting", "ck_setting_return_abandon_range", "return_abandon_minutes BETWEEN 1 AND 1440"),
    ("setting", "ck_setting_return_abandon_gt_warn", "return_abandon_minutes > return_warn_minutes"),
    ("setting", "ck_setting_return_missing_days_range", "return_missing_days BETWEEN 1 AND 60"),
    ("setting", "ck_setting_handover_warn_hours_range", "handover_warn_hours BETWEEN 1 AND 168"),
    ("setting", "ck_setting_claim_deadline_days_range", "claim_deadline_days BETWEEN 1 AND 90"),
    ("setting", "ck_setting_claim_due_soon_hours_range", "claim_due_soon_hours BETWEEN 1 AND 168"),
)
# CHECK Phase 1 bị thay ở trên — downgrade tạo lại đúng tập cũ.
OLD_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("package", "ck_package_warehouse_status_enum", f"warehouse_status IN ({_OLD_WAREHOUSE})"),
    ("session", "ck_session_type_enum", "type IN ('PACK')"),
    ("session", "ck_session_cancel_reason_enum", f"cancel_reason IN ({_OLD_CANCEL})"),
)

ARCHIVE = "phase2_archive"
# Mã do sequence cấp: (sequence, bảng, cột, tiền tố) — khôi phục `setval` không lùi.
CODE_SEQUENCES: tuple[tuple[str, str, str, str], ...] = (
    ("return_case_code_seq", "return_case", "code", "HH-"),
    ("claim_code_seq", "claim", "code", "KN-"),
    ("placeholder_code_seq", "package", "tracking_number", "TAM-"),
)
# 9 bảng mới: chép nguyên bảng (thứ tự = thứ tự khôi phục theo khóa ngoại).
NEW_TABLES = (
    "return_case",
    "return_case_package",
    "inspection_line",
    "snapshot",
    "claim",
    "claim_evidence",
    "claim_note",
    "evidence_pack",
    "recon_alert",
)
_RETURN_SESSIONS = "SELECT id FROM session WHERE type = 'RETURN'"
# Dòng Phase 2 trong bảng Phase 1: (bảng archive, bảng nguồn, điều kiện). Thứ tự = thứ tự khôi phục.
ROW_ARCHIVES: tuple[tuple[str, str, str], ...] = (
    ("placeholder_package", "package", "is_placeholder"),
    (
        "return_status_history",
        "status_history",
        "left(to_status, 7) = 'RETURN_' OR left(coalesce(from_status, ''), 7) = 'RETURN_' "
        "OR package_id IN (SELECT id FROM package WHERE is_placeholder)",
    ),
    ("return_session", "session", "type = 'RETURN'"),
    ("return_session_event", "session_event", f"session_id IN ({_RETURN_SESSIONS})"),
    ("return_clip", "clip", f"session_id IN ({_RETURN_SESSIONS})"),
    ("return_approval_request", "approval_request", f"session_id IN ({_RETURN_SESSIONS})"),
    ("return_export", "export", f"session_id IN ({_RETURN_SESSIONS})"),
)
# Cột Phase 2 của bảng Phase 1 (khóa `id`) — mất khi drop cột nếu không chép.
_SESSION_COLS = (
    "return_case_id, operator_name, inspection_conclusion, inspection_note, inspection_saved_at, "
    "inspection_lines_mode, inspection_corrections, camera_clock"
)
_SETTING_COLS = (
    "return_warn_minutes, return_abandon_minutes, return_missing_days, handover_warn_hours, "
    "claim_deadline_days, claim_due_soon_hours, recon_start_at"
)
COLUMN_ARCHIVES: tuple[tuple[str, str, str], ...] = (
    (
        "package_cols",
        "package",
        "SELECT id, created_at, status_changed_at, is_placeholder, warehouse_status, "
        "CAST(NULL AS text) AS downgraded_to FROM package",
    ),
    (
        "session_cols",
        "session",
        f"SELECT id, {_SESSION_COLS} FROM session WHERE type <> 'RETURN' AND "
        "num_nonnulls(return_case_id, operator_name, inspection_conclusion, inspection_note, "
        "inspection_saved_at, inspection_lines_mode, inspection_corrections, camera_clock) > 0",
    ),
    ("station_cols", "station", "SELECT id, kind, work_mode, operator_name FROM station"),
    ("setting_cols", "setting", f"SELECT id, {_SETTING_COLS} FROM setting"),
    ("shop_cols", "shop", "SELECT id, last_return_cursor FROM shop WHERE last_return_cursor IS NOT NULL"),
)
ARCHIVE_TABLES_0003 = (
    *NEW_TABLES,
    *(name for name, _, _ in ROW_ARCHIVES),
    *(name for name, _, _ in COLUMN_ARCHIVES),
    "meta",
)


def _retention_min_days() -> int:
    """`RETENTION_CLIP_MIN_DAYS` (02a §9) — đọc env trực tiếp, migration không phụ thuộc code ứng dụng."""
    return int(os.environ.get("RETENTION_CLIP_MIN_DAYS", "60"))


def _add_check(table: str, name: str, expr: str) -> None:
    op.execute(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}')
    op.execute(f'ALTER TABLE "{table}" ADD CONSTRAINT {name} CHECK ({expr})')


def _backfill_packages() -> int:
    """`created_at = updated_at`; `status_changed_at` = lần đổi trạng thái gần nhất, không có → `updated_at`."""
    bind = op.get_bind()
    last = None
    total = 0
    while True:
        ids = (
            bind.execute(
                sa.text(
                    "SELECT id FROM package WHERE (CAST(:last AS uuid) IS NULL OR id > CAST(:last AS uuid)) "
                    "ORDER BY id LIMIT :n"
                ),
                {"last": last, "n": BACKFILL_BATCH},
            )
            .scalars()
            .all()
        )
        if not ids:
            return total
        bind.execute(
            sa.text(
                """
                UPDATE package p
                SET created_at = p.updated_at,
                    status_changed_at = coalesce(h.max_at, p.updated_at)
                FROM (
                    SELECT pk.id, (SELECT max(sh.at) FROM status_history sh WHERE sh.package_id = pk.id) AS max_at
                    FROM package pk WHERE pk.id = ANY(:ids)
                ) h
                WHERE p.id = h.id
                """
            ),
            {"ids": list(ids)},
        )
        total += len(ids)
        last = str(ids[-1])


def _raise_retention_to_minimum() -> None:
    minimum = _retention_min_days()
    bind = op.get_bind()
    current = bind.execute(sa.text("SELECT retention_clip_days FROM setting WHERE id = 1")).scalar()
    if current is None or current >= minimum:
        return
    bind.execute(
        sa.text("UPDATE setting SET retention_clip_days = :m, updated_at = now() WHERE id = 1"),
        {"m": minimum},
    )
    bind.execute(
        sa.text(
            "INSERT INTO audit_log (user_id, action, object_type, object_id, at, data) "
            "VALUES (NULL, 'RETENTION_RAISED_TO_MINIMUM', 'SETTING', '1', now(), "
            "jsonb_build_object('retention_clip_days', jsonb_build_object('old', CAST(:old AS int), 'new', CAST(:new AS int))))"
        ),
        {"old": current, "new": minimum},
    )


LOCK_TIMEOUT = "5s"


def upgrade() -> None:
    # Chờ khóa tối đa 5 giây (service chưa dừng → lỗi rõ, cả migration lùi) — G3 M-F2.
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    for name in SEQUENCES:
        op.execute(f"CREATE SEQUENCE IF NOT EXISTS {name} START WITH 1 INCREMENT BY 1 NO CYCLE")

    op.create_table(
        "return_case",
        sa.Column(
            "code",
            sa.Text(),
            server_default=sa.text("'HH-' || lpad(nextval('return_case_code_seq')::text, 6, '0')"),
            nullable=False,
        ),
        sa.Column("order_id", sa.UUID(), nullable=True),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("platform_return_sn", sa.Text(), nullable=True),
        sa.Column("platform_status", sa.Text(), nullable=True),
        sa.Column("needs_parcel", sa.Boolean(), nullable=True),
        sa.Column("return_tracking_number", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("reason_text", sa.Text(), nullable=True),
        sa.Column(
            "requested_items",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("seller_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reported_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expected_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("conclusion", sa.Text(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("merged_into_id", sa.UUID(), nullable=True),
        sa.Column("signal_keys", postgresql.ARRAY(sa.Text()), server_default="{}", nullable=False),
        sa.Column("manual_link_only", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("force_note", sa.Text(), nullable=True),
        sa.Column("pending_merge_order_id", sa.UUID(), nullable=True),
        sa.Column("single_session", sa.Boolean(), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "kind IN ('FAILED_DELIVERY', 'BUYER_RETURN', 'REFUND_ONLY', 'UNANNOUNCED', 'UNIDENTIFIED')",
            name=op.f("ck_return_case_kind_enum"),
        ),
        sa.CheckConstraint("source IN ('PLATFORM', 'WAREHOUSE')", name=op.f("ck_return_case_source_enum")),
        sa.CheckConstraint(
            "status IN ('EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'RECEIVED_OK', 'RECEIVED_ISSUE', 'MISSING', 'CANCELLED', 'NO_PARCEL')",
            name=op.f("ck_return_case_status_enum"),
        ),
        sa.ForeignKeyConstraint(
            ["merged_into_id"],
            ["return_case.id"],
            name=op.f("fk_return_case_merged_into_id_return_case"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["order_id"], ["order.id"], name=op.f("fk_return_case_order_id_order"), ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_return_case")),
    )
    op.create_index(op.f("ix_return_case_order_id"), "return_case", ["order_id"], unique=False)
    op.create_index(
        "ix_return_case_return_tracking_upper",
        "return_case",
        [sa.literal_column("upper(return_tracking_number)")],
        unique=False,
    )
    op.create_index(
        "ix_return_case_signal_keys", "return_case", ["signal_keys"], unique=False, postgresql_using="gin"
    )
    op.create_index(
        op.f("ix_return_case_status_expected_since"),
        "return_case",
        ["status", "expected_since"],
        unique=False,
    )
    op.create_index("uq_return_case_code", "return_case", ["code"], unique=True)
    op.create_index(
        "uq_return_case_open_order",
        "return_case",
        ["order_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'MISSING')"),
    )
    op.create_index(
        "uq_return_case_platform_return_sn",
        "return_case",
        ["platform_return_sn"],
        unique=True,
        postgresql_where=sa.text("platform_return_sn IS NOT NULL"),
    )
    op.create_table(
        "claim",
        sa.Column(
            "code",
            sa.Text(),
            server_default=sa.text("'KN-' || lpad(nextval('claim_code_seq')::text, 6, '0')"),
            nullable=False,
        ),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=True),
        sa.Column("return_case_id", sa.UUID(), nullable=True),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("counterparty", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="NEW", nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("owner_user_id", sa.UUID(), nullable=True),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deadline_source", sa.Text(), nullable=True),
        sa.Column("platform_claim_ref", sa.Text(), nullable=True),
        sa.Column("recovered_amount", sa.BigInteger(), nullable=True),
        sa.Column("close_reason", sa.Text(), nullable=True),
        sa.Column("due_soon_notified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "counterparty IN ('PLATFORM', 'CARRIER')", name=op.f("ck_claim_counterparty_enum")
        ),
        sa.CheckConstraint(
            "source IN ('AUTO_RETURN', 'MANUAL', 'RECON', 'LEGACY_HOLD')", name=op.f("ck_claim_source_enum")
        ),
        sa.CheckConstraint(
            "status IN ('NEW', 'SUBMITTED', 'WAITING', 'WON', 'LOST', 'CLOSED')",
            name=op.f("ck_claim_status_enum"),
        ),
        sa.CheckConstraint(
            "type IN ('DAMAGED', 'MISSING_ITEM', 'WRONG_ITEM', 'EMPTY_BOX', 'OTHER', 'BUYER_CLAIM', 'LOST_IN_TRANSIT')",
            name=op.f("ck_claim_type_enum"),
        ),
        sa.CheckConstraint("recovered_amount >= 0", name=op.f("ck_claim_recovered_amount_non_negative")),
        sa.ForeignKeyConstraint(["created_by"], ["user.id"], name=op.f("fk_claim_created_by_user")),
        sa.ForeignKeyConstraint(
            ["order_id"], ["order.id"], name=op.f("fk_claim_order_id_order"), ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["user.id"], name=op.f("fk_claim_owner_user_id_user")),
        sa.ForeignKeyConstraint(
            ["package_id"], ["package.id"], name=op.f("fk_claim_package_id_package"), ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["return_case_id"],
            ["return_case.id"],
            name=op.f("fk_claim_return_case_id_return_case"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_claim")),
    )
    op.create_index(op.f("ix_claim_owner_user_id_status"), "claim", ["owner_user_id", "status"], unique=False)
    op.create_index(op.f("ix_claim_package_id"), "claim", ["package_id"], unique=False)
    op.create_index(op.f("ix_claim_status_deadline_at"), "claim", ["status", "deadline_at"], unique=False)
    op.create_index("uq_claim_code", "claim", ["code"], unique=True)
    op.create_index(
        "uq_claim_open_package_type",
        "claim",
        ["package_id", "type"],
        unique=True,
        postgresql_where=sa.text("status <> 'CLOSED' AND source <> 'LEGACY_HOLD'"),
    )
    op.create_table(
        "return_case_package",
        sa.Column("return_case_id", sa.UUID(), nullable=False),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["package.id"],
            name=op.f("fk_return_case_package_package_id_package"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["return_case_id"],
            ["return_case.id"],
            name=op.f("fk_return_case_package_return_case_id_return_case"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("return_case_id", "package_id", name=op.f("pk_return_case_package")),
    )
    op.create_index(
        op.f("ix_return_case_package_package_id"), "return_case_package", ["package_id"], unique=False
    )
    op.create_table(
        "claim_note",
        sa.Column("claim_id", sa.UUID(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("author_user_id", sa.UUID(), nullable=True),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "kind IN ('NOTE', 'STATUS_CHANGE', 'SYSTEM')", name=op.f("ck_claim_note_kind_enum")
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["claim.id"], name=op.f("fk_claim_note_claim_id_claim"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_claim_note")),
    )
    op.create_index(op.f("ix_claim_note_claim_id_at"), "claim_note", ["claim_id", "at"], unique=False)
    op.create_table(
        "evidence_pack",
        sa.Column("claim_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.Text(), server_default="QUEUED", nullable=False),
        sa.Column("progress", sa.Integer(), server_default="0", nullable=False),
        sa.Column("path", sa.Text(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column(
            "missing",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'READY', 'FAILED')", name=op.f("ck_evidence_pack_status_enum")
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["claim.id"], name=op.f("fk_evidence_pack_claim_id_claim"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["created_by"], ["user.id"], name=op.f("fk_evidence_pack_created_by_user")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_evidence_pack")),
    )
    op.create_index(
        op.f("ix_evidence_pack_created_by_created_at"),
        "evidence_pack",
        ["created_by", "created_at"],
        unique=False,
    )
    op.create_index(
        "uq_evidence_pack_active_claim",
        "evidence_pack",
        ["claim_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('QUEUED', 'RUNNING')"),
    )
    op.create_table(
        "inspection_line",
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("order_item_id", sa.UUID(), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("product_name", sa.Text(), nullable=False),
        sa.Column("variation", sa.Text(), nullable=True),
        sa.Column("image_url", sa.Text(), nullable=True),
        sa.Column("quantity_sent", sa.Integer(), nullable=False),
        sa.Column("quantity_requested", sa.Integer(), nullable=False),
        sa.Column("quantity_received", sa.Integer(), nullable=False),
        sa.Column("condition", sa.Text(), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "condition IN ('OK', 'DAMAGED', 'MISSING_ITEM', 'WRONG_ITEM', 'EMPTY_BOX', 'OTHER')",
            name=op.f("ck_inspection_line_condition_enum"),
        ),
        sa.CheckConstraint(
            "quantity_sent BETWEEN 0 AND 999 AND quantity_requested BETWEEN 0 AND 999 AND quantity_received BETWEEN 0 AND 999",
            name=op.f("ck_inspection_line_quantity_range"),
        ),
        sa.ForeignKeyConstraint(
            ["order_item_id"],
            ["order_item.id"],
            name=op.f("fk_inspection_line_order_item_id_order_item"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=op.f("fk_inspection_line_session_id_session"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_inspection_line")),
        sa.UniqueConstraint(
            "session_id", "order_item_id", name=op.f("uq_inspection_line_session_id_order_item_id")
        ),
    )
    op.create_table(
        "recon_alert",
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("rule", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="OPEN", nullable=False),
        sa.Column(
            "context",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("context_key", sa.Text(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution_action", sa.Text(), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("resolved_by", sa.UUID(), nullable=True),
        sa.Column("to_status", sa.Text(), nullable=True),
        sa.Column("claim_id", sa.UUID(), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "resolution_action IN ('RESOLVE', 'ADJUST_STATUS', 'OPEN_CLAIM')",
            name=op.f("ck_recon_alert_resolution_action_enum"),
        ),
        sa.CheckConstraint(
            "rule IN ('SHIPPED_NOT_PACKED', 'CANCELLED_AFTER_PACK', 'RETURN_OVERDUE', 'RETURN_UNANNOUNCED', 'PACKED_NOT_HANDED_OVER', 'RETURN_DONE_NOT_RECEIVED', 'UNVERIFIED_STALE')",
            name=op.f("ck_recon_alert_rule_enum"),
        ),
        sa.CheckConstraint(
            "severity IN ('HIGH', 'MEDIUM', 'LOW')", name=op.f("ck_recon_alert_severity_enum")
        ),
        sa.CheckConstraint(
            "status IN ('OPEN', 'RESOLVED', 'AUTO_RESOLVED')", name=op.f("ck_recon_alert_status_enum")
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["claim.id"], name=op.f("fk_recon_alert_claim_id_claim"), ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["package_id"], ["package.id"], name=op.f("fk_recon_alert_package_id_package"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["resolved_by"], ["user.id"], name=op.f("fk_recon_alert_resolved_by_user")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_recon_alert")),
    )
    op.create_index(
        op.f("ix_recon_alert_package_id_rule_context_key"),
        "recon_alert",
        ["package_id", "rule", "context_key"],
        unique=False,
    )
    op.create_index(
        op.f("ix_recon_alert_status_severity_detected_at"),
        "recon_alert",
        ["status", "severity", "detected_at"],
        unique=False,
    )
    op.create_index(
        "uq_recon_alert_open",
        "recon_alert",
        ["package_id", "rule"],
        unique=True,
        postgresql_where=sa.text("status = 'OPEN'"),
    )
    op.create_table(
        "snapshot",
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("camera_role", sa.Text(), server_default="CAM1", nullable=False),
        sa.Column("taken_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("path", sa.Text(), nullable=True),
        sa.Column("sha256", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("status", sa.Text(), server_default="READY", nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint("camera_role IN ('CAM1', 'CAM2')", name=op.f("ck_snapshot_camera_role_enum")),
        sa.CheckConstraint("kind IN ('MANUAL', 'PACK_CLOSE')", name=op.f("ck_snapshot_kind_enum")),
        sa.CheckConstraint("status IN ('READY', 'DELETED')", name=op.f("ck_snapshot_status_enum")),
        sa.ForeignKeyConstraint(
            ["session_id"], ["session.id"], name=op.f("fk_snapshot_session_id_session"), ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_snapshot")),
    )
    op.create_index(
        "ix_snapshot_retention_candidates",
        "snapshot",
        ["taken_at"],
        unique=False,
        postgresql_where=sa.text("status = 'READY'"),
    )
    op.create_index(op.f("ix_snapshot_session_id"), "snapshot", ["session_id"], unique=False)
    op.create_index(
        "uq_snapshot_pack_close_session",
        "snapshot",
        ["session_id"],
        unique=True,
        postgresql_where=sa.text("kind = 'PACK_CLOSE'"),
    )
    op.create_table(
        "claim_evidence",
        sa.Column("claim_id", sa.UUID(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("session_id", sa.UUID(), nullable=True),
        sa.Column("snapshot_id", sa.UUID(), nullable=True),
        sa.Column("auto", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("added_by", sa.UUID(), nullable=True),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "(kind = 'SESSION') = (session_id IS NOT NULL)",
            name=op.f("ck_claim_evidence_session_matches_kind"),
        ),
        sa.CheckConstraint(
            "(kind = 'SNAPSHOT') = (snapshot_id IS NOT NULL)",
            name=op.f("ck_claim_evidence_snapshot_matches_kind"),
        ),
        sa.CheckConstraint("kind IN ('SESSION', 'SNAPSHOT')", name=op.f("ck_claim_evidence_kind_enum")),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["claim.id"], name=op.f("fk_claim_evidence_claim_id_claim"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["session_id"],
            ["session.id"],
            name=op.f("fk_claim_evidence_session_id_session"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["snapshot.id"],
            name=op.f("fk_claim_evidence_snapshot_id_snapshot"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_claim_evidence")),
        sa.UniqueConstraint("claim_id", "session_id", name=op.f("uq_claim_evidence_claim_id_session_id")),
        sa.UniqueConstraint("claim_id", "snapshot_id", name=op.f("uq_claim_evidence_claim_id_snapshot_id")),
    )
    op.create_index(op.f("ix_claim_evidence_session_id"), "claim_evidence", ["session_id"], unique=False)
    op.create_index(op.f("ix_claim_evidence_snapshot_id"), "claim_evidence", ["snapshot_id"], unique=False)
    op.add_column(
        "package",
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.add_column(
        "package",
        sa.Column(
            "status_changed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )
    op.add_column(
        "package", sa.Column("is_placeholder", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column("session", sa.Column("return_case_id", sa.UUID(), nullable=True))
    op.add_column("session", sa.Column("operator_name", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("inspection_conclusion", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("inspection_note", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("inspection_saved_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("session", sa.Column("inspection_lines_mode", sa.Text(), nullable=True))
    op.add_column(
        "session", sa.Column("inspection_corrections", postgresql.JSONB(astext_type=sa.Text()), nullable=True)
    )
    op.add_column(
        "session", sa.Column("camera_clock", postgresql.JSONB(astext_type=sa.Text()), nullable=True)
    )
    op.create_index(op.f("ix_session_return_case_id"), "session", ["return_case_id"], unique=False)
    op.create_foreign_key(
        op.f("fk_session_return_case_id_return_case"),
        "session",
        "return_case",
        ["return_case_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column(
        "setting", sa.Column("return_warn_minutes", sa.Integer(), server_default="20", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("return_abandon_minutes", sa.Integer(), server_default="45", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("return_missing_days", sa.Integer(), server_default="7", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("handover_warn_hours", sa.Integer(), server_default="24", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("claim_deadline_days", sa.Integer(), server_default="7", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("claim_due_soon_hours", sa.Integer(), server_default="48", nullable=False)
    )
    op.add_column(
        "setting",
        sa.Column(
            "recon_start_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
    )
    op.add_column("shop", sa.Column("last_return_cursor", sa.DateTime(timezone=True), nullable=True))
    op.add_column("station", sa.Column("kind", sa.Text(), server_default="PACK", nullable=False))
    op.add_column("station", sa.Column("work_mode", sa.Text(), server_default="PACK", nullable=False))
    op.add_column("station", sa.Column("operator_name", sa.Text(), nullable=True))

    for table, name, expr in NEW_CHECKS:
        _add_check(table, name, expr)

    _backfill_packages()
    # Sau backfill (G3 M-F2): UPDATE từng dòng không phải cập nhật index này.
    op.create_index(
        op.f("ix_package_warehouse_status_status_changed_at"),
        "package",
        ["warehouse_status", "status_changed_at"],
        unique=False,
    )
    op.execute("UPDATE setting SET recon_start_at = now() WHERE id = 1")
    _raise_retention_to_minimum()
    _restore_phase2(op.get_bind())


# ---------------------------------------------------------------- phase2_archive (T-120)


def _scalar(bind: sa.Connection, sql: str) -> Any:
    return bind.execute(sa.text(sql)).scalar()


def _columns(bind: sa.Connection, schema: str, table: str) -> list[str]:
    return list(
        bind.execute(
            sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
            ),
            {"s": schema, "t": table},
        ).scalars()
    )


def _copy_back(bind: sa.Connection, archived: str, target: str, on_conflict: str = "") -> int:
    """`phase2_archive.<archived>` → `public.<target>` theo các cột chung (đúng tên, không phụ thuộc thứ tự)."""
    available = set(_columns(bind, ARCHIVE, archived))
    cols = ", ".join(f'"{c}"' for c in _columns(bind, "public", target) if c in available)
    result = bind.execute(
        sa.text(f'INSERT INTO "{target}" ({cols}) SELECT {cols} FROM {ARCHIVE}.{archived} {on_conflict}')
    )
    return int(result.rowcount or 0)


ALLOW_UNCUT_ENV = "AICAM_DOWNGRADE_ALLOW_UNCUT_RETURN_CLIPS"


def _guard_no_active_return_session(bind: sa.Connection) -> None:
    """Phiên RETURN đang mở không chuyển được sang code cũ (không có màn / job cho nó) → dừng, không đổi gì.

    Clip `PENDING` / `FAILED` của phiên RETURN: code cũ không giữ video thô cho chúng → J-02 cũ xóa video thô là
    mất bằng chứng chưa cắt → từ chối (G3 M-F4, DEC-338) trừ khi ops đặt `AICAM_DOWNGRADE_ALLOW_UNCUT_RETURN_CLIPS=1`
    (chấp nhận mất)."""
    n = _scalar(
        bind,
        "SELECT count(*) FROM session WHERE type = 'RETURN' AND status IN ('OPEN', 'MISMATCH', 'WAITING_APPROVAL')",
    )
    if n:
        raise RuntimeError(
            f"Không downgrade 0003: còn {n} phiên nhận hàng hoàn đang mở — hoàn tất / hủy ở station (hoặc chờ J-07 "
            "tự đóng) rồi chạy lại; không có gì bị thay đổi. Xem docs/ops.md mục Rollback Phase 2."
        )
    uncut = _scalar(
        bind,
        "SELECT count(*) FROM clip c JOIN session s ON s.id = c.session_id "
        "WHERE s.type = 'RETURN' AND c.status IN ('PENDING', 'FAILED')",
    )
    # Phiên RETURN đã kết thúc mà J-01 chưa tạo dòng clip nào (J-01 tạo dòng mọi vai trong một transaction trước khi cắt):
    # job còn trong hàng đợi / chờ settle → worker cũ bỏ qua nó sau downgrade, mất clip (G5 BUG-G5-P2-2).
    unbuilt = _scalar(
        bind,
        "SELECT count(*) FROM session s WHERE s.type = 'RETURN' AND s.ended_at IS NOT NULL "
        "AND s.status IN ('COMPLETED', 'CANCELLED', 'ABANDONED', 'SUPERSEDED') "
        "AND NOT EXISTS (SELECT 1 FROM clip c WHERE c.session_id = s.id)",
    )
    if (uncut or unbuilt) and os.environ.get(ALLOW_UNCUT_ENV) != "1":
        raise RuntimeError(
            f"Không downgrade 0003: {uncut} clip phiên nhận hàng hoàn chưa cắt được (PENDING / FAILED), {unbuilt} "
            "phiên đã kết thúc chưa được tạo clip (J-01 còn trong hàng đợi) — code cũ không giữ video thô cho chúng. "
            "Chạy worker tới khi hàng đợi `video` rỗng, bấm Thử lại (API-46) và chờ READY rồi chạy lại; chấp nhận mất thì đặt "
            f"{ALLOW_UNCUT_ENV}=1. Không có gì bị thay đổi. Xem docs/ops.md mục Rollback Phase 2."
        )


def _archive_phase2(bind: sa.Connection) -> dict[str, int]:
    """Chép mọi dữ liệu Phase 2 sang `phase2_archive` (chưa xóa gì — xóa sau khi chép đủ, cùng transaction)."""
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {ARCHIVE}")
    for name in ARCHIVE_TABLES_0003:  # lần downgrade trước đã được 0003 khôi phục hết vào bảng chính
        op.execute(f"DROP TABLE IF EXISTS {ARCHIVE}.{name}")
    for table in NEW_TABLES:
        op.execute(f"CREATE TABLE {ARCHIVE}.{table} AS SELECT * FROM public.{table}")
    for name, source, where in ROW_ARCHIVES:
        op.execute(f"CREATE TABLE {ARCHIVE}.{name} AS SELECT * FROM public.{source} WHERE {where}")
    for name, _, select in COLUMN_ARCHIVES:
        op.execute(f"CREATE TABLE {ARCHIVE}.{name} AS {select}")
    # Kiện hoàn → trạng thái cuối không phải hoàn trong lịch sử (không có → DELIVERED) cho code cũ.
    bind.execute(
        sa.text(
            f"UPDATE {ARCHIVE}.package_cols a SET downgraded_to = coalesce(("
            "  SELECT h.to_status FROM status_history h WHERE h.package_id = a.id AND left(h.to_status, 7) <> 'RETURN_'"
            "  ORDER BY h.at DESC, h.id DESC LIMIT 1), CASE WHEN a.is_placeholder THEN 'CANCELLED' ELSE 'DELIVERED' END) "
            "WHERE left(a.warehouse_status, 7) = 'RETURN_'"
        )
    )
    sequences = {
        name: int(
            _scalar(bind, f"SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM {name}")
        )
        for name, _, _, _ in CODE_SEQUENCES
    }
    counts = {
        name: int(_scalar(bind, f"SELECT count(*) FROM {ARCHIVE}.{name}"))
        for name in ARCHIVE_TABLES_0003
        if name != "meta"
    }
    op.execute(f"CREATE TABLE {ARCHIVE}.meta (key text PRIMARY KEY, value jsonb NOT NULL)")
    bind.execute(
        sa.text(
            f"INSERT INTO {ARCHIVE}.meta (key, value) VALUES ('sequences', CAST(:seq AS jsonb)), "
            "('counts', CAST(:counts AS jsonb)), ('downgraded_at', to_jsonb(now()))"
        ),
        {"seq": json.dumps(sequences), "counts": json.dumps(counts)},
    )
    log.info("0003 downgrade: chép sang %s %s", ARCHIVE, json.dumps(counts, ensure_ascii=False))
    unfinished = _scalar(
        bind, f"SELECT count(*) FROM {ARCHIVE}.return_clip WHERE status IN ('PENDING', 'FAILED')"
    )
    if unfinished:  # chỉ tới đây khi ops đặt AICAM_DOWNGRADE_ALLOW_UNCUT_RETURN_CLIPS=1
        log.warning(
            "0003 downgrade: %s clip phiên hoàn chưa cắt được (PENDING / FAILED) — ops chấp nhận mất video thô "
            "của chúng (%s=1)",
            unfinished,
            ALLOW_UNCUT_ENV,
        )
    return counts


def _remove_archived_rows(bind: sa.Connection) -> None:
    """Sau khi gỡ cấu trúc: xóa dòng Phase 2 khỏi bảng Phase 1 (đã chép), kiện hoàn về trạng thái cũ."""
    sessions = f"SELECT id FROM {ARCHIVE}.return_session"
    for table in ("clip", "session_event", "approval_request", "export"):
        op.execute(f"DELETE FROM {table} WHERE session_id IN ({sessions})")
    op.execute(f"DELETE FROM session WHERE id IN ({sessions})")
    op.execute(f"DELETE FROM status_history WHERE id IN (SELECT id FROM {ARCHIVE}.return_status_history)")
    op.execute(
        f"UPDATE package p SET warehouse_status = a.downgraded_to FROM {ARCHIVE}.package_cols a "
        "WHERE a.id = p.id AND a.downgraded_to IS NOT NULL"
    )
    op.execute(
        f"DELETE FROM package p WHERE p.id IN (SELECT id FROM {ARCHIVE}.placeholder_package) "
        "AND NOT EXISTS (SELECT 1 FROM session s WHERE s.package_id = p.id)"
    )
    # Kiện tạm còn phiên không phải RETURN (hiếm — chỉnh tay API-122): giữ lại, trạng thái `CANCELLED` (không
    # vào
    # luồng đóng gói / bàn giao của code cũ; mã `TAM-` tự nói là kiện tạm) thay vì `DELIVERED` giả (G3 M-F9,
    # DEC-338).
    kept = (
        bind.execute(
            sa.text(
                f"SELECT tracking_number FROM package WHERE id IN (SELECT id FROM {ARCHIVE}.placeholder_package)"
            )
        )
        .scalars()
        .all()
    )
    if kept:
        log.warning(
            "0003 downgrade: giữ %s kiện tạm còn phiên đóng gói (CANCELLED): %s", len(kept), ", ".join(kept)
        )


def _guard_placeholder_codes(bind: sa.Connection) -> None:
    """Mã `TAM-` của kiện tạm trong archive đã bị kiện khác (id khác) dùng khi chạy code cũ → dừng với lỗi rõ thay
    vì vi phạm unique giữa chừng (G3 M-F7)."""
    dup = (
        bind.execute(
            sa.text(
                f"SELECT a.tracking_number FROM {ARCHIVE}.placeholder_package a JOIN package p "
                "ON upper(p.tracking_number) = upper(a.tracking_number) AND p.id <> a.id"
            )
        )
        .scalars()
        .all()
    )
    if dup:
        raise RuntimeError(
            f"0003: không khôi phục được {len(dup)} kiện tạm vì mã đã bị kiện khác dùng khi chạy bản cũ: "
            f"{', '.join(sorted(dup)[:10])} — đổi mã kiện kia (hoặc xóa nếu nhập nhầm) rồi chạy lại."
        )


def _restore_phase2(bind: sa.Connection) -> None:
    """Nâng cấp lại sau downgrade: `phase2_archive` → bảng mới / cột mới (DEC-252, DEC-270, DEC-331)."""
    if _scalar(bind, f"SELECT to_regclass('{ARCHIVE}.meta')") is None:
        return
    restored: dict[str, int] = {}
    _guard_placeholder_codes(bind)
    restored["placeholder_package"] = _copy_back(
        bind, "placeholder_package", "package", "ON CONFLICT (id) DO NOTHING"
    )
    # Kiện: code cũ chưa đổi trạng thái từ lúc downgrade → trả trạng thái hoàn + mốc; đã đổi → giữ trạng thái
    # mới.
    changed = bind.execute(
        sa.text(
            f"SELECT count(*) FROM package p JOIN {ARCHIVE}.package_cols a ON a.id = p.id "
            "WHERE a.downgraded_to IS NOT NULL AND NOT a.is_placeholder AND p.warehouse_status <> a.downgraded_to"
        )
    ).scalar()
    bind.execute(
        sa.text(
            f"UPDATE package p SET created_at = a.created_at, is_placeholder = a.is_placeholder, "
            "warehouse_status = CASE WHEN p.warehouse_status = coalesce(a.downgraded_to, a.warehouse_status) "
            "  THEN a.warehouse_status ELSE p.warehouse_status END, "
            "status_changed_at = CASE WHEN p.warehouse_status = coalesce(a.downgraded_to, a.warehouse_status) "
            "  THEN a.status_changed_at ELSE p.status_changed_at END "
            f"FROM {ARCHIVE}.package_cols a WHERE p.id = a.id"
        )
    )
    if changed:
        log.warning(
            "0003: %s kiện hoàn đã đổi trạng thái khi chạy code cũ — giữ trạng thái mới (J-14 đối soát)",
            changed,
        )
    restored["return_status_history"] = _copy_back(
        bind, "return_status_history", "status_history", "ON CONFLICT (id) DO NOTHING"
    )
    order = (
        ("return_case", "return_case"),
        ("return_case_package", "return_case_package"),
        ("return_session", "session"),
        ("return_session_event", "session_event"),
        ("return_clip", "clip"),
        ("return_approval_request", "approval_request"),
        ("return_export", "export"),
        ("inspection_line", "inspection_line"),
        ("snapshot", "snapshot"),
        ("claim", "claim"),
        ("claim_evidence", "claim_evidence"),
        ("claim_note", "claim_note"),
        ("evidence_pack", "evidence_pack"),
        ("recon_alert", "recon_alert"),
    )
    for archived, target in order:
        restored[archived] = _copy_back(bind, archived, target)
    _detach_changed_packages(bind)
    bind.execute(
        sa.text(
            f"UPDATE session s SET ({_SESSION_COLS}) = (SELECT {_SESSION_COLS} FROM {ARCHIVE}.session_cols a "
            f"WHERE a.id = s.id) WHERE s.id IN (SELECT id FROM {ARCHIVE}.session_cols)"
        )
    )
    bind.execute(
        sa.text(
            f"UPDATE station s SET kind = a.kind, work_mode = a.work_mode, operator_name = a.operator_name "
            f"FROM {ARCHIVE}.station_cols a WHERE a.id = s.id"
        )
    )
    bind.execute(
        sa.text(
            f"UPDATE setting s SET ({_SETTING_COLS}) = (SELECT {_SETTING_COLS} FROM {ARCHIVE}.setting_cols a "
            f"WHERE a.id = s.id) WHERE s.id IN (SELECT id FROM {ARCHIVE}.setting_cols)"
        )
    )
    bind.execute(
        sa.text(
            f"UPDATE shop s SET last_return_cursor = a.last_return_cursor FROM {ARCHIVE}.shop_cols a WHERE a.id = s.id"
        )
    )
    _restore_sequences(bind)
    expected = bind.execute(sa.text(f"SELECT value FROM {ARCHIVE}.meta WHERE key = 'counts'")).scalar() or {}
    short = {k: (expected.get(k), v) for k, v in restored.items() if expected.get(k) not in (None, v)}
    log.info("0003: khôi phục từ %s %s", ARCHIVE, json.dumps(restored, ensure_ascii=False))
    if short:
        log.warning("0003: số dòng khôi phục khác lúc chép (chép, khôi phục) %s", short)


_OPEN_CASES = "'EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'MISSING'"
_ACTIVE_PACKAGES = (
    "'RETURN_EXPECTED', 'RETURN_INSPECTING', 'RETURN_MISSING', 'RETURN_RECEIVED_OK', 'RETURN_RECEIVED_ISSUE'"
)


def _detach_changed_packages(bind: sa.Connection) -> None:
    """G3 BB-15 (DEC-338): kiện hoàn mà bản cũ đã đổi trạng thái (vd. xác nhận đã giao) không còn chờ về — gỡ khỏi
    hồ sơ hàng hoàn đang mở; hồ sơ không còn kiện nào trong luồng hoàn → `CANCELLED` (như API-122 → DELIVERED).
    Hồ sơ còn kiện khác: trạng thái hồ sơ được tính lại ở thao tác / J-14 kế tiếp. Log từng hồ sơ."""
    detached = bind.execute(
        sa.text(
            "DELETE FROM return_case_package rcp USING return_case rc, package p, "
            f"{ARCHIVE}.package_cols a WHERE rcp.return_case_id = rc.id AND rc.status IN ({_OPEN_CASES}) "
            "AND p.id = rcp.package_id AND a.id = p.id AND a.downgraded_to IS NOT NULL AND NOT a.is_placeholder "
            "AND p.warehouse_status <> a.warehouse_status RETURNING rc.id, rc.code, p.tracking_number"
        )
    ).all()
    if not detached:
        return
    cancelled = (
        bind.execute(
            sa.text(
                f"UPDATE return_case rc SET status = 'CANCELLED', updated_at = now() WHERE rc.id = ANY(:ids) "
                f"AND rc.status IN ({_OPEN_CASES}) AND NOT EXISTS (SELECT 1 FROM return_case_package x JOIN package p "
                f"ON p.id = x.package_id WHERE x.return_case_id = rc.id AND p.warehouse_status IN ({_ACTIVE_PACKAGES})) "
                "RETURNING rc.code"
            ),
            {"ids": list({r.id for r in detached})},
        )
        .scalars()
        .all()
    )
    log.warning(
        "0003: gỡ %s kiện đã đổi trạng thái khi chạy bản cũ khỏi hồ sơ mở (%s); hủy hồ sơ không còn kiện: %s",
        len(detached),
        ", ".join(f"{r.code}/{r.tracking_number}" for r in detached),
        ", ".join(cancelled) or "-",
    )


def _restore_sequences(bind: sa.Connection) -> None:
    """`setval` = max(giá trị lúc downgrade, mã lớn nhất đang có, kể cả `LEGACY_HOLD` còn trong archive) — mã mới
    không trùng mã đã cấp (R2-6)."""
    saved = bind.execute(sa.text(f"SELECT value FROM {ARCHIVE}.meta WHERE key = 'sequences'")).scalar() or {}
    legacy = _scalar(bind, f"SELECT to_regclass('{ARCHIVE}.legacy_claims')") is not None
    for seq, table, column, prefix in CODE_SEQUENCES:
        sources = [f'SELECT {column} AS code FROM "{table}"']
        if table == "claim" and legacy:
            sources.append(f"SELECT code FROM {ARCHIVE}.legacy_claims")
        top = _scalar(
            bind,
            f"SELECT max(substring(code FROM {len(prefix) + 1})::bigint) FROM ({' UNION ALL '.join(sources)}) x "
            f"WHERE code ~ '^{prefix}[0-9]+$'",
        )
        value = max(int(saved.get(seq) or 0), int(top or 0))
        if value > 0:
            bind.execute(sa.text("SELECT setval(CAST(:seq AS regclass), :v)"), {"seq": seq, "v": value})


def downgrade() -> None:
    bind = op.get_bind()
    _guard_no_active_return_session(bind)
    _archive_phase2(bind)
    for table, name, _ in NEW_CHECKS:
        op.execute(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}')
    op.drop_column("station", "operator_name")
    op.drop_column("station", "work_mode")
    op.drop_column("station", "kind")
    op.drop_column("shop", "last_return_cursor")
    op.drop_column("setting", "recon_start_at")
    op.drop_column("setting", "claim_due_soon_hours")
    op.drop_column("setting", "claim_deadline_days")
    op.drop_column("setting", "handover_warn_hours")
    op.drop_column("setting", "return_missing_days")
    op.drop_column("setting", "return_abandon_minutes")
    op.drop_column("setting", "return_warn_minutes")
    op.drop_constraint(op.f("fk_session_return_case_id_return_case"), "session", type_="foreignkey")
    op.drop_index(op.f("ix_session_return_case_id"), table_name="session")
    op.drop_column("session", "camera_clock")
    op.drop_column("session", "inspection_corrections")
    op.drop_column("session", "inspection_lines_mode")
    op.drop_column("session", "inspection_saved_at")
    op.drop_column("session", "inspection_note")
    op.drop_column("session", "inspection_conclusion")
    op.drop_column("session", "operator_name")
    op.drop_column("session", "return_case_id")
    op.drop_index(op.f("ix_package_warehouse_status_status_changed_at"), table_name="package")
    op.drop_column("package", "is_placeholder")
    op.drop_column("package", "status_changed_at")
    op.drop_column("package", "created_at")
    op.drop_index(op.f("ix_claim_evidence_snapshot_id"), table_name="claim_evidence")
    op.drop_index(op.f("ix_claim_evidence_session_id"), table_name="claim_evidence")
    op.drop_table("claim_evidence")
    op.drop_index(
        "uq_snapshot_pack_close_session",
        table_name="snapshot",
        postgresql_where=sa.text("kind = 'PACK_CLOSE'"),
    )
    op.drop_index(op.f("ix_snapshot_session_id"), table_name="snapshot")
    op.drop_index(
        "ix_snapshot_retention_candidates",
        table_name="snapshot",
        postgresql_where=sa.text("status = 'READY'"),
    )
    op.drop_table("snapshot")
    op.drop_index(
        "uq_recon_alert_open", table_name="recon_alert", postgresql_where=sa.text("status = 'OPEN'")
    )
    op.drop_index(op.f("ix_recon_alert_status_severity_detected_at"), table_name="recon_alert")
    op.drop_index(op.f("ix_recon_alert_package_id_rule_context_key"), table_name="recon_alert")
    op.drop_table("recon_alert")
    op.drop_table("inspection_line")
    op.drop_index(
        "uq_evidence_pack_active_claim",
        table_name="evidence_pack",
        postgresql_where=sa.text("status IN ('QUEUED', 'RUNNING')"),
    )
    op.drop_index(op.f("ix_evidence_pack_created_by_created_at"), table_name="evidence_pack")
    op.drop_table("evidence_pack")
    op.drop_index(op.f("ix_claim_note_claim_id_at"), table_name="claim_note")
    op.drop_table("claim_note")
    op.drop_index(op.f("ix_return_case_package_package_id"), table_name="return_case_package")
    op.drop_table("return_case_package")
    op.drop_index(
        "uq_claim_open_package_type",
        table_name="claim",
        postgresql_where=sa.text("status <> 'CLOSED' AND source <> 'LEGACY_HOLD'"),
    )
    op.drop_index("uq_claim_code", table_name="claim")
    op.drop_index(op.f("ix_claim_status_deadline_at"), table_name="claim")
    op.drop_index(op.f("ix_claim_package_id"), table_name="claim")
    op.drop_index(op.f("ix_claim_owner_user_id_status"), table_name="claim")
    op.drop_table("claim")
    op.drop_index(
        "uq_return_case_platform_return_sn",
        table_name="return_case",
        postgresql_where=sa.text("platform_return_sn IS NOT NULL"),
    )
    op.drop_index(
        "uq_return_case_open_order",
        table_name="return_case",
        postgresql_where=sa.text("status IN ('EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'MISSING')"),
    )
    op.drop_index("uq_return_case_code", table_name="return_case")
    op.drop_index(op.f("ix_return_case_status_expected_since"), table_name="return_case")
    op.drop_index("ix_return_case_signal_keys", table_name="return_case", postgresql_using="gin")
    op.drop_index("ix_return_case_return_tracking_upper", table_name="return_case")
    op.drop_index(op.f("ix_return_case_order_id"), table_name="return_case")
    op.drop_table("return_case")
    _remove_archived_rows(bind)
    for table, name, expr in OLD_CHECKS:
        op.execute(f'ALTER TABLE "{table}" ADD CONSTRAINT {name} CHECK ({expr})')
    for name in SEQUENCES:
        op.execute(f"DROP SEQUENCE IF EXISTS {name}")
