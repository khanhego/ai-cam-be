"""returns_recon_claims — schema Phase 2 (02a §3, T-101)

Chỉ thêm (tương thích ngược — code Phase 1 vẫn chạy): 3 sequence, 9 bảng mới, cột mới nullable / có default,
CHECK mở rộng bằng `ADD CONSTRAINT … NOT VALID` + `VALIDATE CONSTRAINT` (RB-23; tập cũ ⊂ tập mới).
Backfill `package.created_at` / `status_changed_at` theo lô 5.000 dòng (DEC-225); `setting.recon_start_at = now()`
(DEC-228); `retention_clip_days` < `RETENTION_CLIP_MIN_DAYS` → nâng lên sàn + audit (DEC-257).

Downgrade (T-101, DEC-301): chỉ khi DB **chưa có dữ liệu Phase 2** (guard) — gỡ cấu trúc, CHECK về tập cũ.
Có dữ liệu Phase 2 → dừng; downgrade chuyển dữ liệu sang `phase2_archive` (DEC-252, DEC-270) làm ở T-120.

Revision ID: 0003
Revises: 0002
"""

import os
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, Sequence[str], None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

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

# Bảng / dòng mang dữ liệu Phase 2: có thì downgrade cấu trúc không được chạy (mất bằng chứng).
PHASE2_DATA_GUARDS: tuple[tuple[str, str], ...] = (
    ("return_case", "SELECT count(*) FROM return_case"),
    ("claim", "SELECT count(*) FROM claim"),
    ("recon_alert", "SELECT count(*) FROM recon_alert"),
    ("snapshot", "SELECT count(*) FROM snapshot"),
    ("inspection_line", "SELECT count(*) FROM inspection_line"),
    ("evidence_pack", "SELECT count(*) FROM evidence_pack"),
    (
        "session RETURN",
        "SELECT count(*) FROM session WHERE type = 'RETURN' OR cancel_reason = 'NOT_A_RETURN'",
    ),
    (
        "package RETURN_* / kiện tạm",
        "SELECT count(*) FROM package WHERE warehouse_status LIKE 'RETURN%' OR is_placeholder",
    ),
)


def _retention_min_days() -> int:
    """`RETENTION_CLIP_MIN_DAYS` (02a §9) — đọc env trực tiếp, migration không phụ thuộc code ứng dụng."""
    return int(os.environ.get("RETENTION_CLIP_MIN_DAYS", "60"))


def _add_check(table: str, name: str, expr: str) -> None:
    op.execute(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}')
    op.execute(f'ALTER TABLE "{table}" ADD CONSTRAINT {name} CHECK ({expr}) NOT VALID')
    op.execute(f'ALTER TABLE "{table}" VALIDATE CONSTRAINT {name}')


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


def upgrade() -> None:
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
    op.create_index(
        op.f("ix_package_warehouse_status_status_changed_at"),
        "package",
        ["warehouse_status", "status_changed_at"],
        unique=False,
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
    op.execute("UPDATE setting SET recon_start_at = now() WHERE id = 1")
    _raise_retention_to_minimum()


def _guard_no_phase2_data() -> None:
    bind = op.get_bind()
    found = {label: n for label, sql in PHASE2_DATA_GUARDS if (n := bind.execute(sa.text(sql)).scalar())}
    if found:
        raise RuntimeError(
            "Không downgrade 0003: DB đã có dữ liệu Phase 2 "
            f"({', '.join(f'{k}={v}' for k, v in found.items())}). Downgrade giữ dữ liệu sang phase2_archive "
            "(DEC-252) chưa có — xem docs/ops.md (T-120)."
        )


def downgrade() -> None:
    _guard_no_phase2_data()
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
    for table, name, expr in OLD_CHECKS:
        op.execute(f'ALTER TABLE "{table}" ADD CONSTRAINT {name} CHECK ({expr})')
    for name in SEQUENCES:
        op.execute(f"DROP SEQUENCE IF EXISTS {name}")
