# ruff: noqa: S608 — SQL ghép từ hằng của migration (tên bảng / cột / giá trị trạng thái), không có input ngoài
"""phase3_schema — schema Phase 3 (02a §3 Migration 0006, T-201)

Chỉ thêm (tương thích ngược về schema): 9 bảng mới (`package_order`, `share_link`, `share_item`, `backup_run`,
`backup_object`, `notify_channel`, `notify_event`, `notify_message`, `notify_provider_token`), cột mới nullable / có
default, CHECK mở rộng (tập cũ ⊂ tập mới: `shop.platform` + `TIKTOK`, `clip.status` / `snapshot.status` + `MISSING`)
và CHECK mới trên cột mới. Cả migration là MỘT transaction, `lock_timeout` 5 giây như 0003 (dừng mọi service trước
khi migrate — docs/ops.md §7.2).

Backfill (bước 4 — hằng trong migration, **không** import code ứng dụng):
- `order.platform_status_group` theo bảng Shopee 02 §5.3 (`IN_CANCEL` → `CANCEL_REQUESTED`; chữ lạ / đơn file →
  `UNKNOWN`) — một câu `UPDATE` chỉ chạm dòng có nhóm khác `UNKNOWN` (cột mới có default, không ghi lại cả bảng).
- `return_case.platform_status_group` (Shopee), `return_case.shop_id` ← `order.shop_id`.
- `shop.grant_ref = platform_shop_id` (Shopee — một shop một ủy quyền).
- `claim.submitted_at` / `result_at` từ `audit_log` `CLAIM_UPDATE` (`data.after.status`: lần đầu `SUBMITTED`; lần
  cuối `WON` / `LOST` — DEC-461); hồ sơ không có audit → `updated_at` nếu trạng thái hiện tại tương ứng.
Index trên bảng cũ (báo cáo, tra mã) tạo **sau** backfill (bước 5). Log số dòng từng phần + số đơn `UNKNOWN`.

Đo trên 1 triệu đơn (máy dev): xem `tests/integration/test_perf_migration_0006.py` và docs/ops.md §7.2.

Revision ID: 0006
Revises: 0005
"""

import json
import logging
import time
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: Union[str, Sequence[str], None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

LOCK_TIMEOUT = "5s"

# Bảng Shopee 02 §5.3 — chép hằng (migration không import `platforms/shopee/mapping.py`).
SHOPEE_ORDER_GROUPS: dict[str, tuple[str, ...]] = {
    "UNPAID": ("UNPAID",),
    "AWAITING_SHIPMENT": ("READY_TO_SHIP", "PROCESSED", "RETRY_SHIP"),
    "SHIPPED": ("SHIPPED",),
    "DELIVERED": ("TO_CONFIRM_RECEIVE", "COMPLETED"),
    "CANCEL_REQUESTED": ("IN_CANCEL",),
    "CANCELLED": ("CANCELLED",),
    "RETURNING": ("TO_RETURN",),
}
SHOPEE_RETURN_GROUPS: dict[str, tuple[str, ...]] = {
    "REQUESTED": ("REQUESTED", "JUDGING", "SELLER_DISPUTE"),
    "ACCEPTED": ("PROCESSING", "ACCEPTED"),
    "CANCELLED": ("CANCELLED",),
    "DONE": ("REFUND_PAID",),
    "CLOSED": ("CLOSED",),
}
ORDER_GROUPS = (*SHOPEE_ORDER_GROUPS, "UNKNOWN")
UNKNOWN_SQL = "'UNKNOWN'"
RETURN_GROUPS = tuple(SHOPEE_RETURN_GROUPS)

# (bảng, tên, biểu thức mới, biểu thức cũ | None) — CHECK đổi tập (drop + create) hoặc mới trên cột mới.
CHECKS: tuple[tuple[str, str, str, str | None], ...] = (
    ("shop", "ck_shop_platform_enum", "platform IN ('SHOPEE', 'TIKTOK')", "platform IN ('SHOPEE')"),
    (
        "clip",
        "ck_clip_status_enum",
        "status IN ('PENDING', 'READY', 'FAILED', 'DELETED', 'MISSING')",
        "status IN ('PENDING', 'READY', 'FAILED', 'DELETED')",
    ),
    (
        "snapshot",
        "ck_snapshot_status_enum",
        "status IN ('READY', 'DELETED', 'MISSING')",
        "status IN ('READY', 'DELETED')",
    ),
    (
        "order",
        "ck_order_platform_status_group_enum",
        "platform_status_group IN (" + ", ".join(f"'{g}'" for g in ORDER_GROUPS) + ")",
        None,
    ),
    (
        "return_case",
        "ck_return_case_platform_status_group_enum",
        "platform_status_group IN (" + ", ".join(f"'{g}'" for g in RETURN_GROUPS) + ")",
        None,
    ),
    (
        "claim_evidence",
        "ck_claim_evidence_removed_reason_matches",
        "(removed_at IS NULL) = (removed_reason IS NULL)",
        None,
    ),
    (
        "session",
        "ck_session_cancel_cause_enum",
        "cancel_cause IN ('WRONG_SCAN', 'NOT_A_RETURN', 'OTHER')",
        None,
    ),
    ("session", "ck_session_wrong_scan_code_enum", "wrong_scan_code IN ('WRONG_SCAN', 'NOT_A_RETURN')", None),
    (
        "session",
        "ck_session_wrong_scan_matches_code",
        "(wrong_scan_at IS NULL) = (wrong_scan_code IS NULL)",
        None,
    ),
    (
        "setting",
        "ck_setting_refund_only_default_hours_range",
        "refund_only_default_hours BETWEEN 1 AND 168",
        None,
    ),
    ("setting", "ck_setting_quiet_start_ne_end", "quiet_start <> quiet_end", None),
    ("setting", "ck_setting_backup_upload_mbps_range", "backup_upload_mbps BETWEEN 1 AND 1000", None),
)

NEW_TABLES = (
    "notify_event",
    "notify_provider_token",
    "backup_run",
    "notify_channel",
    "notify_message",
    "package_order",
    "share_link",
    "backup_object",
    "share_item",
)


def _case(column: str, groups: dict[str, tuple[str, ...]], default: str) -> str:
    whens = " ".join(
        f"WHEN {column} IN ({', '.join(repr(s) for s in statuses)}) THEN '{group}'"
        for group, statuses in groups.items()
    )
    return f"CASE {whens} ELSE {default} END"


def _in_list(groups: dict[str, tuple[str, ...]]) -> str:
    return ", ".join(repr(s) for statuses in groups.values() for s in statuses)


def _scalar(bind: sa.Connection, sql: str, params: dict[str, Any] | None = None) -> Any:
    return bind.execute(sa.text(sql), params or {}).scalar()


# ---------------------------------------------------------------- upgrade: cấu trúc


def _create_tables() -> None:
    op.create_table(
        "notify_event",
        sa.Column("code", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "data",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "severity IN ('HIGH', 'MEDIUM', 'INFO')", name=op.f("ck_notify_event_severity_enum")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notify_event")),
        sa.UniqueConstraint("code", "dedupe_key", name=op.f("uq_notify_event_code_dedupe_key")),
    )
    op.create_index(
        "ix_notify_event_unprocessed_occurred_at",
        "notify_event",
        ["occurred_at"],
        unique=False,
        postgresql_where=sa.text("processed_at IS NULL"),
    )
    op.create_table(
        "notify_provider_token",
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("access_token_enc", sa.LargeBinary(), nullable=True),
        sa.Column("refresh_token_enc", sa.LargeBinary(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("provider IN ('ZALO_OA')", name=op.f("ck_notify_provider_token_provider_enum")),
        sa.PrimaryKeyConstraint("provider", name=op.f("pk_notify_provider_token")),
    )
    op.create_table(
        "backup_run",
        sa.Column("kind", sa.Text(), server_default="DB", nullable=False),
        sa.Column("trigger", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="RUNNING", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("object_key", sa.Text(), nullable=True),
        sa.Column("imports_object_key", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=True),
        sa.Column("key_fingerprint", sa.Text(), nullable=True),
        sa.Column("cloud_deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint("kind IN ('DB')", name=op.f("ck_backup_run_kind_enum")),
        sa.CheckConstraint(
            "status IN ('RUNNING', 'SUCCESS', 'FAILED')", name=op.f("ck_backup_run_status_enum")
        ),
        sa.CheckConstraint("trigger IN ('SCHEDULE', 'MANUAL')", name=op.f("ck_backup_run_trigger_enum")),
        sa.ForeignKeyConstraint(["created_by"], ["user.id"], name=op.f("fk_backup_run_created_by_user")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_backup_run")),
    )
    op.create_index(
        "ix_backup_run_key_fingerprint_live",
        "backup_run",
        ["key_fingerprint"],
        unique=False,
        postgresql_where=sa.text("status = 'SUCCESS' AND cloud_deleted_at IS NULL"),
    )
    op.create_index(op.f("ix_backup_run_started_at"), "backup_run", ["started_at"], unique=False)
    op.create_index(
        "uq_backup_run_running_kind",
        "backup_run",
        ["kind"],
        unique=True,
        postgresql_where=sa.text("status = 'RUNNING'"),
    )
    op.create_table(
        "notify_channel",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("target", sa.Text(), nullable=False),
        sa.Column("events", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("last_status", sa.Text(), server_default="NEVER", nullable=False),
        sa.Column("last_sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "last_status IN ('OK', 'ERROR', 'NEVER')", name=op.f("ck_notify_channel_last_status_enum")
        ),
        sa.CheckConstraint("type IN ('TELEGRAM', 'ZALO_OA')", name=op.f("ck_notify_channel_type_enum")),
        sa.CheckConstraint("cardinality(events) >= 1", name=op.f("ck_notify_channel_events_not_empty")),
        sa.ForeignKeyConstraint(["created_by"], ["user.id"], name=op.f("fk_notify_channel_created_by_user")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notify_channel")),
    )
    op.create_index(
        "uq_notify_channel_lower_name", "notify_channel", [sa.literal_column("lower(name)")], unique=True
    )
    op.create_table(
        "notify_message",
        sa.Column("channel_id", sa.UUID(), nullable=False),
        sa.Column("event_code", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="QUEUED", nullable=False),
        sa.Column(
            "items",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("item_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("send_after", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "severity IN ('HIGH', 'MEDIUM', 'INFO')", name=op.f("ck_notify_message_severity_enum")
        ),
        sa.CheckConstraint(
            "status IN ('QUEUED', 'HELD', 'SENT', 'RETRYING', 'DROPPED', 'SKIPPED')",
            name=op.f("ck_notify_message_status_enum"),
        ),
        sa.ForeignKeyConstraint(
            ["channel_id"],
            ["notify_channel.id"],
            name=op.f("fk_notify_message_channel_id_notify_channel"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notify_message")),
    )
    op.create_index(
        op.f("ix_notify_message_channel_id_created_at"),
        "notify_message",
        ["channel_id", "created_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notify_message_channel_id_sent_at"),
        "notify_message",
        ["channel_id", "sent_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_notify_message_status_send_after"), "notify_message", ["status", "send_after"], unique=False
    )
    op.create_table(
        "package_order",
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(
            ["order_id"], ["order.id"], name=op.f("fk_package_order_order_id_order"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["package_id"],
            ["package.id"],
            name=op.f("fk_package_order_package_id_package"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("package_id", "order_id", name=op.f("pk_package_order")),
    )
    op.create_index(op.f("ix_package_order_order_id"), "package_order", ["order_id"], unique=False)
    op.create_table(
        "share_link",
        sa.Column("status", sa.Text(), server_default="CREATING", nullable=False),
        sa.Column("source_type", sa.Text(), nullable=False),
        sa.Column("claim_id", sa.UUID(), nullable=True),
        sa.Column("package_id", sa.UUID(), nullable=False),
        sa.Column("layout", sa.Text(), nullable=False),
        sa.Column("include_snapshots", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("recipient", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("object_prefix", sa.Text(), nullable=False),
        sa.Column("url_enc", sa.LargeBinary(), nullable=True),
        sa.Column(
            "object_keys",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("progress", sa.Integer(), server_default="0", nullable=False),
        sa.Column("step", sa.Text(), nullable=True),
        sa.Column("step_index", sa.Integer(), nullable=True),
        sa.Column("step_total", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_by", sa.UUID(), nullable=True),
        sa.Column("cloud_deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("job_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint("layout IN ('SIDE_BY_SIDE', 'CAM1')", name=op.f("ck_share_link_layout_enum")),
        sa.CheckConstraint(
            "source_type IN ('CLAIM', 'SESSION')", name=op.f("ck_share_link_source_type_enum")
        ),
        sa.CheckConstraint(
            "status IN ('CREATING', 'ACTIVE', 'FAILED', 'REVOKED', 'EXPIRED')",
            name=op.f("ck_share_link_status_enum"),
        ),
        sa.ForeignKeyConstraint(
            ["claim_id"], ["claim.id"], name=op.f("fk_share_link_claim_id_claim"), ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["created_by"], ["user.id"], name=op.f("fk_share_link_created_by_user")),
        sa.ForeignKeyConstraint(
            ["package_id"], ["package.id"], name=op.f("fk_share_link_package_id_package"), ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_share_link")),
    )
    op.create_index(op.f("ix_share_link_claim_id"), "share_link", ["claim_id"], unique=False)
    op.create_index(op.f("ix_share_link_created_at"), "share_link", ["created_at"], unique=False)
    op.create_index(
        op.f("ix_share_link_created_by_created_at"), "share_link", ["created_by", "created_at"], unique=False
    )
    op.create_index(op.f("ix_share_link_package_id"), "share_link", ["package_id"], unique=False)
    op.create_index(
        op.f("ix_share_link_status_expires_at"), "share_link", ["status", "expires_at"], unique=False
    )
    op.create_index("uq_share_link_object_prefix", "share_link", ["object_prefix"], unique=True)
    op.create_table(
        "backup_object",
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("clip_id", sa.UUID(), nullable=True),
        sa.Column("snapshot_id", sa.UUID(), nullable=True),
        sa.Column("run_id", sa.UUID(), nullable=True),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="PENDING", nullable=False),
        sa.Column("sha256", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("encrypted_size", sa.BigInteger(), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cloud_deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("cloud_present", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("cloud_key_fingerprint", sa.Text(), nullable=True),
        sa.Column("hash_override", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("sha256_actual", sa.Text(), nullable=True),
        sa.Column("resolution_action", sa.Text(), nullable=True),
        sa.Column("resolution_note", sa.Text(), nullable=True),
        sa.Column("resolved_by", sa.UUID(), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "kind IN ('DB_DUMP', 'IMPORTS', 'CLIP', 'SNAPSHOT')", name=op.f("ck_backup_object_kind_enum")
        ),
        sa.CheckConstraint("reason IN ('EVIDENCE', 'ALL_PACK')", name=op.f("ck_backup_object_reason_enum")),
        sa.CheckConstraint(
            "resolution_action IN ('UPLOAD_ANYWAY', 'IGNORE', 'RETRY', 'ACCEPT_RESTORED')",
            name=op.f("ck_backup_object_resolution_action_enum"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'UPLOADING', 'UPLOADED', 'FAILED', 'HASH_MISMATCH', 'SOURCE_DELETED', 'IGNORED', 'CLOUD_DELETED')",
            name=op.f("ck_backup_object_status_enum"),
        ),
        sa.CheckConstraint(
            "(resolution_action IS NULL) = (resolved_at IS NULL)",
            name=op.f("ck_backup_object_resolution_matches_at"),
        ),
        sa.CheckConstraint(
            "cloud_present = (cloud_key_fingerprint IS NOT NULL)",
            name=op.f("ck_backup_object_cloud_present_matches_key"),
        ),
        sa.ForeignKeyConstraint(
            ["clip_id"], ["clip.id"], name=op.f("fk_backup_object_clip_id_clip"), ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["backup_run.id"], name=op.f("fk_backup_object_run_id_backup_run"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            ["snapshot.id"],
            name=op.f("fk_backup_object_snapshot_id_snapshot"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_backup_object")),
    )
    op.create_index(
        "ix_backup_object_cloud_key_fingerprint",
        "backup_object",
        ["cloud_key_fingerprint"],
        unique=False,
        postgresql_where=sa.text("cloud_present"),
    )
    op.create_index(op.f("ix_backup_object_run_id"), "backup_object", ["run_id"], unique=False)
    op.create_index(
        op.f("ix_backup_object_status_next_attempt_at"),
        "backup_object",
        ["status", "next_attempt_at"],
        unique=False,
    )
    op.create_index(
        "ix_backup_object_uploading_updated_at",
        "backup_object",
        ["status", "updated_at"],
        unique=False,
        postgresql_where=sa.text("status = 'UPLOADING'"),
    )
    op.create_index(
        "uq_backup_object_clip_id",
        "backup_object",
        ["clip_id"],
        unique=True,
        postgresql_where=sa.text("clip_id IS NOT NULL"),
    )
    op.create_index("uq_backup_object_object_key", "backup_object", ["object_key"], unique=True)
    op.create_index(
        "uq_backup_object_snapshot_id",
        "backup_object",
        ["snapshot_id"],
        unique=True,
        postgresql_where=sa.text("snapshot_id IS NOT NULL"),
    )
    op.create_table(
        "share_item",
        sa.Column("share_id", sa.UUID(), nullable=False),
        sa.Column("ord", sa.SmallInteger(), nullable=False),
        sa.Column("session_id", sa.UUID(), nullable=False),
        sa.Column("video_key", sa.Text(), nullable=True),
        sa.Column("video_sha256", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("source_sha256", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("snapshot_ids", postgresql.ARRAY(sa.UUID()), server_default="{}", nullable=False),
        sa.CheckConstraint("ord BETWEEN 1 AND 4", name=op.f("ck_share_item_ord_range")),
        sa.ForeignKeyConstraint(
            ["session_id"], ["session.id"], name=op.f("fk_share_item_session_id_session"), ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["share_id"],
            ["share_link.id"],
            name=op.f("fk_share_item_share_id_share_link"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("share_id", "ord", name=op.f("pk_share_item")),
    )
    op.create_index(op.f("ix_share_item_session_id"), "share_item", ["session_id"], unique=False)


def _add_columns() -> None:
    op.add_column("claim", sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("claim", sa.Column("result_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("claim_evidence", sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("claim_evidence", sa.Column("removed_by", sa.UUID(), nullable=True))
    op.add_column("claim_evidence", sa.Column("removed_reason", sa.Text(), nullable=True))
    op.add_column(
        "claim_evidence", sa.Column("backfilled", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column(
        "order", sa.Column("platform_status_group", sa.Text(), server_default="UNKNOWN", nullable=False)
    )
    op.add_column("return_case", sa.Column("shop_id", sa.UUID(), nullable=True))
    op.add_column("return_case", sa.Column("platform_status_group", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("cancel_cause", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("wrong_scan_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("session", sa.Column("wrong_scan_by", sa.UUID(), nullable=True))
    op.add_column("session", sa.Column("wrong_scan_code", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("wrong_scan_note", sa.Text(), nullable=True))
    op.add_column("session", sa.Column("review_confirmed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("session", sa.Column("review_confirmed_by", sa.UUID(), nullable=True))
    op.add_column("session", sa.Column("review_confirmed_note", sa.Text(), nullable=True))
    op.add_column(
        "setting", sa.Column("packer_name_required", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("refund_only_default_hours", sa.Integer(), server_default="48", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("quiet_hours_enabled", sa.Boolean(), server_default="true", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("quiet_start", sa.Time(), server_default=sa.text("'22:00'"), nullable=False)
    )
    op.add_column(
        "setting", sa.Column("quiet_end", sa.Time(), server_default=sa.text("'07:00'"), nullable=False)
    )
    op.add_column(
        "setting", sa.Column("backup_enabled", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column("setting", sa.Column("backup_confirmed_fingerprint", sa.Text(), nullable=True))
    op.add_column("setting", sa.Column("backup_confirmed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("setting", sa.Column("backup_confirmed_by", sa.UUID(), nullable=True))
    op.add_column(
        "setting", sa.Column("backup_upload_mbps", sa.Integer(), server_default="10", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("backup_all_pack_clips", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column(
        "setting", sa.Column("backup_restore_pending", sa.Boolean(), server_default="false", nullable=False)
    )
    op.add_column("shop", sa.Column("grant_ref", sa.Text(), nullable=True))
    op.add_column("shop", sa.Column("shop_cipher", sa.Text(), nullable=True))
    op.add_column("shop", sa.Column("region", sa.Text(), nullable=True))
    op.add_column(
        "shop",
        sa.Column(
            "sync_warnings",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    op.add_column("shop", sa.Column("error_since", sa.DateTime(timezone=True), nullable=True))
    op.add_column("shop", sa.Column("disconnected_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("shop", sa.Column("disconnected_by", sa.UUID(), nullable=True))
    op.create_foreign_key(
        op.f("fk_return_case_shop_id_shop"), "return_case", "shop", ["shop_id"], ["id"], ondelete="SET NULL"
    )
    op.create_foreign_key(
        op.f("fk_session_review_confirmed_by_user"),
        "session",
        "user",
        ["review_confirmed_by"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        op.f("fk_session_wrong_scan_by_user"),
        "session",
        "user",
        ["wrong_scan_by"],
        ["id"],
        ondelete="SET NULL",
    )


def _add_checks() -> None:
    for table, name, expr, _old in CHECKS:
        op.execute(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}')
        op.execute(f'ALTER TABLE "{table}" ADD CONSTRAINT {name} CHECK ({expr})')


def _create_indexes() -> None:
    """Bước 5: index trên bảng cũ tạo SAU backfill (UPDATE không phải cập nhật index từng dòng — như 0003)."""
    op.create_index(op.f("ix_claim_created_at"), "claim", ["created_at"], unique=False)
    op.create_index(op.f("ix_claim_result_at"), "claim", ["result_at"], unique=False)
    op.create_index(op.f("ix_claim_submitted_at"), "claim", ["submitted_at"], unique=False)
    op.create_index(
        "ix_claim_evidence_removed_at",
        "claim_evidence",
        ["removed_at"],
        unique=False,
        postgresql_where=sa.text("removed_at IS NOT NULL"),
    )
    op.create_index("ix_order_platform_order_sn", "order", ["platform_order_sn"], unique=False)
    op.create_index(op.f("ix_order_platform_status_group"), "order", ["platform_status_group"], unique=False)
    op.create_index(op.f("ix_order_shop_id"), "order", ["shop_id"], unique=False)
    op.create_index(op.f("ix_return_case_created_at"), "return_case", ["created_at"], unique=False)
    op.create_index(op.f("ix_return_case_received_at"), "return_case", ["received_at"], unique=False)
    op.create_index(op.f("ix_return_case_shop_id"), "return_case", ["shop_id"], unique=False)
    op.create_index("ix_session_type_status_ended", "session", ["type", "status", "ended_at"], unique=False)
    op.create_index(
        "ix_session_wrong_scan_package_id",
        "session",
        ["package_id"],
        unique=False,
        postgresql_where=sa.text("wrong_scan_at IS NOT NULL"),
    )
    op.create_index(op.f("ix_shop_platform_grant_ref"), "shop", ["platform", "grant_ref"], unique=False)
    op.create_index("ix_status_history_to_status_at", "status_history", ["to_status", "at"], unique=False)


# ---------------------------------------------------------------- upgrade: backfill (bước 4)


def _backfill(bind: sa.Connection) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    stats["order_group"] = bind.execute(
        sa.text(
            f'UPDATE "order" SET platform_status_group = {_case("platform_status", SHOPEE_ORDER_GROUPS, UNKNOWN_SQL)} '
            f"WHERE platform_status IN ({_in_list(SHOPEE_ORDER_GROUPS)})"
        )
    ).rowcount
    unknown = bind.execute(
        sa.text(
            "SELECT platform_status, count(*) FROM \"order\" WHERE platform_status_group = 'UNKNOWN' "
            "AND platform_status IS NOT NULL GROUP BY platform_status ORDER BY 2 DESC LIMIT 20"
        )
    ).all()
    stats["order_unknown"] = {str(r[0]): int(r[1]) for r in unknown}
    stats["return_group"] = bind.execute(
        sa.text(
            f"UPDATE return_case SET platform_status_group = {_case('platform_status', SHOPEE_RETURN_GROUPS, 'NULL')} "
            f"WHERE platform_status IN ({_in_list(SHOPEE_RETURN_GROUPS)})"
        )
    ).rowcount
    stats["return_shop"] = bind.execute(
        sa.text(
            'UPDATE return_case rc SET shop_id = o.shop_id FROM "order" o '
            "WHERE o.id = rc.order_id AND o.shop_id IS NOT NULL AND rc.shop_id IS NULL"
        )
    ).rowcount
    stats["shop_grant_ref"] = bind.execute(
        sa.text(
            "UPDATE shop SET grant_ref = platform_shop_id WHERE platform = 'SHOPEE' AND grant_ref IS NULL"
        )
    ).rowcount
    stats.update(_backfill_claim_times(bind))
    return stats


def _backfill_claim_times(bind: sa.Connection) -> dict[str, int]:
    """DEC-461: `submitted_at` = lần đầu sang `SUBMITTED`; `result_at` = lần cuối sang `WON` / `LOST` (audit
    `CLAIM_UPDATE` ghi `data.after` = ảnh chụp hồ sơ sau khi đổi). Không có audit → `updated_at` nếu trạng thái hiện
    tại đúng là trạng thái đó."""
    submitted = bind.execute(
        sa.text(
            "UPDATE claim c SET submitted_at = a.at FROM ("
            "  SELECT object_id, min(at) AS at FROM audit_log WHERE action = 'CLAIM_UPDATE' "
            "  AND object_type = 'CLAIM' AND data -> 'after' ->> 'status' = 'SUBMITTED' GROUP BY object_id"
            ") a WHERE a.object_id = c.id::text AND c.submitted_at IS NULL"
        )
    ).rowcount
    result = bind.execute(
        sa.text(
            "UPDATE claim c SET result_at = a.at FROM ("
            "  SELECT object_id, max(at) AS at FROM audit_log WHERE action = 'CLAIM_UPDATE' "
            "  AND object_type = 'CLAIM' AND data -> 'after' ->> 'status' IN ('WON', 'LOST') GROUP BY object_id"
            ") a WHERE a.object_id = c.id::text AND c.result_at IS NULL"
        )
    ).rowcount
    submitted_fallback = bind.execute(
        sa.text(
            "UPDATE claim SET submitted_at = updated_at WHERE status = 'SUBMITTED' AND submitted_at IS NULL"
        )
    ).rowcount
    result_fallback = bind.execute(
        sa.text(
            "UPDATE claim SET result_at = updated_at WHERE status IN ('WON', 'LOST') AND result_at IS NULL"
        )
    ).rowcount
    return {
        "claim_submitted_audit": int(submitted or 0),
        "claim_result_audit": int(result or 0),
        "claim_submitted_fallback": int(submitted_fallback or 0),
        "claim_result_fallback": int(result_fallback or 0),
    }


def upgrade() -> None:
    bind = op.get_bind()
    # Chờ khóa tối đa 5 giây (service chưa dừng → lỗi rõ, cả migration lùi) — như 0003 (G3 M-F2).
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    started = time.monotonic()
    _create_tables()
    _add_columns()
    _add_checks()
    stats = _backfill(bind)
    _create_indexes()
    stats["seconds"] = round(time.monotonic() - started, 1)
    log.info("0006: backfill %s", json.dumps(stats, ensure_ascii=False))
    if stats["order_unknown"]:
        log.warning(
            "0006: đơn có trạng thái sàn chưa ánh xạ → nhóm UNKNOWN (không đổi trạng thái kho): %s",
            json.dumps(stats["order_unknown"], ensure_ascii=False),
        )


# ---------------------------------------------------------------- downgrade


def downgrade() -> None:
    op.drop_index("ix_status_history_to_status_at", table_name="status_history")
    op.drop_index(op.f("ix_shop_platform_grant_ref"), table_name="shop")
    op.drop_column("shop", "disconnected_by")
    op.drop_column("shop", "disconnected_at")
    op.drop_column("shop", "error_since")
    op.drop_column("shop", "sync_warnings")
    op.drop_column("shop", "region")
    op.drop_column("shop", "shop_cipher")
    op.drop_column("shop", "grant_ref")
    op.drop_column("setting", "backup_restore_pending")
    op.drop_column("setting", "backup_all_pack_clips")
    op.drop_column("setting", "backup_upload_mbps")
    op.drop_column("setting", "backup_confirmed_by")
    op.drop_column("setting", "backup_confirmed_at")
    op.drop_column("setting", "backup_confirmed_fingerprint")
    op.drop_column("setting", "backup_enabled")
    op.drop_column("setting", "quiet_end")
    op.drop_column("setting", "quiet_start")
    op.drop_column("setting", "quiet_hours_enabled")
    op.drop_column("setting", "refund_only_default_hours")
    op.drop_column("setting", "packer_name_required")
    op.drop_constraint(op.f("fk_session_wrong_scan_by_user"), "session", type_="foreignkey")
    op.drop_constraint(op.f("fk_session_review_confirmed_by_user"), "session", type_="foreignkey")
    op.drop_index(
        "ix_session_wrong_scan_package_id",
        table_name="session",
        postgresql_where=sa.text("wrong_scan_at IS NOT NULL"),
    )
    op.drop_index("ix_session_type_status_ended", table_name="session")
    op.drop_column("session", "review_confirmed_note")
    op.drop_column("session", "review_confirmed_by")
    op.drop_column("session", "review_confirmed_at")
    op.drop_column("session", "wrong_scan_note")
    op.drop_column("session", "wrong_scan_code")
    op.drop_column("session", "wrong_scan_by")
    op.drop_column("session", "wrong_scan_at")
    op.drop_column("session", "cancel_cause")
    op.drop_constraint(op.f("fk_return_case_shop_id_shop"), "return_case", type_="foreignkey")
    op.drop_index(op.f("ix_return_case_shop_id"), table_name="return_case")
    op.drop_index(op.f("ix_return_case_received_at"), table_name="return_case")
    op.drop_index(op.f("ix_return_case_created_at"), table_name="return_case")
    op.drop_column("return_case", "platform_status_group")
    op.drop_column("return_case", "shop_id")
    op.drop_index(op.f("ix_order_shop_id"), table_name="order")
    op.drop_index(op.f("ix_order_platform_status_group"), table_name="order")
    op.drop_index("ix_order_platform_order_sn", table_name="order")
    op.drop_column("order", "platform_status_group")
    op.drop_index(
        "ix_claim_evidence_removed_at",
        table_name="claim_evidence",
        postgresql_where=sa.text("removed_at IS NOT NULL"),
    )
    op.drop_column("claim_evidence", "backfilled")
    op.drop_column("claim_evidence", "removed_reason")
    op.drop_column("claim_evidence", "removed_by")
    op.drop_column("claim_evidence", "removed_at")
    op.drop_index(op.f("ix_claim_submitted_at"), table_name="claim")
    op.drop_index(op.f("ix_claim_result_at"), table_name="claim")
    op.drop_index(op.f("ix_claim_created_at"), table_name="claim")
    op.drop_column("claim", "result_at")
    op.drop_column("claim", "submitted_at")
    for table, name, _expr, old in reversed(CHECKS):
        op.execute(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS {name}')
        if old is not None:
            op.execute(f'ALTER TABLE "{table}" ADD CONSTRAINT {name} CHECK ({old})')
    op.drop_index(op.f("ix_share_item_session_id"), table_name="share_item")
    op.drop_table("share_item")
    op.drop_index(
        "uq_backup_object_snapshot_id",
        table_name="backup_object",
        postgresql_where=sa.text("snapshot_id IS NOT NULL"),
    )
    op.drop_index("uq_backup_object_object_key", table_name="backup_object")
    op.drop_index(
        "uq_backup_object_clip_id",
        table_name="backup_object",
        postgresql_where=sa.text("clip_id IS NOT NULL"),
    )
    op.drop_index(
        "ix_backup_object_uploading_updated_at",
        table_name="backup_object",
        postgresql_where=sa.text("status = 'UPLOADING'"),
    )
    op.drop_index(op.f("ix_backup_object_status_next_attempt_at"), table_name="backup_object")
    op.drop_index(op.f("ix_backup_object_run_id"), table_name="backup_object")
    op.drop_index(
        "ix_backup_object_cloud_key_fingerprint",
        table_name="backup_object",
        postgresql_where=sa.text("cloud_present"),
    )
    op.drop_table("backup_object")
    op.drop_index("uq_share_link_object_prefix", table_name="share_link")
    op.drop_index(op.f("ix_share_link_status_expires_at"), table_name="share_link")
    op.drop_index(op.f("ix_share_link_package_id"), table_name="share_link")
    op.drop_index(op.f("ix_share_link_created_by_created_at"), table_name="share_link")
    op.drop_index(op.f("ix_share_link_created_at"), table_name="share_link")
    op.drop_index(op.f("ix_share_link_claim_id"), table_name="share_link")
    op.drop_table("share_link")
    op.drop_index(op.f("ix_package_order_order_id"), table_name="package_order")
    op.drop_table("package_order")
    op.drop_index(op.f("ix_notify_message_status_send_after"), table_name="notify_message")
    op.drop_index(op.f("ix_notify_message_channel_id_sent_at"), table_name="notify_message")
    op.drop_index(op.f("ix_notify_message_channel_id_created_at"), table_name="notify_message")
    op.drop_table("notify_message")
    op.drop_index("uq_notify_channel_lower_name", table_name="notify_channel")
    op.drop_table("notify_channel")
    op.drop_index(
        "uq_backup_run_running_kind", table_name="backup_run", postgresql_where=sa.text("status = 'RUNNING'")
    )
    op.drop_index(op.f("ix_backup_run_started_at"), table_name="backup_run")
    op.drop_index(
        "ix_backup_run_key_fingerprint_live",
        table_name="backup_run",
        postgresql_where=sa.text("status = 'SUCCESS' AND cloud_deleted_at IS NULL"),
    )
    op.drop_table("backup_run")
    op.drop_table("notify_provider_token")
    op.drop_index(
        "ix_notify_event_unprocessed_occurred_at",
        table_name="notify_event",
        postgresql_where=sa.text("processed_at IS NULL"),
    )
    op.drop_table("notify_event")
