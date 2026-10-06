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
import os
import time
import uuid
from typing import Any, Sequence, Union
from zoneinfo import ZoneInfo

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


# Thứ tự chép lại khi nâng cấp lại (theo khóa ngoại).
RESTORE_ORDER = (
    "notify_channel",
    "notify_event",
    "notify_message",
    "notify_provider_token",
    "backup_run",
    "backup_object",
    "share_link",
    "share_item",
    "package_order",
)

ARCHIVE = "phase3_archive"
ALLOW_ACTIVE_SHARES_ENV = "AICAM_DOWNGRADE_ALLOW_ACTIVE_SHARES"
_SESSION_COLS = (
    "cancel_cause, wrong_scan_at, wrong_scan_by, wrong_scan_code, wrong_scan_note, review_confirmed_at, "
    "review_confirmed_by, review_confirmed_note"
)
_SETTING_COLS = (
    "packer_name_required, refund_only_default_hours, quiet_hours_enabled, quiet_start, quiet_end, backup_enabled, "
    "backup_confirmed_fingerprint, backup_confirmed_at, backup_confirmed_by, backup_upload_mbps, "
    "backup_all_pack_clips, backup_restore_pending"
)
_SHOP_COLS = "grant_ref, shop_cipher, region, sync_warnings, error_since, disconnected_at, disconnected_by"
_EVIDENCE_COLS = (
    "id, claim_id, kind, session_id, snapshot_id, auto, added_by, added_at, removed_at, removed_by, removed_reason, "
    "backfilled"
)
ALLOW_DETACH_ENV = "AICAM_DOWNGRADE_DETACH_FOREIGN_ORDERS"
# Người dùng hệ thống đứng tên hồ sơ `LEGACY_HOLD` do lùi tạo (DEC-338 — cùng id với 0004, không đăng nhập được).
SYSTEM_USER_ID = "00000000-0000-7000-8000-00000000a1c0"
SYSTEM_USER_NAME = "Hệ thống (bảo vệ bằng chứng Phase 2)"
# BR-39 (DEC-491): lý do hủy loại phiên khỏi bằng chứng tự chọn — hằng như `claims.EXCLUDED_CANCEL_REASONS`.
EXCLUDED_CANCEL_REASONS = ("WRONG_SCAN", "NOT_A_RETURN")
# Shop Shopee còn kết nối trừ shop `CONNECTED` mới nhất — Phase 2 chỉ dùng một shop (DEC-12) → bị ngắt khi lùi.
_EXTRA_SHOPEE_SQL = (
    "SELECT id FROM shop WHERE platform = 'SHOPEE' AND auth_status <> 'DISCONNECTED' AND id <> COALESCE(("
    "  SELECT id FROM shop WHERE platform = 'SHOPEE' AND auth_status = 'CONNECTED' "
    "  ORDER BY created_at DESC, id DESC LIMIT 1), '00000000-0000-0000-0000-000000000000')"
)
# Đơn ngoài (DEC-509): đơn của shop TikTok + đơn của shop Shopee sẽ bị ngắt — J-06 Phase 2 không lọc shop.
_FOREIGN_ORDERS_SQL = (
    'SELECT o.id FROM "order" o JOIN shop s ON s.id = o.shop_id '
    f"WHERE s.platform = 'TIKTOK' OR s.id IN ({_EXTRA_SHOPEE_SQL})"
)
# Cột mới của bảng cũ (khóa `id`) — mất khi drop cột nếu không chép: (bảng archive, nguồn, SELECT).
COLUMN_ARCHIVES: tuple[tuple[str, str, str], ...] = (
    ("shop_cols", "shop", f"SELECT id, {_SHOP_COLS} FROM shop WHERE platform = 'SHOPEE'"),
    # Nhóm chỉ trả lại khi chữ trạng thái chưa đổi lúc chạy Phase 2 (đã đổi → nhóm backfill lại theo chữ mới).
    (
        "order_cols",
        "order",
        "SELECT id, platform_status, platform_status_group FROM \"order\" WHERE platform_status_group <> 'UNKNOWN'",
    ),
    (
        "return_case_cols",
        "return_case",
        "SELECT id, shop_id, platform_status, platform_status_group FROM return_case "
        "WHERE shop_id IS NOT NULL OR platform_status_group IS NOT NULL",
    ),
    (
        "claim_cols",
        "claim",
        "SELECT id, submitted_at, result_at FROM claim WHERE num_nonnulls(submitted_at, result_at) > 0",
    ),
    # Nguyên dòng (dòng đã bỏ bị xóa ở bước 5 — nâng cấp lại chèn lại theo cặp; dòng 4b giữ cờ `backfilled`).
    (
        "claim_evidence_cols",
        "claim_evidence",
        f"SELECT {_EVIDENCE_COLS} FROM claim_evidence WHERE removed_at IS NOT NULL OR backfilled",
    ),
    (
        "session_cols",
        "session",
        f"SELECT id, {_SESSION_COLS} FROM session WHERE num_nonnulls({_SESSION_COLS}) > 0",
    ),
    ("setting_cols", "setting", f"SELECT id, {_SETTING_COLS} FROM setting"),
)
# Dòng / quan hệ cần trả lại khi nâng cấp lại.
ROW_ARCHIVES: tuple[tuple[str, str], ...] = (
    ("tiktok_shops", "SELECT * FROM shop WHERE platform = 'TIKTOK'"),
    (
        "order_shop",
        "SELECT o.id, o.shop_id FROM \"order\" o JOIN shop s ON s.id = o.shop_id WHERE s.platform = 'TIKTOK'",
    ),
    # Cặp do backfill 4b thêm — nâng cấp lại không thêm lại cặp người dùng đã bỏ khi chạy Phase 2 (DEC-498).
    ("backfill_prior_pairs", "SELECT claim_id, session_id FROM claim_evidence WHERE backfilled"),
    ("missing_clips", "SELECT id FROM clip WHERE status = 'MISSING'"),
    ("missing_snapshots", "SELECT id, deleted_at FROM snapshot WHERE status = 'MISSING'"),  # v0.3 (DEC-524)
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


def _excluded_session_sql(alias: str) -> str:
    """Vị từ "phiên bị loại khỏi bằng chứng tự chọn" (BR-39 v0.4, DEC-514, 515, 521) — hằng trong migration,
    cùng luật `sessions.queries.excluded_return_sql`: lý do hiệu lực (`cancel_cause` của Supervisor, không thì
    `cancel_reason` của station) ∈ lý do loại, hoặc đã đánh dấu quét nhầm."""
    reasons = ", ".join(f"'{r}'" for r in EXCLUDED_CANCEL_REASONS)
    return (
        f"(COALESCE({alias}.cancel_cause, {alias}.cancel_reason, '') IN ({reasons}) "
        f"OR {alias}.wrong_scan_at IS NOT NULL)"
    )


def _review_needed_sql(alias: str) -> str:
    """Phiên Supervisor hủy trước Phase 3 (không mã lý do) — vào bằng chứng nhưng "Cần soát" (DEC-516)."""
    return (
        f"({alias}.cancel_reason = 'SUPERVISOR' AND {alias}.cancel_cause IS NULL "
        f"AND {alias}.review_confirmed_at IS NULL AND {alias}.wrong_scan_at IS NULL)"
    )


def _archive_exists(bind: sa.Connection) -> bool:
    return _scalar(bind, f"SELECT to_regclass('{ARCHIVE}.meta')") is not None


def _backfill_prior_sessions(bind: sa.Connection) -> dict[str, int]:
    """Bước 4b (BR-39 v0.3, DEC-498): hồ sơ khiếu nại mở từ Phase 2 thêm phiên mở hoàn đã hủy / bỏ dở có clip
    của kiện / hồ sơ hàng hoàn (trừ phiên bị loại) — `auto = true`, `backfilled = true`, `ON CONFLICT DO
    NOTHING`. Nâng cấp lại: bỏ qua cặp (hồ sơ, phiên) do 4b lần trước thêm mà người dùng đã bỏ khi chạy Phase 2.
    Audit `CLAIM_EVIDENCE_UPDATE` một dòng / hồ sơ (`reason = BACKFILL_BR39`, người dùng null); `version + 1`."""
    skip = "true"
    if _archive_exists(bind):
        skip = (
            f"NOT EXISTS (SELECT 1 FROM {ARCHIVE}.backfill_prior_pairs b WHERE b.claim_id = c.id "
            "AND b.session_id = s.id)"
        )
    rows = bind.execute(
        sa.text(
            "WITH added AS ("
            "  INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_by, added_at, backfilled) "
            "  SELECT gen_random_uuid(), c.id, 'SESSION', s.id, true, NULL, now(), true "
            "  FROM claim c JOIN session s ON s.type = 'RETURN' AND s.status IN ('CANCELLED', 'ABANDONED') AND ("
            "    s.package_id = c.package_id"
            "    OR (c.return_case_id IS NOT NULL AND s.return_case_id = c.return_case_id)"
            "    OR s.package_id IN (SELECT rcp.package_id FROM return_case_package rcp "
            "                        WHERE rcp.return_case_id = c.return_case_id))"
            "  WHERE c.status <> 'CLOSED' AND c.source <> 'LEGACY_HOLD'"
            "    AND EXISTS (SELECT 1 FROM clip k WHERE k.session_id = s.id AND k.status <> 'DELETED')"
            f"   AND NOT ({_excluded_session_sql('s')}) AND {skip}"
            "  ON CONFLICT (claim_id, session_id) DO NOTHING"
            "  RETURNING claim_id, session_id"
            "), logged AS ("
            "  SELECT a.claim_id, a.session_id, c.code, s.started_at, "
            f"   {_review_needed_sql('s')} AS review_needed "
            "  FROM added a JOIN claim c ON c.id = a.claim_id JOIN session s ON s.id = a.session_id"
            ") SELECT claim_id, array_agg(session_id::text ORDER BY session_id), "
            "  COALESCE(jsonb_agg(jsonb_build_object('claim_code', code, 'session_id', session_id, "
            "    'started_at', started_at) ORDER BY started_at) FILTER (WHERE review_needed), '[]'::jsonb) "
            "FROM logged GROUP BY claim_id"
        )
    ).all()
    review = [item for r in rows for item in r[2]]
    if review:
        # v0.3 (DEC-516): CSKH soát — phiên vào bằng chứng nhưng không bao giờ là phiên chính tới khi xác nhận.
        log.warning("backfill_review_needed %s", json.dumps(review, ensure_ascii=False, default=str))
    for claim_id, session_ids, _ in rows:
        bind.execute(
            sa.text(
                "INSERT INTO audit_log (user_id, action, object_type, object_id, at, data) VALUES "
                "(NULL, 'CLAIM_EVIDENCE_UPDATE', 'CLAIM', :id, now(), jsonb_build_object('reason', 'BACKFILL_BR39', "
                "'session_ids', CAST(:sids AS jsonb)))"
            ),
            {"id": str(claim_id), "sids": json.dumps(list(session_ids))},
        )
    if rows:
        bind.execute(
            sa.text("UPDATE claim SET version = version + 1 WHERE id = ANY(:ids)"),
            {"ids": [r[0] for r in rows]},
        )
    return {
        "prior_rows": sum(len(r[1]) for r in rows),
        "prior_claims": len(rows),
        "prior_review_needed": len(review),
    }


def _log_cancel_candidates(bind: sa.Connection) -> int:
    """(4c) v0.3 (DEC-519): kiện bị hủy oan khi Phase 2 coi `IN_CANCEL` là hủy — chỉ đếm + log (sửa cần
    `transition` + audit của code: ops chạy `aicam fix-cancel-requests`, docs/ops.md §7.2)."""
    rows = bind.execute(
        sa.text(
            "SELECT p.tracking_number, p.warehouse_status, o.platform_order_sn, o.platform_status_group "
            'FROM package p JOIN "order" o ON o.id = p.order_id '
            "WHERE p.warehouse_status IN ('CANCELLED', 'CANCELLED_AFTER_PACK') "
            "AND o.platform_status_group NOT IN ('CANCELLED', 'UNKNOWN') ORDER BY p.tracking_number"
        )
    ).all()
    if rows:
        log.warning(
            "0006: %s kiện có thể bị hủy oan (đơn nay không ở nhóm Đã hủy) — chạy `aicam fix-cancel-requests` "
            "(dry-run rồi --apply): %s",
            len(rows),
            ", ".join(f"{r[0]} ({r[1]}, đơn {r[2]} {r[3]})" for r in rows[:50]),
        )
    return len(rows)


# ---------------------------------------------------------------- nâng cấp lại (bước 6) — phase3_archive


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


def _copy_back(bind: sa.Connection, archived: str, target: str, where: str = "true", suffix: str = "") -> int:
    """`phase3_archive.<archived>` → `public.<target>` theo các cột chung (đúng tên, không phụ thuộc thứ tự)."""
    available = set(_columns(bind, ARCHIVE, archived))
    cols = ", ".join(f'"{c}"' for c in _columns(bind, "public", target) if c in available)
    result = bind.execute(
        sa.text(
            f'INSERT INTO "{target}" ({cols}) SELECT {cols} FROM {ARCHIVE}.{archived} a WHERE {where} {suffix}'
        )
    )
    return int(result.rowcount or 0)


def _update_from(bind: sa.Connection, archived: str, target: str, cols: str, where: str = "true") -> int:
    names = [c.strip() for c in cols.split(",")]
    sets = ", ".join(f"{c} = a.{c}" for c in names)
    result = bind.execute(
        sa.text(f'UPDATE "{target}" t SET {sets} FROM {ARCHIVE}.{archived} a WHERE a.id = t.id AND ({where})')
    )
    return int(result.rowcount or 0)


def _restore_phase3(bind: sa.Connection) -> dict[str, int]:
    """Nâng cấp lại sau downgrade (02a §3 bước 6, như DEC-331): archive → shop, cột, bảng; rồi drop schema."""
    if _scalar(bind, f"SELECT to_regclass('{ARCHIVE}.meta')") is None:
        return {}
    out: dict[str, int] = {}
    out["tiktok_shops"] = _copy_back(bind, "tiktok_shops", "shop", suffix="ON CONFLICT (id) DO NOTHING")
    out["shop_cols"] = _update_from(bind, "shop_cols", "shop", _SHOP_COLS)
    # Shop Shopee bị ngắt lúc lùi (Phase 2 chỉ một shop): còn ngắt + token chưa đổi → trả trạng thái cũ.
    out["reconnected_shops"] = int(
        bind.execute(
            sa.text(
                f"UPDATE shop s SET auth_status = a.auth_status FROM {ARCHIVE}.reconnected_shops a "
                "WHERE a.id = s.id AND s.auth_status = 'DISCONNECTED' "
                "AND s.access_token_enc IS NOT DISTINCT FROM a.access_token_enc"
            )
        ).rowcount
        or 0
    )
    out["order_shop"] = int(
        bind.execute(
            sa.text(
                f'UPDATE "order" o SET shop_id = a.shop_id FROM {ARCHIVE}.order_shop a '
                "WHERE a.id = o.id AND o.shop_id IS NULL AND EXISTS (SELECT 1 FROM shop s WHERE s.id = a.shop_id)"
            )
        ).rowcount
        or 0
    )
    out["order_cols"] = _update_from(
        bind,
        "order_cols",
        "order",
        "platform_status_group",
        "t.platform_status IS NOT DISTINCT FROM a.platform_status",
    )
    out["return_case_cols"] = int(
        bind.execute(
            sa.text(
                f"UPDATE return_case t SET shop_id = COALESCE(t.shop_id, a.shop_id), "
                "platform_status_group = CASE WHEN t.platform_status IS NOT DISTINCT FROM a.platform_status "
                "THEN a.platform_status_group ELSE t.platform_status_group END "
                f"FROM {ARCHIVE}.return_case_cols a WHERE a.id = t.id"
            )
        ).rowcount
        or 0
    )
    out["claim_cols"] = int(
        bind.execute(
            sa.text(
                "UPDATE claim t SET submitted_at = COALESCE(a.submitted_at, t.submitted_at), "
                f"result_at = COALESCE(a.result_at, t.result_at) FROM {ARCHIVE}.claim_cols a WHERE a.id = t.id"
            )
        ).rowcount
        or 0
    )
    out["claim_evidence_cols"] = _update_from(
        bind, "claim_evidence_cols", "claim_evidence", "removed_at, removed_by, removed_reason, backfilled"
    )
    out.update(_restore_removed_evidence(bind))
    out.update(_restore_detached(bind))
    out["session_cols"] = _update_from(bind, "session_cols", "session", _SESSION_COLS)
    out["setting_cols"] = _update_from(bind, "setting_cols", "setting", _SETTING_COLS)
    for table in RESTORE_ORDER:
        out[table] = _copy_back(bind, table, table)
    expected = _scalar(bind, f"SELECT value FROM {ARCHIVE}.meta WHERE key = 'counts'") or {}
    short = {
        k: (expected.get(k), v)
        for k, v in out.items()
        if k in RESTORE_ORDER and expected.get(k) not in (None, v)
    }
    if short:
        log.warning("0006: số dòng khôi phục khác lúc chép (chép, khôi phục) %s", short)
    op.execute(f"DROP SCHEMA {ARCHIVE} CASCADE")
    return out


def _restore_removed_evidence(bind: sa.Connection) -> dict[str, int]:
    """Nâng cấp lại: dòng đã bỏ (xóa ở bước 5 lúc lùi) chèn lại vào hồ sơ gốc — trừ khi (hồ sơ, phiên) /
    (hồ sơ, ảnh) đã có dòng (người dùng thêm lại khi chạy Phase 2 → giữ dòng hiện có, log
    `restore_removed_conflict`); hồ sơ hệ thống `LEGACY_HOLD` do lùi tạo còn nguyên → xóa, đã bị đổi → giữ."""
    conflicts = bind.execute(
        sa.text(
            f"SELECT a.claim_id::text, COALESCE(a.session_id, a.snapshot_id)::text FROM {ARCHIVE}.claim_evidence_cols a "
            "WHERE a.removed_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM claim_evidence e WHERE e.id = a.id) "
            "AND EXISTS (SELECT 1 FROM claim_evidence e WHERE e.claim_id = a.claim_id "
            "AND (e.session_id = a.session_id OR e.snapshot_id = a.snapshot_id))"
        )
    ).all()
    for claim_id, target in conflicts:
        log.warning(
            "restore_removed_conflict claim_id=%s target=%s — giữ dòng người dùng thêm lại", claim_id, target
        )
    reinserted = bind.execute(
        sa.text(
            f"INSERT INTO claim_evidence ({_EVIDENCE_COLS}) SELECT {_EVIDENCE_COLS} FROM {ARCHIVE}.claim_evidence_cols a "
            "WHERE a.removed_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM claim_evidence e WHERE e.id = a.id) "
            "AND EXISTS (SELECT 1 FROM claim c WHERE c.id = a.claim_id) "
            "AND NOT EXISTS (SELECT 1 FROM claim_evidence e WHERE e.claim_id = a.claim_id "
            "AND (e.session_id = a.session_id OR e.snapshot_id = a.snapshot_id))"
        )
    ).rowcount
    unchanged = (
        f"SELECT d.claim_id FROM {ARCHIVE}.downgrade_removed_claims d JOIN claim c ON c.id = d.claim_id "
        "WHERE c.updated_at = d.updated_at AND c.version = d.version "
        "AND (SELECT count(*) FROM claim_evidence e WHERE e.claim_id = c.id) = d.evidence_count "
        "AND (SELECT count(*) FROM claim_note n WHERE n.claim_id = c.id) = d.note_count"
    )
    kept = bind.execute(
        sa.text(
            f"SELECT c.code FROM {ARCHIVE}.downgrade_removed_claims d JOIN claim c ON c.id = d.claim_id "
            f"WHERE d.claim_id NOT IN ({unchanged})"
        )
    ).all()
    if kept:
        log.warning(
            "0006: giữ %s hồ sơ LEGACY_HOLD do lùi tạo vì đã bị sửa khi chạy Phase 2: %s",
            len(kept),
            ", ".join(r[0] for r in kept),
        )
    dropped = bind.execute(sa.text(f"DELETE FROM claim WHERE id IN ({unchanged})")).rowcount
    with bind.begin_nested() as sp:  # người dùng hệ thống còn được tham chiếu (0004, hồ sơ giữ lại) → giữ
        try:
            bind.execute(sa.text('DELETE FROM "user" WHERE id = CAST(:u AS uuid)'), {"u": SYSTEM_USER_ID})
        except sa.exc.IntegrityError:
            sp.rollback()
    return {
        "removed_evidence_reinserted": int(reinserted or 0),
        "removed_evidence_conflicts": len(conflicts),
        "legacy_hold_claims_dropped": int(dropped or 0),
    }


def _restore_detached(bind: sa.Connection) -> dict[str, int]:
    """Nâng cấp lại: kiện của đơn ngoài gắn lại đơn nếu vẫn chưa gắn; clip `MISSING` lúc lùi (thành `FAILED`)
    trả `MISSING` nếu vẫn `FAILED`."""
    reattached = bind.execute(
        sa.text(
            f"UPDATE package p SET order_id = d.order_id FROM {ARCHIVE}.detached_packages d "
            'WHERE p.id = d.package_id AND p.order_id IS NULL AND EXISTS (SELECT 1 FROM "order" o WHERE o.id = d.order_id)'
        )
    ).rowcount
    total = int(_scalar(bind, f"SELECT count(*) FROM {ARCHIVE}.detached_packages"))
    if total != reattached:
        log.warning(
            "0006: %s kiện của đơn ngoài đã được gắn đơn khác khi chạy Phase 2 — giữ như hiện tại",
            total - int(reattached or 0),
        )
    missing = bind.execute(
        sa.text(
            f"UPDATE clip c SET status = 'MISSING' FROM {ARCHIVE}.missing_clips m WHERE c.id = m.id AND c.status = 'FAILED'"
        )
    ).rowcount
    missing_snaps = bind.execute(
        sa.text(
            f"UPDATE snapshot s SET status = 'MISSING' FROM {ARCHIVE}.missing_snapshots m WHERE s.id = m.id "
            "AND s.status = 'DELETED' AND s.deleted_at IS NOT DISTINCT FROM m.deleted_at"
        )
    ).rowcount
    return {
        "detached_reattached": int(reattached or 0),
        "missing_clips": int(missing or 0),
        "missing_snapshots": int(missing_snaps or 0),
    }


# ---------------------------------------------------------------- downgrade (02a §3, DEC-475) — chép trước, xóa sau


def _guard_active_shares(bind: sa.Connection) -> None:
    """Bước 1a: link còn đang tạo / đang hoạt động → Phase 2 không thu hồi / hết hạn được (không có J-25) → từ chối."""
    rows = bind.execute(
        sa.text(
            "SELECT id::text, status, recipient FROM share_link WHERE status IN ('CREATING', 'ACTIVE') "
            "ORDER BY created_at LIMIT 20"
        )
    ).all()
    if rows and os.environ.get(ALLOW_ACTIVE_SHARES_ENV) != "1":
        listed = ", ".join(f"{r.id} ({r.status})" for r in rows)
        raise RuntimeError(
            f"Không downgrade 0006: còn {len(rows)} link chia sẻ đang tạo / đang hoạt động ({listed}) — bản Phase 2 "
            "không thu hồi / hết hạn được. Thu hồi ở màn Link chia sẻ rồi chạy lại; chấp nhận để link sống tới khi "
            f"bản cloud tự hết hạn thì đặt {ALLOW_ACTIVE_SHARES_ENV}=1. Không có gì bị thay đổi. Xem docs/ops.md §7.2."
        )
    if rows:
        log.warning("0006 downgrade: %s link vẫn sống (ops đặt %s=1)", len(rows), ALLOW_ACTIVE_SHARES_ENV)


def _archive_phase3(bind: sa.Connection) -> dict[str, int]:
    """Bước 2: chép bảng mới + cột mới + shop TikTok / quan hệ sang `phase3_archive` (chưa xóa gì)."""
    op.execute(
        f"DROP SCHEMA IF EXISTS {ARCHIVE} CASCADE"
    )  # archive cũ chỉ còn khi lần nâng cấp trước lỗi giữa chừng
    op.execute(f"CREATE SCHEMA {ARCHIVE}")
    for table in NEW_TABLES:
        op.execute(f'CREATE TABLE {ARCHIVE}.{table} AS SELECT * FROM public."{table}"')
    for name, _source, select in COLUMN_ARCHIVES:
        op.execute(f"CREATE TABLE {ARCHIVE}.{name} AS {select}")
    for name, select in ROW_ARCHIVES:
        op.execute(f"CREATE TABLE {ARCHIVE}.{name} AS {select}")
    # Bước 3 (chép trước): shop Shopee sẽ bị ngắt — Phase 2 chỉ dùng shop CONNECTED mới nhất (DEC-12).
    op.execute(
        f"CREATE TABLE {ARCHIVE}.reconnected_shops AS SELECT id, auth_status, access_token_enc FROM shop "
        f"WHERE id IN ({_EXTRA_SHOPEE_SQL})"
    )
    op.execute(
        f"CREATE TABLE {ARCHIVE}.detached_packages (package_id uuid PRIMARY KEY, order_id uuid NOT NULL)"
    )
    op.execute(
        f"CREATE TABLE {ARCHIVE}.downgrade_removed_claims (claim_id uuid PRIMARY KEY, "
        "updated_at timestamptz NOT NULL, version int NOT NULL, evidence_count int NOT NULL, note_count int NOT NULL)"
    )
    names = [
        *NEW_TABLES,
        *(n for n, _, _ in COLUMN_ARCHIVES),
        *(n for n, _ in ROW_ARCHIVES),
        "reconnected_shops",
    ]
    counts = {name: int(_scalar(bind, f"SELECT count(*) FROM {ARCHIVE}.{name}")) for name in names}
    op.execute(f"CREATE TABLE {ARCHIVE}.meta (key text PRIMARY KEY, value jsonb NOT NULL)")
    bind.execute(
        sa.text(
            f"INSERT INTO {ARCHIVE}.meta (key, value) VALUES ('counts', CAST(:counts AS jsonb)), "
            "('downgraded_at', to_jsonb(now()))"
        ),
        {"counts": json.dumps(counts)},
    )
    log.info("0006 downgrade: chép sang %s %s", ARCHIVE, json.dumps(counts, ensure_ascii=False))
    return counts


def _guard_foreign_orders(bind: sa.Connection) -> None:
    """Bước 1b (DEC-509): còn kiện của đơn ngoài (shop TikTok / shop Shopee sẽ bị ngắt) → J-06 Phase 2 gọi Shopee
    bằng mã đơn lạ, có thể kẹt cả lô → từ chối, in số kiện theo trạng thái; cờ cho phép → tách kiện (bước 4)."""
    rows = bind.execute(
        sa.text(
            "SELECT p.warehouse_status, count(*) FROM package p "
            f"WHERE p.order_id IN ({_FOREIGN_ORDERS_SQL}) GROUP BY 1 ORDER BY 1"
        )
    ).all()
    if rows and os.environ.get(ALLOW_DETACH_ENV) != "1":
        listed = ", ".join(f"{status}: {n}" for status, n in rows)
        raise RuntimeError(
            f"Không downgrade 0006: còn kiện của đơn TikTok / shop Shopee sẽ bị ngắt ({listed}) — bản Phase 2 tra "
            "vận chuyển mọi kiện bằng token một shop Shopee. Chấp nhận tách các kiện này khỏi đơn trong thời gian chạy "
            f"Phase 2 thì đặt {ALLOW_DETACH_ENV}=1 (nâng cấp lại gắn lại). Không có gì bị thay đổi. Xem docs/ops.md §7.2."
        )


def _detach_foreign_packages(bind: sa.Connection) -> int:
    """Bước 4: kiện của đơn ngoài → `order_id = NULL` (J-06 Phase 2 join `order` → bỏ qua; J-05 chỉ lấy kiện
    chưa xác minh → không tra). Đơn giữ dòng để nâng cấp lại gắn lại."""
    bind.execute(
        sa.text(
            f"INSERT INTO {ARCHIVE}.detached_packages (package_id, order_id) "
            f"SELECT p.id, p.order_id FROM package p WHERE p.order_id IN ({_FOREIGN_ORDERS_SQL})"
        )
    )
    return int(
        bind.execute(
            sa.text(
                f"UPDATE package p SET order_id = NULL FROM {ARCHIVE}.detached_packages d WHERE p.id = d.package_id"
            )
        ).rowcount
        or 0
    )


def _keep_days(bind: sa.Connection) -> int:
    floor = int(os.environ.get("RETENTION_CLIP_MIN_DAYS", "60"))
    return max(int(_scalar(bind, "SELECT retention_clip_days FROM setting WHERE id = 1") or 0), floor)


# Bằng chứng đã bỏ còn trong hạn giữ BR-38 (tập `B`): clip không `DELETED` / ảnh `READY` của dòng đã bỏ mà
# max(lúc kết thúc / lúc chụp, lúc bỏ) + số ngày giữ > bây giờ.
_HELD_CLIPS_SQL = (
    "SELECT k.id, k.session_id, ce.id AS evidence_id, ce.claim_id, ce.removed_at, "
    "GREATEST(k.end_at, ce.removed_at) + (:days * interval '1 day') AS keep_until "
    "FROM claim_evidence ce JOIN clip k ON k.session_id = ce.session_id AND k.status <> 'DELETED' "
    "WHERE ce.removed_at IS NOT NULL AND GREATEST(k.end_at, ce.removed_at) + (:days * interval '1 day') > now()"
)
_HELD_SNAPSHOTS_SQL = (
    "SELECT sn.id, sn.session_id, ce.id AS evidence_id, ce.claim_id, ce.removed_at, "
    "GREATEST(sn.taken_at, ce.removed_at) + (:days * interval '1 day') AS keep_until "
    "FROM claim_evidence ce JOIN snapshot sn ON sn.id = ce.snapshot_id AND sn.status = 'READY' "
    "WHERE ce.removed_at IS NOT NULL AND GREATEST(sn.taken_at, ce.removed_at) + (:days * interval '1 day') > now()"
)
# Luật bảo vệ của Phase 2 (`media.protection` ở `main`, chép như 0004 `PROTECTED_SESSIONS_SQL` — không import code).
_PHASE2_PROTECTED_SESSIONS_SQL = """
WITH cases AS (
    SELECT rc.id, rcp.package_id
    FROM return_case rc JOIN return_case_package rcp ON rcp.return_case_id = rc.id
    WHERE rc.status IN ('EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'MISSING')
       OR (rc.status IN ('RECEIVED_OK', 'RECEIVED_ISSUE')
           AND COALESCE(rc.received_at, rc.updated_at) > now() - interval '7 days')
       OR (rc.status = 'NO_PARCEL' AND COALESCE(rc.reported_at, rc.created_at) > now() - interval '30 days')
)
SELECT ce.session_id
FROM claim_evidence ce JOIN claim c ON c.id = ce.claim_id
WHERE ce.kind = 'SESSION' AND (c.status <> 'CLOSED' OR c.closed_at >= now() - :days * interval '1 day')
UNION
SELECT s.id FROM session s
WHERE s.package_id IN (SELECT package_id FROM cases)
  AND (s.type = 'RETURN'
       OR (s.type = 'PACK' AND s.status = 'COMPLETED' AND NOT EXISTS (
           SELECT 1 FROM session s2
           WHERE s2.package_id = s.package_id AND s2.type = 'PACK' AND s2.status = 'COMPLETED'
             AND (s2.ended_at > s.ended_at OR (s2.ended_at = s.ended_at AND s2.id > s.id)))))
UNION
SELECT s.id FROM session s WHERE s.return_case_id IN (SELECT id FROM cases)
"""
_PHASE2_EVIDENCE_SNAPSHOTS_SQL = (
    "SELECT ce.snapshot_id FROM claim_evidence ce JOIN claim c ON c.id = ce.claim_id "
    "WHERE ce.kind = 'SNAPSHOT' AND (c.status <> 'CLOSED' OR c.closed_at >= now() - :days * interval '1 day')"
)


def _tz() -> ZoneInfo:
    return ZoneInfo(os.environ.get("TZ_DISPLAY", "Asia/Ho_Chi_Minh"))


def _hold_removed_evidence(bind: sa.Connection, days: int) -> tuple[set[str], set[str]]:
    """Bước 5 (DEC-497): bằng chứng đã bỏ còn hạn giữ → mỗi kiện một hồ sơ hệ thống `LEGACY_HOLD` `CLOSED`
    (`closed_at` = lúc bỏ muộn nhất) — luật Phase 2 (`closed_at ≥ now − số ngày giữ`) giữ đúng hạn BR-38 cho cả
    clip lẫn ảnh rồi tự hết. Xóa dòng đã bỏ (đã chép ở bước 2). Trả tập `B` (id clip, id ảnh)."""
    clips = bind.execute(sa.text(_HELD_CLIPS_SQL), {"days": days}).mappings().all()
    snaps = bind.execute(sa.text(_HELD_SNAPSHOTS_SQL), {"days": days}).mappings().all()
    held_clips = {str(r["id"]) for r in clips}
    held_snaps = {str(r["id"]) for r in snaps}
    if clips or snaps:
        bind.execute(
            sa.text(
                'INSERT INTO "user" (id, username, display_name, role, password_hash, is_active) VALUES '
                "(CAST(:u AS uuid), 'system_phase2_hold', :name, 'SUPERVISOR', '!', false) ON CONFLICT (id) DO NOTHING"
            ),
            {"u": SYSTEM_USER_ID, "name": SYSTEM_USER_NAME},
        )
    info = {
        str(r.id): r
        for r in bind.execute(
            sa.text(
                "SELECT ce.id, ce.removed_at, ce.removed_reason, c.code, c.package_id FROM claim_evidence ce "
                "JOIN claim c ON c.id = ce.claim_id WHERE ce.removed_at IS NOT NULL"
            )
        ).all()
    }
    groups: dict[str, dict[str, Any]] = {}
    for kind, rows in (("SESSION", clips), ("SNAPSHOT", snaps)):
        for r in rows:
            row = info[str(r["evidence_id"])]
            g = groups.setdefault(
                str(row.package_id),
                {"closed_at": row.removed_at, "keep_until": r["keep_until"], "sessions": set(), "snapshots": set(),
                 "notes": {}},
            )  # fmt: skip
            g["closed_at"] = max(g["closed_at"], row.removed_at)
            g["keep_until"] = max(g["keep_until"], r["keep_until"])
            (g["sessions"] if kind == "SESSION" else g["snapshots"]).add(
                str(r["session_id"] if kind == "SESSION" else r["id"])
            )
            g["notes"].setdefault(str(r["evidence_id"]), (row, r["keep_until"]))
    tz = _tz()
    for package_id, g in sorted(groups.items()):
        claim_id = str(uuid.uuid4())
        keep_label = g["keep_until"].astimezone(tz).strftime("%d/%m/%Y")
        bind.execute(
            sa.text(
                "INSERT INTO claim (id, package_id, type, counterparty, status, source, deadline_at, deadline_source, "
                "close_reason, closed_at, created_by, created_at, updated_at, version) VALUES (CAST(:id AS uuid), "
                "CAST(:pkg AS uuid), 'OTHER', 'PLATFORM', 'CLOSED', 'LEGACY_HOLD', :closed, 'DEFAULT', :reason, :closed, "
                "CAST(:u AS uuid), now(), now(), 1)"
            ),
            {
                "id": claim_id,
                "pkg": package_id,
                "closed": g["closed_at"],
                "reason": f"Bằng chứng đã bỏ — giữ tới {keep_label}",
                "u": SYSTEM_USER_ID,
            },
        )
        for sid in sorted(g["sessions"]):
            bind.execute(
                sa.text(
                    "INSERT INTO claim_evidence (id, claim_id, kind, session_id, auto, added_by, added_at) VALUES "
                    "(gen_random_uuid(), CAST(:c AS uuid), 'SESSION', CAST(:s AS uuid), false, NULL, now())"
                ),
                {"c": claim_id, "s": sid},
            )
        for snap in sorted(g["snapshots"]):
            bind.execute(
                sa.text(
                    "INSERT INTO claim_evidence (id, claim_id, kind, snapshot_id, auto, added_by, added_at) VALUES "
                    "(gen_random_uuid(), CAST(:c AS uuid), 'SNAPSHOT', CAST(:s AS uuid), false, NULL, now())"
                ),
                {"c": claim_id, "s": snap},
            )
        for row, keep in sorted(g["notes"].values(), key=lambda v: (v[0].removed_at, str(v[0].id))):
            text = (
                f"Bằng chứng đã bỏ khỏi {row.code} lúc {row.removed_at.astimezone(tz).strftime('%H:%M %d/%m/%Y')} "
                f"(lý do: {row.removed_reason}) — giữ tới {keep.astimezone(tz).strftime('%d/%m/%Y')}"
            )
            bind.execute(
                sa.text(
                    "INSERT INTO claim_note (id, claim_id, kind, text, author_user_id, at) VALUES "
                    "(gen_random_uuid(), CAST(:c AS uuid), 'SYSTEM', :t, NULL, now())"
                ),
                {"c": claim_id, "t": text},
            )
        bind.execute(
            sa.text(
                f"INSERT INTO {ARCHIVE}.downgrade_removed_claims SELECT c.id, c.updated_at, c.version, "
                "(SELECT count(*) FROM claim_evidence e WHERE e.claim_id = c.id), "
                "(SELECT count(*) FROM claim_note n WHERE n.claim_id = c.id) FROM claim c WHERE c.id = CAST(:c AS uuid)"
            ),
            {"c": claim_id},
        )
    removed = bind.execute(sa.text("DELETE FROM claim_evidence WHERE removed_at IS NOT NULL")).rowcount
    log.info(
        "0006 downgrade: %s dòng bằng chứng đã bỏ; %s clip + %s ảnh còn hạn giữ → %s hồ sơ LEGACY_HOLD đã đóng",
        removed,
        len(held_clips),
        len(held_snaps),
        len(groups),
    )
    return held_clips, held_snaps


def _check_subset(bind: sa.Connection, held_clips: set[str], held_snaps: set[str], days: int) -> None:
    """Bước 6 (như 0004 `_check_subset`): mọi clip / ảnh của `B` thuộc tập được bảo vệ theo luật Phase 2 sau
    bước 5; thiếu → `raise` (cả transaction lùi)."""
    protected_clips = {
        str(r[0])
        for r in bind.execute(
            sa.text(
                f"SELECT k.id FROM clip k WHERE k.held OR k.session_id IN ({_PHASE2_PROTECTED_SESSIONS_SQL})"
            ),
            {"days": days},
        ).all()
    }
    protected_snaps = {
        str(r[0])
        for r in bind.execute(
            sa.text(
                f"SELECT sn.id FROM snapshot sn WHERE sn.session_id IN ({_PHASE2_PROTECTED_SESSIONS_SQL}) "
                f"OR sn.id IN ({_PHASE2_EVIDENCE_SNAPSHOTS_SQL})"
            ),
            {"days": days},
        ).all()
    }
    missing = sorted((held_clips - protected_clips) | (held_snaps - protected_snaps))
    log.info(
        "0006 downgrade: |B| = %s, được bảo vệ sau = %s clip + %s ảnh, thiếu = %s",
        len(held_clips) + len(held_snaps),
        len(protected_clips),
        len(protected_snaps),
        len(missing),
    )
    if missing:
        raise RuntimeError(
            f"0006 downgrade: {len(missing)} clip / ảnh bằng chứng đã bỏ còn hạn giữ không được luật Phase 2 bảo vệ "
            f"({', '.join(missing[:5])}) — dừng, không đổi dữ liệu (DEC-497)."
        )


def _missing_to_phase2(bind: sa.Connection) -> int:
    """Bước 7: Phase 2 không có `MISSING` → clip `FAILED` (J-11 Phase 2 chỉ đẩy lại J-01 — vô hại; D2 đếm
    `CLIP_FAILED`); ảnh `DELETED` (Phase 2 ảnh chỉ `READY` / `DELETED`; J-02 Phase 2 chỉ xét `READY`). Danh sách ở
    `missing_clips` / `missing_snapshots`, nâng cấp lại trả `MISSING`."""
    clips = bind.execute(sa.text("UPDATE clip SET status = 'FAILED' WHERE status = 'MISSING'")).rowcount
    snaps = bind.execute(sa.text("UPDATE snapshot SET status = 'DELETED' WHERE status = 'MISSING'")).rowcount
    return int(clips or 0) + int(snaps or 0)


def _prepare_phase2(bind: sa.Connection) -> None:
    """Bước 3, 8 (phần dữ liệu): ngắt shop Shopee thừa; đơn TikTok rời shop; xóa shop TikTok (đã chép)."""
    disconnected = bind.execute(
        sa.text(
            f"UPDATE shop s SET auth_status = 'DISCONNECTED' FROM {ARCHIVE}.reconnected_shops a WHERE a.id = s.id"
        )
    ).rowcount
    detached = bind.execute(
        sa.text(
            "UPDATE \"order\" o SET shop_id = NULL FROM shop s WHERE s.id = o.shop_id AND s.platform = 'TIKTOK'"
        )
    ).rowcount
    removed = bind.execute(sa.text("DELETE FROM shop WHERE platform = 'TIKTOK'")).rowcount
    log.info(
        "0006 downgrade: ngắt %s shop Shopee (Phase 2 một shop), %s đơn TikTok rời shop, xóa %s shop TikTok",
        disconnected,
        detached,
        removed,
    )


def upgrade() -> None:
    bind = op.get_bind()
    # Chờ khóa tối đa 5 giây (service chưa dừng → lỗi rõ, cả migration lùi) — như 0003 (G3 M-F2).
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    started = time.monotonic()
    _create_tables()
    _add_columns()
    _add_checks()
    stats = _backfill(bind)
    stats.update(_backfill_prior_sessions(bind))
    stats["cancel_revert_candidates"] = _log_cancel_candidates(bind)
    restored = _restore_phase3(bind)
    _create_indexes()
    stats["seconds"] = round(time.monotonic() - started, 1)
    log.info("0006: backfill %s", json.dumps(stats, ensure_ascii=False))
    if restored:
        log.info("0006: khôi phục từ %s %s", ARCHIVE, json.dumps(restored, ensure_ascii=False))
    if stats["order_unknown"]:
        log.warning(
            "0006: đơn có trạng thái sàn chưa ánh xạ → nhóm UNKNOWN (không đổi trạng thái kho): %s",
            json.dumps(stats["order_unknown"], ensure_ascii=False),
        )


# ---------------------------------------------------------------- downgrade


def downgrade() -> None:
    bind = op.get_bind()
    op.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
    _guard_active_shares(bind)
    _guard_foreign_orders(bind)
    _archive_phase3(bind)
    detached = _detach_foreign_packages(bind)
    days = _keep_days(bind)
    held_clips, held_snaps = _hold_removed_evidence(bind, days)
    _check_subset(bind, held_clips, held_snaps, days)
    missing = _missing_to_phase2(bind)
    log.info(
        "0006 downgrade: tách %s kiện của đơn ngoài, %s clip / ảnh MISSING về trạng thái Phase 2",
        detached,
        missing,
    )
    _prepare_phase2(bind)
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
