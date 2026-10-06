"""Sao lưu cloud (02 §5.1 BACKUP_RUN / BACKUP_OBJECT, 02a §3 — migration 0006; ADR-010)."""

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

RUN_KINDS = ("DB",)
RUN_TRIGGERS = ("SCHEDULE", "MANUAL")
RUN_STATUSES = ("RUNNING", "SUCCESS", "FAILED")
OBJECT_KINDS = ("DB_DUMP", "IMPORTS", "CLIP", "SNAPSHOT")
OBJECT_STATUSES = (
    "PENDING",
    "UPLOADING",
    "UPLOADED",
    "FAILED",
    "HASH_MISMATCH",
    "SOURCE_DELETED",
    "IGNORED",
    "CLOUD_DELETED",
)
OBJECT_REASONS = ("EVIDENCE", "ALL_PACK")
RESOLUTION_ACTIONS = ("UPLOAD_ANYWAY", "IGNORE", "RETRY", "ACCEPT_RESTORED")


class BackupRun(UUIDPk, Base):
    __tablename__ = "backup_run"
    __table_args__ = (
        Index(None, "started_at"),
        Index(
            "ix_backup_run_key_fingerprint_live",
            "key_fingerprint",
            postgresql_where=text("status = 'SUCCESS' AND cloud_deleted_at IS NULL"),
        ),
        # Một lượt chạy mỗi loại (§6 — hai lượt J-20).
        Index("uq_backup_run_running_kind", "kind", unique=True, postgresql_where=text("status = 'RUNNING'")),
        enum_check("kind", RUN_KINDS),
        enum_check("trigger", RUN_TRIGGERS),
        enum_check("status", RUN_STATUSES),
    )

    kind: Mapped[str] = mapped_column(Text, default="DB", server_default="DB")
    trigger: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="RUNNING", server_default="RUNNING")
    started_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    finished_at: Mapped[datetime | None]
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    object_key: Mapped[str | None] = mapped_column(Text)
    imports_object_key: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id"))
    key_fingerprint: Mapped[str | None] = mapped_column(Text)
    cloud_deleted_at: Mapped[datetime | None]


class BackupObject(UUIDPk, Base):
    """`status` = việc cần làm; `cloud_present` + `cloud_key_fingerprint` = sự thật trên cloud (DEC-522)."""

    __tablename__ = "backup_object"
    __table_args__ = (
        Index("uq_backup_object_object_key", "object_key", unique=True),
        Index(
            "uq_backup_object_clip_id", "clip_id", unique=True, postgresql_where=text("clip_id IS NOT NULL")
        ),
        Index(
            "uq_backup_object_snapshot_id",
            "snapshot_id",
            unique=True,
            postgresql_where=text("snapshot_id IS NOT NULL"),
        ),
        Index(None, "status", "next_attempt_at"),
        Index(
            "ix_backup_object_uploading_updated_at",
            "status",
            "updated_at",
            postgresql_where=text("status = 'UPLOADING'"),
        ),
        Index(
            "ix_backup_object_cloud_key_fingerprint",
            "cloud_key_fingerprint",
            postgresql_where=text("cloud_present"),
        ),
        Index(None, "run_id"),
        enum_check("kind", OBJECT_KINDS),
        enum_check("status", OBJECT_STATUSES),
        enum_check("reason", OBJECT_REASONS),
        enum_check("resolution_action", RESOLUTION_ACTIONS),
        CheckConstraint("(resolution_action IS NULL) = (resolved_at IS NULL)", name="resolution_matches_at"),
        CheckConstraint(
            "cloud_present = (cloud_key_fingerprint IS NOT NULL)", name="cloud_present_matches_key"
        ),
    )

    kind: Mapped[str] = mapped_column(Text)
    clip_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("clip.id", ondelete="RESTRICT"))
    snapshot_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("snapshot.id", ondelete="RESTRICT"))
    run_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("backup_run.id", ondelete="CASCADE"))
    object_key: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="PENDING", server_default="PENDING")
    sha256: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    encrypted_size: Mapped[int | None] = mapped_column(BigInteger)
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    next_attempt_at: Mapped[datetime | None]
    uploaded_at: Mapped[datetime | None]
    cloud_deleted_at: Mapped[datetime | None]
    last_error: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    cloud_present: Mapped[bool] = mapped_column(default=False, server_default="false")
    cloud_key_fingerprint: Mapped[str | None] = mapped_column(Text)
    hash_override: Mapped[bool] = mapped_column(default=False, server_default="false")
    sha256_actual: Mapped[str | None] = mapped_column(Text)
    resolution_action: Mapped[str | None] = mapped_column(Text)
    resolution_note: Mapped[str | None] = mapped_column(Text)
    resolved_by: Mapped[uuid.UUID | None]
    resolved_at: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
