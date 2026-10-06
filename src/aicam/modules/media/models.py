import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, ForeignKey, Index, Numeric, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

CAMERA_ROLES = ("CAM1", "CAM2")
# `MISSING` (0006): DB có dòng nhưng máy chủ không có tệp (EX-K8, EX-K9 — DEC-520, 524).
CLIP_STATUSES = ("PENDING", "READY", "FAILED", "DELETED", "MISSING")
EXPORT_LAYOUTS = ("CAM1", "CAM2", "SIDE_BY_SIDE")
EXPORT_STATUSES = ("QUEUED", "RUNNING", "READY", "FAILED")
SNAPSHOT_KINDS = ("MANUAL", "PACK_CLOSE")
SNAPSHOT_STATUSES = ("READY", "DELETED", "MISSING")


class VideoSegment(UUIDPk, Base):
    __tablename__ = "video_segment"
    __table_args__ = (UniqueConstraint("camera_id", "start_at"), Index(None, "camera_id", "end_at"))

    camera_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("camera.id", ondelete="CASCADE"))
    start_at: Mapped[datetime]
    end_at: Mapped[datetime]
    path: Mapped[str] = mapped_column(Text)
    size_bytes: Mapped[int] = mapped_column(BigInteger)


class Clip(UUIDPk, Base):
    """Clip gốc bất biến (ADR-008). `retention_until` không lưu, tính từ setting khi đọc (DEC-30)."""

    __tablename__ = "clip"
    __table_args__ = (
        UniqueConstraint("session_id", "camera_role"),
        Index(
            "ix_clip_retention_candidates",
            "end_at",
            postgresql_where=text("held = false AND status = 'READY'"),
        ),
        enum_check("camera_role", CAMERA_ROLES),
        enum_check("status", CLIP_STATUSES),
    )

    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="RESTRICT"))
    camera_role: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="PENDING", server_default="PENDING")
    start_at: Mapped[datetime]
    end_at: Mapped[datetime]
    duration_s: Mapped[Decimal | None] = mapped_column(Numeric(8, 2))
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(Text)
    flags: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list, server_default="{}")
    held: Mapped[bool] = mapped_column(default=False, server_default="false")
    held_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id", ondelete="SET NULL"))
    held_at: Mapped[datetime | None]
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    # 0002 (DEC-102): thời điểm retention xóa file; ánh xạ giây trong clip → giờ thực từng đoạn.
    deleted_at: Mapped[datetime | None]
    timeline: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB)


class Export(UUIDPk, Base):
    __tablename__ = "export"
    __table_args__ = (
        Index(None, "created_by", "created_at"),
        enum_check("layout", EXPORT_LAYOUTS),
        enum_check("status", EXPORT_STATUSES),
    )

    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="CASCADE"))
    layout: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="QUEUED", server_default="QUEUED")
    progress: Mapped[int] = mapped_column(default=0, server_default="0")
    path_video: Mapped[str | None] = mapped_column(Text)
    path_info: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    expires_at: Mapped[datetime | None]


class Snapshot(UUIDPk, Base):
    """Ảnh Cam 1: chụp tay ở phiên RETURN (API-103) hoặc lúc đóng gói (J-17, `PACK_CLOSE`) — 02a §3."""

    __tablename__ = "snapshot"
    __table_args__ = (
        Index(None, "session_id"),
        Index(
            "uq_snapshot_pack_close_session",
            "session_id",
            unique=True,
            postgresql_where=text("kind = 'PACK_CLOSE'"),
        ),
        Index("ix_snapshot_retention_candidates", "taken_at", postgresql_where=text("status = 'READY'")),
        enum_check("kind", SNAPSHOT_KINDS),
        enum_check("camera_role", CAMERA_ROLES),
        enum_check("status", SNAPSHOT_STATUSES),
    )

    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="RESTRICT"))
    kind: Mapped[str] = mapped_column(Text)
    camera_role: Mapped[str] = mapped_column(Text, default="CAM1", server_default="CAM1")
    taken_at: Mapped[datetime]
    path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None]
    status: Mapped[str] = mapped_column(Text, default="READY", server_default="READY")
    deleted_at: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
