import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, Text, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

SESSION_TYPES = ("PACK",)
SESSION_STATUSES = (
    "OPEN",
    "MISMATCH",
    "WAITING_APPROVAL",
    "COMPLETED",
    "CANCELLED",
    "ABANDONED",
    "SUPERSEDED",
)
ACTIVE_STATUSES = ("OPEN", "MISMATCH", "WAITING_APPROVAL")
CANCEL_REASONS = ("OUT_OF_STOCK", "WRONG_SCAN", "OTHER", "SUPERVISOR")
SESSION_FLAGS = (
    "UNVERIFIED",
    "CAM2_UNVERIFIED",
    "LABEL_ON_TRAY",
    "VIDEO_INCOMPLETE",
    "REPACK",
    "HAD_MISMATCH",
    "CLOSED_BY_SUPERVISOR",
)

_ACTIVE_SQL = "status IN ('OPEN', 'MISMATCH', 'WAITING_APPROVAL')"


class PackSession(UUIDPk, Base):
    """Phiên đóng gói. Bảng `session` (02a §3)."""

    __tablename__ = "session"
    __table_args__ = (
        # BR-02: mỗi station tối đa một phiên đang hoạt động.
        Index("uq_session_active_station", "station_id", unique=True, postgresql_where=text(_ACTIVE_SQL)),
        # Một kiện không được mở ở hai station cùng lúc.
        Index("uq_session_active_package", "package_id", unique=True, postgresql_where=text(_ACTIVE_SQL)),
        Index(None, "package_id"),
        Index(None, "started_at"),
        Index(None, "station_id", "started_at"),
        enum_check("type", SESSION_TYPES),
        enum_check("status", SESSION_STATUSES),
        enum_check("cancel_reason", CANCEL_REASONS),
    )

    type: Mapped[str] = mapped_column(Text, default="PACK", server_default="PACK")
    package_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("package.id", ondelete="RESTRICT"))
    station_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("station.id", ondelete="RESTRICT"))
    status: Mapped[str] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    ended_at: Mapped[datetime | None]
    open_code: Mapped[str] = mapped_column(Text)
    close_code: Mapped[str | None] = mapped_column(Text)
    cam2_code: Mapped[str | None] = mapped_column(Text)
    flags: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list, server_default="{}")
    cancel_reason: Mapped[str | None] = mapped_column(Text)
    note: Mapped[str | None] = mapped_column(Text)
    supersedes_session_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("session.id", ondelete="SET NULL")
    )
    package_status_before: Mapped[str | None] = mapped_column(Text)
    status_before_approval: Mapped[str | None] = mapped_column(Text)
    # {"source": "SCAN" | "CAM2", "expected": "...", "actual": "..."} — 02 API-10 (DEC-39).
    mismatch: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    cam2_seen_match: Mapped[bool] = mapped_column(default=False, server_default="false")
    warn_notified: Mapped[bool] = mapped_column(default=False, server_default="false")


class SessionEvent(UUIDPk, Base):
    __tablename__ = "session_event"
    __table_args__ = (Index(None, "session_id", "at"),)

    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="CASCADE"))
    type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    at: Mapped[datetime] = mapped_column(default=utcnow)


class ScanDedup(Base):
    """Chống xử lý trùng API-11 khi station retry (DEC-29). Dọn sau 10 phút (J-11)."""

    __tablename__ = "scan_dedup"

    client_scan_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    station_id: Mapped[uuid.UUID]
    response: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
