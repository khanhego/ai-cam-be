import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Index, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

SESSION_TYPES = ("PACK", "RETURN")
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
CANCEL_REASONS = ("OUT_OF_STOCK", "WRONG_SCAN", "OTHER", "SUPERVISOR", "NOT_A_RETURN")
SESSION_FLAGS = (
    "UNVERIFIED",
    "CAM2_UNVERIFIED",
    "LABEL_ON_TRAY",
    "VIDEO_INCOMPLETE",
    "REPACK",
    "HAD_MISMATCH",
    "CLOSED_BY_SUPERVISOR",
    # Phase 2 (02 §5.2, §6.3 #1).
    "AUTO_CLOSED",
    "ORDER_CANCELLED",
    "NO_PACK_CLIP",
    "UNANNOUNCED",
    "UNIDENTIFIED",
    "INSPECTION_CORRECTED",
    # Phase 3 (02 §5.1 SESSION, DEC-494): người mua đang xin hủy khi phiên PACK mở.
    "ORDER_CANCEL_REQUESTED",
)
# Kết luận phiên hoàn / tình trạng dòng (02 §5.2).
INSPECTION_CONCLUSIONS = ("OK", "DAMAGED", "MISSING_ITEM", "WRONG_ITEM", "EMPTY_BOX", "OTHER")
INSPECTION_LINES_MODES = ("FULL", "REFERENCE")
# 0006 (DEC-521): mã lý do Supervisor chọn khi hủy phiên RETURN qua API-21 (`cancel_reason` giữ `SUPERVISOR`).
CANCEL_CAUSES = ("WRONG_SCAN", "NOT_A_RETURN", "OTHER")
# 0006 (DEC-515): mã đánh dấu quét nhầm qua API-189.
WRONG_SCAN_CODES = ("WRONG_SCAN", "NOT_A_RETURN")

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
        Index(None, "return_case_id"),
        enum_check("inspection_conclusion", INSPECTION_CONCLUSIONS),
        enum_check("inspection_lines_mode", INSPECTION_LINES_MODES),
        CheckConstraint("type = 'RETURN' OR return_case_id IS NULL", name="return_case_only_return"),
        # 0006
        enum_check("cancel_cause", CANCEL_CAUSES),
        enum_check("wrong_scan_code", WRONG_SCAN_CODES),
        CheckConstraint(
            "(wrong_scan_at IS NULL) = (wrong_scan_code IS NULL)", name="wrong_scan_matches_code"
        ),
        Index(
            "ix_session_wrong_scan_package_id",
            "package_id",
            postgresql_where=text("wrong_scan_at IS NOT NULL"),
        ),
        Index("ix_session_type_status_ended", "type", "status", "ended_at"),
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
    # 0003 — phiên RETURN (02a §3).
    return_case_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("return_case.id", ondelete="SET NULL")
    )
    operator_name: Mapped[str | None] = mapped_column(Text)
    inspection_conclusion: Mapped[str | None] = mapped_column(Text)
    inspection_note: Mapped[str | None] = mapped_column(Text)
    inspection_saved_at: Mapped[datetime | None]
    inspection_lines_mode: Mapped[str | None] = mapped_column(Text)
    # Lịch sử sửa kết luận API-113: [{by, at, reason, before}] (DEC-261).
    inspection_corrections: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB)
    # Độ lệch giờ camera chụp lúc đóng phiên (PACK + RETURN) cho `info.json` (DEC-261).
    camera_clock: Mapped[list[dict[str, Any]] | None] = mapped_column(JSONB)
    # 0006 — BR-39 (DEC-515, 516, 521, 529): mã lý do Supervisor hủy; đánh dấu quét nhầm;
    # xác nhận phiên hoàn thật.
    cancel_cause: Mapped[str | None] = mapped_column(Text)
    wrong_scan_at: Mapped[datetime | None]
    wrong_scan_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id", ondelete="SET NULL"))
    wrong_scan_code: Mapped[str | None] = mapped_column(Text)
    wrong_scan_note: Mapped[str | None] = mapped_column(Text)
    review_confirmed_at: Mapped[datetime | None]
    review_confirmed_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id", ondelete="SET NULL"))
    review_confirmed_note: Mapped[str | None] = mapped_column(Text)


class SessionEvent(UUIDPk, Base):
    __tablename__ = "session_event"
    __table_args__ = (Index(None, "session_id", "at"),)

    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="CASCADE"))
    type: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    at: Mapped[datetime] = mapped_column(default=utcnow)


class InspectionLine(UUIDPk, Base):
    """Dòng kiểm của phiên RETURN (02a §3, BR-22)."""

    __tablename__ = "inspection_line"
    __table_args__ = (
        UniqueConstraint("session_id", "order_item_id"),
        CheckConstraint(
            "quantity_sent BETWEEN 0 AND 999 AND quantity_requested BETWEEN 0 AND 999 "
            "AND quantity_received BETWEEN 0 AND 999",
            name="quantity_range",
        ),
        enum_check("condition", INSPECTION_CONCLUSIONS),
    )

    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="CASCADE"))
    order_item_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("order_item.id", ondelete="SET NULL"))
    position: Mapped[int]
    product_name: Mapped[str] = mapped_column(Text)
    variation: Mapped[str | None] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(Text)
    quantity_sent: Mapped[int]
    quantity_requested: Mapped[int]
    quantity_received: Mapped[int]
    condition: Mapped[str | None] = mapped_column(Text)
    note: Mapped[str | None] = mapped_column(Text)


class ScanDedup(Base):
    """Chống xử lý trùng API-11 khi station retry (DEC-29). Dọn sau 10 phút (J-11)."""

    __tablename__ = "scan_dedup"

    client_scan_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    station_id: Mapped[uuid.UUID]
    response: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
