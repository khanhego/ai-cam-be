"""Cảnh báo lệch trạng thái (02 §5.1 RECON_ALERT, 02a §3, BR-26)."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

RECON_RULES = (
    "SHIPPED_NOT_PACKED",
    "CANCELLED_AFTER_PACK",
    "RETURN_OVERDUE",
    "RETURN_UNANNOUNCED",
    "PACKED_NOT_HANDED_OVER",
    "RETURN_DONE_NOT_RECEIVED",
    "UNVERIFIED_STALE",
)
RECON_SEVERITIES = ("HIGH", "MEDIUM", "LOW")
RECON_STATUSES = ("OPEN", "RESOLVED", "AUTO_RESOLVED")
RESOLUTION_ACTIONS = ("RESOLVE", "ADJUST_STATUS", "OPEN_CLAIM")


class ReconAlert(UUIDPk, Base):
    __tablename__ = "recon_alert"
    __table_args__ = (
        # BR-26: một cảnh báo mở mỗi (kiện, quy tắc).
        Index(
            "uq_recon_alert_open", "package_id", "rule", unique=True, postgresql_where=text("status = 'OPEN'")
        ),
        Index(None, "status", "severity", "detected_at"),
        Index(None, "package_id", "rule", "context_key"),
        enum_check("rule", RECON_RULES),
        enum_check("severity", RECON_SEVERITIES),
        enum_check("status", RECON_STATUSES),
        enum_check("resolution_action", RESOLUTION_ACTIONS),
    )

    package_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("package.id", ondelete="CASCADE"))
    rule: Mapped[str] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="OPEN", server_default="OPEN")
    context: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=text("'{}'::jsonb"))
    # "Đợt" vi phạm (DEC-226): có alert RESOLVED cùng (kiện, quy tắc, key) → không tạo lại.
    context_key: Mapped[str] = mapped_column(Text)
    detected_at: Mapped[datetime] = mapped_column(default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(default=utcnow)
    closed_at: Mapped[datetime | None]
    resolution_action: Mapped[str | None] = mapped_column(Text)
    resolution_note: Mapped[str | None] = mapped_column(Text)
    resolved_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id"))
    to_status: Mapped[str | None] = mapped_column(Text)
    claim_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("claim.id", ondelete="SET NULL"))
