import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

APPROVAL_TYPES = ("MISMATCH", "ASSIST", "REPACK")
APPROVAL_STATUSES = ("PENDING", "RESOLVED", "WITHDRAWN")
DECISIONS = ("CONTINUE", "CLOSE_WITH_NOTE", "CANCEL_SESSION", "APPROVE_REPACK", "REJECT")


class ApprovalRequest(UUIDPk, Base):
    __tablename__ = "approval_request"
    __table_args__ = (
        Index(
            "uq_approval_request_pending_station",
            "station_id",
            unique=True,
            postgresql_where=text("status = 'PENDING'"),
        ),
        enum_check("type", APPROVAL_TYPES),
        enum_check("status", APPROVAL_STATUSES),
        enum_check("decision", DECISIONS),
    )

    station_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("station.id", ondelete="CASCADE"))
    # null với REPACK (chưa có phiên).
    session_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("session.id", ondelete="CASCADE"))
    tracking_number: Mapped[str] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="PENDING", server_default="PENDING")
    context: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    decision: Mapped[str | None] = mapped_column(Text)
    decided_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id", ondelete="SET NULL"))
    decided_at: Mapped[datetime | None]
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
