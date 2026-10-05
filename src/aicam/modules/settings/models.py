from datetime import datetime

from sqlalchemy import CheckConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, utcnow


class Setting(Base):
    """Một dòng duy nhất (id = 1) — 02a §3, ràng buộc theo 02 API-80."""

    __tablename__ = "setting"
    __table_args__ = (
        CheckConstraint("id = 1", name="single_row"),
        CheckConstraint("retention_raw_days BETWEEN 1 AND 365", name="raw_days_range"),
        CheckConstraint("retention_clip_days BETWEEN 1 AND 365", name="clip_days_range"),
        CheckConstraint("retention_clip_days >= retention_raw_days", name="clip_ge_raw"),
        CheckConstraint("session_abandon_minutes > session_warn_minutes", name="abandon_gt_warn"),
        # Phase 2 (02 API-80, migration 0003).
        CheckConstraint("return_warn_minutes BETWEEN 1 AND 1440", name="return_warn_range"),
        CheckConstraint("return_abandon_minutes BETWEEN 1 AND 1440", name="return_abandon_range"),
        CheckConstraint("return_abandon_minutes > return_warn_minutes", name="return_abandon_gt_warn"),
        CheckConstraint("return_missing_days BETWEEN 1 AND 60", name="return_missing_days_range"),
        CheckConstraint("handover_warn_hours BETWEEN 1 AND 168", name="handover_warn_hours_range"),
        CheckConstraint("claim_deadline_days BETWEEN 1 AND 90", name="claim_deadline_days_range"),
        CheckConstraint("claim_due_soon_hours BETWEEN 1 AND 168", name="claim_due_soon_hours_range"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    retention_raw_days: Mapped[int] = mapped_column(default=30, server_default="30")
    retention_clip_days: Mapped[int] = mapped_column(default=90, server_default="90")
    session_warn_minutes: Mapped[int] = mapped_column(default=15, server_default="15")
    session_abandon_minutes: Mapped[int] = mapped_column(default=30, server_default="30")
    return_warn_minutes: Mapped[int] = mapped_column(default=20, server_default="20")
    return_abandon_minutes: Mapped[int] = mapped_column(default=45, server_default="45")
    return_missing_days: Mapped[int] = mapped_column(default=7, server_default="7")
    handover_warn_hours: Mapped[int] = mapped_column(default=24, server_default="24")
    claim_deadline_days: Mapped[int] = mapped_column(default=7, server_default="7")
    claim_due_soon_hours: Mapped[int] = mapped_column(default=48, server_default="48")
    # Mốc bắt đầu đối soát BR-10/14/20 (DEC-228) — đặt lúc chạy migration 0003.
    recon_start_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
