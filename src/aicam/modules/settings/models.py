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
    )

    id: Mapped[int] = mapped_column(primary_key=True, default=1)
    retention_raw_days: Mapped[int] = mapped_column(default=30, server_default="30")
    retention_clip_days: Mapped[int] = mapped_column(default=90, server_default="90")
    session_warn_minutes: Mapped[int] = mapped_column(default=15, server_default="15")
    session_abandon_minutes: Mapped[int] = mapped_column(default=30, server_default="30")
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
