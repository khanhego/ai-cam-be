import uuid
from datetime import datetime, time

from sqlalchemy import CheckConstraint, Text, func, text
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
        # Phase 3 (02a §3, migration 0006).
        CheckConstraint(
            "refund_only_default_hours BETWEEN 1 AND 168", name="refund_only_default_hours_range"
        ),
        CheckConstraint("quiet_start <> quiet_end", name="quiet_start_ne_end"),
        CheckConstraint("backup_upload_mbps BETWEEN 1 AND 1000", name="backup_upload_mbps_range"),
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
    # Phase 3 (0006): FR-03.16, BR-40 / L13, BR-36 giờ yên lặng, sao lưu cloud (DEC-499).
    packer_name_required: Mapped[bool] = mapped_column(default=False, server_default="false")
    refund_only_default_hours: Mapped[int] = mapped_column(default=48, server_default="48")
    quiet_hours_enabled: Mapped[bool] = mapped_column(default=True, server_default="true")
    quiet_start: Mapped[time] = mapped_column(default=time(22, 0), server_default=text("'22:00'"))
    quiet_end: Mapped[time] = mapped_column(default=time(7, 0), server_default=text("'07:00'"))
    backup_enabled: Mapped[bool] = mapped_column(default=False, server_default="false")
    backup_confirmed_fingerprint: Mapped[str | None] = mapped_column(Text)
    backup_confirmed_at: Mapped[datetime | None]
    backup_confirmed_by: Mapped[uuid.UUID | None]
    backup_upload_mbps: Mapped[int] = mapped_column(default=10, server_default="10")
    backup_all_pack_clips: Mapped[bool] = mapped_column(default=False, server_default="false")
    backup_restore_pending: Mapped[bool] = mapped_column(default=False, server_default="false")
