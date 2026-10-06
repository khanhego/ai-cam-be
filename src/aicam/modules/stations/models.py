import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Index, LargeBinary, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

CAMERA_ROLES = ("CAM1", "CAM2")
CAMERA_STATUSES = ("ONLINE", "OFFLINE")
# Phase 2 (02 §5.1): loại bàn và chế độ đang chạy; `work_mode = kind` khi `kind` ≠ BOTH.
STATION_KINDS = ("PACK", "RETURN", "BOTH")
WORK_MODES = ("PACK", "RETURN")


class Station(UUIDPk, Base):
    __tablename__ = "station"
    __table_args__ = (
        Index("uq_station_name_lower", text("lower(name)"), unique=True),
        enum_check("kind", STATION_KINDS),
        enum_check("work_mode", WORK_MODES),
        CheckConstraint("kind = 'BOTH' OR work_mode = kind", name="work_mode_matches_kind"),
        CheckConstraint("char_length(operator_name) BETWEEN 2 AND 40", name="operator_name_length"),
    )

    name: Mapped[str] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(default=True, server_default="true")
    account_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("user.id", ondelete="SET NULL"), unique=True
    )
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    kind: Mapped[str] = mapped_column(Text, default="PACK", server_default="PACK")
    work_mode: Mapped[str] = mapped_column(Text, default="PACK", server_default="PACK")
    # Người kiểm hàng hoàn (BR-28): xóa khi station đăng xuất (API-03) / bị thu hồi phiên (API-91).
    operator_name: Mapped[str | None] = mapped_column(Text)


class Camera(UUIDPk, Base):
    __tablename__ = "camera"
    __table_args__ = (
        UniqueConstraint("station_id", "role"),
        enum_check("role", CAMERA_ROLES),
        enum_check("status", CAMERA_STATUSES),
    )

    station_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("station.id", ondelete="CASCADE"))
    role: Mapped[str] = mapped_column(Text)
    rtsp_url: Mapped[str] = mapped_column(Text)
    username: Mapped[str | None] = mapped_column(Text)
    password_enc: Mapped[bytes | None] = mapped_column(LargeBinary)
    mediamtx_path: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="OFFLINE", server_default="OFFLINE")
    last_seen_at: Mapped[datetime | None]
    clock_offset_ms: Mapped[int | None]
    roi: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
