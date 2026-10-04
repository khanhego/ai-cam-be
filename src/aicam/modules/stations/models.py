import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, LargeBinary, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

CAMERA_ROLES = ("CAM1", "CAM2")
CAMERA_STATUSES = ("ONLINE", "OFFLINE")


class Station(UUIDPk, Base):
    __tablename__ = "station"
    __table_args__ = (Index("uq_station_name_lower", text("lower(name)"), unique=True),)

    name: Mapped[str] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(default=True, server_default="true")
    account_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("user.id", ondelete="SET NULL"), unique=True
    )
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())


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
