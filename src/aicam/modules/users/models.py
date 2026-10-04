import uuid
from datetime import datetime

from sqlalchemy import ForeignKey, Index, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

ROLES = ("ADMIN", "SUPERVISOR", "CSKH", "STATION")
CLIENTS = ("STATION", "DASHBOARD")


class User(UUIDPk, Base):
    __tablename__ = "user"
    __table_args__ = (
        Index("uq_user_username_lower", text("lower(username)"), unique=True),
        enum_check("role", ROLES),
    )

    username: Mapped[str] = mapped_column(Text)
    display_name: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    password_hash: Mapped[str] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(default=True, server_default="true")
    failed_logins: Mapped[int] = mapped_column(default=0, server_default="0")
    locked_until: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())


class RefreshToken(UUIDPk, Base):
    __tablename__ = "refresh_token"
    __table_args__ = (Index(None, "user_id"), enum_check("client", CLIENTS))

    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id", ondelete="CASCADE"))
    token_hash: Mapped[str] = mapped_column(Text, unique=True)
    client: Mapped[str] = mapped_column(Text)
    expires_at: Mapped[datetime]
    revoked_at: Mapped[datetime | None]
    replaced_by: Mapped[uuid.UUID | None]
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
