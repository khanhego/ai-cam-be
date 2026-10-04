"""Nhật ký thao tác nhạy cảm, chỉ INSERT (NFR-15, 02a §3)."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Identity, Index, Text
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, utcnow

# Danh sách action theo 02 API-92.
ACTIONS = frozenset(
    {
        "LOGIN",
        "VIEW_CLIP",
        "EXPORT_CLIP",
        "DOWNLOAD_EXPORT",
        "HOLD_CLIP",
        "UNHOLD_CLIP",
        "DELETE_CLIP",
        "REBUILD_CLIP",
        "APPROVAL_DECISION",
        "IMPORT_COMMIT",
        "SETTINGS_UPDATE",
        "STATION_UPDATE",
        "CAMERA_UPDATE",
        "USER_UPDATE",
        "SESSIONS_REVOKED",
        "SHOP_CONNECT",
        "ORDER_OVERWRITTEN_BY_API",
    }
)


class AuditLog(Base):
    __tablename__ = "audit_log"
    __table_args__ = (
        Index(None, "at"),
        Index(None, "object_type", "object_id"),
        Index(None, "user_id", "at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    user_id: Mapped[uuid.UUID | None]
    action: Mapped[str] = mapped_column(Text)
    object_type: Mapped[str | None] = mapped_column(Text)
    object_id: Mapped[str | None] = mapped_column(Text)
    ip: Mapped[str | None] = mapped_column(INET)
    at: Mapped[datetime] = mapped_column(default=utcnow)
    data: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


def record(
    session: AsyncSession,
    action: str,
    *,
    user_id: uuid.UUID | None,
    object_type: str | None = None,
    object_id: str | uuid.UUID | None = None,
    ip: str | None = None,
    data: dict[str, Any] | None = None,
) -> AuditLog:
    """Thêm một dòng audit vào transaction hiện tại (commit cùng thao tác nghiệp vụ)."""
    if action not in ACTIONS:
        raise ValueError(f"Audit action không hợp lệ: {action}")
    entry = AuditLog(
        user_id=user_id,
        action=action,
        object_type=object_type,
        object_id=str(object_id) if object_id is not None else None,
        ip=ip,
        data=data,
    )
    session.add(entry)
    return entry
