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
        "SHOP_DISCONNECT",  # Phase 3 API-154
        "ORDER_OVERWRITTEN_BY_API",
        # Phase 2 (02 §6.2 API-92, §6.3 #19, §6.5 #1).
        "STATION_WORK_MODE",
        "STATION_OPERATOR",
        "INSPECTION_CORRECT",
        "RETURN_LINK_ORDER",
        "RETURN_CASE_MERGED",
        "RETURN_FORCE_NEW",
        "RECON_RESOLVE",
        "WAREHOUSE_STATUS_ADJUST",
        "CLAIM_CREATE",
        "CLAIM_UPDATE",
        "CLAIM_EVIDENCE_UPDATE",
        "EXPORT_CLAIM_PACK",
        "DOWNLOAD_CLAIM_PACK",
        "VIEW_SNAPSHOT",
        "RETENTION_REDUCED",
        "RETENTION_RAISED_TO_MINIMUM",
        "CLIP_PROTECTION_MIGRATED",
        # Phase 3 (02 API-92) — thêm dần theo task; đủ 15 + v0.3 ở T-228.
        "CLAIM_EVIDENCE_REMOVE",
        "SESSION_WRONG_SCAN_MARK",  # API-189 (v0.3)
        "SESSION_WRONG_SCAN_UNMARK",
        "SESSION_RETURN_CONFIRM",
        "PACKAGE_CANCEL_REVERT",  # BR-21 v0.4 (T-285)
        "REPORT_EXPORT",  # API-153 (T-217, FR-09.06)
        # Link chia sẻ (02 §6.2 API-160..163, J-25 — M16).
        "SHARE_CREATE",
        "SHARE_REVOKE",
        "SHARE_EXPIRE",  # J-25 (người dùng null)
        # Thiếu tệp (02 API-92 v0.4 — DEC-530, T-291).
        "MEDIA_MARK_MISSING",
        "MEDIA_MISSING_RECOVERED",
        # Sao lưu cloud (02 §6.2 API-180..188, API-186 CLI — M15).
        "BACKUP_TEST",
        "BACKUP_SETTINGS_UPDATE",
        "BACKUP_KEY_CONFIRM",
        "BACKUP_RUN_NOW",
        "BACKUP_REUPLOAD_OLD_KEY",
        "BACKUP_ISSUE_RESOLVE",
        "BACKUP_RESTORE_VERIFIED",  # CLI backup-verify đạt (người dùng null)
        "BACKUP_VERIFY_ACCEPT",  # CLI backup-verify --accept (v0.3, DEC-518)
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
