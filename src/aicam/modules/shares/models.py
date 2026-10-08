"""Link chia sẻ bằng chứng (02 §5.1 SHARE_LINK / SHARE_ITEM, 02a §3 — migration 0006)."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    Index,
    LargeBinary,
    SmallInteger,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

SHARE_STATUSES = ("CREATING", "ACTIVE", "FAILED", "REVOKED", "EXPIRED")
SHARE_SOURCE_TYPES = ("CLAIM", "SESSION")
SHARE_LAYOUTS = ("SIDE_BY_SIDE", "CAM1")


class ShareLink(UUIDPk, Base):
    __tablename__ = "share_link"
    __table_args__ = (
        Index("uq_share_link_object_prefix", "object_prefix", unique=True),
        Index(None, "status", "expires_at"),
        Index(None, "created_by", "created_at"),
        Index(None, "claim_id"),
        Index(None, "package_id"),
        Index(None, "created_at"),
        enum_check("status", SHARE_STATUSES),
        enum_check("source_type", SHARE_SOURCE_TYPES),
        enum_check("layout", SHARE_LAYOUTS),
    )

    status: Mapped[str] = mapped_column(Text, default="CREATING", server_default="CREATING")
    source_type: Mapped[str] = mapped_column(Text)
    claim_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("claim.id", ondelete="SET NULL"))
    package_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("package.id", ondelete="RESTRICT"))
    layout: Mapped[str] = mapped_column(Text)
    include_snapshots: Mapped[bool] = mapped_column(default=True, server_default="true")
    recipient: Mapped[str] = mapped_column(Text)
    expires_at: Mapped[datetime]
    # `share/{token}/` — token 256 bit, không trả API (NFR-42).
    object_prefix: Mapped[str] = mapped_column(Text)
    url_enc: Mapped[bytes | None] = mapped_column(LargeBinary)  # Fernet
    object_keys: Mapped[list[Any]] = mapped_column(JSONB, default=list, server_default=text("'[]'::jsonb"))
    progress: Mapped[int] = mapped_column(default=0, server_default="0")
    step: Mapped[str | None] = mapped_column(Text)
    step_index: Mapped[int | None]
    step_total: Mapped[int | None]
    error_code: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    revoked_at: Mapped[datetime | None]
    revoked_by: Mapped[uuid.UUID | None]
    cloud_deleted_at: Mapped[datetime | None]
    job_started_at: Mapped[datetime | None]


class ShareItem(Base):
    __tablename__ = "share_item"
    __table_args__ = (
        Index(None, "session_id"),
        CheckConstraint("ord BETWEEN 1 AND 4", name="ord_range"),
    )

    share_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("share_link.id", ondelete="CASCADE"), primary_key=True
    )
    ord: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("session.id", ondelete="RESTRICT"))
    video_key: Mapped[str | None] = mapped_column(Text)
    video_sha256: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    # {"CAM1": sha, "CAM2": sha} — băm clip gốc dùng dựng video.
    source_sha256: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    snapshot_ids: Mapped[list[uuid.UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), default=list, server_default="{}"
    )
