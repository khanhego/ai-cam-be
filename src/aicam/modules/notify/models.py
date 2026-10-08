"""Thông báo (02 §5.1 NOTIFY_*, 02a §3 — migration 0006; BR-36, DEC-443, DEC-445)."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Index, LargeBinary, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

CHANNEL_TYPES = ("TELEGRAM", "ZALO_OA")
CHANNEL_LAST_STATUSES = ("OK", "ERROR", "NEVER")
SEVERITIES = ("HIGH", "MEDIUM", "INFO")
MESSAGE_STATUSES = ("QUEUED", "HELD", "SENT", "RETRYING", "DROPPED", "SKIPPED")
TOKEN_PROVIDERS = ("ZALO_OA",)


class NotifyChannel(UUIDPk, Base):
    __tablename__ = "notify_channel"
    __table_args__ = (
        Index("uq_notify_channel_lower_name", text("lower(name)"), unique=True),
        CheckConstraint("cardinality(events) >= 1", name="events_not_empty"),
        enum_check("type", CHANNEL_TYPES),
        enum_check("last_status", CHANNEL_LAST_STATUSES),
    )

    name: Mapped[str] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text)
    target: Mapped[str] = mapped_column(Text)
    events: Mapped[list[str]] = mapped_column(ARRAY(Text))
    enabled: Mapped[bool] = mapped_column(default=True, server_default="true")
    last_status: Mapped[str] = mapped_column(Text, default="NEVER", server_default="NEVER")
    last_sent_at: Mapped[datetime | None]
    last_error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id"))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())


class NotifyEvent(UUIDPk, Base):
    """Sự kiện nội bộ — `data` không chứa dữ liệu người mua (FR-06.09)."""

    __tablename__ = "notify_event"
    __table_args__ = (
        UniqueConstraint("code", "dedupe_key"),
        Index(
            "ix_notify_event_unprocessed_occurred_at",
            "occurred_at",
            postgresql_where=text("processed_at IS NULL"),
        ),
        enum_check("severity", SEVERITIES),
    )

    code: Mapped[str] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(Text)
    dedupe_key: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(default=utcnow)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict, server_default=text("'{}'::jsonb"))
    processed_at: Mapped[datetime | None]


class NotifyMessage(UUIDPk, Base):
    __tablename__ = "notify_message"
    __table_args__ = (
        Index(None, "status", "send_after"),
        Index(None, "channel_id", "created_at"),
        Index(None, "channel_id", "sent_at"),
        enum_check("status", MESSAGE_STATUSES),
        enum_check("severity", SEVERITIES),
    )

    channel_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("notify_channel.id", ondelete="CASCADE"))
    event_code: Mapped[str] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="QUEUED", server_default="QUEUED")
    items: Mapped[list[Any]] = mapped_column(JSONB, default=list, server_default=text("'[]'::jsonb"))
    item_count: Mapped[int] = mapped_column(default=0, server_default="0")
    text: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    send_after: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    sent_at: Mapped[datetime | None]
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    next_attempt_at: Mapped[datetime | None]
    last_error: Mapped[str | None] = mapped_column(Text)


class NotifyProviderToken(Base):
    """Token Zalo OA xoay vòng — mã hóa Fernet (DEC-445)."""

    __tablename__ = "notify_provider_token"
    __table_args__ = (enum_check("provider", TOKEN_PROVIDERS),)

    provider: Mapped[str] = mapped_column(Text, primary_key=True)
    access_token_enc: Mapped[bytes | None] = mapped_column(LargeBinary)
    refresh_token_enc: Mapped[bytes | None] = mapped_column(LargeBinary)
    expires_at: Mapped[datetime | None]
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
