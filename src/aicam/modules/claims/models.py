"""Hồ sơ khiếu nại, bằng chứng, ghi chú, gói bằng chứng (02 §5.1, 02a §3, BR-27). Mã `KN-` + 6 số."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    Index,
    Sequence,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

CLAIM_TYPES = (
    "DAMAGED",
    "MISSING_ITEM",
    "WRONG_ITEM",
    "EMPTY_BOX",
    "OTHER",
    "BUYER_CLAIM",
    "LOST_IN_TRANSIT",
)
COUNTERPARTIES = ("PLATFORM", "CARRIER")
CLAIM_STATUSES = ("NEW", "SUBMITTED", "WAITING", "WON", "LOST", "CLOSED")
CLAIM_SOURCES = ("AUTO_RETURN", "MANUAL", "RECON", "LEGACY_HOLD")
EVIDENCE_KINDS = ("SESSION", "SNAPSHOT")
NOTE_KINDS = ("NOTE", "STATUS_CHANGE", "SYSTEM")
PACK_STATUSES = ("QUEUED", "RUNNING", "READY", "FAILED")

CLAIM_CODE_SEQ = Sequence("claim_code_seq", metadata=Base.metadata)
CLAIM_CODE_DEFAULT = "'KN-' || lpad(nextval('claim_code_seq')::text, 6, '0')"


class Claim(UUIDPk, Base):
    __tablename__ = "claim"
    __table_args__ = (
        Index("uq_claim_code", "code", unique=True),
        # BR-27 (R-25): một hồ sơ chưa đóng mỗi (kiện, loại); LEGACY_HOLD không tính.
        Index(
            "uq_claim_open_package_type",
            "package_id",
            "type",
            unique=True,
            postgresql_where=text("status <> 'CLOSED' AND source <> 'LEGACY_HOLD'"),
        ),
        Index(None, "status", "deadline_at"),
        Index(None, "owner_user_id", "status"),
        Index(None, "package_id"),
        CheckConstraint("recovered_amount >= 0", name="recovered_amount_non_negative"),
        enum_check("type", CLAIM_TYPES),
        enum_check("counterparty", COUNTERPARTIES),
        enum_check("status", CLAIM_STATUSES),
        enum_check("source", CLAIM_SOURCES),
    )

    code: Mapped[str] = mapped_column(Text, server_default=text(CLAIM_CODE_DEFAULT))
    package_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("package.id", ondelete="RESTRICT"))
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("order.id", ondelete="SET NULL"))
    return_case_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("return_case.id", ondelete="SET NULL")
    )
    type: Mapped[str] = mapped_column(Text)
    counterparty: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, default="NEW", server_default="NEW")
    source: Mapped[str] = mapped_column(Text)
    owner_user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id"))
    deadline_at: Mapped[datetime | None]
    deadline_source: Mapped[str | None] = mapped_column(Text)
    platform_claim_ref: Mapped[str | None] = mapped_column(Text)
    recovered_amount: Mapped[int | None] = mapped_column(BigInteger)
    close_reason: Mapped[str | None] = mapped_column(Text)
    due_soon_notified_at: Mapped[datetime | None]
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("user.id"))  # null = hệ thống
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
    closed_at: Mapped[datetime | None]
    version: Mapped[int] = mapped_column(default=1, server_default="1")


class ClaimEvidence(UUIDPk, Base):
    __tablename__ = "claim_evidence"
    __table_args__ = (
        UniqueConstraint("claim_id", "session_id"),
        UniqueConstraint("claim_id", "snapshot_id"),
        Index(None, "session_id"),
        Index(None, "snapshot_id"),
        CheckConstraint("(kind = 'SESSION') = (session_id IS NOT NULL)", name="session_matches_kind"),
        CheckConstraint("(kind = 'SNAPSHOT') = (snapshot_id IS NOT NULL)", name="snapshot_matches_kind"),
        enum_check("kind", EVIDENCE_KINDS),
    )

    claim_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("claim.id", ondelete="CASCADE"))
    kind: Mapped[str] = mapped_column(Text)
    session_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("session.id", ondelete="RESTRICT"))
    snapshot_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("snapshot.id", ondelete="RESTRICT"))
    auto: Mapped[bool] = mapped_column(default=False, server_default="false")
    added_by: Mapped[uuid.UUID | None]
    added_at: Mapped[datetime] = mapped_column(default=utcnow)


class ClaimNote(UUIDPk, Base):
    """Chỉ INSERT (service không có update / delete)."""

    __tablename__ = "claim_note"
    __table_args__ = (Index(None, "claim_id", "at"), enum_check("kind", NOTE_KINDS))

    claim_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("claim.id", ondelete="CASCADE"))
    kind: Mapped[str] = mapped_column(Text)
    text: Mapped[str] = mapped_column(Text)
    author_user_id: Mapped[uuid.UUID | None]
    at: Mapped[datetime] = mapped_column(default=utcnow)


class EvidencePack(UUIDPk, Base):
    __tablename__ = "evidence_pack"
    __table_args__ = (
        Index(
            "uq_evidence_pack_active_claim",
            "claim_id",
            unique=True,
            postgresql_where=text("status IN ('QUEUED', 'RUNNING')"),
        ),
        Index(None, "created_by", "created_at"),
        enum_check("status", PACK_STATUSES),
    )

    claim_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("claim.id", ondelete="CASCADE"))
    status: Mapped[str] = mapped_column(Text, default="QUEUED", server_default="QUEUED")
    progress: Mapped[int] = mapped_column(default=0, server_default="0")
    path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    missing: Mapped[list[Any]] = mapped_column(JSONB, default=list, server_default=text("'[]'::jsonb"))
    error: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id"))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    expires_at: Mapped[datetime | None]
