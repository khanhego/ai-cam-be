"""Hồ sơ hàng hoàn (02 §5.1 RETURN_CASE, 02a §3). Mã `HH-` + 6 số từ sequence (DEC-231)."""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, Sequence, Text, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

RETURN_KINDS = ("FAILED_DELIVERY", "BUYER_RETURN", "REFUND_ONLY", "UNANNOUNCED", "UNIDENTIFIED")
RETURN_CASE_STATUSES = (
    "EXPECTED",
    "INSPECTING",
    "PARTIALLY_RECEIVED",
    "RECEIVED_OK",
    "RECEIVED_ISSUE",
    "MISSING",
    "CANCELLED",
    "NO_PARCEL",
)
# Hồ sơ "mở": mỗi đơn tối đa một (DEC-248, partial unique).
OPEN_CASE_STATUSES = ("EXPECTED", "INSPECTING", "PARTIALLY_RECEIVED", "MISSING")
RETURN_SOURCES = ("PLATFORM", "WAREHOUSE")

_OPEN_SQL = "status IN ('EXPECTED', 'INSPECTING', 'PARTIALLY_RECEIVED', 'MISSING')"

RETURN_CASE_CODE_SEQ = Sequence("return_case_code_seq", metadata=Base.metadata)
# Kiện tạm `TAM-` + 6 số (02 §6.5 #9) — dùng bởi `returns.create_unidentified()`.
PLACEHOLDER_CODE_SEQ = Sequence("placeholder_code_seq", metadata=Base.metadata)
RETURN_CASE_CODE_DEFAULT = "'HH-' || lpad(nextval('return_case_code_seq')::text, 6, '0')"


class ReturnCase(UUIDPk, Base):
    __tablename__ = "return_case"
    __table_args__ = (
        Index("uq_return_case_code", "code", unique=True),
        Index(
            "uq_return_case_platform_return_sn",
            "platform_return_sn",
            unique=True,
            postgresql_where=text("platform_return_sn IS NOT NULL"),
        ),
        Index("uq_return_case_open_order", "order_id", unique=True, postgresql_where=text(_OPEN_SQL)),
        Index(None, "order_id"),
        Index(None, "status", "expected_since"),
        Index("ix_return_case_return_tracking_upper", text("upper(return_tracking_number)")),
        Index(
            "ix_return_case_return_tracking_upper_pattern",
            text("upper(return_tracking_number) text_pattern_ops"),
        ),
        Index("ix_return_case_signal_keys", "signal_keys", postgresql_using="gin"),
        enum_check("kind", RETURN_KINDS),
        enum_check("status", RETURN_CASE_STATUSES),
        enum_check("source", RETURN_SOURCES),
    )

    code: Mapped[str] = mapped_column(Text, server_default=text(RETURN_CASE_CODE_DEFAULT))
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("order.id", ondelete="SET NULL"))
    kind: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text)
    platform_return_sn: Mapped[str | None] = mapped_column(Text)
    platform_status: Mapped[str | None] = mapped_column(Text)
    needs_parcel: Mapped[bool | None]
    return_tracking_number: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    # Có thể chứa thông tin người mua → không log, không trả `raw_payload` qua API (02a §3).
    reason_text: Mapped[str | None] = mapped_column(Text)
    requested_items: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, default=list, server_default=text("'[]'::jsonb")
    )
    seller_due_at: Mapped[datetime | None]
    reported_at: Mapped[datetime | None]
    expected_since: Mapped[datetime | None]
    received_at: Mapped[datetime | None]
    conclusion: Mapped[str | None] = mapped_column(Text)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
    merged_into_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("return_case.id", ondelete="SET NULL")
    )
    # Khóa tín hiệu theo đợt `RETURN:{return_sn}` / `FAILED:{order_sn}:{logistics_update_time}` (DEC-267).
    signal_keys: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list, server_default="{}")
    manual_link_only: Mapped[bool] = mapped_column(default=False, server_default="false")
    force_note: Mapped[str | None] = mapped_column(Text)
    pending_merge_order_id: Mapped[uuid.UUID | None]
    # Chốt lúc mở phiên đầu (R3-10).
    single_session: Mapped[bool | None]


class ReturnCasePackage(Base):
    __tablename__ = "return_case_package"
    __table_args__ = (Index(None, "package_id"),)

    return_case_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("return_case.id", ondelete="CASCADE"), primary_key=True
    )
    package_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("package.id", ondelete="RESTRICT"), primary_key=True
    )


# API-104 tìm tiền tố mã chiều về (T-118, DEC-334, migration 0005).
Index(
    "ix_return_case_return_tracking_upper_pattern",
    func.upper(ReturnCase.return_tracking_number).label("return_tracking_upper"),
    postgresql_ops={"return_tracking_upper": "text_pattern_ops"},
)
