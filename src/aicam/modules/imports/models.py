import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKey, Index, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

IMPORT_STATUSES = ("PREVIEW", "COMMITTED", "REJECTED", "EXPIRED")


class CsvImport(UUIDPk, Base):
    __tablename__ = "csv_import"
    __table_args__ = (Index(None, "created_at"), enum_check("status", IMPORT_STATUSES))

    status: Mapped[str] = mapped_column(Text, default="PREVIEW", server_default="PREVIEW")
    file_name: Mapped[str] = mapped_column(Text)
    # File gốc giữ 90 ngày, J-11 xóa (02a §7).
    file_path: Mapped[str | None] = mapped_column(Text)
    counts: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    errors: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    preview_rows: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("user.id", ondelete="RESTRICT"))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    committed_at: Mapped[datetime | None]
    expires_at: Mapped[datetime]
