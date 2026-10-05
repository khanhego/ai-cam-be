import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Index, LargeBinary, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from aicam.core.db import Base, UUIDPk, enum_check, utcnow

PLATFORMS = ("SHOPEE",)
AUTH_STATUSES = ("CONNECTED", "EXPIRED", "DISCONNECTED")
ORDER_SOURCES = ("API", "CSV")
# 02 §5 (MISMATCH là trạng thái phiên, không phải kiện — DEC-24).
WAREHOUSE_STATUSES = (
    "NEW",
    "PACKING",
    "PACKED",
    "HANDED_OVER",
    "DELIVERED",
    "CANCELLED",
    "CANCELLED_AFTER_PACK",
    # Phase 2 (02 §5.2, migration 0003).
    "RETURN_EXPECTED",
    "RETURN_INSPECTING",
    "RETURN_RECEIVED_OK",
    "RETURN_RECEIVED_ISSUE",
    "RETURN_MISSING",
)
RETURN_STATUSES = tuple(s for s in WAREHOUSE_STATUSES if s.startswith("RETURN_"))
HISTORY_SOURCES = ("PLATFORM", "WAREHOUSE", "MANUAL")


class Shop(UUIDPk, Base):
    __tablename__ = "shop"
    __table_args__ = (
        UniqueConstraint("platform", "platform_shop_id"),
        enum_check("platform", PLATFORMS),
        enum_check("auth_status", AUTH_STATUSES),
    )

    platform: Mapped[str] = mapped_column(Text)
    platform_shop_id: Mapped[str] = mapped_column(Text)
    name: Mapped[str | None] = mapped_column(Text)
    auth_status: Mapped[str] = mapped_column(Text, default="DISCONNECTED", server_default="DISCONNECTED")
    access_token_enc: Mapped[bytes | None] = mapped_column(LargeBinary)
    refresh_token_enc: Mapped[bytes | None] = mapped_column(LargeBinary)
    auth_expires_at: Mapped[datetime | None]
    last_synced_at: Mapped[datetime | None]
    last_sync_cursor: Mapped[datetime | None]
    last_error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_return_cursor: Mapped[datetime | None]  # J-13 (0003)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())


class Order(UUIDPk, Base):
    __tablename__ = "order"
    __table_args__ = (enum_check("source", ORDER_SOURCES),)

    shop_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("shop.id", ondelete="SET NULL"))
    # MVP một shop: mã đơn duy nhất toàn cục (DEC-12).
    platform_order_sn: Mapped[str] = mapped_column(Text, unique=True)
    platform_status: Mapped[str | None] = mapped_column(Text)
    buyer_note: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text, default="API", server_default="API")
    csv_import_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("csv_import.id", ondelete="SET NULL"))
    created_at_platform: Mapped[datetime | None]
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())


class OrderItem(UUIDPk, Base):
    __tablename__ = "order_item"
    __table_args__ = (Index(None, "order_id"), CheckConstraint("quantity > 0", name="quantity_positive"))

    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("order.id", ondelete="CASCADE"))
    sku: Mapped[str | None] = mapped_column(Text)
    product_name: Mapped[str] = mapped_column(Text)
    variation: Mapped[str | None] = mapped_column(Text)
    quantity: Mapped[int]
    image_url: Mapped[str | None] = mapped_column(Text)


class Package(UUIDPk, Base):
    __tablename__ = "package"
    __table_args__ = (
        Index("uq_package_tracking_number_upper", text("upper(tracking_number)"), unique=True),
        Index(None, "warehouse_status", "updated_at"),
        # BR-12, BR-14 (DEC-225, DEC-255): mốc vào trạng thái hiện tại.
        Index(None, "warehouse_status", "status_changed_at"),
        enum_check("warehouse_status", WAREHOUSE_STATUSES),
    )

    # null khi kiện chưa xác minh (BR-04).
    order_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("order.id", ondelete="SET NULL"))
    tracking_number: Mapped[str] = mapped_column(Text)
    warehouse_status: Mapped[str] = mapped_column(Text, default="NEW", server_default="NEW")
    platform_logistics_status: Mapped[str | None] = mapped_column(Text)
    verified: Mapped[bool] = mapped_column(default=True, server_default="true")
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow, server_default=func.now())
    # 0003 (DEC-225): `status_changed_at` đổi trong `orders.transition()`; backfill từ `status_history`.
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    status_changed_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    # Kiện tạm `TAM-…` của phiên hoàn chưa xác định (02 §6.3 #2, DEC-260).
    is_placeholder: Mapped[bool] = mapped_column(default=False, server_default="false")


class StatusHistory(UUIDPk, Base):
    __tablename__ = "status_history"
    __table_args__ = (Index(None, "package_id", "at"), enum_check("source", HISTORY_SOURCES))

    package_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("package.id", ondelete="CASCADE"))
    source: Mapped[str] = mapped_column(Text)
    from_status: Mapped[str | None] = mapped_column(Text)
    to_status: Mapped[str] = mapped_column(Text)
    at: Mapped[datetime] = mapped_column(default=utcnow)
    actor_user_id: Mapped[uuid.UUID | None]
    actor_label: Mapped[str | None] = mapped_column(Text)
