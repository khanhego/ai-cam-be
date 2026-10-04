"""Nạp mọi model để Base.metadata đầy đủ (Alembic, test). Không import file này từ module nghiệp vụ."""

from aicam.core.audit import AuditLog
from aicam.core.db import Base
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.imports.models import CsvImport
from aicam.modules.media.models import Clip, Export, VideoSegment
from aicam.modules.orders.models import Order, OrderItem, Package, Shop, StatusHistory
from aicam.modules.sessions.models import PackSession, ScanDedup, SessionEvent
from aicam.modules.settings.models import Setting
from aicam.modules.stations.models import Camera, Station
from aicam.modules.users.models import RefreshToken, User

__all__ = [
    "ApprovalRequest",
    "AuditLog",
    "Base",
    "Camera",
    "Clip",
    "CsvImport",
    "Export",
    "Order",
    "OrderItem",
    "PackSession",
    "Package",
    "RefreshToken",
    "ScanDedup",
    "SessionEvent",
    "Setting",
    "Shop",
    "Station",
    "StatusHistory",
    "User",
    "VideoSegment",
]
