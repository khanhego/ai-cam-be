"""Nạp mọi model để Base.metadata đầy đủ (Alembic, test). Không import file này từ module nghiệp vụ."""

from aicam.core.audit import AuditLog
from aicam.core.db import Base
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.backup.models import BackupObject, BackupRun
from aicam.modules.claims.models import Claim, ClaimEvidence, ClaimNote, EvidencePack
from aicam.modules.imports.models import CsvImport
from aicam.modules.media.models import Clip, Export, Snapshot, VideoSegment
from aicam.modules.notify.models import NotifyChannel, NotifyEvent, NotifyMessage, NotifyProviderToken
from aicam.modules.orders.models import Order, OrderItem, Package, PackageOrder, Shop, StatusHistory
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.sessions.models import InspectionLine, PackSession, ScanDedup, SessionEvent
from aicam.modules.settings.models import Setting
from aicam.modules.shares.models import ShareItem, ShareLink
from aicam.modules.stations.models import Camera, Station
from aicam.modules.users.models import RefreshToken, User

__all__ = [
    "ApprovalRequest",
    "AuditLog",
    "BackupObject",
    "BackupRun",
    "Base",
    "Camera",
    "Claim",
    "ClaimEvidence",
    "ClaimNote",
    "Clip",
    "CsvImport",
    "EvidencePack",
    "Export",
    "InspectionLine",
    "NotifyChannel",
    "NotifyEvent",
    "NotifyMessage",
    "NotifyProviderToken",
    "Order",
    "OrderItem",
    "PackSession",
    "Package",
    "PackageOrder",
    "ReconAlert",
    "RefreshToken",
    "ReturnCase",
    "ReturnCasePackage",
    "ScanDedup",
    "SessionEvent",
    "Setting",
    "ShareItem",
    "ShareLink",
    "Shop",
    "Snapshot",
    "Station",
    "StatusHistory",
    "User",
    "VideoSegment",
]
