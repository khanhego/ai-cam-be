"""Schema API-70..73, 154..156 (02 §6.2 "API-70 mở rộng", "API-71 / API-72 / API-155 / API-154")."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class SyncWarning(BaseModel):
    code: str  # TRACKING_OWNED_BY_OTHER_SHOP (EX-T2)
    message: str
    at: datetime | None = None
    tracking_number: str | None = None
    order_sn: str | None = None


class ShopOut(BaseModel):
    id: uuid.UUID
    platform: str
    name: str | None
    auth_status: str
    auth_expires_at: datetime | None
    last_synced_at: datetime | None
    today_synced_orders: int
    last_error: dict[str, Any] | None
    # Phase 3 (02 §6.2 API-70).
    region: str | None = None
    sync_warnings: list[SyncWarning] = []
    disconnected_at: datetime | None = None
    sync_in_progress: bool = False


class PlatformInfo(BaseModel):
    platform: str
    enabled: bool
    returns_enabled: bool
    configured: bool


class ShopList(BaseModel):
    platforms: list[PlatformInfo]
    items: list[ShopOut]


class ShopBrief(BaseModel):
    id: uuid.UUID
    platform: str
    name: str | None
    auth_status: str


class ShopBriefList(BaseModel):
    items: list[ShopBrief]


class AuthUrlOut(BaseModel):
    url: str


class QueuedOut(BaseModel):
    queued: bool = True
