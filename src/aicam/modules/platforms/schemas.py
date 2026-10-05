"""Schema API-70..73 (02 §6 "API-70..73")."""

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ShopOut(BaseModel):
    id: uuid.UUID
    platform: str
    name: str | None
    auth_status: str
    auth_expires_at: datetime | None
    last_synced_at: datetime | None
    today_synced_orders: int
    last_error: dict[str, Any] | None


class ShopList(BaseModel):
    items: list[ShopOut]


class AuthUrlOut(BaseModel):
    url: str


class QueuedOut(BaseModel):
    queued: bool = True
