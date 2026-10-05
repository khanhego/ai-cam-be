"""Schema API-80, API-81 (02 §6.2)."""

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

Days = Field(ge=1, le=365)
Minutes = Field(ge=1, le=1440)


class SettingsIn(BaseModel):
    retention_raw_days: int = Days
    retention_clip_days: int = Days
    session_warn_minutes: int = Minutes
    session_abandon_minutes: int = Minutes


class SettingsOut(SettingsIn):
    updated_at: datetime


ComponentStatus = Literal["OK", "ERROR"]


class DiskOut(BaseModel):
    total_bytes: int
    used_bytes: int
    percent: int


class CameraHealth(BaseModel):
    id: uuid.UUID
    station_name: str
    role: str
    status: str
    clock_offset_ms: int | None
    last_seen_at: datetime | None


class SyncHealth(BaseModel):
    shop_id: uuid.UUID
    last_success_at: datetime | None
    last_error: dict[str, Any] | None


class HealthOut(BaseModel):
    db: ComponentStatus
    redis: ComponentStatus
    mediamtx: ComponentStatus
    disk: DiskOut | None
    cameras: list[CameraHealth]
    sync: list[SyncHealth]
