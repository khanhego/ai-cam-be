"""Schema API-40..46 (02 §6.2)."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

ExportLayout = Literal["CAM1", "CAM2", "SIDE_BY_SIDE"]


class PlayUrlOut(BaseModel):
    url: str
    expires_at: datetime


class HoldIn(BaseModel):
    held: bool


class UserBrief(BaseModel):
    id: uuid.UUID
    display_name: str


class HoldOut(BaseModel):
    id: uuid.UUID
    held: bool
    held_by: UserBrief | None
    held_at: datetime | None
    retention_until: datetime | None


class RebuildOut(BaseModel):
    queued: bool


class ExportIn(BaseModel):
    layout: ExportLayout


class ExportCreated(BaseModel):
    id: uuid.UUID
    status: Literal["QUEUED", "RUNNING", "READY", "FAILED"]
    progress: int


class ExportFiles(BaseModel):
    video: str
    info: str


class ExportOut(BaseModel):
    id: uuid.UUID
    session_id: uuid.UUID
    layout: ExportLayout
    status: Literal["QUEUED", "RUNNING", "READY", "FAILED"]
    progress: int
    sha256: str | None
    source_clip_sha256: dict[str, str | None]
    files: ExportFiles | None
    expires_at: datetime | None
