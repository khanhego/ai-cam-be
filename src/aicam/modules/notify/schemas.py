"""Schema API-170..176 (02 §6.2 "API-170..176 — thông báo"). Kiểm giá trị (tên, Chat ID, sự kiện, giờ) ở
service để trả 422 `fields` tiếng Việt đúng chữ 02 (như module `shares`)."""

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from aicam.core.pagination import Page
from aicam.modules.notify.catalog import ChannelType, EventCode, Severity

ChannelLastStatus = Literal["OK", "ERROR", "NEVER"]
MessageStatus = Literal["QUEUED", "HELD", "SENT", "RETRYING", "DROPPED", "SKIPPED"]


class ProviderState(BaseModel):
    configured: bool


class Providers(BaseModel):
    TELEGRAM: ProviderState
    ZALO_OA: ProviderState


class QuietHours(BaseModel):
    enabled: bool
    start: str  # "HH:MM" giờ Việt Nam
    end: str


class EventInfo(BaseModel):
    code: EventCode
    label: str
    severity: Severity
    suggested_channel: str | None


class ChannelError(BaseModel):
    code: str
    message: str
    at: datetime | None = None
    provider_code: str | None = None


class ChannelOut(BaseModel):
    id: uuid.UUID
    name: str
    type: ChannelType
    target: str
    events: list[EventCode]
    enabled: bool
    last_status: ChannelLastStatus
    last_sent_at: datetime | None
    last_error: ChannelError | None
    created_at: datetime


class ChannelsOut(BaseModel):
    """API-170."""

    providers: Providers
    quiet_hours: QuietHours
    events: list[EventInfo]
    items: list[ChannelOut]


class ChannelCreateIn(BaseModel):
    """API-171."""

    name: str = Field(max_length=200)
    type: ChannelType
    target: str = Field(max_length=200)
    events: list[str] = Field(default_factory=list, max_length=20)
    enabled: bool = True


class ChannelUpdateIn(BaseModel):
    """API-172 — mọi trường tùy chọn như API-171."""

    name: str | None = Field(default=None, max_length=200)
    type: ChannelType | None = None
    target: str | None = Field(default=None, max_length=200)
    events: list[str] | None = Field(default=None, max_length=20)
    enabled: bool | None = None


class TestSendOut(BaseModel):
    """API-174."""

    ok: Literal[True]
    sent_at: datetime


class ChannelBrief(BaseModel):
    id: uuid.UUID
    name: str


class MessageOut(BaseModel):
    id: uuid.UUID
    channel: ChannelBrief
    event_code: EventCode
    event_label: str
    item_count: int
    text: str | None
    status: MessageStatus
    attempts: int
    last_error: str | None
    created_at: datetime
    sent_at: datetime | None
    next_attempt_at: datetime | None


class MessagePage(Page[MessageOut]):
    """API-175 — 30 ngày, mới nhất trước."""
