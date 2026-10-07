"""API-170..176 — thông báo Telegram / Zalo OA (02 §6.2). Chỉ ADMIN (`notify.manage`)."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.notify import schemas as s
from aicam.modules.notify import service

router = APIRouter(tags=["notify"])

Admin = Annotated[Principal, Depends(require_roles("ADMIN"))]
DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.get("/notify/channels", response_model=s.ChannelsOut)
async def list_channels(_: Admin, db: DbSession, settings: AppSettings) -> s.ChannelsOut:
    """API-170 (FR-06.04, 06.07): kênh + nhà cung cấp đã cấu hình + giờ yên lặng + danh mục N01..N10."""
    return await service.list_channels(db, settings)


@router.post("/notify/channels", response_model=s.ChannelOut, status_code=201)
async def create_channel(
    body: s.ChannelCreateIn, p: Admin, db: DbSession, settings: AppSettings
) -> s.ChannelOut:
    """API-171 (FR-06.04): thêm kênh."""
    return await service.create_channel(db, body, p, settings)


@router.patch("/notify/channels/{channel_id}", response_model=s.ChannelOut)
async def update_channel(
    channel_id: uuid.UUID, body: s.ChannelUpdateIn, p: Admin, db: DbSession, settings: AppSettings
) -> s.ChannelOut:
    """API-172 (FR-06.04, 06.07): sửa kênh."""
    return await service.update_channel(db, channel_id, body, p, settings)


@router.delete("/notify/channels/{channel_id}", status_code=204)
async def delete_channel(channel_id: uuid.UUID, p: Admin, db: DbSession) -> Response:
    """API-173 (FR-06.04): xóa kênh — tin đang chờ của kênh bị bỏ."""
    await service.delete_channel(db, channel_id, p)
    return Response(status_code=204)


@router.post("/notify/channels/{channel_id}/test", response_model=s.TestSendOut)
async def test_channel(
    channel_id: uuid.UUID, p: Admin, db: DbSession, settings: AppSettings
) -> s.TestSendOut:
    """API-174 (FR-06.10): gửi thử ≤ 10 giây — 502 `NOTIFY_SEND_FAILED`, 504 `NOTIFY_TIMEOUT`."""
    return await service.test_send(db, channel_id, p, settings)


@router.get("/notify/messages", response_model=s.MessagePage)
async def list_messages(
    _: Admin,
    db: DbSession,
    channel_id: uuid.UUID | None = None,
    status: s.MessageStatus | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> s.MessagePage:
    """API-175 (FR-06.10): nhật ký gửi 30 ngày, mới nhất trước."""
    return await service.list_messages(
        db, channel_id=channel_id, status=status, page=page, page_size=page_size
    )


@router.put("/notify/quiet-hours", response_model=s.QuietHours)
async def update_quiet_hours(body: s.QuietHours, p: Admin, db: DbSession) -> s.QuietHours:
    """API-176 (FR-06.08): giờ yên lặng."""
    return await service.update_quiet_hours(db, body, p)
