"""Đẩy job media vào Celery (J-01 queue `video`, J-03 queue `export`) — luôn sau commit (02a §6).

Không import `aicam.workers` ở mức module (tránh vòng import; api chỉ cần gửi message). Test thay `sender`
bằng `set_sender()` để không gửi message thật lên broker của stack dev.
"""

import asyncio
import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.db import after_commit
from aicam.core.settings import get_settings

BUILD_CLIPS = "media.build_session_clips"
RENDER_EXPORT = "media.render_export"
FLAG_ORDER_CANCELLED = "sessions.flag_order_cancelled"
CAPTURE_PACK_SNAPSHOT = "media.capture_pack_snapshot"

Sender = Callable[[str, list[Any], str, float], None]


def _celery_send(task: str, args: list[Any], queue: str, countdown: float) -> None:
    from aicam.workers.celery_app import app

    app.send_task(task, args=args, queue=queue, countdown=max(0.0, countdown))


_sender: Sender = _celery_send


def set_sender(sender: Sender | None) -> None:
    """Chỉ dùng trong test. None → về gửi Celery thật."""
    global _sender
    _sender = sender or _celery_send


async def send(task: str, args: list[Any], queue: str, countdown: float = 0.0) -> None:
    await asyncio.to_thread(_sender, task, args, queue, countdown)


def clip_build_delay(ended_at: datetime) -> float:
    """Giây chờ tới khi video phủ `ended_at + đệm` đã được MediaMTX ghi ra đĩa (spike S3)."""
    settings = get_settings()
    ready = ended_at.timestamp() + settings.clip_padding_s + settings.clip_settle_s
    return max(0.0, ready - clock.now().timestamp())


def enqueue_build_clips(session: AsyncSession, session_id: uuid.UUID, ended_at: datetime) -> None:
    """J-01 sau khi phiên đóng / hủy / bỏ dở / cắt lại; chạy sau commit."""

    async def _send() -> None:
        await send(BUILD_CLIPS, [str(session_id)], "video", clip_build_delay(ended_at))

    after_commit(session, _send)


def enqueue_render_export(session: AsyncSession, export_id: uuid.UUID) -> None:
    async def _send() -> None:
        await send(RENDER_EXPORT, [str(export_id)], "export")

    after_commit(session, _send)


def enqueue_capture_pack_snapshot(session: AsyncSession, session_id: uuid.UUID) -> None:
    """J-17 (queue `video`) sau khi J-01 cắt xong clip Cam 1 của phiên PACK `COMPLETED` (DEC-227)."""

    async def _send() -> None:
        await send(CAPTURE_PACK_SNAPSHOT, [str(session_id)], "video")

    after_commit(session, _send)


def enqueue_flag_order_cancelled(session: AsyncSession, package_id: uuid.UUID) -> None:
    """BR-21 (DEC-266): đơn hủy khi kiện `PACKING` → task riêng gắn cờ phiên, sau commit của job đồng bộ."""

    async def _send() -> None:
        await send(FLAG_ORDER_CANCELLED, [str(package_id)], "default")

    after_commit(session, _send)
