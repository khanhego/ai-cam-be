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
