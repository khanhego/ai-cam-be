"""Ghi sự kiện / cờ phiên — dùng chung `service`, `return_scan` (tránh import vòng)."""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.sessions.models import PackSession, SessionEvent


def record_event(session: AsyncSession, pack: PackSession, kind: str, **payload: Any) -> None:
    session.add(SessionEvent(session_id=pack.id, type=kind, payload=payload or None, at=clock.now()))


def set_flag(pack: PackSession, flag: str) -> None:
    if flag not in pack.flags:
        pack.flags = [*pack.flags, flag]
