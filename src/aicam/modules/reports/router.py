"""API-32 — 02 §6.2."""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.reports import service

router = APIRouter(tags=["reports"])


@router.get("/reports/daily", response_model=service.DailyOut)
async def daily(
    p: Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))],
    db: Annotated[AsyncSession, Depends(get_session)],
    settings: Annotated[Settings, Depends(get_settings)],
    date: date | None = None,
) -> service.DailyOut:
    """Số liệu ngày (giờ VN) + trạng thái station + mục cần xử lý (FR-09.01)."""
    return service.for_role(await service.daily(db, date, settings), p.role)
