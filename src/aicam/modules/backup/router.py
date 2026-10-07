"""API-180..188 — sao lưu cloud (02 §6.2). Chỉ ADMIN."""

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.backup import schemas as s
from aicam.modules.backup import service

router = APIRouter(tags=["backup"])

AdminOnly = Annotated[Principal, Depends(require_roles("ADMIN"))]
DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]


@router.post("/backup/test", response_model=s.TestOut)
async def test_connection(p: AdminOnly, db: DbSession, settings: AppSettings) -> s.TestOut:
    """API-183 (FR-02.17): kiểm tra kết nối kho lưu (ghi / đọc / xóa một tệp 1 KB, ≤ 10 giây)."""
    return await service.test_connection(db, settings, p)
