"""API-180..188 — sao lưu cloud (02 §6.2). Chỉ ADMIN."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query
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


@router.get("/backup", response_model=s.BackupStatusOut)
async def get_status(_: AdminOnly, db: DbSession, settings: AppSettings) -> s.BackupStatusOut:
    """API-180 (FR-02.15, 02.17, 02.18): trạng thái sao lưu + lịch sử 14 ngày."""
    return await service.status(db, settings)


@router.put("/backup/settings", response_model=s.BackupStatusOut)
async def put_settings(
    body: s.BackupSettingsIn, p: AdminOnly, db: DbSession, settings: AppSettings
) -> s.BackupStatusOut:
    """API-181: bật / tắt, tốc độ tải (1–1000 Mbit/s), sao lưu mọi clip đóng gói."""
    return await service.update_settings(db, body, p, settings)


@router.post("/backup/confirm-key", response_model=s.BackupStatusOut)
async def confirm_key(
    body: s.ConfirmKeyIn, p: AdminOnly, db: DbSession, settings: AppSettings
) -> s.BackupStatusOut:
    """API-182 (FR-02.17): xác nhận đã cất bản sao khóa giải mã theo dấu vân tay."""
    return await service.confirm_key(db, body, p, settings)


@router.post("/backup/run-db", response_model=s.RunNowOut, status_code=202)
async def run_db(p: AdminOnly, db: DbSession, settings: AppSettings) -> s.RunNowOut:
    """API-184: sao lưu DB ngay (J-20)."""
    return await service.run_now(db, p, settings)


@router.get("/backup/issues", response_model=s.IssuesPage)
async def list_issues(
    _: AdminOnly,
    db: DbSession,
    kind: s.IssueKind | None = None,
    include_resolved: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> s.IssuesPage:
    """API-185 (EX-K6, EX-K9): lệch mã băm / không thấy tệp tại kho / lỗi tải."""
    return await service.issues(db, kind, include_resolved, page, page_size)
