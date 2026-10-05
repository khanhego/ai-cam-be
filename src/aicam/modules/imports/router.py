"""API-50..54 — nhập đơn từ file (02 §6 "API-50 / API-51")."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.pagination import Page
from aicam.core.settings import Settings, get_settings
from aicam.modules.imports import parser, service
from aicam.modules.imports.schemas import ImportCommitOut, ImportItem, ImportPreviewOut

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Manager = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]

router = APIRouter(tags=["imports"])


@router.post("/imports", response_model=ImportPreviewOut, status_code=status.HTTP_201_CREATED)
async def upload(file: UploadFile, p: Manager, db: DbSession, settings: AppSettings) -> ImportPreviewOut:
    """API-50: multipart field `file` (.csv UTF-8 / .xlsx, ≤ 5 MB, ≤ 5.000 dòng) → bản xem trước 30 phút."""
    content = await file.read(parser.MAX_BYTES + 1)
    return await service.upload(db, file_name=file.filename or "", content=content, p=p, settings=settings)


@router.post("/imports/{import_id}/commit", response_model=ImportCommitOut)
async def commit(import_id: uuid.UUID, p: Manager, db: DbSession, settings: AppSettings) -> ImportCommitOut:
    """API-51: chỉ người tạo; từ chối khi còn dòng lỗi / quá 30 phút (EX-P11)."""
    return await service.commit_import(db, import_id, p, settings)


@router.get("/imports", response_model=Page[ImportItem])
async def history(
    _: Manager,
    db: DbSession,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> Page[ImportItem]:
    """API-52: lịch sử nhập, mới nhất trước."""
    return await service.history(db, page, page_size)


@router.get("/imports/template", response_class=FileResponse)
async def template(_: Manager) -> FileResponse:
    """API-53: file mẫu (UTF-8 có BOM để Excel đọc đúng tiếng Việt)."""
    return FileResponse(service.TEMPLATE, media_type="text/csv; charset=utf-8", filename="mau-nhap-don.csv")


@router.get("/imports/{import_id}/file", response_class=FileResponse)
async def original_file(
    import_id: uuid.UUID, _: Manager, db: DbSession, settings: AppSettings
) -> FileResponse:
    """API-54: tải file gốc (giữ 90 ngày, sau đó 410 FILE_EXPIRED)."""
    path, name = await service.original_file(db, import_id, settings)
    media = parser.EXTENSIONS.get(path.suffix.lower(), "application/octet-stream")
    return FileResponse(path, media_type=media, filename=name)
