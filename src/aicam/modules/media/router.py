"""API-40..46 — 02 §6.2."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.media import exports, service
from aicam.modules.media.schemas import (
    ExportCreated,
    ExportIn,
    ExportOut,
    HoldIn,
    HoldOut,
    PlayUrlOut,
    RebuildOut,
)

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
Viewer = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH", "STATION"))]
Staff = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]
Manager = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))]

router = APIRouter(tags=["media"])


@router.get("/clips/{clip_id}/play-url", response_model=PlayUrlOut)
async def play_url(clip_id: uuid.UUID, p: Viewer, db: DbSession, settings: AppSettings) -> PlayUrlOut:
    """API-40: URL phát ký HMAC, hạn 10 phút. STATION: chỉ phiên station mình trong ngày."""
    return await service.play_url(db, clip_id, p, settings)


@router.get("/media/clips/{clip_id}", response_class=FileResponse)
async def clip_media(
    clip_id: uuid.UUID,
    request: Request,
    db: DbSession,
    settings: AppSettings,
    uid: Annotated[uuid.UUID, Query()],
    exp: Annotated[int, Query()],
    sig: Annotated[str, Query(max_length=128)],
) -> FileResponse:
    """API-41: video/mp4 hỗ trợ Range (206); xác thực bằng chữ ký, không cần Bearer."""
    path = await service.open_clip_media(
        db, clip_id, uid=uid, exp=exp, sig=sig, range_header=request.headers.get("range"),
        ip=request.client.host if request.client else None, settings=settings,
    )  # fmt: skip
    return FileResponse(path, media_type="video/mp4", headers={"Cache-Control": "private, max-age=600"})


@router.put("/clips/{clip_id}/hold", response_model=HoldOut)
async def hold(clip_id: uuid.UUID, body: HoldIn, p: Staff, db: DbSession) -> HoldOut:
    """API-42: giữ / bỏ giữ clip (FR-02.09)."""
    return await service.set_hold(db, clip_id, body.held, p)


@router.post("/sessions/{session_id}/clips/rebuild", response_model=RebuildOut, status_code=202)
async def rebuild(session_id: uuid.UUID, p: Manager, db: DbSession) -> RebuildOut:
    """API-46: cắt lại clip FAILED."""
    return await service.rebuild(db, session_id, p)


@router.post("/sessions/{session_id}/exports", response_model=ExportCreated, status_code=202)
async def create_export(
    session_id: uuid.UUID, body: ExportIn, p: Staff, db: DbSession, settings: AppSettings
) -> ExportCreated:
    """API-43: tạo bản xuất (bất đồng bộ, J-03)."""
    return await exports.create_export(db, session_id, body.layout, p, settings)


@router.get("/exports/{export_id}", response_model=ExportOut)
async def get_export(export_id: uuid.UUID, p: Staff, db: DbSession, settings: AppSettings) -> ExportOut:
    """API-44: trạng thái + link tải ký HMAC (người tạo hoặc ADMIN)."""
    return await exports.get_export(db, export_id, p, settings)


@router.get("/media/exports/{export_id}/{file}", response_class=FileResponse)
async def export_media(
    export_id: uuid.UUID,
    file: str,
    request: Request,
    db: DbSession,
    settings: AppSettings,
    uid: Annotated[uuid.UUID, Query()],
    exp: Annotated[int, Query()],
    sig: Annotated[str, Query(max_length=128)],
) -> FileResponse:
    """API-45: tải `video.mp4` / `info.json` bằng chữ ký."""
    path, filename = await exports.open_export_file(
        db, export_id, file, uid=uid, exp=exp, sig=sig, range_header=request.headers.get("range"),
        ip=request.client.host if request.client else None, settings=settings,
    )  # fmt: skip
    media_type = "video/mp4" if file == "video.mp4" else "application/json"
    return FileResponse(path, media_type=media_type, filename=filename)
