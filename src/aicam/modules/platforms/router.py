"""API-70..73 — kết nối Shopee (02 §6 "API-70..73", FR-05.01, 05.02)."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.platforms import service
from aicam.modules.platforms.base import PlatformAdapter
from aicam.modules.platforms.schemas import AuthUrlOut, QueuedOut, ShopList

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
AdminOnly = Annotated[Principal, Depends(require_roles("ADMIN"))]


def get_platform_adapter(settings: AppSettings) -> PlatformAdapter:
    return service.get_adapter(settings)


Adapter = Annotated[PlatformAdapter, Depends(get_platform_adapter)]

router = APIRouter(tags=["shops"])


@router.get("/shops", response_model=ShopList)
async def list_shops(_: AdminOnly, db: DbSession, settings: AppSettings) -> ShopList:
    """API-70: trạng thái kết nối sàn."""
    return ShopList(items=await service.list_shops(db, settings))


@router.post("/shops/shopee/auth-url", response_model=AuthUrlOut)
async def auth_url(p: AdminOnly, adapter: Adapter, settings: AppSettings, response: Response) -> AuthUrlOut:
    """API-71: URL ủy quyền Shopee; `state` chống CSRF lưu Redis 10 phút + cookie HttpOnly băm `state` gắn
    trình duyệt (G3-N7). Chưa cấu hình → 503."""
    url, state = await service.auth_url(adapter, settings, p.user_id)
    response.set_cookie(
        service.STATE_COOKIE,
        service.state_fingerprint(state),
        max_age=service.STATE_TTL_S,
        path=service.CALLBACK_PATH,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",  # Shopee chuyển hướng về bằng điều hướng GET cấp cao nhất: Lax vẫn gửi cookie
    )
    return AuthUrlOut(url=url)


@router.get("/shops/shopee/callback", response_class=RedirectResponse, status_code=status.HTTP_302_FOUND)
async def callback(
    request: Request,
    db: DbSession,
    adapter: Adapter,
    settings: AppSettings,
    state: str | None = None,
    code: str | None = None,
    shop_id: str | None = None,
) -> RedirectResponse:
    """API-72 (công khai + `state`): đổi code → token, lưu shop, J-04 ngay; 302 về D7 `?result=`."""
    result = await service.handle_callback(
        db, adapter, settings, state=state, code=code, shop_id=shop_id,
        ip=request.client.host if request.client else None,
        state_cookie=request.cookies.get(service.STATE_COOKIE),
    )  # fmt: skip
    response = RedirectResponse(f"{service.RESULT_PATH}?result={result}", status_code=status.HTTP_302_FOUND)
    response.delete_cookie(service.STATE_COOKIE, path=service.CALLBACK_PATH)
    return response


@router.post("/shops/{shop_id}/sync", response_model=QueuedOut, status_code=status.HTTP_202_ACCEPTED)
async def sync(shop_id: uuid.UUID, _: AdminOnly, db: DbSession, settings: AppSettings) -> QueuedOut:
    """API-73: đồng bộ ngay (J-04 queue `sync`); đang chạy → 409 SYNC_IN_PROGRESS."""
    await service.request_sync(db, shop_id, settings)
    return QueuedOut()
