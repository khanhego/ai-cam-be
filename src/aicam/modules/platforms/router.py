"""API-70..73, 154..156 — kết nối sàn (02 §6.2 "API-70 mở rộng", "API-71 / API-72 / API-155 / API-154";
FR-05.01, 05.02, 05.13, 05.14, 05.20)."""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response, status
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import Principal, require_roles
from aicam.core.settings import Settings, get_settings
from aicam.modules.platforms import connect, registry, service
from aicam.modules.platforms.base import PlatformAdapter
from aicam.modules.platforms.schemas import AuthUrlOut, QueuedOut, ShopBriefList, ShopList, ShopOut

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
AdminOnly = Annotated[Principal, Depends(require_roles("ADMIN"))]
Viewer = Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR", "CSKH"))]


def get_platform_adapter(settings: AppSettings) -> PlatformAdapter:
    """Adapter Shopee (tên giữ từ Phase 1 — test / station thay bằng mock qua `dependency_overrides`)."""
    return service.get_adapter(settings)


def get_tiktok_adapter(settings: AppSettings) -> PlatformAdapter:
    return registry.adapter_for(registry.TIKTOK, settings)


Adapter = Annotated[PlatformAdapter, Depends(get_platform_adapter)]
TikTokAdapter = Annotated[PlatformAdapter, Depends(get_tiktok_adapter)]

router = APIRouter(tags=["shops"])


def _pick(platform: str, shopee: PlatformAdapter, tiktok: PlatformAdapter) -> PlatformAdapter:
    return tiktok if platform == registry.TIKTOK else shopee


@router.get("/shops", response_model=ShopList)
async def list_shops(_: AdminOnly, db: DbSession, settings: AppSettings) -> ShopList:
    """API-70: cấu hình từng sàn + mọi shop (kể cả đã ngắt)."""
    return await connect.list_shops(db, settings)


@router.get("/shops/brief", response_model=ShopBriefList)
async def shops_brief(_: Viewer, db: DbSession) -> ShopBriefList:
    """API-156: danh sách shop rút gọn cho bộ lọc sàn / shop (D3, D14, D15, D16, D20)."""
    return await connect.brief(db)


@router.post("/shops/{platform}/auth-url", response_model=AuthUrlOut)
async def auth_url(
    platform: str,
    p: AdminOnly,
    db: DbSession,
    shopee: Adapter,
    tiktok: TikTokAdapter,
    settings: AppSettings,
    response: Response,
) -> AuthUrlOut:
    """API-71 (`shopee` | `tiktok`): URL ủy quyền; `state` chống CSRF lưu Redis 10 phút + cookie HttpOnly băm
    `state` gắn trình duyệt (G3-N7). Sàn lạ → 404; chưa cấu hình → 503."""
    code = connect.parse_platform(platform)
    url, state = await connect.auth_url(db, _pick(code, shopee, tiktok), settings, code, p.user_id)
    response.set_cookie(
        connect.state_cookie(code),
        connect.state_fingerprint(state),
        max_age=connect.STATE_TTL_S,
        path=connect.callback_path(code),
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",  # sàn chuyển hướng về bằng điều hướng GET cấp cao nhất: Lax vẫn gửi cookie
    )
    return AuthUrlOut(url=url)


async def _callback(
    platform: str,
    request: Request,
    db: AsyncSession,
    adapter: PlatformAdapter,
    settings: Settings,
    state: str | None,
    code: str | None,
    shop_id: str | None,
) -> RedirectResponse:
    out = await connect.handle_callback(
        db, platform, adapter, settings, state=state, code=code, shop_id=shop_id,
        ip=request.client.host if request.client else None,
        state_cookie=request.cookies.get(connect.state_cookie(platform)),
    )  # fmt: skip
    query = f"platform={platform.lower()}&result={out.result}"
    if out.result == "connected":
        query += f"&count={out.count}"
    response = RedirectResponse(f"{connect.RESULT_PATH}?{query}", status_code=status.HTTP_302_FOUND)
    response.delete_cookie(connect.state_cookie(platform), path=connect.callback_path(platform))
    return response


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
    """API-72 (công khai + `state`): đổi code → token, lưu shop, J-04 ngay; 302 về D7
    `?platform&result&count`."""
    return await _callback(registry.SHOPEE, request, db, adapter, settings, state, code, shop_id)


@router.get("/shops/tiktok/callback", response_class=RedirectResponse, status_code=status.HTTP_302_FOUND)
async def tiktok_callback(
    request: Request,
    db: DbSession,
    adapter: TikTokAdapter,
    settings: AppSettings,
    state: str | None = None,
    code: str | None = None,
) -> RedirectResponse:
    """API-155 (công khai + `state`): TikTok gửi `code`, `state` (tham số khác bỏ qua) → mọi shop của lần ủy
    quyền được thêm / cập nhật; 302 về D7."""
    return await _callback(registry.TIKTOK, request, db, adapter, settings, state, code, None)


@router.post("/shops/{shop_id}/sync", response_model=QueuedOut, status_code=status.HTTP_202_ACCEPTED)
async def sync(shop_id: uuid.UUID, _: AdminOnly, db: DbSession, settings: AppSettings) -> QueuedOut:
    """API-73: đồng bộ ngay (J-04 một shop); đang chạy → 409 SYNC_IN_PROGRESS; sàn tắt → 503."""
    await connect.request_sync(db, shop_id, settings)
    return QueuedOut()


@router.post("/shops/{shop_id}/disconnect", response_model=ShopOut)
async def disconnect(
    shop_id: uuid.UUID, p: AdminOnly, request: Request, db: DbSession, settings: AppSettings
) -> ShopOut:
    """API-154: ngắt một shop (idempotent); shop khác không đổi; dữ liệu giữ nguyên (EX-T7)."""
    return await connect.disconnect(
        db, shop_id, p.user_id, request.client.host if request.client else None, settings
    )
