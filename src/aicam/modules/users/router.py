"""API-01..04 (auth), API-90..92 (tài khoản, nhật ký) — 02 §6."""

import uuid
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, Query, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.db import get_session
from aicam.core.deps import CurrentPrincipal, Principal, require_roles
from aicam.core.pagination import Page, PageParams
from aicam.core.settings import Settings, get_settings
from aicam.modules.users import service
from aicam.modules.users.schemas import (
    AuditLogOut,
    LoginIn,
    LoginOut,
    MeOut,
    RefreshIn,
    Role,
    TokenOut,
    UserCreateIn,
    UserListItem,
    UserPatchIn,
)

DbSession = Annotated[AsyncSession, Depends(get_session)]
AppSettings = Annotated[Settings, Depends(get_settings)]
AdminOnly = Annotated[Principal, Depends(require_roles("ADMIN"))]

router = APIRouter()


def _client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


def _set_refresh_cookie(
    response: Response, client: str, issued: service.IssuedRefresh, settings: Settings
) -> None:
    response.set_cookie(
        service.COOKIE_NAMES[client],
        issued.token,
        max_age=issued.max_age,
        path=service.COOKIE_PATH,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="strict",
    )


@router.post("/auth/login", response_model=LoginOut, tags=["auth"])
async def login(
    body: LoginIn, request: Request, response: Response, db: DbSession, settings: AppSettings
) -> LoginOut:
    out, issued = await service.login(
        db,
        username=body.username,
        password=body.password,
        client=body.client,
        ip=_client_ip(request),
        settings=settings,
    )
    _set_refresh_cookie(response, body.client, issued, settings)
    return out


@router.post("/auth/refresh", response_model=TokenOut, tags=["auth"])
async def refresh(
    body: RefreshIn,
    response: Response,
    db: DbSession,
    settings: AppSettings,
    rt_station: Annotated[str | None, Cookie()] = None,
    rt_dashboard: Annotated[str | None, Cookie()] = None,
) -> TokenOut:
    token = rt_station if body.client == "STATION" else rt_dashboard
    out, issued = await service.refresh(db, token=token, client=body.client, settings=settings)
    _set_refresh_cookie(response, body.client, issued, settings)
    return out


@router.post("/auth/logout", status_code=204, tags=["auth"])
async def logout(
    principal: CurrentPrincipal,
    response: Response,
    db: DbSession,
    rt_station: Annotated[str | None, Cookie()] = None,
    rt_dashboard: Annotated[str | None, Cookie()] = None,
) -> Response:
    client = "STATION" if principal.role == "STATION" else "DASHBOARD"
    await service.logout(db, rt_station if client == "STATION" else rt_dashboard)
    response.status_code = 204
    response.delete_cookie(service.COOKIE_NAMES[client], path=service.COOKIE_PATH)
    return response


@router.get("/me", response_model=MeOut, tags=["auth"])
async def me(principal: CurrentPrincipal, db: DbSession) -> MeOut:
    return await service.me(db, principal.user_id)


@router.get("/users", response_model=Page[UserListItem], tags=["users"])
async def list_users(
    _: AdminOnly,
    db: DbSession,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    role: Role | None = None,
) -> Page[UserListItem]:
    return await service.list_users(db, PageParams(page=page, page_size=page_size), role)


@router.post("/users", response_model=UserListItem, status_code=201, tags=["users"])
async def create_user(body: UserCreateIn, principal: AdminOnly, db: DbSession) -> UserListItem:
    return await service.create_user(db, body, principal.user_id, principal.ip)


@router.patch("/users/{user_id}", response_model=UserListItem, tags=["users"])
async def patch_user(
    user_id: uuid.UUID, body: UserPatchIn, principal: AdminOnly, db: DbSession
) -> UserListItem:
    return await service.patch_user(db, user_id, body, principal.user_id, principal.ip)


@router.post("/users/{user_id}/revoke-sessions", status_code=204, tags=["users"])
async def revoke_sessions(user_id: uuid.UUID, principal: AdminOnly, db: DbSession) -> Response:
    await service.revoke_sessions(db, user_id, principal.user_id, principal.ip)
    return Response(status_code=204)


@router.get("/audit-logs", response_model=Page[AuditLogOut], tags=["users"])
async def audit_logs(
    _: AdminOnly,
    db: DbSession,
    settings: AppSettings,
    user_id: uuid.UUID | None = None,
    action: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> Page[AuditLogOut]:
    return await service.list_audit(
        db,
        PageParams(page=page, page_size=page_size),
        user_id=user_id,
        action=action,
        date_from=date_from,
        date_to=date_to,
        tz=settings.tz_display,
    )
