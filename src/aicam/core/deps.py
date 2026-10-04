"""Dependency xác thực / phân quyền dùng chung. Chỉ dựa vào claim JWT, không đọc bảng user (02a §2)."""

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Request

from aicam.core.errors import AppError
from aicam.core.security import TokenError, decode_access_token
from aicam.core.settings import Settings, get_settings

ROLES = ("ADMIN", "SUPERVISOR", "CSKH", "STATION")


@dataclass(frozen=True)
class Principal:
    user_id: uuid.UUID
    role: str
    station_id: uuid.UUID | None
    ip: str | None


def _unauthenticated() -> AppError:
    return AppError(
        "UNAUTHENTICATED",
        "Phiên đăng nhập đã hết hạn. Đăng nhập lại.",
        401,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_principal(request: Request, settings: Annotated[Settings, Depends(get_settings)]) -> Principal:
    header = request.headers.get("Authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise _unauthenticated()
    try:
        claims = decode_access_token(settings.jwt_secret, token)
    except TokenError as exc:
        raise _unauthenticated() from exc
    return Principal(
        user_id=claims.user_id,
        role=claims.role,
        station_id=claims.station_id,
        ip=request.client.host if request.client else None,
    )


CurrentPrincipal = Annotated[Principal, Depends(get_principal)]


def require_roles(*roles: str) -> Callable[[Principal], Awaitable[Principal]]:
    """`Depends(require_roles("ADMIN", "SUPERVISOR"))` — server luôn chặn theo ma trận 01 §5.1."""
    unknown = set(roles) - set(ROLES)
    if unknown:
        raise ValueError(f"Role không tồn tại: {unknown}")

    async def _check(principal: CurrentPrincipal) -> Principal:
        if principal.role not in roles:
            raise AppError("FORBIDDEN", "Tài khoản không có quyền thực hiện thao tác này.", 403)
        return principal

    return _check
