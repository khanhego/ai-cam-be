"""Đăng nhập, refresh token xoay vòng, quản lý tài khoản (02a §4 API-01..04, API-90..92)."""

import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.core.audit import AuditLog
from aicam.core.errors import AppError
from aicam.core.pagination import Page, PageParams
from aicam.core.redis import get_redis
from aicam.core.security import (
    encode_access_token,
    hash_password,
    new_refresh_token,
    sha256_hex,
    verify_dummy,
    verify_password,
)
from aicam.core.settings import Settings
from aicam.modules.stations import service as stations
from aicam.modules.users.models import RefreshToken, User
from aicam.modules.users.permissions import PERMISSIONS
from aicam.modules.users.schemas import (
    AuditLogOut,
    AuditUser,
    Client,
    LoginOut,
    MeOut,
    StationRef,
    TokenOut,
    UserCreateIn,
    UserListItem,
    UserOut,
    UserPatchIn,
)

COOKIE_NAMES: dict[str, str] = {"STATION": "rt_station", "DASHBOARD": "rt_dashboard"}
COOKIE_PATH = "/api/v1/auth"
IP_WINDOW_S = 300


@dataclass
class IssuedRefresh:
    token: str
    max_age: int


def _invalid_credentials() -> AppError:
    return AppError("INVALID_CREDENTIALS", "Sai tài khoản hoặc mật khẩu.", 401)


def _unauthenticated() -> AppError:
    return AppError("UNAUTHENTICATED", "Phiên đăng nhập đã hết hạn. Đăng nhập lại.", 401)


async def _station_ref(session: AsyncSession, user: User) -> StationRef | None:
    if user.role != "STATION":
        return None
    station = await stations.get_station_by_account(session, user.id)
    return StationRef(id=station.id, name=station.name) if station else None


async def user_out(session: AsyncSession, user: User) -> UserOut:
    return UserOut(
        id=user.id,
        username=user.username,
        display_name=user.display_name,
        role=user.role,
        station=await _station_ref(session, user),
    )


async def _ip_guard(ip: str | None, settings: Settings) -> None:
    if not ip:
        return
    fails = await get_redis().get(f"login_fail_ip:{ip}")
    if fails is not None and int(fails) >= settings.login_ip_max_fails:
        raise AppError(
            "RATE_LIMITED", "Thao tác quá nhanh, thử lại sau.", 429, headers={"Retry-After": str(IP_WINDOW_S)}
        )


async def _ip_fail(ip: str | None) -> None:
    if not ip:
        return
    key = f"login_fail_ip:{ip}"
    redis = get_redis()
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, IP_WINDOW_S)


def _refresh_days(client: str, settings: Settings) -> int:
    return settings.refresh_days_station if client == "STATION" else settings.refresh_days_dashboard


def _issue_refresh(
    session: AsyncSession, user: User, client: str, settings: Settings
) -> tuple[RefreshToken, str]:
    token, token_hash = new_refresh_token()
    row = RefreshToken(
        user_id=user.id,
        token_hash=token_hash,
        client=client,
        expires_at=clock.now() + timedelta(days=_refresh_days(client, settings)),
    )
    session.add(row)
    return row, token


async def _station_id_for(session: AsyncSession, user: User) -> uuid.UUID | None:
    """Tài khoản STATION phải gắn một station đang bật (409 STATION_INACTIVE)."""
    if user.role != "STATION":
        return None
    station = await stations.get_station_by_account(session, user.id)
    if station is None or not station.is_active:
        raise AppError("STATION_INACTIVE", "Station này đang tắt. Liên hệ Admin.", 409)
    return station.id


async def login(
    session: AsyncSession,
    *,
    username: str,
    password: str,
    client: Client,
    ip: str | None,
    settings: Settings,
) -> tuple[LoginOut, IssuedRefresh]:
    await _ip_guard(ip, settings)
    user = await session.scalar(
        select(User).where(func.lower(User.username) == username.strip().lower()).with_for_update()
    )
    now = clock.now()
    if user is not None and user.locked_until and user.locked_until > now:
        raise AppError(
            "ACCOUNT_LOCKED",
            "Đăng nhập sai quá nhiều lần. Thử lại sau.",
            423,
            {"until": clock.iso_z(user.locked_until)},
        )
    if user is None:
        verify_dummy(password)
    if user is None or not verify_password(user.password_hash, password):
        await _ip_fail(ip)
        if user is not None:
            user.failed_logins += 1
            if user.failed_logins >= settings.login_max_fails:
                user.locked_until = now + timedelta(minutes=settings.login_lock_minutes)
                user.failed_logins = 0
            await session.commit()
        raise _invalid_credentials()
    if not user.is_active:
        raise AppError("ACCOUNT_DISABLED", "Tài khoản đã bị khóa. Liên hệ Admin.", 403)
    if (client == "STATION") != (user.role == "STATION"):
        message = (
            "Tài khoản này không dùng cho station. Đăng nhập dashboard tại /admin."
            if client == "STATION"
            else "Tài khoản station chỉ đăng nhập tại màn station."
        )
        raise AppError("WRONG_CLIENT", message, 403)
    station_id = await _station_id_for(session, user)

    user.failed_logins = 0
    user.locked_until = None
    _, refresh = _issue_refresh(session, user, client, settings)
    access, expires = encode_access_token(
        settings.jwt_secret, user.id, user.role, station_id, settings.access_token_minutes
    )
    audit.record(
        session,
        "LOGIN",
        user_id=user.id,
        object_type="USER",
        object_id=user.id,
        ip=ip,
        data={"client": client},
    )
    out = LoginOut(
        access_token=access,
        expires_in=int((expires - now).total_seconds()),
        user=await user_out(session, user),
    )
    await session.commit()
    return out, IssuedRefresh(refresh, _refresh_days(client, settings) * 86400)


async def refresh(
    session: AsyncSession, *, token: str | None, client: Client, settings: Settings
) -> tuple[TokenOut, IssuedRefresh]:
    if not token:
        raise _unauthenticated()
    row = await session.scalar(
        select(RefreshToken).where(RefreshToken.token_hash == sha256_hex(token)).with_for_update()
    )
    now = clock.now()
    if row is None or row.client != client:
        raise _unauthenticated()
    if row.revoked_at is not None:
        # Token đã xoay vòng bị dùng lại → nghi bị đánh cắp: thu hồi mọi phiên của user (02a API-02).
        await revoke_all(session, row.user_id)
        await session.commit()
        raise _unauthenticated()
    if row.expires_at <= now:
        raise _unauthenticated()
    user = await session.get(User, row.user_id)
    if user is None or not user.is_active:
        raise _unauthenticated()
    station_id = await _station_id_for(session, user)

    new_row, new_token = _issue_refresh(session, user, client, settings)
    await session.flush()
    row.revoked_at = now
    row.replaced_by = new_row.id
    access, expires = encode_access_token(
        settings.jwt_secret, user.id, user.role, station_id, settings.access_token_minutes
    )
    await session.commit()
    return (
        TokenOut(access_token=access, expires_in=int((expires - now).total_seconds())),
        IssuedRefresh(new_token, _refresh_days(client, settings) * 86400),
    )


async def logout(session: AsyncSession, token: str | None) -> None:
    if token:
        await session.execute(
            update(RefreshToken)
            .where(RefreshToken.token_hash == sha256_hex(token), RefreshToken.revoked_at.is_(None))
            .values(revoked_at=clock.now())
        )
        await session.commit()


async def revoke_all(session: AsyncSession, user_id: uuid.UUID) -> None:
    await session.execute(
        update(RefreshToken)
        .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
        .values(revoked_at=clock.now())
    )


async def me(session: AsyncSession, user_id: uuid.UUID) -> MeOut:
    user = await session.get(User, user_id)
    if user is None or not user.is_active:
        raise _unauthenticated()
    base = await user_out(session, user)
    return MeOut(**base.model_dump(), permissions=PERMISSIONS[user.role])


async def _list_item(session: AsyncSession, user: User) -> UserListItem:
    base = await user_out(session, user)
    return UserListItem(**base.model_dump(), is_active=user.is_active, created_at=user.created_at)


async def list_users(session: AsyncSession, page: PageParams, role: str | None) -> Page[UserListItem]:
    query = select(User)
    if role:
        query = query.where(User.role == role)
    total = await session.scalar(select(func.count()).select_from(query.subquery())) or 0
    rows = (
        await session.scalars(query.order_by(User.created_at).offset(page.offset).limit(page.page_size))
    ).all()
    items = [await _list_item(session, u) for u in rows]
    return Page(items=items, page=page.page, page_size=page.page_size, total=total)


async def create_user(
    session: AsyncSession, data: UserCreateIn, actor: uuid.UUID, ip: str | None
) -> UserListItem:
    exists = await session.scalar(select(User.id).where(func.lower(User.username) == data.username))
    if exists:
        raise AppError(
            "USERNAME_TAKEN", "Tên đăng nhập đã tồn tại.", 409, {"fields": {"username": "Đã tồn tại"}}
        )
    user = User(
        username=data.username,
        display_name=data.display_name,
        role=data.role,
        password_hash=hash_password(data.password),
    )
    session.add(user)
    await session.flush()
    audit.record(
        session,
        "USER_UPDATE",
        user_id=actor,
        object_type="USER",
        object_id=user.id,
        ip=ip,
        data={"op": "create"},
    )
    out = await _list_item(session, user)
    await session.commit()
    return out


async def _active_admins(session: AsyncSession) -> int:
    return (
        await session.scalar(select(func.count()).where(User.role == "ADMIN", User.is_active.is_(True))) or 0
    )


async def patch_user(
    session: AsyncSession, user_id: uuid.UUID, data: UserPatchIn, actor: uuid.UUID, ip: str | None
) -> UserListItem:
    user = await session.get(User, user_id, with_for_update=True)
    if user is None:
        raise AppError("NOT_FOUND", "Không tìm thấy tài khoản.", 404)
    losing_admin = (
        user.role == "ADMIN"
        and user.is_active
        and ((data.role is not None and data.role != "ADMIN") or data.is_active is False)
    )
    if losing_admin:
        # Khóa mọi dòng Admin để hai request song song không cùng hạ Admin cuối (02a API-90).
        await session.execute(select(User.id).where(User.role == "ADMIN").with_for_update())
        if await _active_admins(session) <= 1:
            raise AppError("LAST_ADMIN", "Phải còn ít nhất một Admin.", 409)
    role_changed = data.role is not None and data.role != user.role
    changed = data.model_dump(exclude_unset=True, exclude={"password"})
    for field, value in changed.items():
        setattr(user, field, value)
    if data.password is not None:
        user.password_hash = hash_password(data.password)
        changed["password"] = "***"  # noqa: S105 — che giá trị khi ghi audit
    if data.is_active is False or data.password is not None or role_changed:
        await revoke_all(session, user.id)
    audit.record(
        session, "USER_UPDATE", user_id=actor, object_type="USER", object_id=user.id, ip=ip, data=changed
    )
    out = await _list_item(session, user)
    await session.commit()
    return out


async def revoke_sessions(
    session: AsyncSession, user_id: uuid.UUID, actor: uuid.UUID, ip: str | None
) -> None:
    if await session.get(User, user_id) is None:
        raise AppError("NOT_FOUND", "Không tìm thấy tài khoản.", 404)
    await revoke_all(session, user_id)
    audit.record(session, "SESSIONS_REVOKED", user_id=actor, object_type="USER", object_id=user_id, ip=ip)
    await session.commit()


def _vn_day_bounds(day: date, tz: str, end: bool) -> datetime:
    local = datetime.combine(day + timedelta(days=1) if end else day, time.min, tzinfo=ZoneInfo(tz))
    return local


async def list_audit(
    session: AsyncSession,
    page: PageParams,
    *,
    user_id: uuid.UUID | None,
    action: str | None,
    date_from: date | None,
    date_to: date | None,
    tz: str,
) -> Page[AuditLogOut]:
    query = select(AuditLog, User.display_name).outerjoin(User, User.id == AuditLog.user_id)
    if user_id:
        query = query.where(AuditLog.user_id == user_id)
    if action:
        query = query.where(AuditLog.action == action)
    if date_from:
        query = query.where(AuditLog.at >= _vn_day_bounds(date_from, tz, end=False))
    if date_to:
        query = query.where(AuditLog.at < _vn_day_bounds(date_to, tz, end=True))
    total = await session.scalar(select(func.count()).select_from(query.subquery())) or 0
    rows = (
        await session.execute(
            query.order_by(AuditLog.at.desc(), AuditLog.id.desc()).offset(page.offset).limit(page.page_size)
        )
    ).all()
    items = [
        AuditLogOut(
            at=log.at,
            user=AuditUser(id=log.user_id, display_name=name) if log.user_id and name else None,
            action=log.action,
            object_type=log.object_type,
            object_id=log.object_id,
            data=log.data,
        )
        for log, name in rows
    ]
    return Page(items=items, page=page.page, page_size=page.page_size, total=total)
