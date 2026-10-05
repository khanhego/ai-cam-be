"""Truy vấn đọc tài khoản cho module khác (không phụ thuộc module nào để tránh vòng import)."""

import uuid
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.users.models import User


@dataclass(frozen=True)
class UserRef:
    id: uuid.UUID
    username: str
    display_name: str
    role: str
    is_active: bool


async def get_user_ref(session: AsyncSession, user_id: uuid.UUID) -> UserRef | None:
    user = await session.get(User, user_id)
    if user is None:
        return None
    return UserRef(user.id, user.username, user.display_name, user.role, user.is_active)
