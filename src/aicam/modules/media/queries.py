"""Truy vấn đọc clip cho module khác (T-14 thêm service đầy đủ)."""

import uuid
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.media.models import Clip


async def clips_of_session(session: AsyncSession, session_id: uuid.UUID) -> Sequence[Clip]:
    return (
        await session.scalars(select(Clip).where(Clip.session_id == session_id).order_by(Clip.camera_role))
    ).all()
