"""Đọc / ghi bảng `setting` một dòng (02a §3). API-80 ở T-18."""

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.settings.models import Setting


async def get(session: AsyncSession) -> Setting:
    row = await session.get(Setting, 1)
    if row is None:  # migration luôn seed; phòng DB test bị xóa tay
        row = Setting(id=1)
        session.add(row)
        await session.flush()
    return row
