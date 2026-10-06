"""Bảo vệ bằng chứng (ADR-009, BR-09, DEC-251): khóa clip khi gắn bằng chứng.

Chỉ import model (không import service) để `claims`, `returns` dùng được mà không vòng import.
"""

import uuid
from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.media.models import Clip


async def lock_session_clips(session: AsyncSession, session_ids: Iterable[uuid.UUID]) -> list[Clip]:
    """DEC-251 (R-7): khóa clip của các phiên `ORDER BY id FOR UPDATE` trước khi ghi bằng chứng.

    J-02 khóa từng clip rồi kiểm lại bảo vệ dưới khóa → ai khóa trước thắng: hồ sơ ghi xong thì J-02 thấy
    bằng chứng và bỏ qua; J-02 xóa trước thì hồ sơ thấy clip `DELETED` (hiện ở `missing`)."""
    ids = sorted(set(session_ids))
    if not ids:
        return []
    rows = await session.scalars(
        select(Clip)
        .where(Clip.session_id.in_(ids))
        .order_by(Clip.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return list(rows.all())
