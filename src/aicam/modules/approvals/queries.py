"""Truy vấn đọc yêu cầu duyệt cho module khác (không phụ thuộc sessions → tránh vòng import)."""

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.approvals.models import ApprovalRequest


async def pending_for_station(session: AsyncSession, station_id: uuid.UUID) -> ApprovalRequest | None:
    result: ApprovalRequest | None = await session.scalar(
        select(ApprovalRequest).where(
            ApprovalRequest.station_id == station_id, ApprovalRequest.status == "PENDING"
        )
    )
    return result
