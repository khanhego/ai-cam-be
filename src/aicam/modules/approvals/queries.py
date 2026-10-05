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


def set_tray_match(approval: ApprovalRequest, match: str | None) -> bool:
    """Khay đổi trong lúc chờ duyệt MISMATCH / ASSIST → cập nhật `context.tray_match` (DEC-112)."""
    if approval.type == "REPACK" or match is None:
        return False
    context = dict(approval.context or {})
    if context.get("tray_match") == match:
        return False
    context["tray_match"] = match
    approval.context = context
    return True
