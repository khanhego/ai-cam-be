"""Dựng item API-20 / WS-02 `approval.*` từ bản ghi (không phụ thuộc sessions.service → tránh vòng import)."""

from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.approvals.schemas import ApprovalItem, UserBrief
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.schemas import StationRef
from aicam.modules.stations.models import Station
from aicam.modules.users.queries import get_user_ref


async def user_brief(session: AsyncSession, approval: ApprovalRequest) -> UserBrief | None:
    if approval.decided_by is None:
        return None
    ref = await get_user_ref(session, approval.decided_by)
    return UserBrief(id=ref.id, display_name=ref.display_name) if ref else None


async def approval_item(session: AsyncSession, approval: ApprovalRequest) -> ApprovalItem:
    station = await session.get(Station, approval.station_id)
    pack = await session.get(PackSession, approval.session_id) if approval.session_id else None
    return ApprovalItem(
        id=approval.id,
        type=approval.type,
        status=approval.status,
        station=StationRef(id=approval.station_id, name=station.name if station else ""),
        session_id=approval.session_id,
        tracking_number=approval.tracking_number,
        context=approval.context,
        created_at=approval.created_at,
        decision=approval.decision,
        decided_by=await user_brief(session, approval),
        decided_at=approval.decided_at,
        note=approval.note,
        session_type=pack.type if pack else ("PACK" if approval.type == "REPACK" else None),
        operator_name=pack.operator_name if pack else None,
    )
