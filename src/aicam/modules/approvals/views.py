"""Dựng item API-20 / WS-02 `approval.*` từ bản ghi (không phụ thuộc sessions.service → tránh vòng import)."""

import uuid
from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.approvals.schemas import ApprovalItem, ReturnSummary, UserBrief
from aicam.modules.media.models import Snapshot
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.schemas import StationRef
from aicam.modules.stations.models import Station
from aicam.modules.users.queries import get_user_ref


async def user_brief(session: AsyncSession, approval: ApprovalRequest) -> UserBrief | None:
    if approval.decided_by is None:
        return None
    ref = await get_user_ref(session, approval.decided_by)
    return UserBrief(id=ref.id, display_name=ref.display_name) if ref else None


async def snapshot_counts(session: AsyncSession, session_ids: Sequence[uuid.UUID]) -> dict[uuid.UUID, int]:
    """Số ảnh chụp tay còn `READY` theo phiên — một truy vấn gộp cho cả trang API-20."""
    if not session_ids:
        return {}
    rows = await session.execute(
        select(Snapshot.session_id, func.count())
        .where(
            Snapshot.session_id.in_(sorted(set(session_ids))),
            Snapshot.kind == "MANUAL",
            Snapshot.status == "READY",
        )
        .group_by(Snapshot.session_id)
    )
    return {sid: int(n) for sid, n in rows.all()}


def return_summary(pack: PackSession | None, counts: dict[uuid.UUID, int]) -> ReturnSummary | None:
    if pack is None or pack.type != "RETURN":
        return None
    return ReturnSummary(
        conclusion=pack.inspection_conclusion if pack.inspection_saved_at is not None else None,
        snapshot_count=counts.get(pack.id, 0),
        opened_at=pack.started_at,
    )


async def approval_item(
    session: AsyncSession, approval: ApprovalRequest, counts: dict[uuid.UUID, int] | None = None
) -> ApprovalItem:
    station = await session.get(Station, approval.station_id)
    pack = await session.get(PackSession, approval.session_id) if approval.session_id else None
    if counts is None and pack is not None and pack.type == "RETURN":
        counts = await snapshot_counts(session, [pack.id])
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
        return_summary=return_summary(pack, counts or {}),
    )
