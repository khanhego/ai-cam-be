"""Khối phiên RETURN của API-10 / WS `station.state` (02 §6.2 API-10; 02a §4.1 "build_state() RETURN").

3 truy vấn thêm: hồ sơ + đếm kiện, dòng kiểm, ảnh + phiên PACK hiệu lực (ảnh — T-109).
"""

import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.media import snapshots
from aicam.modules.media.models import Snapshot
from aicam.modules.media.queries import clips_of_session
from aicam.modules.orders.models import Package
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.returns.views import reason_label
from aicam.modules.sessions import inspection
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.return_scan import effective_pack_session
from aicam.modules.sessions.schemas import (
    PackReference,
    PackReferenceClip,
    ReturnCaseState,
    SessionOut,
    SnapshotOut,
    SnapshotRef,
)
from aicam.modules.stations.models import Station


async def _case_state(session: AsyncSession, case: ReturnCase) -> ReturnCaseState:
    total, received = (
        await session.execute(
            select(
                func.count(),
                func.count().filter(
                    Package.warehouse_status.in_(("RETURN_RECEIVED_OK", "RETURN_RECEIVED_ISSUE"))
                ),
            )
            .select_from(ReturnCasePackage)
            .join(Package, Package.id == ReturnCasePackage.package_id)
            .where(ReturnCasePackage.return_case_id == case.id)
        )
    ).one()
    return ReturnCaseState(
        id=case.id,
        code=case.code,
        kind=case.kind,
        status=case.status,
        platform_return_sn=case.platform_return_sn,
        return_tracking_number=case.return_tracking_number,
        reason=case.reason,
        reason_text=case.reason_text,
        reason_label=reason_label(case.reason),
        package_count=int(total or 0),
        received_count=int(received or 0),
    )


async def pack_reference(
    session: AsyncSession, package_id: uuid.UUID, settings: Settings, uid: uuid.UUID | None
) -> PackReference | None:
    pack = await effective_pack_session(session, package_id)
    if pack is None:
        return None
    station = await session.get(Station, pack.station_id)
    clips = await clips_of_session(session, pack.id)
    shot = await snapshots.pack_close_of(session, pack.id)
    return PackReference(
        session_id=pack.id,
        ended_at=pack.ended_at,
        station_name=station.name if station else "",
        clips=[PackReferenceClip(id=c.id, camera_role=c.camera_role, status=c.status) for c in clips],
        snapshot=SnapshotRef(id=shot.id, url=snapshots.url_for(settings, shot.id, uid))
        if shot and uid
        else None,
    )


# BR-37 (L11, DEC-447): station tự hủy phiên mở hoàn trong 60 giây đầu khi chưa lưu kết luận,
# chưa chụp ảnh tay.
SELF_CANCEL_WINDOW = timedelta(seconds=60)


async def has_manual_snapshot(session: AsyncSession, session_id: uuid.UUID) -> bool:
    """Đã chụp ảnh tay (mọi trạng thái — ảnh đã chụp rồi bị xóa vẫn tính là đã chụp)."""
    return bool(
        await session.scalar(
            select(func.count()).where(Snapshot.session_id == session_id, Snapshot.kind == "MANUAL")
        )
    )


async def self_cancel_until(session: AsyncSession, pack: PackSession) -> datetime | None:
    """API-10 `session.self_cancel_until` (chỉ phiên RETURN `OPEN`): `started_at + 60 giây` khi chưa lưu
    kết luận và chưa có ảnh tay; ngược lại None (chỉ "Gọi quản lý"). API-12 quyết lại dưới khóa station."""
    if pack.type != "RETURN" or pack.status != "OPEN" or pack.inspection_saved_at is not None:
        return None
    if await has_manual_snapshot(session, pack.id):
        return None
    return pack.started_at + SELF_CANCEL_WINDOW


async def fill(
    session: AsyncSession, out: SessionOut, pack: PackSession, settings: Settings, uid: uuid.UUID | None
) -> None:
    """Điền `return_case`, `inspection`, `snapshots`, `pack_reference` cho phiên RETURN.

    URL ảnh ký theo tài khoản station (`uid`) — station tải bằng `<img>` không cần Bearer (API-106)."""
    case = await session.get(ReturnCase, pack.return_case_id) if pack.return_case_id else None
    if case is not None:
        out.return_case = await _case_state(session, case)
    out.inspection = inspection.inspection_out(pack, await inspection.lines_of(session, pack.id))
    out.snapshots = (
        [
            SnapshotOut(
                id=s.id, kind="MANUAL", taken_at=s.taken_at, url=snapshots.url_for(settings, s.id, uid)
            )
            for s in await snapshots.of_session(session, pack.id)
        ]
        if uid
        else []
    )
    out.pack_reference = await pack_reference(session, pack.package_id, settings, uid)
    out.self_cancel_until = await self_cancel_until(session, pack)


def _day_start(tz: str) -> datetime:
    zone = ZoneInfo(tz)
    today = clock.now().astimezone(zone).date()
    return datetime(today.year, today.month, today.day, tzinfo=zone)


async def today_return_counts(session: AsyncSession, station_id: uuid.UUID, tz: str) -> tuple[int, int]:
    """Số phiên hoàn hoàn tất hôm nay (giờ VN) của station và số có vấn đề (kết luận ≠ OK)."""
    total, issue = (
        await session.execute(
            select(
                func.count(),
                func.count().filter(PackSession.inspection_conclusion != "OK"),
            ).where(
                PackSession.station_id == station_id,
                PackSession.type == "RETURN",
                PackSession.status == "COMPLETED",
                PackSession.ended_at >= _day_start(tz),
            )
        )
    ).one()
    return int(total or 0), int(issue or 0)
