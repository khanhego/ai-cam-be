"""BR-39 (L11, FR-08.07): phiên mở hoàn trước, phiên chính — một luật dùng chung cho tạo hồ sơ
(API-131 / tự tạo), API-132, J-16 (và J-24 ở T-225). DEC-448: suy ra lúc đọc, không lưu cột.

- Phiên mở hoàn "trước" (`prior_return`): phiên RETURN `CANCELLED` / `ABANDONED` của kiện / hồ sơ hàng hoàn có
  ≥ 1 clip không `DELETED`, bắt đầu trước phiên RETURN hoàn tất mới nhất (hoặc chưa có phiên hoàn tất).
- Phiên chính (`primary`): phiên RETURN có clip (không `DELETED`) bắt đầu sớm nhất trong bằng chứng;
  không có → phiên PACK hiệu lực (nếu có trong bằng chứng).

Luật loại phiên quét nhầm / "Cần soát" (BR-39 v0.3–v0.5: `EXCLUDED_CANCEL_REASONS`, `excluded_return_sql`,
`review_needed`) thêm ở T-279 / T-281 qua tham số `excluded` của các hàm dưới.
"""

import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.returns.models import ReturnCasePackage
from aicam.modules.sessions.models import PackSession

PRIOR_STATUSES = ("CANCELLED", "ABANDONED")


async def scope_condition(
    db: AsyncSession, package_id: uuid.UUID, case_id: uuid.UUID | None
) -> ColumnElement[bool]:
    """Phiên thuộc kiện của hồ sơ, mọi kiện của hồ sơ hàng hoàn, hoặc gắn hồ sơ hàng hoàn đó (như API-134)."""
    package_ids = {package_id}
    if case_id is not None:
        package_ids.update(
            (
                await db.scalars(
                    select(ReturnCasePackage.package_id).where(ReturnCasePackage.return_case_id == case_id)
                )
            ).all()
        )
    cond: ColumnElement[bool] = PackSession.package_id.in_(sorted(package_ids))
    if case_id is not None:
        cond = or_(cond, PackSession.return_case_id == case_id)
    return cond


async def live_clip_sessions(db: AsyncSession, session_ids: Iterable[uuid.UUID]) -> set[uuid.UUID]:
    """Phiên có ≥ 1 clip không `DELETED` (clip đang cắt / lỗi vẫn tính — bằng chứng còn có thể có)."""
    ids = sorted(set(session_ids))
    if not ids:
        return set()
    rows = await db.scalars(
        select(Clip.session_id).where(Clip.session_id.in_(ids), Clip.status != "DELETED").distinct()
    )
    return set(rows.all())


async def latest_completed_return_start(
    db: AsyncSession, package_id: uuid.UUID, case_id: uuid.UUID | None
) -> datetime | None:
    started: datetime | None = await db.scalar(
        select(PackSession.started_at)
        .where(
            await scope_condition(db, package_id, case_id),
            PackSession.type == "RETURN",
            PackSession.status == "COMPLETED",
        )
        .order_by(PackSession.started_at.desc())
        .limit(1)
    )
    return started


def is_prior_return(s: PackSession, latest_completed_start: datetime | None) -> bool:
    return (
        s.type == "RETURN"
        and s.status in PRIOR_STATUSES
        and (latest_completed_start is None or s.started_at < latest_completed_start)
    )


async def prior_return_sessions(
    db: AsyncSession, package_id: uuid.UUID, case_id: uuid.UUID | None
) -> list[PackSession]:
    """Phiên mở hoàn trước có clip của kiện / hồ sơ hàng hoàn, sớm trước (BR-39)."""
    candidates = (
        await db.scalars(
            select(PackSession)
            .where(
                await scope_condition(db, package_id, case_id),
                PackSession.type == "RETURN",
                PackSession.status.in_(PRIOR_STATUSES),
            )
            .order_by(PackSession.started_at, PackSession.id)
        )
    ).all()
    if not candidates:
        return []
    with_clip = await live_clip_sessions(db, [s.id for s in candidates])
    latest = await latest_completed_return_start(db, package_id, case_id)
    return [s for s in candidates if s.id in with_clip and is_prior_return(s, latest)]


def primary_session(
    sessions: Sequence[PackSession],
    with_clip: set[uuid.UUID],
    effective_pack_id: uuid.UUID | None,
    excluded: set[uuid.UUID] | None = None,
) -> uuid.UUID | None:
    """DEC-448: phiên RETURN có clip sớm nhất (theo `started_at`) trong bằng chứng, trừ phiên bị loại;
    không có → phiên PACK hiệu lực nếu nằm trong bằng chứng."""
    skip = excluded or set()
    returns = sorted(
        (s for s in sessions if s.type == "RETURN" and s.id in with_clip and s.id not in skip),
        key=lambda s: (s.started_at, s.id),
    )
    if returns:
        return returns[0].id
    if effective_pack_id is not None and any(s.id == effective_pack_id for s in sessions):
        return effective_pack_id
    return None


# ---------------------------------------------------------------- BR-38 (L15): giữ sau khi bỏ


async def keep_until(
    db: AsyncSession,
    session_ids: Iterable[uuid.UUID],
    snapshot_ids: Iterable[uuid.UUID],
    base: datetime | dict[uuid.UUID, datetime],
    days: int,
) -> dict[uuid.UUID, datetime]:
    """Hạn giữ khi bỏ khỏi hồ sơ = max(`end_at` muộn nhất của clip phiên / `taken_at` ảnh, mốc bỏ) + số ngày
    giữ (BR-38). `base` = mốc bỏ chung (API-132 `removal_keep_until` = lúc hiện tại) hoặc theo id (dòng đã
    bỏ). Khóa: id phiên / id ảnh."""
    keep = timedelta(days=days)
    sids, snaps = sorted(set(session_ids)), sorted(set(snapshot_ids))
    out: dict[uuid.UUID, datetime] = {}

    def _base(key: uuid.UUID) -> datetime:
        return base[key] if isinstance(base, dict) else base

    if sids:
        ends = dict(
            (
                await db.execute(
                    select(Clip.session_id, func.max(Clip.end_at))
                    .where(Clip.session_id.in_(sids))
                    .group_by(Clip.session_id)
                )
            ).all()
        )
        for sid in sids:
            end = ends.get(sid)
            out[sid] = max(end, _base(sid)) + keep if end else _base(sid) + keep
    if snaps:
        taken = dict(
            (await db.execute(select(Snapshot.id, Snapshot.taken_at).where(Snapshot.id.in_(snaps)))).all()
        )
        for snap in snaps:
            at = taken.get(snap)
            out[snap] = max(at, _base(snap)) + keep if at else _base(snap) + keep
    return out
