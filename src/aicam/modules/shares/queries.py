"""Đọc link chia sẻ cho module khác (API-31 / API-132 `shares[]`, API-189 `affected_shares[]`).

Chỉ phụ thuộc model `shares` + `users` (không import `claims` / `orders` — tránh vòng import: `shares.service`
đọc bằng chứng của `claims`, còn `claims.views` / `orders.packages` đọc file này).
"""

import uuid
from collections.abc import Iterable
from datetime import datetime

from cryptography.fernet import InvalidToken
from sqlalchemy import ColumnElement, and_, case, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.shares.models import ShareItem, ShareLink
from aicam.modules.shares.schemas import AffectedShare, ShareBrief, UserBrief
from aicam.modules.users.models import User

LIVE = ("CREATING", "ACTIVE")  # "đang hoạt động" (API-161 `status=ACTIVE` gồm `CREATING`)
REVOKERS = ("ADMIN", "SUPERVISOR")  # CSKH chỉ thu hồi link mình tạo (FR-07.08)
BRIEF_LIMIT = 3


def effective_status(link: ShareLink, now: datetime | None = None) -> str:
    """`ACTIVE` đã qua `expires_at` mà J-25 chưa chạy → `EXPIRED` (URL ký cũng đã hết hạn)."""
    if link.status == "ACTIVE" and link.expires_at <= (now or clock.now()):
        return "EXPIRED"
    return link.status


def status_sql(now: datetime) -> ColumnElement[str]:
    """Bản SQL của `effective_status` (lọc / đếm API-161)."""
    return case(
        (and_(ShareLink.status == "ACTIVE", ShareLink.expires_at <= now), "EXPIRED"),
        else_=ShareLink.status,
    )


def can_revoke(link: ShareLink, user_id: uuid.UUID, role: str | None, now: datetime | None = None) -> bool:
    return effective_status(link, now) in LIVE and (role in REVOKERS or link.created_by == user_id)


def revoke_pending(link: ShareLink) -> bool:
    """D21 "Đang thu hồi — chờ Internet" (EX-S7): đã thu hồi, đối tượng trên cloud chưa xóa xong."""
    return link.status == "REVOKED" and link.cloud_deleted_at is None


def share_url(settings: Settings, link: ShareLink, now: datetime | None = None) -> str | None:
    """URL W1 chỉ khi `ACTIVE` còn hạn (02 API-161). Không log URL (NFR-42)."""
    if effective_status(link, now) != "ACTIVE" or not link.url_enc:
        return None
    try:
        return Cipher(settings.fernet_key).decrypt(link.url_enc)
    except InvalidToken:  # FERNET_KEY đổi / mất (02 API-186 bí mật phải cất) — link vẫn chạy trên cloud
        return None


async def users_brief(db: AsyncSession, ids: Iterable[uuid.UUID | None]) -> dict[uuid.UUID, UserBrief]:
    wanted = sorted({i for i in ids if i is not None})
    if not wanted:
        return {}
    rows = (await db.scalars(select(User).where(User.id.in_(wanted)))).all()
    return {u.id: UserBrief(id=u.id, display_name=u.display_name) for u in rows}


async def _briefs(
    db: AsyncSession,
    cond: ColumnElement[bool],
    *,
    viewer: uuid.UUID,
    role: str | None,
    settings: Settings,
) -> tuple[list[ShareBrief], int]:
    now = clock.now()
    status = status_sql(now)
    rows = (
        await db.scalars(
            select(ShareLink)
            .where(cond, ShareLink.status != "FAILED")
            .order_by(ShareLink.created_at.desc(), ShareLink.id.desc())
            .limit(BRIEF_LIMIT)
        )
    ).all()
    active = await db.scalar(select(func.count()).select_from(ShareLink).where(cond, status.in_(LIVE))) or 0
    counts = dict(
        (
            await db.execute(
                select(ShareItem.share_id, func.count())
                .where(ShareItem.share_id.in_([r.id for r in rows]))
                .group_by(ShareItem.share_id)
            )
        ).all()
    )
    briefs = [
        ShareBrief(
            id=r.id,
            status=effective_status(r, now),
            recipient=r.recipient,
            expires_at=r.expires_at,
            session_count=counts.get(r.id, 0),
            url=share_url(settings, r, now),
            can_revoke=can_revoke(r, viewer, role, now),
            revoke_pending=revoke_pending(r),
            created_at=r.created_at,
        )
        for r in rows
    ]
    return briefs, active


def _has_session_of_package(package_id: uuid.UUID) -> ColumnElement[bool]:
    from aicam.modules.sessions.models import PackSession

    return exists(
        select(ShareItem.share_id)
        .join(PackSession, PackSession.id == ShareItem.session_id)
        .where(ShareItem.share_id == ShareLink.id, PackSession.package_id == package_id)
    )


async def package_shares(
    db: AsyncSession, package_id: uuid.UUID, *, viewer: uuid.UUID, role: str | None, settings: Settings
) -> tuple[list[ShareBrief], int]:
    """API-31: ≤ 3 link mới nhất (mọi trạng thái trừ `FAILED`) có phiên của kiện + số link đang hoạt động."""
    cond = or_(ShareLink.package_id == package_id, _has_session_of_package(package_id))
    return await _briefs(db, cond, viewer=viewer, role=role, settings=settings)


async def claim_shares(
    db: AsyncSession, claim_id: uuid.UUID, *, viewer: uuid.UUID, role: str | None, settings: Settings
) -> tuple[list[ShareBrief], int]:
    """API-132: link tạo từ hồ sơ (`share_link.claim_id`) — DEC-666."""
    return await _briefs(db, ShareLink.claim_id == claim_id, viewer=viewer, role=role, settings=settings)


async def affected_shares(
    db: AsyncSession, session_id: uuid.UUID, *, viewer: uuid.UUID, role: str | None
) -> list[AffectedShare]:
    """API-189 `MARK_WRONG_SCAN` (DEC-531): link `CREATING` / `ACTIVE` (còn hạn) chứa phiên — mọi nguồn."""
    now = clock.now()
    rows = (
        await db.scalars(
            select(ShareLink)
            .where(
                status_sql(now).in_(LIVE),
                exists(
                    select(ShareItem.share_id).where(
                        ShareItem.share_id == ShareLink.id, ShareItem.session_id == session_id
                    )
                ),
            )
            .order_by(ShareLink.created_at.desc(), ShareLink.id.desc())
        )
    ).all()
    users = await users_brief(db, [r.created_by for r in rows])
    return [
        AffectedShare(
            id=r.id,
            recipient=r.recipient,
            status=effective_status(r, now),
            expires_at=r.expires_at,
            created_by=users.get(r.created_by),
            can_revoke=can_revoke(r, viewer, role, now),
        )
        for r in rows
    ]
