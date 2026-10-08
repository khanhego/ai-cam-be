"""G3V-1 (DEC-932): API-160 nguồn PHIÊN ∥ API-189 `MARK_WRONG_SCAN` cùng phiên — 2 connection, dữ liệu commit
thật, TRUNCATE sau.

API-160 nguồn PHIÊN đọc phiên `FOR SHARE` (+ `populate_existing`) và kiểm `held_back` **sau** khóa; MARK khóa
phiên `FOR UPDATE` rồi mới truy `affected_shares` → không có link chứa phiên quét nhầm mà MARK không liệt kê:
- link khóa trước → MARK chờ, thấy link vừa commit trong `affected_shares[]`;
- MARK khóa trước → API-160 chờ, đọc lại phiên đã đánh dấu → 409 `SESSION_EXCLUDED`, không tạo link.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.db import commit, dispose_engine, init_engine, sessionmaker
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.claims import review
from aicam.modules.claims.models import Claim
from aicam.modules.claims.schemas import ReviewIn
from aicam.modules.shares import service as shares
from aicam.modules.shares.models import ShareLink
from aicam.modules.shares.schemas import ShareCreateIn

from .factories import make_user
from .shares_fixtures import make_share_world, share_settings, share_store

__all__ = ["share_settings", "share_store"]

pytestmark = pytest.mark.integration

TABLES = (
    "share_item, share_link, claim_note, claim_evidence, evidence_pack, claim, recon_alert, snapshot, "
    "clip, session_event, session, return_case_package, return_case, status_history, order_item, "
    'package_order, package, "order", shop, station'
)
HOLD = 0.5  # giây giữ transaction để bên kia phải chờ khóa


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, share_store: object, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[AsyncEngine]:
    monkeypatch.setattr(shares, "enqueue_build", lambda db, sid: None)
    monkeypatch.setattr(shares, "publish_updated", lambda db, link: None)
    engine = init_engine(migrated_database_url)
    yield engine
    clock.reset()
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
        await conn.execute(
            text("DELETE FROM \"user\" WHERE username LIKE 'tst_g3v1_%' OR username LIKE 'tst_share_%'")
        )
    await dispose_engine()


async def _world(settings: Settings, n: int) -> tuple[uuid.UUID, uuid.UUID, int, Principal]:
    async with sessionmaker()() as db:
        w = await make_share_world(db, settings, n)
        user = await make_user(db, f"tst_g3v1_{n}", "CSKH")
        await db.commit()
        p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)
        return w.claim.id, w.ret_a.id, w.claim.version, p


def _share_body(session_id: uuid.UUID) -> ShareCreateIn:
    return ShareCreateIn(
        source_type="SESSION", session_id=session_id, session_ids=[session_id], layout="CAM1",
        include_snapshots=False, recipient="Bưu cục Q7", expires_days=1,
    )  # fmt: skip


async def _mark(claim_id: uuid.UUID, session_id: uuid.UUID, version: int, p: Principal) -> list[uuid.UUID]:
    async def conflict(cid: uuid.UUID) -> AppError:
        return AppError("VERSION_CONFLICT", "x", 409)

    async with sessionmaker()() as db:
        body = ReviewIn(version=version, action="MARK_WRONG_SCAN", reason_code="WRONG_SCAN", note="Quét nhầm")
        result = await review.review_return_session(
            db, claim_id, session_id, body, p, version_conflict=conflict
        )
        await commit(db)
        return [a.id for a in result.affected_shares]


async def _share(session_id: uuid.UUID, p: Principal, settings: Settings) -> str:
    async with sessionmaker()() as db:
        try:
            out = await shares.create(db, _share_body(session_id), p, settings)
            return str(out.id)
        except AppError as exc:
            await db.rollback()
            return exc.code


async def _links(session: AsyncSession) -> int:
    return int(await session.scalar(select(func.count()).select_from(ShareLink)) or 0)


async def test_share_locks_first_mark_waits_and_lists_link(
    committed: AsyncEngine, share_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """API-160 đã đọc phiên (chưa commit) → MARK chờ khóa phiên, rồi `affected_shares[]` có link vừa tạo."""
    claim_id, sid, version, p = await _world(share_settings, 71)
    loaded, release = asyncio.Event(), asyncio.Event()
    real = shares._source_ref

    async def paused(db: Any, *args: Any) -> Any:
        out = await real(db, *args)
        loaded.set()
        await release.wait()
        return out

    monkeypatch.setattr(shares, "_source_ref", paused)
    share_task = asyncio.create_task(_share(sid, p, share_settings))
    await asyncio.wait_for(asyncio.shield(loaded.wait()), timeout=10)
    mark_task = asyncio.create_task(_mark(claim_id, sid, version, p))
    try:
        await asyncio.sleep(HOLD)
    finally:
        release.set()
    link_id, affected = await asyncio.wait_for(asyncio.gather(share_task, mark_task), timeout=20)

    assert link_id != "SESSION_EXCLUDED"
    assert [str(a) for a in affected] == [link_id]


async def test_mark_locks_first_share_waits_and_is_excluded(
    committed: AsyncEngine, share_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MARK khóa phiên (chưa commit) → API-160 chờ, đọc lại phiên đã đánh dấu → 409 `SESSION_EXCLUDED`."""
    claim_id, sid, version, p = await _world(share_settings, 72)
    marked, release = asyncio.Event(), asyncio.Event()
    real = review.share_queries.affected_shares

    async def paused(db: Any, *args: Any, **kw: Any) -> Any:
        out = await real(db, *args, **kw)
        marked.set()
        await release.wait()
        return out

    monkeypatch.setattr(review.share_queries, "affected_shares", paused)
    mark_task = asyncio.create_task(_mark(claim_id, sid, version, p))
    await asyncio.wait_for(marked.wait(), timeout=10)
    share_task = asyncio.create_task(_share(sid, p, share_settings))
    try:
        await asyncio.sleep(HOLD)
    finally:
        release.set()
    affected, code = await asyncio.wait_for(asyncio.gather(mark_task, share_task), timeout=20)

    assert affected == []
    assert code == "SESSION_EXCLUDED"
    async with sessionmaker()() as db:
        assert await _links(db) == 0
        claim = await db.get(Claim, claim_id)
        assert claim is not None
        assert claim.version == version + 1
