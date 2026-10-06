"""BR-39 / FR-08.07 (L11, T-213): hồ sơ khiếu nại tự thêm phiên mở hoàn **trước** đã hủy / bỏ dở có clip;
phiên chính = phiên mở hoàn có clip sớm nhất (DEC-448 — suy ra lúc đọc) cho API-132 và J-16.

Kịch bản loại phiên quét nhầm / "Cần soát" (v0.3–v0.5) ở T-279 / T-281."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.deps import Principal
from aicam.core.settings import Settings
from aicam.modules.claims import pack as claim_pack
from aicam.modules.claims import service as claims
from aicam.modules.claims import views as claim_views
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order, pack_session_with_clips, return_session

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 6, 1, 51, tzinfo=UTC)  # 08:51 giờ VN


async def _return(
    db: AsyncSession,
    station: Station,
    package: Package,
    case: ReturnCase,
    started: datetime,
    status: str,
    *,
    conclusion: str | None = None,
    clip_status: str | None = "READY",
    cancel_reason: str | None = None,
) -> PackSession:
    s = return_session(station, package, case, status=status, conclusion=conclusion)
    s.started_at, s.ended_at = started, started + timedelta(minutes=3)
    s.cancel_reason = cancel_reason
    db.add(s)
    await db.flush()
    if clip_status is not None:
        db.add(
            Clip(session_id=s.id, camera_role="CAM1", status=clip_status, start_at=s.started_at,
                 end_at=s.ended_at, path=f"clips/{s.id}.mp4", flags=[])
        )  # fmt: skip
        await db.flush()
    return s


async def _setup(db: AsyncSession) -> tuple[Station, Package, ReturnCase, PackSession]:
    clock.freeze(T0 + timedelta(hours=3))
    _, station = await make_station_account(db, "tst_station_br39", "TST Station BR39")
    order, (package,) = await make_order(db, 41, warehouse_status="RETURN_EXPECTED")
    case = await buyer_return_case(db, order, 41)
    pack = await pack_session_with_clips(db, station, package, T0 - timedelta(days=5))
    return station, package, case, pack


async def _evidence(db: AsyncSession, claim_id: uuid.UUID) -> set[uuid.UUID]:
    rows = await db.scalars(select(ClaimEvidence.session_id).where(ClaimEvidence.claim_id == claim_id))
    return {sid for sid in rows.all() if sid}


async def test_abandoned_prior_session_is_evidence_and_primary(
    db: AsyncSession, test_settings: Settings
) -> None:
    """BR-39 ví dụ: 08:51 phiên A bỏ dở (mất điện), 10:15 phiên B "Hộp rỗng" → KN tự tạo gồm đóng gói + A
    (chính) + B; A `prior_return`; API-132 `prior_return_sessions = [A]`."""
    station, package, case, pack = await _setup(db)
    a = await _return(db, station, package, case, T0, "ABANDONED")
    b = await _return(
        db, station, package, case, T0 + timedelta(minutes=84), "COMPLETED", conclusion="EMPTY_BOX"
    )

    created = await claims.create_from_return(db, b, case)

    assert created is not None
    claim = created.claim
    assert await _evidence(db, claim.id) == {pack.id, a.id, b.id}
    detail = await claim_views.claim_detail(db, claim.id, uuid.uuid4(), test_settings)
    flags = {e.session.id: (e.primary, e.prior_return, e.auto) for e in detail.evidence if e.session}
    assert flags == {pack.id: (False, False, True), a.id: (True, True, True), b.id: (False, False, True)}
    assert [e.session.id for e in detail.evidence if e.session] == [pack.id, a.id, b.id]
    assert [(p.session_id, p.status, p.in_evidence) for p in detail.prior_return_sessions] == [
        (a.id, "ABANDONED", True)
    ]


async def test_prior_session_without_live_clip_is_skipped(db: AsyncSession, test_settings: Settings) -> None:
    """Phiên hủy / bỏ dở không có clip / clip đã `DELETED` → không thêm. BR-39 "mọi phiên": phiên bỏ dở bắt
    đầu SAU phiên hoàn tất vẫn vào bằng chứng nhưng không phải "phiên trước" (`prior_return = false`)."""
    station, package, case, pack = await _setup(db)
    no_clip = await _return(db, station, package, case, T0, "ABANDONED", clip_status=None)
    deleted = await _return(db, station, package, case, T0 + timedelta(minutes=5), "CANCELLED",
                            clip_status="DELETED", cancel_reason="OTHER")  # fmt: skip
    done = await _return(
        db, station, package, case, T0 + timedelta(minutes=30), "COMPLETED", conclusion="DAMAGED"
    )
    later = await _return(db, station, package, case, T0 + timedelta(minutes=60), "ABANDONED")

    created = await claims.create_from_return(db, done, case)

    assert created is not None
    evidence = await _evidence(db, created.claim.id)
    assert evidence == {pack.id, done.id, later.id}
    assert not {no_clip.id, deleted.id} & evidence
    detail = await claim_views.claim_detail(db, created.claim.id, uuid.uuid4(), test_settings)
    flags = {e.session.id: (e.primary, e.prior_return) for e in detail.evidence if e.session}
    assert flags == {pack.id: (False, False), done.id: (True, False), later.id: (False, False)}
    assert detail.prior_return_sessions == []


async def test_manual_claim_includes_prior_sessions(db: AsyncSession, test_settings: Settings) -> None:
    """API-131 (tạo tay, hồ sơ hàng hoàn chưa có phiên hoàn tất): phiên bỏ dở có clip vào bằng chứng tự chọn,
    là phiên chính; không có phiên RETURN có clip → phiên chính là đóng gói hiệu lực."""
    station, package, case, pack = await _setup(db)
    a = await _return(db, station, package, case, T0, "ABANDONED")
    user = await make_user(db, "tst_cskh_br39", "CSKH")
    p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)

    created = await claims.create_manual(
        db,
        ClaimCreateIn(
            package_id=package.id, type="MISSING_ITEM", counterparty="PLATFORM", return_case_id=case.id
        ),
        p,
    )

    assert await _evidence(db, created.id) == {pack.id, a.id}
    detail = await claim_views.claim_detail(db, created.id, user.id, test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [a.id]

    other_order, (other,) = await make_order(db, 44, warehouse_status="DELIVERED")
    other_pack = await pack_session_with_clips(db, station, other, T0 - timedelta(days=2))
    plain = await claims.create_manual(
        db, ClaimCreateIn(package_id=other.id, type="OTHER", counterparty="CARRIER"), p
    )
    detail = await claim_views.claim_detail(db, plain.id, user.id, test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [other_pack.id]
    assert detail.prior_return_sessions == []
    assert other_order is not None


async def test_evidence_pack_orders_primary_and_prior_folders(db: AsyncSession) -> None:
    """J-16: thư mục đóng gói → phiên chính (`mo-hoan`) → phiên trước khác (`mo-hoan-phien-truoc`) → phiên
    hoàn tất (`mo-hoan`), sắp theo giờ."""
    station, package, case, pack = await _setup(db)
    a1 = await _return(db, station, package, case, T0, "ABANDONED")
    a2 = await _return(
        db, station, package, case, T0 + timedelta(minutes=10), "CANCELLED", cancel_reason="OTHER"
    )
    b = await _return(
        db, station, package, case, T0 + timedelta(minutes=84), "COMPLETED", conclusion="EMPTY_BOX"
    )
    created = await claims.create_from_return(db, b, case)
    assert created is not None
    claim = await db.get(Claim, created.claim.id)
    assert claim is not None

    rows, _ = await claim_pack._session_rows(db, claim)

    assert [(s.id, main, kind) for s, main, kind in rows] == [
        (pack.id, True, "dong-goi"),
        (a1.id, True, "mo-hoan"),
        (a2.id, True, "mo-hoan-phien-truoc"),
        (b.id, True, "mo-hoan"),
    ]
