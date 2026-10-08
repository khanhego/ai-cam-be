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
from aicam.modules.claims.schemas import ClaimCreateIn, EvidenceIn
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


# ---------------------------------------------------------------- BR-39 v0.3 (T-279): phiên quét nhầm bị loại


async def _snaps(db: AsyncSession, claim_id: uuid.UUID) -> list[uuid.UUID]:
    rows = await db.scalars(select(ClaimEvidence.snapshot_id).where(ClaimEvidence.claim_id == claim_id))
    return [sid for sid in rows.all() if sid]


async def _claim_of(db: AsyncSession, done: PackSession, case: ReturnCase) -> Claim:
    created = await claims.create_from_return(db, done, case)
    assert created is not None
    claim = await db.get(Claim, created.claim.id)
    assert claim is not None
    return claim


@pytest.mark.parametrize("reason", ["WRONG_SCAN", "NOT_A_RETURN"])
async def test_wrong_scan_session_excluded_never_primary(
    db: AsyncSession, test_settings: Settings, reason: str
) -> None:
    """(2), (4) — G2-1: C hủy `WRONG_SCAN` / `NOT_A_RETURN` sau 25 giây có clip, D `COMPLETED` → KN không
    có C,
    D là phiên chính, `excluded_return_sessions = [C]`; zip J-16 không có C."""
    station, package, case, pack = await _setup(db)
    c = await _return(db, station, package, case, T0, "CANCELLED", cancel_reason=reason)
    c.ended_at = c.started_at + timedelta(seconds=25)
    d = await _return(
        db, station, package, case, T0 + timedelta(minutes=5), "COMPLETED", conclusion="EMPTY_BOX"
    )

    claim = await _claim_of(db, d, case)

    assert await _evidence(db, claim.id) == {pack.id, d.id}
    detail = await claim_views.claim_detail(db, claim.id, uuid.uuid4(), test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [d.id]
    assert [
        (x.session_id, x.evidence_exclusion, x.in_evidence, x.cancel_reason)
        for x in detail.excluded_return_sessions
    ] == [(c.id, "STATION_CANCEL", False, reason)]
    assert detail.prior_return_sessions == []
    rows, _ = await claim_pack._session_rows(db, claim)
    assert c.id not in {s.id for s, _, _ in rows}
    assert [s.id for s, main, kind in rows if kind == "mo-hoan"] == [d.id]


async def test_manually_added_wrong_scan_session_never_primary(
    db: AsyncSession, test_settings: Settings
) -> None:
    """(3): CSKH thêm tay C (API-134) → C có trong bằng chứng nhưng phiên chính vẫn D; J-16 C là `phien-khac`
    (không thư mục chính), chip `evidence_exclusion = STATION_CANCEL`."""
    station, package, case, pack = await _setup(db)
    c = await _return(db, station, package, case, T0, "CANCELLED", cancel_reason="WRONG_SCAN")
    d = await _return(
        db, station, package, case, T0 + timedelta(minutes=5), "COMPLETED", conclusion="DAMAGED"
    )
    claim = await _claim_of(db, d, case)
    user = await make_user(db, "tst_cskh_t279", "CSKH")
    p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)
    await claims.set_evidence(
        db,
        claim,
        EvidenceIn(
            version=claim.version, session_ids=[pack.id, c.id, d.id], snapshot_ids=await _snaps(db, claim.id)
        ),
        p,
    )

    detail = await claim_views.claim_detail(db, claim.id, user.id, test_settings)
    by_id = {e.session.id: e for e in detail.evidence if e.session}
    assert c.id in by_id
    assert (by_id[c.id].primary, by_id[c.id].prior_return, by_id[c.id].auto) == (False, False, False)
    assert by_id[c.id].session.evidence_exclusion == "STATION_CANCEL"  # type: ignore[union-attr]
    assert [sid for sid, e in by_id.items() if e.primary] == [d.id]
    assert [(x.session_id, x.in_evidence) for x in detail.excluded_return_sessions] == [(c.id, True)]
    rows, _ = await claim_pack._session_rows(db, claim)
    assert [(s.id, main, kind) for s, main, kind in rows] == [
        (pack.id, True, "dong-goi"),
        (d.id, True, "mo-hoan"),
        (c.id, False, "phien-khac"),
    ]


async def test_only_excluded_return_session_falls_back_to_pack(
    db: AsyncSession, test_settings: Settings
) -> None:
    """Không có phiên RETURN hợp lệ khác: phiên quét nhầm (thêm tay) vẫn không là phiên chính → đóng gói."""
    station, package, case, pack = await _setup(db)
    c = await _return(db, station, package, case, T0, "CANCELLED", cancel_reason="NOT_A_RETURN")
    user = await make_user(db, "tst_cskh_t279b", "CSKH")
    p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)
    created = await claims.create_manual(
        db,
        ClaimCreateIn(package_id=package.id, type="OTHER", counterparty="PLATFORM", return_case_id=case.id),
        p,
    )
    assert await _evidence(db, created.id) == {pack.id}
    await claims.set_evidence(
        db,
        created,
        EvidenceIn(
            version=created.version, session_ids=[pack.id, c.id], snapshot_ids=await _snaps(db, created.id)
        ),
        p,
    )
    detail = await claim_views.claim_detail(db, created.id, user.id, test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [pack.id]


async def test_supervisor_cancel_other_is_regular_evidence(db: AsyncSession, test_settings: Settings) -> None:
    """(5): Supervisor hủy, `cancel_cause = OTHER` (kiện hoàn thật) → vào bằng chứng, là phiên chính được."""
    station, package, case, pack = await _setup(db)
    a = await _return(db, station, package, case, T0, "CANCELLED", cancel_reason="SUPERVISOR")
    a.cancel_cause = "OTHER"
    b = await _return(
        db, station, package, case, T0 + timedelta(minutes=30), "COMPLETED", conclusion="EMPTY_BOX"
    )

    claim = await _claim_of(db, b, case)

    assert await _evidence(db, claim.id) == {pack.id, a.id, b.id}
    detail = await claim_views.claim_detail(db, claim.id, uuid.uuid4(), test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [a.id]
    assert detail.excluded_return_sessions == []


async def test_excluded_session_clip_still_protected_and_not_counted(
    db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """(6) J-02 không xóa clip C khi hồ sơ hàng hoàn còn mở (BR-09 b); (7) API-32 `returns_dropped_7d` và
    API-30 `return_dropped` không tính C (cùng vị từ `dropped_return_filter` — J-26 N03 dùng lại ở T-227)."""
    from aicam.modules.media import protection
    from aicam.modules.orders import packages as package_views
    from aicam.modules.reports import service as reports
    from aicam.modules.sessions.queries import dropped_return_filter

    station, package, case, _ = await _setup(db)
    c = await _return(db, station, package, case, T0, "CANCELLED", cancel_reason="WRONG_SCAN")
    a = await _return(db, station, package, case, T0 + timedelta(minutes=1), "ABANDONED")
    now = clock.now()

    protected = set((await db.scalars(protection.case_session_ids(now))).all())
    assert {c.id, a.id} <= protected
    dropped = set(
        (
            await db.scalars(
                select(PackSession.id).where(dropped_return_filter(), PackSession.package_id == package.id)
            )
        ).all()
    )
    assert dropped == {a.id}
    report = await reports.daily(db, None, test_settings)
    assert report.counts.returns_dropped_7d == 1
    page = await package_views.search(
        db, tz=test_settings.tz_display, page=1, page_size=20, return_dropped=True
    )
    assert [i.id for i in page.items] == [package.id]


async def test_primary_kept_but_flagged_when_cam1_missing(db: AsyncSession, test_settings: Settings) -> None:
    """G3-EV-4: giữ BR-39 — phiên chính = lần mở hộp đầu (A) dù Cam 1 thiếu tệp; API-132 / API-164 báo
    `primary_unavailable` + lý do; README J-16 ghi "phiên chính thiếu tệp"."""
    from zoneinfo import ZoneInfo

    from aicam.modules.shares import service as shares

    station, package, case, _pack = await _setup(db)
    a = await _return(db, station, package, case, T0, "ABANDONED", clip_status="MISSING")
    b = await _return(
        db, station, package, case, T0 + timedelta(minutes=84), "COMPLETED", conclusion="EMPTY_BOX"
    )
    created = await claims.create_from_return(db, b, case)
    assert created is not None
    detail = await claim_views.claim_detail(db, created.claim.id, uuid.uuid4(), test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [a.id]
    assert (detail.primary_unavailable, detail.primary_unavailable_reason) == (True, "CLIP_MISSING")
    opts = await shares.options(db, created.claim.id, None, test_settings)
    assert (opts.primary_unavailable, opts.primary_unavailable_reason) == (True, "CLIP_MISSING")
    claim = await db.get(Claim, created.claim.id)
    assert claim is not None
    note = await claim_pack._primary_note(db, claim, ZoneInfo("Asia/Ho_Chi_Minh"))
    assert "PHIÊN CHÍNH THIẾU TỆP" in note
    assert "08:51 06/10/2026" in note
    assert "CLIP_MISSING" in note

    # Cam 1 về lại READY → hết cờ
    cam1 = await db.scalar(select(Clip).where(Clip.session_id == a.id, Clip.camera_role == "CAM1"))
    assert cam1 is not None
    cam1.status = "READY"
    await db.flush()
    detail = await claim_views.claim_detail(db, created.claim.id, uuid.uuid4(), test_settings)
    assert (detail.primary_unavailable, detail.primary_unavailable_reason) == (False, None)
    assert await claim_pack._primary_note(db, claim, ZoneInfo("Asia/Ho_Chi_Minh")) == ""
