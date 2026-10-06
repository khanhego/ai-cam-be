"""T-281 — API-189 soát phiên mở hoàn (BR-39 v0.4, EX-R21; 02a §5 BR-39 kịch bản (10)–(13)) + vị từ SQL /
Python một luật (`sessions.queries`) + G2R2-1 (Supervisor hủy RETURN có / không mã lý do)."""

import itertools
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.deps import Principal
from aicam.core.settings import Settings
from aicam.modules.claims import pack as claim_pack
from aicam.modules.claims import service as claims
from aicam.modules.claims import views as claim_views
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.media import protection
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package
from aicam.modules.reports import service as reports
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions import queries
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order, pack_session_with_clips, return_session

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 6, 1, 51, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _clock(redis_client: object) -> None:
    clock.freeze(T0 + timedelta(hours=3))


async def _return(
    db: AsyncSession, station: Station, package: Package, case: ReturnCase, started: datetime, status: str,
    *, conclusion: str | None = None, clip: bool = True, **kw: Any,
) -> PackSession:  # fmt: skip
    s = return_session(station, package, case, status=status, conclusion=conclusion)
    s.started_at, s.ended_at = started, started + timedelta(minutes=3)
    for k, v in kw.items():
        setattr(s, k, v)
    db.add(s)
    await db.flush()
    if clip:
        db.add(Clip(session_id=s.id, camera_role="CAM1", status="READY", start_at=s.started_at,
                    end_at=s.ended_at,
                    path=f"clips/{s.id}.mp4", flags=[]))  # fmt: skip
        await db.flush()
    return s


async def _setup(db: AsyncSession, n: int = 41) -> tuple[Station, Package, ReturnCase, PackSession]:
    _, station = await make_station_account(db, f"tst_st281_{n}", f"TST 281 {n}")
    order, (package,) = await make_order(db, n, warehouse_status="RETURN_EXPECTED")
    case = await buyer_return_case(db, order, n)
    pack = await pack_session_with_clips(db, station, package, T0 - timedelta(days=5))
    return station, package, case, pack


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> tuple[dict[str, str], Principal]:
    username = f"tst_{role.lower()}_{uuid.uuid4().hex[:6]}"
    user = await make_user(db, username, role, display_name=f"{role} QA")
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, Principal(
        user_id=user.id, role=role, station_id=None, ip=None
    )


async def _manual(db: AsyncSession, p: Principal, package: Package, case: ReturnCase, kind: str) -> Claim:
    return await claims.create_manual(
        db,
        ClaimCreateIn(package_id=package.id, type=kind, counterparty="PLATFORM", return_case_id=case.id),
        p,
    )


async def _review(
    api: AsyncClient, headers: dict[str, str], claim: Claim | dict[str, Any], session_id: uuid.UUID,
    action: str,
    note: str = "Xem video: kiện của đơn khác", **extra: Any,
) -> Any:  # fmt: skip
    cid = claim.id if isinstance(claim, Claim) else claim["id"]
    version = claim.version if isinstance(claim, Claim) else claim["version"]
    return await api.post(
        f"/api/v1/claims/{cid}/return-sessions/{session_id}/review", headers=headers,
        json={"version": version, "action": action, "note": note, **extra},
    )  # fmt: skip


async def _active(db: AsyncSession, claim_id: uuid.UUID) -> set[uuid.UUID]:
    rows = await db.scalars(
        select(ClaimEvidence.session_id).where(
            ClaimEvidence.claim_id == claim_id,
            ClaimEvidence.removed_at.is_(None),
            ClaimEvidence.session_id.is_not(None),
        )
    )
    return set(rows.all())  # type: ignore[arg-type]


def _primary(detail: dict[str, Any]) -> list[str]:
    return [e["session"]["id"] for e in detail["evidence"] if e["primary"]]


# ---------------------------------------------------------------- (10) MARK nhiều hồ sơ


async def test_mark_wrong_scan_soft_removes_from_open_claims_only(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    station, package, case, _ = await _setup(db)
    a = await _return(db, station, package, case, T0, "ABANDONED")
    b = await _return(
        db, station, package, case, T0 + timedelta(minutes=30), "CANCELLED", cancel_reason="OTHER"
    )
    headers, p = await _login(api, db, "CSKH")
    c1 = await _manual(db, p, package, case, "EMPTY_BOX")
    c2 = await _manual(db, p, package, case, "DAMAGED")
    closed = await _manual(db, p, package, case, "OTHER")
    closed.status, closed.closed_at = "CLOSED", clock.now()
    await db.flush()
    assert {a.id, b.id} <= await _active(db, c1.id)
    before = (await api.get(f"/api/v1/claims/{c1.id}", headers=headers)).json()
    assert _primary(before) == [str(a.id)]

    res = await _review(api, headers, before, a.id, "MARK_WRONG_SCAN", reason_code="WRONG_SCAN")

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["version"] == before["version"] + 1
    assert _primary(body) == [str(b.id)]  # phiên chính chuyển sang B
    assert str(a.id) not in {e["session"]["id"] for e in body["evidence"] if e["session"]}
    removed = next(e for e in body["removed_evidence"] if e["session"] and e["session"]["id"] == str(a.id))
    assert removed["removed"]["reason"] == "Đánh dấu quét nhầm: Xem video: kiện của đơn khác"
    excluded = body["excluded_return_sessions"]
    assert [(x["session_id"], x["evidence_exclusion"], x["in_evidence"]) for x in excluded] == [
        (str(a.id), "MARKED", False)
    ]
    assert excluded[0]["wrong_scan"]["code"] == "WRONG_SCAN"
    assert excluded[0]["wrong_scan"]["by"]["display_name"] == "CSKH QA"
    assert a.id not in await _active(db, c2.id)  # hồ sơ mở khác cũng bỏ
    assert a.id in await _active(db, closed.id)  # hồ sơ đã đóng giữ nguyên
    await db.refresh(c2)
    assert c2.version == 2
    removes = (await db.scalars(select(AuditLog).where(AuditLog.action == "CLAIM_EVIDENCE_REMOVE"))).all()
    assert sorted(r.object_id for r in removes if r.data["session_id"] == str(a.id)) == sorted(
        [str(c1.id), str(c2.id)]
    )
    mark = await db.scalar(select(AuditLog).where(AuditLog.action == "SESSION_WRONG_SCAN_MARK"))
    assert mark is not None
    assert sorted(mark.data["removed_from_claims"]) == sorted([str(c1.id), str(c2.id)])
    # J-02 không xóa clip A trước `keep_until` (dòng đã bỏ còn hạn bảo vệ — BR-38)
    cutoff = clock.now() - timedelta(days=await claims.keep_days(db))
    assert a.id in set((await db.scalars(protection.claim_session_ids(cutoff))).all())
    # N03 / D2 không đếm A; B (OTHER) vẫn tính
    report = await reports.daily(db, None, test_settings)
    assert report.counts.returns_dropped_7d == 1
    # J-16: không có A
    claim = await db.get(Claim, c1.id)
    assert claim is not None
    rows, _ = await claim_pack._session_rows(db, claim)
    assert a.id not in {s.id for s, _, _ in rows}


# ---------------------------------------------------------------- (11) UNMARK


async def test_unmark_does_not_re_add_then_manual_add_can_be_primary(
    api: AsyncClient, db: AsyncSession
) -> None:
    station, package, case, _ = await _setup(db)
    a = await _return(db, station, package, case, T0, "ABANDONED")
    d = await _return(
        db, station, package, case, T0 + timedelta(minutes=9), "COMPLETED", conclusion="EMPTY_BOX"
    )
    headers, p = await _login(api, db, "SUPERVISOR")
    claim = await _manual(db, p, package, case, "EMPTY_BOX")
    marked = (await _review(api, headers, claim, a.id, "MARK_WRONG_SCAN", reason_code="NOT_A_RETURN")).json()
    assert _primary(marked) == [str(d.id)]

    unmarked = await _review(api, headers, marked, a.id, "UNMARK_WRONG_SCAN", note="Nhầm — đúng kiện này")

    assert unmarked.status_code == 200, unmarked.text
    body = unmarked.json()
    assert body["excluded_return_sessions"] == []
    assert a.id not in await _active(db, claim.id)  # không tự vào lại
    assert _primary(body) == [str(d.id)]
    snaps = [e["snapshot"]["id"] for e in body["evidence"] if e["snapshot"]]
    sessions_now = [e["session"]["id"] for e in body["evidence"] if e["session"]]
    put = await api.put(
        f"/api/v1/claims/{claim.id}/evidence", headers=headers,
        json={"version": body["version"], "session_ids": [*sessions_now, str(a.id)], "snapshot_ids": snaps},
    )  # fmt: skip
    assert put.status_code == 200, put.text
    assert _primary(put.json()) == [str(a.id)]  # thêm lại tay → lại là phiên chính (sớm nhất)
    unmark_audit = await db.scalar(select(AuditLog).where(AuditLog.action == "SESSION_WRONG_SCAN_UNMARK"))
    assert unmark_audit is not None


# ---------------------------------------------------------------- (12) Cần soát + G2R2-1


async def test_review_needed_never_primary_until_confirmed(api: AsyncClient, db: AsyncSession) -> None:
    """Phiên Supervisor hủy trước Phase 3 (không mã lý do) vào bằng chứng, "Cần soát", không phiên chính, zip
    thư mục `mo-hoan-can-soat`; `CONFIRM_RETURN` → phiên chính."""
    station, package, case, _ = await _setup(db)
    r = await _return(db, station, package, case, T0, "CANCELLED", cancel_reason="SUPERVISOR")
    later = await _return(db, station, package, case, T0 + timedelta(minutes=20), "ABANDONED")
    headers, p = await _login(api, db, "CSKH")
    claim = await _manual(db, p, package, case, "EMPTY_BOX")
    detail = (await api.get(f"/api/v1/claims/{claim.id}", headers=headers)).json()

    assert r.id in await _active(db, claim.id)
    assert _primary(detail) == [str(later.id)]
    ev = next(e for e in detail["evidence"] if e["session"] and e["session"]["id"] == str(r.id))
    assert (ev["session"]["review_needed"], ev["session"]["evidence_exclusion"]) == (True, None)
    assert [(x["session_id"], x["in_evidence"]) for x in detail["review_sessions"]] == [(str(r.id), True)]
    loaded = await db.get(Claim, claim.id)
    assert loaded is not None
    rows, _ = await claim_pack._session_rows(db, loaded)
    assert {s.id: kind for s, _, kind in rows}[r.id] == "mo-hoan-can-soat"

    res = await _review(api, headers, detail, r.id, "CONFIRM_RETURN", note="Xem video: kiện hoàn thật")

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["review_sessions"] == []
    assert _primary(body) == [str(r.id)]
    ev = next(e for e in body["evidence"] if e["session"] and e["session"]["id"] == str(r.id))
    assert ev["session"]["return_confirmed"]["note"] == "Xem video: kiện hoàn thật"
    confirm = await db.scalar(select(AuditLog).where(AuditLog.action == "SESSION_RETURN_CONFIRM"))
    assert confirm is not None
    assert confirm.data["overridden_cause"] is None


async def test_supervisor_cancel_with_reason_code_excluded_g2r2_1(
    db: AsyncSession, test_settings: Settings
) -> None:
    """(8) G2R2-1: Supervisor hủy (API-21) `reason_code = WRONG_SCAN` phiên > 60 giây có clip + phiên sau
    `EMPTY_BOX` → KN tự tạo không có phiên đó, phiên chính là phiên sau; `cancel_reason` vẫn `SUPERVISOR`."""
    station, package, case, _ = await _setup(db)
    c = await _return(
        db, station, package, case, T0, "CANCELLED", cancel_reason="SUPERVISOR", cancel_cause="WRONG_SCAN"
    )
    c.ended_at = c.started_at + timedelta(minutes=4)
    d = await _return(
        db, station, package, case, T0 + timedelta(minutes=10), "COMPLETED", conclusion="EMPTY_BOX"
    )

    created = await claims.create_from_return(db, d, case)

    assert created is not None
    assert c.id not in await _active(db, created.claim.id)
    detail = await claim_views.claim_detail(db, created.claim.id, uuid.uuid4(), test_settings)
    assert [e.session.id for e in detail.evidence if e.session and e.primary] == [d.id]
    assert [
        (x.session_id, x.evidence_exclusion, x.cancel_reason, x.cancel_cause)
        for x in detail.excluded_return_sessions
    ] == [(c.id, "SUPERVISOR_CANCEL", "SUPERVISOR", "WRONG_SCAN")]
    claim = await db.get(Claim, created.claim.id)
    assert claim is not None
    rows, _ = await claim_pack._session_rows(db, claim)
    assert c.id not in {s.id for s, _, _ in rows}


# ---------------------------------------------------------------- (13) lỗi


async def test_review_errors(api: AsyncClient, db: AsyncSession) -> None:
    station, package, case, _ = await _setup(db)
    a = await _return(db, station, package, case, T0, "ABANDONED")
    done = await _return(db, station, package, case, T0 + timedelta(minutes=9), "COMPLETED", conclusion="OK")
    station_cancel = await _return(db, station, package, case, T0 + timedelta(minutes=1), "CANCELLED",
                                   cancel_reason="WRONG_SCAN")  # fmt: skip
    _, other_package, other_case, _ = await _setup(db, 44)
    foreign = await _return(db, station, other_package, other_case, T0, "ABANDONED")
    headers, p = await _login(api, db, "CSKH")
    claim = await _manual(db, p, package, case, "EMPTY_BOX")

    async def code(
        session_id: uuid.UUID, action: str, version: int | None = None, **kw: Any
    ) -> tuple[int, str]:
        res = await _review(
            api, headers, {"id": claim.id, "version": version or claim.version}, session_id, action, **kw
        )
        return res.status_code, res.json().get("error", {}).get("code", "")

    bad = await _review(api, headers, claim, a.id, "MARK_WRONG_SCAN", note="abc")
    assert bad.status_code == 422
    assert bad.json()["error"]["details"]["fields"] == {
        "reason_code": "Chọn lý do.",
        "note": "Nhập ghi chú (5–500 ký tự).",
    }
    assert (await _review(api, headers, claim, a.id, "DELETE")).status_code == 422
    assert await code(done.id, "MARK_WRONG_SCAN", reason_code="WRONG_SCAN") == (409, "SESSION_NOT_ELIGIBLE")
    assert await code(station_cancel.id, "MARK_WRONG_SCAN", reason_code="WRONG_SCAN") == (
        409,
        "SESSION_NOT_ELIGIBLE",
    )
    assert await code(a.id, "UNMARK_WRONG_SCAN") == (409, "SESSION_NOT_ELIGIBLE")
    assert await code(a.id, "CONFIRM_RETURN") == (409, "SESSION_NOT_ELIGIBLE")
    assert await code(foreign.id, "MARK_WRONG_SCAN", reason_code="WRONG_SCAN") == (404, "NOT_FOUND")
    assert await code(a.id, "MARK_WRONG_SCAN", version=claim.version + 5, reason_code="WRONG_SCAN") == (
        409,
        "VERSION_CONFLICT",
    )
    missing = await api.post(
        f"/api/v1/claims/{uuid.uuid4()}/return-sessions/{a.id}/review", headers=headers,
        json={"version": 1, "action": "MARK_WRONG_SCAN", "reason_code": "WRONG_SCAN",
              "note": "Quét nhầm kiện"},
    )  # fmt: skip
    assert missing.status_code == 404
    claim.status, claim.closed_at = "CLOSED", clock.now()
    await db.flush()
    assert await code(a.id, "MARK_WRONG_SCAN", reason_code="WRONG_SCAN") == (409, "CLAIM_CLOSED")
    station_headers = await api.post(
        "/api/v1/auth/login",
        json={"username": (await make_station_account(db, "tst_st281x", "X"))[0].username,
              "password": PASSWORD, "client": "STATION"},
    )  # fmt: skip
    forbidden = await _review(
        api,
        {"Authorization": f"Bearer {station_headers.json()['access_token']}"},
        claim,
        a.id,
        "UNMARK_WRONG_SCAN",
    )
    assert forbidden.status_code == 403
    assert (
        await db.scalar(select(func.count()).select_from(AuditLog).where(AuditLog.action.like("SESSION_%")))
        == 0
    )


# ---------------------------------------------------------------- một luật SQL = Python


async def test_sql_and_python_predicates_agree(db: AsyncSession) -> None:
    """`excluded_return_sql` / `review_needed_sql` / `dropped_return_filter` ≡ bản Python trên mọi tổ hợp."""
    station, package, case, _ = await _setup(db)
    combos = itertools.product(
        ("RETURN", "PACK"),
        ("CANCELLED", "ABANDONED", "COMPLETED"),
        (None, "WRONG_SCAN", "NOT_A_RETURN", "OTHER", "SUPERVISOR", "OUT_OF_STOCK"),
        (None, "WRONG_SCAN", "NOT_A_RETURN", "OTHER"),
        (False, True),
        (False, True),
    )
    made: list[PackSession] = []
    for i, (kind, status, reason, cause, marked, confirmed) in enumerate(combos):
        if kind == "PACK" and (cause or marked or confirmed):
            continue
        s = PackSession(
            id=uuid.uuid4(), type=kind, package_id=package.id, station_id=station.id, status=status,
            started_at=T0 + timedelta(seconds=i), ended_at=T0 + timedelta(seconds=i + 1), open_code="X",
            cancel_reason=reason, cancel_cause=cause, flags=[],
            wrong_scan_at=T0 if marked else None, wrong_scan_code="WRONG_SCAN" if marked else None,
            review_confirmed_at=T0 if confirmed else None,
            return_case_id=case.id if kind == "RETURN" else None,
        )  # fmt: skip
        made.append(s)
    db.add_all(made)
    await db.flush()
    ids = [s.id for s in made]
    for sql, py in (
        (queries.excluded_return_sql(), queries.excluded),
        (queries.review_needed_sql(), queries.review_needed),
        (queries.dropped_return_filter(), queries.dropped),
    ):
        hit = set((await db.scalars(select(PackSession.id).where(PackSession.id.in_(ids), sql))).all())
        assert hit == {s.id for s in made if py(s)}
    assert len(made) > 200
