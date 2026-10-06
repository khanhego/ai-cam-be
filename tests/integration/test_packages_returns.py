"""API-30 / 31 / 32 mở rộng + API-113 (T-115; FR-07.01, 07.02, 09.01, 04.11, 02.11).

TC-07.31, 07.32 (API), 07.34 (API), 07.35, TC-09.20, TC-ST.03, TC-P2.06 (API-113), TC-02.33 bước 1
(sửa kết luận OK → vấn đề tạo hồ sơ khiếu nại).
"""

import uuid
from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.modules.claims.models import Claim
from aicam.modules.media.models import Snapshot
from aicam.modules.orders.models import Package
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_user
from .returns_helpers import Desk, buyer_return_case, make_desk, make_order, pack_session_with_clips

pytestmark = pytest.mark.integration


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession) -> Desk:
    api._transport.app.dependency_overrides[get_platform_adapter] = MockAdapter  # type: ignore[attr-defined]
    return await make_desk(api, db)


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> dict[str, str]:
    user = await make_user(db, f"tst_pr_{role.lower()}", role, display_name=f"QA {role}")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


def _lines(session: dict[str, Any], **override: Any) -> list[dict[str, Any]]:
    return [
        {
            "order_item_id": line["order_item_id"],
            "quantity_received": override.get("quantity_received", line["quantity_received"]),
            "condition": override.get("condition", line["condition"]),
            "note": None,
        }
        for line in session["inspection"]["lines"]
    ]


async def _receive(
    desk: Desk, open_code: str, close_code: str, conclusion: str, **lines: Any
) -> dict[str, Any]:
    opened = (await desk.scan(open_code)).json()
    assert opened["outcome"] == "SESSION_OPENED", opened
    session = opened["state"]["session"]
    saved = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection",
        headers=desk.headers,
        json={"conclusion": conclusion, "note": "", "lines": _lines(session, **lines)},
    )
    assert saved.status_code == 200, saved.text
    closed = (await desk.scan(close_code)).json()
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    return session


async def _received_ok(desk: Desk, db: AsyncSession) -> tuple[Package, ReturnCase, dict[str, Any]]:
    """Kiện 41: phiên PACK có clip + ảnh lúc đóng gói; khách trả → nhận "Nguyên vẹn" (có 1 ảnh chụp tay)."""
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)
    await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=5))
    session = await _receive(desk, "SPXRTTST000041", "SPXTST0000041", "OK")
    db.add(
        Snapshot(session_id=uuid.UUID(session["id"]), kind="MANUAL", camera_role="CAM1", taken_at=clock.now(),
                 path="snapshots/x_01.jpg", sha256="ef" * 32, size_bytes=5, status="READY")
    )  # fmt: skip
    await db.flush()
    return package, case, session


# ---------------------------------------------------------------- API-30


async def test_api30_return_search_filters_and_brief(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-07.30 (API) / 07.31: `q` = mã chiều về hoặc mã `HH-` → kiện; lọc `session_type=RETURN`,
    `warehouse_status=RETURN_RECEIVED_OK`; item có `return_case` brief."""
    package, case, _ = await _received_ok(desk, db)
    await make_order(db, 42, warehouse_status="NEW", status="READY_TO_SHIP")
    headers = await _login(api, db, "CSKH")
    for q in ("SPXRTTST000041", case.code):
        items = (await api.get("/api/v1/packages", params={"q": q}, headers=headers)).json()["items"]
        assert [i["id"] for i in items] == [str(package.id)], q
        assert items[0]["return_case"] == {"id": str(case.id), "code": case.code, "kind": "BUYER_RETURN",
                                           "status": "RECEIVED_OK"}  # fmt: skip
    items = (await api.get("/api/v1/packages", params={"session_type": "RETURN"}, headers=headers)).json()[
        "items"
    ]
    assert [i["id"] for i in items] == [str(package.id)]
    res = await api.get(
        "/api/v1/packages", params={"warehouse_status": "RETURN_RECEIVED_OK"}, headers=headers
    )
    assert [i["id"] for i in res.json()["items"]] == [str(package.id)]
    res = await api.get("/api/v1/packages", params={"q": "SPXTST0000042"}, headers=headers)
    assert res.json()["items"][0]["return_case"] is None


# ---------------------------------------------------------------- API-31


async def test_api31_return_block(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-07.32 (API), FR-02.11: `return_cases[]` (như API-110), `recon_alerts[]`, `claims[]`,
    `allowed_status_targets`; phiên RETURN có `inspection`, `snapshots[]` (URL ký), `can_correct`; phiên
    PACK có `pack_snapshot`."""
    package, case, _ = await _received_ok(desk, db)
    db.add(
        ReconAlert(package_id=package.id, rule="RETURN_OVERDUE", severity="HIGH", context={}, context_key="k")
    )
    await db.flush()
    sup = await _login(api, db, "SUPERVISOR")
    body = (await api.get(f"/api/v1/packages/{package.id}", headers=sup)).json()
    assert body["is_placeholder"] is False
    assert [c["code"] for c in body["return_cases"]] == [case.code]
    assert body["return_cases"][0]["status"] == "RECEIVED_OK"
    assert body["recon_alerts"][0]["br"] == "BR-12"
    assert body["claims"] == []
    assert body["allowed_status_targets"] == []  # RETURN_RECEIVED_OK: không chỉnh tay (01 §7.1)
    ret, pack = body["sessions"][0], body["sessions"][1]
    assert (ret["type"], ret["operator_name"], ret["return_case_id"]) == ("RETURN", "Lan QA", str(case.id))
    assert ret["inspection"]["conclusion"] == "OK"
    assert ret["inspection"]["lines_mode"] == "FULL"
    assert ret["inspection"]["corrections"] == []
    assert ret["can_correct"] is True
    assert len(ret["snapshots"]) == 1
    assert "/media/snapshots/" in ret["snapshots"][0]["url"]
    assert ret["snapshots"][0]["status"] == "READY"
    assert ret["pack_snapshot"] is None
    assert (pack["type"], pack["inspection"], pack["can_correct"]) == ("PACK", None, False)
    assert pack["pack_snapshot"]["status"] == "READY"
    cskh = await _login(api, db, "CSKH")
    body = (await api.get(f"/api/v1/packages/{package.id}", headers=cskh)).json()
    assert body["sessions"][0]["can_correct"] is False


# ---------------------------------------------------------------- API-113


async def test_api113_ok_to_issue_then_back(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-07.34 (API), TC-02.33 bước 1: "Nguyên vẹn" → "Hư hỏng": kiện `RETURN_RECEIVED_ISSUE` (MANUAL), hồ sơ
    `RECEIVED_ISSUE`, hồ sơ khiếu nại tự tạo, `corrections[0]`, cờ, audit; sửa lại "Nguyên vẹn" → hồ sơ khiếu
    nại `AUTO_RETURN` `NEW` → `CLOSED`."""
    package, case, session = await _received_ok(desk, db)
    sup = await _login(api, db, "SUPERVISOR")
    res = await api.put(
        f"/api/v1/sessions/{session['id']}/inspection",
        headers=sup,
        json={"conclusion": "DAMAGED", "note": "Rách tay áo", "reason": "Người kiểm chọn nhầm",
              "lines": _lines(session, condition="DAMAGED")},
    )  # fmt: skip
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["inspection"]["conclusion"] == "DAMAGED"
    assert "INSPECTION_CORRECTED" in body["flags"]
    correction = body["inspection"]["corrections"][0]
    assert (correction["by"]["display_name"], correction["reason"]) == (
        "QA SUPERVISOR",
        "Người kiểm chọn nhầm",
    )
    assert correction["before"]["conclusion"] == "OK"
    assert body["inspection"]["corrected"]["reason"] == "Người kiểm chọn nhầm"
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_RECEIVED_ISSUE"
    refreshed = await db.get(ReturnCase, case.id, populate_existing=True)
    assert refreshed is not None
    assert (refreshed.status, refreshed.conclusion) == ("RECEIVED_ISSUE", "DAMAGED")
    claim = await db.scalar(select(Claim).where(Claim.package_id == package.id))
    assert claim is not None
    assert (claim.type, claim.source, claim.status) == ("DAMAGED", "AUTO_RETURN", "NEW")
    assert await db.scalar(select(AuditLog).where(AuditLog.action == "INSPECTION_CORRECT")) is not None

    res = await api.put(
        f"/api/v1/sessions/{session['id']}/inspection",
        headers=sup,
        json={"conclusion": "OK", "note": "", "reason": "Kiểm lại thấy nguyên vẹn", "lines": _lines(session)},
    )
    assert res.status_code == 200, res.text
    assert len(res.json()["inspection"]["corrections"]) == 2
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_RECEIVED_OK"
    await db.refresh(claim)
    assert (claim.status, claim.close_reason) == ("CLOSED", "Kết luận đã sửa thành Nguyên vẹn")


async def test_api113_errors_and_permissions(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-07.35: quá 7 ngày → `409 CORRECTION_WINDOW_EXPIRED`; lý do < 5 → 422; "Nguyên vẹn" mâu thuẫn dòng →
    `422 CONCLUSION_INCONSISTENT`; phiên PACK → `409 NOT_RETURN_SESSION`; CSKH → 403 (TC-P2.06)."""
    package, _, session = await _received_ok(desk, db)
    sup = await _login(api, db, "SUPERVISOR")
    url = f"/api/v1/sessions/{session['id']}/inspection"
    res = await api.put(
        url, headers=sup, json={"conclusion": "DAMAGED", "reason": "abc", "lines": _lines(session)}
    )
    assert res.status_code == 422
    assert "reason" in res.json()["error"]["details"]["fields"]
    res = await api.put(
        url, headers=sup,
        json={"conclusion": "OK", "reason": "Sửa số lượng", "lines": _lines(session, quantity_received=0)},
    )  # fmt: skip
    assert (res.status_code, res.json()["error"]["code"]) == (422, "CONCLUSION_INCONSISTENT")
    cskh = await _login(api, db, "CSKH")
    res = await api.put(url, headers=cskh, json={"conclusion": "OK", "reason": "Thử quyền", "lines": []})
    assert res.status_code == 403
    pack_id = await db.scalar(
        select(PackSession.id).where(PackSession.package_id == package.id, PackSession.type == "PACK")
    )
    res = await api.put(
        f"/api/v1/sessions/{pack_id}/inspection", headers=sup,
        json={"conclusion": "OK", "reason": "Không phải hoàn", "lines": []},
    )  # fmt: skip
    assert (res.status_code, res.json()["error"]["code"]) == (409, "NOT_RETURN_SESSION")
    await db.execute(
        update(PackSession)
        .where(PackSession.id == uuid.UUID(session["id"]))
        .values(ended_at=clock.now() - timedelta(days=8))
    )
    res = await api.put(
        url, headers=sup, json={"conclusion": "DAMAGED", "reason": "Quá hạn sửa", "lines": []}
    )
    assert (res.status_code, res.json()["error"]["code"]) == (409, "CORRECTION_WINDOW_EXPIRED")


# ---------------------------------------------------------------- API-32


async def test_api32_return_counts_and_attention(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-09.20: số hàng hoàn trong ngày, hồ sơ đang về / quá hạn, cảnh báo mở theo mức, hồ sơ khiếu nại mở /
    sắp hạn; station `work_mode` / `operator_name`; attention mới."""
    await _received_ok(desk, db)
    order2, (pkg2,) = await make_order(db, 43)
    await buyer_return_case(db, order2, 43)
    await _receive(desk, "SPXRTTST000043", "SPXTST0000043", "EMPTY_BOX", quantity_received=0,
                   condition="MISSING_ITEM")  # fmt: skip
    order3, _ = await make_order(db, 44)
    expected = await buyer_return_case(db, order3, 44)
    order4, _ = await make_order(db, 45)
    missing = await buyer_return_case(db, order4, 45)
    missing.status = "MISSING"
    db.add(
        ReconAlert(package_id=pkg2.id, rule="RETURN_OVERDUE", severity="HIGH", context={}, context_key="k")
    )
    await db.flush()
    claim = await db.scalar(select(Claim).where(Claim.package_id == pkg2.id))
    assert claim is not None
    claim.deadline_at = clock.now() + timedelta(hours=10)
    await db.flush()
    headers = await _login(api, db, "SUPERVISOR")
    body = (await api.get("/api/v1/reports/daily", headers=headers)).json()
    counts = body["counts"]
    assert (counts["returns_received"], counts["returns_received_issue"], counts["returns_unidentified"]) == (
        2,
        1,
        0,
    )
    assert (counts["returns_expected"], counts["returns_missing"]) == (1, 1)
    assert counts["recon_open"] == {"HIGH": 1, "MEDIUM": 0, "LOW": 0}
    assert (counts["claims_open"], counts["claims_due_soon"]) == (1, 1)
    assert counts["packed"] == 0  # phiên hoàn không tính vào "đã đóng gói"
    station = next(s for s in body["stations"] if s["id"] == str(desk.station.id))
    assert (station["work_mode"], station["operator_name"], station["state"]) == ("RETURN", "Lan QA", "READY")
    kinds = {a["kind"]: a.get("count") for a in body["attention"]}
    assert kinds["RETURN_MISSING"] == 1
    assert kinds["RECON_HIGH"] == 1
    assert kinds["CLAIM_DUE_SOON"] == 1
    assert expected.status == "EXPECTED"
