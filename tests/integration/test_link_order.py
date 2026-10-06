"""API-105 mở phiên hoàn từ kết quả tìm / chưa xác định / `force_new`, API-112 gắn đơn, API-30
`is_placeholder` (T-119; 02 §6.2, §6.3 #9, §6.4 #2, §6.5 #1; DEC-260, DEC-269, R2-9).

TC-04.08..04.12, TC-04.45, TC-07.33, TC-07.36, TC-P2.06.
"""

import re
import uuid
from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.orders.models import Package, StatusHistory
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_user
from .returns_helpers import Desk, buyer_return_case, make_desk, make_order, pack_session_with_clips

pytestmark = pytest.mark.integration

UNKNOWN = "SPXVN0000000000"


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    return await make_desk(api, db)


async def _open(desk: Desk, **body: Any) -> Response:
    return await desk.api.post(
        "/api/v1/station/return-sessions",
        headers=desk.headers,
        json={"client_scan_id": str(uuid.uuid4()), **body},
    )


async def _conclude_and_close(
    desk: Desk, opened: dict[str, Any], conclusion: str, code: str
) -> dict[str, Any]:
    session = opened["state"]["session"]
    lines = [
        {"order_item_id": line["order_item_id"], "quantity_received": line["quantity_received"],
         "condition": conclusion, "note": None}
        for line in session["inspection"]["lines"]
    ]  # fmt: skip
    saved = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection",
        headers=desk.headers,
        json={"conclusion": conclusion, "note": "ghi chú" if conclusion == "OTHER" else "", "lines": lines},
    )
    assert saved.status_code == 200, saved.text
    closed = (await desk.scan(code)).json()
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    return closed  # type: ignore[no-any-return]


async def _lead(api: AsyncClient, db: AsyncSession, role: str = "SUPERVISOR") -> dict[str, str]:
    user = await make_user(db, f"tst_{role.lower()}_lo", role, display_name=f"{role} LO")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _unidentified_received(desk: Desk, db: AsyncSession, conclusion: str) -> ReturnCase:
    opened = (await _open(desk, unidentified_code=UNKNOWN)).json()
    assert opened["outcome"] == "SESSION_OPENED", opened
    await _conclude_and_close(desk, opened, conclusion, UNKNOWN)
    case = await db.scalar(select(ReturnCase).where(ReturnCase.kind == "UNIDENTIFIED"))
    assert case is not None
    await db.refresh(case)
    return case


# ---------------------------------------------------------------- API-105


async def test_open_unidentified_creates_placeholder(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """TC-04.09: mã lạ → hồ sơ `UNIDENTIFIED` + kiện tạm `TAM-` + 6 số, `open_code` = mã quét, cờ phiên;
    API-30 trả `is_placeholder = true`."""
    res = await _open(desk, unidentified_code=f" {UNKNOWN.lower()} ")

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["outcome"] == "SESSION_OPENED"
    session = body["state"]["session"]
    assert "UNIDENTIFIED" in session["flags"]
    assert session["return_case"]["kind"] == "UNIDENTIFIED"
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    package = await db.get(Package, pack.package_id)
    assert package is not None
    assert pack.open_code == UNKNOWN
    assert re.fullmatch(r"TAM-\d{6}", package.tracking_number)
    assert (package.is_placeholder, package.warehouse_status) == (True, "RETURN_INSPECTING")
    found = (
        await api.get("/api/v1/packages", headers=await _lead(api, db), params={"q": package.tracking_number})
    ).json()
    assert [(i["tracking_number"], i["is_placeholder"]) for i in found["items"]] == [
        (package.tracking_number, True)
    ]


async def test_unidentified_code_matching_existing_case(desk: Desk, db: AsyncSession) -> None:
    """02 §6.3 #9: `unidentified_code` khớp kiện / hồ sơ đã có → xử lý như quét mã đó (không tạo kiện tạm)."""
    order, _ = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)

    body = (await _open(desk, unidentified_code="SPXRTTST000041")).json()

    assert body["outcome"] == "SESSION_OPENED"
    assert body["state"]["session"]["return_case"]["code"] == case.code
    assert await db.scalar(select(Package.id).where(Package.is_placeholder.is_(True))) is None


async def test_open_from_lookup_result(desk: Desk, db: AsyncSession) -> None:
    """TC-04.45 (API): mở bằng `package_id`; kiện chưa gửi đi → 200 `ALERT NOT_SHIPPED`; kiện lạ → 404;
    trùng `client_scan_id` → trả lại kết quả cũ; đang có phiên → 409 `SESSION_ACTIVE`."""
    order, (package,) = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    _, (packed,) = await make_order(db, 10, warehouse_status="PACKED", status="READY_TO_SHIP")

    blocked = (await _open(desk, package_id=str(packed.id))).json()
    assert (blocked["outcome"], blocked["alert"]["code"]) == ("ALERT", "NOT_SHIPPED")
    missing = await _open(desk, package_id=str(uuid.uuid4()))
    assert missing.status_code == 404
    scan_id = str(uuid.uuid4())
    first = await desk.api.post(
        "/api/v1/station/return-sessions", headers=desk.headers,
        json={"package_id": str(package.id), "client_scan_id": scan_id},
    )  # fmt: skip
    assert first.json()["outcome"] == "SESSION_OPENED"
    again = await desk.api.post(
        "/api/v1/station/return-sessions", headers=desk.headers,
        json={"package_id": str(package.id), "client_scan_id": scan_id},
    )  # fmt: skip
    assert again.json()["outcome"] == "SESSION_OPENED"
    busy = await _open(desk, unidentified_code=UNKNOWN)
    assert (busy.status_code, busy.json()["error"]["code"]) == (409, "SESSION_ACTIVE")


async def test_validation_and_work_mode(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> None:
    """Thiếu / thừa lựa chọn, mã sai định dạng → 422; station chế độ đóng gói → 409 `WRONG_WORK_MODE`;
    chưa có người kiểm → `ALERT OPERATOR_REQUIRED`."""
    desk = await make_desk(api, db, operator=None)
    neither = await _open(desk)
    assert neither.status_code == 422
    both = await _open(desk, unidentified_code=UNKNOWN, package_id=str(uuid.uuid4()))
    assert both.status_code == 422
    bad = await _open(desk, unidentified_code="ab")
    assert bad.json()["error"]["details"]["fields"] == {
        "unidentified_code": "Mã không đúng định dạng mã vận đơn / mã đơn"
    }
    no_operator = (await _open(desk, unidentified_code=UNKNOWN)).json()
    assert no_operator["alert"]["code"] == "OPERATOR_REQUIRED"
    packer = await make_desk(api, db, 2, mode="PACK")
    wrong = await _open(packer, unidentified_code=UNKNOWN)
    assert (wrong.status_code, wrong.json()["error"]["code"]) == (409, "WRONG_WORK_MODE")


async def test_force_new_after_received(desk: Desk, db: AsyncSession) -> None:
    """TC-04.11: kiện đã nhận + "Đây là kiện khác — vẫn ghi hình" → hồ sơ `UNIDENTIFIED` `manual_link_only`,
    `force_note`, ghi chú phiên, audit `RETURN_FORCE_NEW`; thiếu ghi chú → 422; không bị gộp tự động."""
    order, (package,) = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    opened = (await desk.scan("SPXRTTST000041")).json()
    await _conclude_and_close(desk, opened, "OK", "SPXTST0000041")

    no_note = await _open(desk, unidentified_code="SPXTST0000041", force_new=True, note="abc")
    assert no_note.json()["error"]["details"]["fields"] == {"note": "Nhập ghi chú 5–200 ký tự"}
    res = await _open(desk, unidentified_code="SPXTST0000041", force_new=True, note="Kiện thứ hai cùng mã")

    assert res.status_code == 200, res.text
    session = res.json()["state"]["session"]
    assert session["return_case"]["kind"] == "UNIDENTIFIED"
    case = await db.scalar(select(ReturnCase).where(ReturnCase.kind == "UNIDENTIFIED"))
    assert case is not None
    assert (case.manual_link_only, case.force_note) == (True, "Kiện thứ hai cùng mã")
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    assert (pack.note, pack.open_code) == ("Kiện thứ hai cùng mã", "SPXTST0000041")
    audit = await db.scalar(
        select(AuditLog).where(AuditLog.action == "RETURN_FORCE_NEW", AuditLog.object_id == str(case.id))
    )
    assert audit is not None
    assert audit.data is not None
    assert audit.data["received_package_id"] == str(package.id)
    # Không vào gộp tự động theo mã (R3-1).
    assert await returns.merge_unidentified_by_code(db, order) == []


async def test_force_new_rejected_when_not_received(desk: Desk, db: AsyncSession) -> None:
    """TC-04.12: `force_new` với kiện chưa nhận → 409 `FORCE_NEW_NOT_ALLOWED`, `details.reason` = mã thật;
    mã lạ → `RETURN_NOT_FOUND`; không tạo hồ sơ."""
    await make_order(db, 42, warehouse_status="HANDED_OVER")

    res = await _open(desk, unidentified_code="SPXTST0000042", force_new=True, note="thử ghi hình")
    unknown = await _open(desk, unidentified_code=UNKNOWN, force_new=True, note="thử ghi hình")

    assert (res.status_code, res.json()["error"]["code"]) == (409, "FORCE_NEW_NOT_ALLOWED")
    assert res.json()["error"]["details"]["reason"] == "OPENABLE"
    assert unknown.json()["error"]["details"]["reason"] == "RETURN_NOT_FOUND"
    assert await db.scalar(select(ReturnCase.id)) is None


# ---------------------------------------------------------------- API-112


async def test_link_order_to_package_without_case(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """TC-07.33: hồ sơ chưa xác định (đã đóng phiên "Hư hỏng") → gắn kiện 46: hồ sơ gắn đơn, `UNANNOUNCED`;
    phiên sang kiện 46 → `RETURN_RECEIVED_ISSUE` (nguồn Tay); kiện tạm xóa; hồ sơ khiếu nại sang kiện 46 +
    thêm phiên PACK hiệu lực; audit `RETURN_LINK_ORDER`."""
    case = await _unidentified_received(desk, db, "DAMAGED")
    placeholder_id = (await returns.packages_of_case(db, case.id))[0].id
    claim = await db.scalar(select(Claim).where(Claim.package_id == placeholder_id))
    assert claim is not None
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")
    pack = await pack_session_with_clips(db, desk.station, package, clock.now() - timedelta(days=3))
    headers = await _lead(api, db)

    res = await api.post(
        f"/api/v1/returns/{case.id}/link-order", headers=headers, json={"package_id": str(package.id)}
    )

    assert res.status_code == 200, res.text
    body = res.json()
    assert (body["id"], body["kind"], body["order"]["platform_order_sn"]) == (
        str(case.id),
        "UNANNOUNCED",
        "2410TST00046",
    )
    assert (body["merged_into"], body["merged_claims"]) == (None, [])
    assert [p["tracking_number"] for p in body["packages"]] == ["SPXTST0000046"]
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_RECEIVED_ISSUE"
    sources = (
        await db.scalars(
            select(StatusHistory.source)
            .where(StatusHistory.package_id == package.id)
            .order_by(StatusHistory.at)
        )
    ).all()
    assert sources[-2:] == ["MANUAL", "MANUAL"]
    assert await db.get(Package, placeholder_id) is None
    await db.refresh(claim)
    assert (claim.package_id, claim.order_id) == (package.id, order.id)
    evidence = set(
        (await db.scalars(select(ClaimEvidence.session_id).where(ClaimEvidence.claim_id == claim.id))).all()
    )
    assert pack.id in evidence
    audit = await db.scalar(
        select(AuditLog).where(AuditLog.action == "RETURN_LINK_ORDER", AuditLog.object_id == str(case.id))
    )
    assert audit is not None


async def test_link_order_merges_into_open_case(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """TC-07.36 (R2-9): đơn 41 có hồ sơ `EXPECTED` → hồ sơ chưa xác định `CANCELLED`, `merged_into` = hồ sơ
    41; kiện 41 `RETURN_RECEIVED_OK` (nguồn Tay); trả chi tiết hồ sơ đích."""
    case = await _unidentified_received(desk, db, "OK")
    order, (package,) = await make_order(db, 41)
    target = await buyer_return_case(db, order, 41)

    res = await api.post(
        f"/api/v1/returns/{case.id}/link-order", headers=await _lead(api, db, "ADMIN"),
        json={"package_id": str(package.id)},
    )  # fmt: skip

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["id"] == str(target.id)
    assert body["merged_into"] == {"id": str(target.id), "code": target.code}
    assert body["status"] == "RECEIVED_OK"
    await db.refresh(case)
    assert (case.status, case.merged_into_id) == ("CANCELLED", target.id)
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_RECEIVED_OK"


async def test_link_order_merges_duplicate_claim(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """DEC-260: kiện đích đã có hồ sơ khiếu nại cùng loại đang mở → `merged_claims` [{from, into}]."""
    case = await _unidentified_received(desk, db, "DAMAGED")
    moved = await db.scalar(select(Claim).where(Claim.type == "DAMAGED"))
    assert moved is not None
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")
    keeper = Claim(package_id=package.id, order_id=order.id, type="DAMAGED", counterparty="PLATFORM",
                   status="NEW", source="MANUAL")  # fmt: skip
    db.add(keeper)
    await db.flush()
    await db.refresh(keeper, ["code"])

    res = await api.post(
        f"/api/v1/returns/{case.id}/link-order",
        headers=await _lead(api, db),
        json={"package_id": str(package.id)},
    )

    assert res.status_code == 200, res.text
    assert res.json()["merged_claims"] == [{"from": moved.code, "into": keeper.code}]
    await db.refresh(moved)
    assert (moved.status, moved.close_reason) == ("CLOSED", f"Gộp vào {keeper.code}")


async def test_link_order_errors(desk: Desk, db: AsyncSession, api: AsyncClient) -> None:
    """NOT_UNIDENTIFIED, PACKAGE_ALREADY_RETURNED, NOT_ELIGIBLE (kiện chưa rời kho), 404, CSKH 403
    (TC-P2.06)."""
    case = await _unidentified_received(desk, db, "OK")
    headers = await _lead(api, db)
    order41, (received,) = await make_order(db, 41)
    normal = await buyer_return_case(db, order41, 41)
    _, (packed,) = await make_order(db, 10, warehouse_status="PACKED", status="READY_TO_SHIP")
    db.add(
        PackSession(
            id=uuid.uuid4(),
            type="RETURN",
            package_id=received.id,
            station_id=desk.station.id,
            return_case_id=normal.id,
            status="COMPLETED",
            started_at=clock.now(),
            ended_at=clock.now(),
            open_code="SPXTST0000041",
            inspection_conclusion="OK",
            flags=[],
        )
    )
    received.warehouse_status = "RETURN_RECEIVED_OK"
    await db.flush()

    async def link(case_id: uuid.UUID, package_id: uuid.UUID, h: dict[str, str] = headers) -> Response:
        return await api.post(
            f"/api/v1/returns/{case_id}/link-order", headers=h, json={"package_id": str(package_id)}
        )

    not_unidentified = await link(normal.id, packed.id)
    assert (not_unidentified.status_code, not_unidentified.json()["error"]["code"]) == (
        409,
        "NOT_UNIDENTIFIED",
    )
    already = await link(case.id, received.id)
    assert (already.status_code, already.json()["error"]["code"]) == (409, "PACKAGE_ALREADY_RETURNED")
    not_eligible = await link(case.id, packed.id)
    assert (not_eligible.status_code, not_eligible.json()["error"]["code"]) == (409, "NOT_ELIGIBLE")
    assert (await link(uuid.uuid4(), packed.id)).status_code == 404
    cskh = await make_user(db, "tst_cskh_lo", "CSKH")
    login = await api.post(
        "/api/v1/auth/login", json={"username": cskh.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    forbidden = await link(case.id, packed.id, {"Authorization": f"Bearer {login.json()['access_token']}"})
    assert forbidden.status_code == 403
