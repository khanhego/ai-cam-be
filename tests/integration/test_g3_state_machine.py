# ruff: noqa: E501 — chuỗi tiếng Việt dài trong docstring / dữ liệu test
"""G3 Phase 2 máy trạng thái hàng hoàn: SM-F1 (hồ sơ hủy), SM-F2 (gộp hồ sơ chưa xác định trên kiện thật),
SM-F8 (quét lại mã lạ), SM-F9 (sửa kết luận lỗi → lỗi khác), J-07 (kết luận đã lưu chưa đủ), SM-F4 (lỗi chuyển
trạng thái có mã)."""

import uuid
from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.claims.models import Claim
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_user
from .returns_helpers import Desk, buyer_return_case, make_desk, make_order

pytestmark = pytest.mark.integration

UNKNOWN = "SPXVN0000000123"


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    return await make_desk(api, db)


async def _open_unidentified(desk: Desk, code: str) -> Response:
    return await desk.api.post(
        "/api/v1/station/return-sessions",
        headers=desk.headers,
        json={"client_scan_id": str(uuid.uuid4()), "unidentified_code": code},
    )


async def _save(desk: Desk, session: dict[str, Any], conclusion: str, note: str = "ghi chú") -> None:
    line_note = "ghi chú dòng" if conclusion == "OTHER" else None
    lines = [{"order_item_id": x["order_item_id"], "quantity_received": x["quantity_received"],
              "condition": conclusion, "note": line_note} for x in session["inspection"]["lines"]]  # fmt: skip
    res = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection", headers=desk.headers,
        json={"conclusion": conclusion, "note": note, "lines": lines},
    )  # fmt: skip
    assert res.status_code == 200, res.text


async def test_sm_f1_cancelled_case_return_tracking_opens_new_case(desk: Desk, db: AsyncSession) -> None:
    """SM-F1 (EX-R7): quét mã chiều về của hồ sơ đã hủy → không mở trên hồ sơ hủy; hồ sơ mới (không báo trước)."""
    order, (package,) = await make_order(db, 81)
    old = await buyer_return_case(db, order, 81)
    old.status = "CANCELLED"
    package.warehouse_status = "DELIVERED"
    await db.flush()
    body = (await desk.scan("SPXRTTST000081")).json()
    assert body["outcome"] == "SESSION_OPENED", body
    assert body["state"]["session"]["return_case"]["id"] != str(old.id)
    case = await db.get(ReturnCase, uuid.UUID(body["state"]["session"]["return_case"]["id"]))
    assert case is not None
    assert case.kind == "UNANNOUNCED"


async def test_sm_f2_unidentified_on_real_package_merges(db: AsyncSession) -> None:
    """SM-F2 (DEC-307c): hồ sơ chưa xác định trên kiện thật chưa xác minh (phiên đã xong) → gộp được khi đơn có."""
    from .factories import make_station_account

    _, station = await make_station_account(db, "tst_smf2", "TST SMF2")
    package = Package(tracking_number="SPXTST0000082", warehouse_status="RETURN_RECEIVED_OK", verified=False)
    db.add(package)
    case = ReturnCase(kind="UNIDENTIFIED", status="RECEIVED_OK", source="WAREHOUSE", single_session=True,
                      signal_keys=[], requested_items=[])  # fmt: skip
    db.add(case)
    await db.flush()
    db.add(ReturnCasePackage(return_case_id=case.id, package_id=package.id))
    now = clock.now()
    db.add(PackSession(type="RETURN", package_id=package.id, station_id=station.id, return_case_id=case.id,
                       status="COMPLETED", started_at=now, ended_at=now, open_code=package.tracking_number,
                       inspection_conclusion="OK", flags=[]))  # fmt: skip
    order = Order(platform_order_sn="2410TST00082", platform_status="COMPLETED")
    db.add(order)
    await db.flush()
    package.order_id = order.id
    await db.flush()
    assert await returns.merge_unidentified(db, case, order, package.tracking_number)
    assert (case.order_id, case.kind) == (order.id, "UNANNOUNCED")


async def test_sm_f8_second_desk_same_unknown_code(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """SM-F8 (DEC-344): bàn khác quét / mở "chưa xác định" cùng mã lạ đang kiểm → thấy hồ sơ đó (đang kiểm ở
    nơi khác), không tạo hồ sơ thứ hai."""
    first = await _open_unidentified(desk, UNKNOWN)
    assert first.json()["outcome"] == "SESSION_OPENED", first.text
    other = await make_desk(api, db, 2)
    scan = (await other.scan(UNKNOWN)).json()
    assert scan["alert"]["code"] == "RETURN_IN_PROGRESS_ELSEWHERE", scan
    again = (await _open_unidentified(other, UNKNOWN)).json()
    assert again["alert"]["code"] == "RETURN_IN_PROGRESS_ELSEWHERE", again
    cases = (await db.scalars(select(ReturnCase.id).where(ReturnCase.kind == "UNIDENTIFIED"))).all()
    assert len(cases) == 1


async def test_sm_f8_received_unknown_code_already_received(desk: Desk, db: AsyncSession) -> None:
    first = await _open_unidentified(desk, UNKNOWN)
    session = first.json()["state"]["session"]
    await _save(desk, session, "OK", note="")
    closed = (await desk.scan(UNKNOWN)).json()
    assert closed["outcome"] == "SESSION_COMPLETED", closed
    again = (await desk.scan(UNKNOWN)).json()
    assert again["outcome"] == "ALERT"
    assert again["alert"]["code"] == "RETURN_ALREADY_RECEIVED"
    assert again["alert"]["data"]["can_record_other"] is True
    reopen = (await _open_unidentified(desk, UNKNOWN)).json()
    assert reopen["alert"]["code"] == "RETURN_ALREADY_RECEIVED"
    cases = (await db.scalars(select(ReturnCase.id).where(ReturnCase.kind == "UNIDENTIFIED"))).all()
    assert len(cases) == 1


async def _lead(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    user = await make_user(db, "tst_smf9_sup", "SUPERVISOR")
    res = await api.post("/api/v1/auth/login",
                         json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"})  # fmt: skip
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _closed_with(desk: Desk, db: AsyncSession, n: int, conclusion: str) -> dict[str, Any]:
    order, _ = await make_order(db, n)
    await buyer_return_case(db, order, n)
    opened = (await desk.scan(f"SPXRTTST{n:06d}")).json()
    session = opened["state"]["session"]
    await _save(desk, session, conclusion)
    assert (await desk.scan(f"SPXRTTST{n:06d}")).json()["outcome"] == "SESSION_COMPLETED"
    return session


async def _correct(
    api: AsyncClient, headers: dict[str, str], session: dict[str, Any], conclusion: str
) -> Response:
    lines = [{"order_item_id": x["order_item_id"], "quantity_received": x["quantity_received"],
              "condition": conclusion, "note": None} for x in session["inspection"]["lines"]]  # fmt: skip
    return await api.put(f"/api/v1/sessions/{session['id']}/inspection", headers=headers,
                         json={"conclusion": conclusion, "note": "ghi chú", "lines": lines,
                               "reason": "Chọn nhầm loại lỗi"})  # fmt: skip


async def test_sm_f9_correct_issue_to_other_issue(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """SM-F9 (DEC-344): DAMAGED → WRONG_ITEM: hồ sơ tự tạo còn NEW → đổi loại + ghi chú; hồ sơ đã gửi sàn →
    giữ + ghi chú, tạo hồ sơ loại mới."""
    headers = await _lead(api, db)
    s1 = await _closed_with(desk, db, 83, "DAMAGED")
    res = await _correct(api, headers, s1, "WRONG_ITEM")
    assert res.status_code == 200, res.text
    claims = (await db.scalars(select(Claim).where(Claim.source == "AUTO_RETURN")
                               .execution_options(populate_existing=True))).all()  # fmt: skip
    assert [(c.type, c.status) for c in claims] == [("WRONG_ITEM", "NEW")]

    s2 = await _closed_with(desk, db, 84, "DAMAGED")
    sent = (
        await db.scalars(select(Claim).where(Claim.type == "DAMAGED", Claim.source == "AUTO_RETURN"))
    ).one()
    sent.status = "SUBMITTED"
    await db.flush()
    res = await _correct(api, headers, s2, "EMPTY_BOX")
    assert res.status_code == 200, res.text
    rows = (await db.scalars(select(Claim).where(Claim.package_id == sent.package_id).order_by(Claim.created_at)
                             .execution_options(populate_existing=True))).all()  # fmt: skip
    assert [(c.type, c.status) for c in rows] == [("DAMAGED", "SUBMITTED"), ("EMPTY_BOX", "NEW")]


async def test_j07_does_not_auto_close_incomplete_inspection(
    desk: Desk, db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """J-07 (DEC-340): kết luận "Khác" đã lưu nhưng ghi chú bị trống → không tự hoàn tất; giữ phiên + cờ, D2."""
    order, _ = await make_order(db, 85)
    await buyer_return_case(db, order, 85)
    session = (await desk.scan("SPXRTTST000085")).json()["state"]["session"]
    await _save(desk, session, "OTHER")
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    pack.inspection_note = None  # dữ liệu cũ / lưu dở
    await db.flush()
    from aicam.realtime import publish

    alerts: list[dict[str, Any]] = []

    async def capture(_station: Any, event: str, data: dict[str, Any]) -> None:
        if event == "alert":
            alerts.append(data)

    monkeypatch.setattr(publish, "to_station", capture)
    clock.advance(timedelta(minutes=50))
    out = await sessions.check_timeouts(db, test_settings)
    await db.refresh(pack)
    assert pack.status == "OPEN"
    # G3 V2-4: station biết lý do cụ thể
    assert [a.get("reason") for a in alerts if a["code"] == "SESSION_WARN"] == ["INSPECTION_INCOMPLETE"]
    assert sessions.AUTO_CLOSE_BLOCKED in pack.flags
    assert out.get("blocked") == 1
    from aicam.modules.reports import service as reports

    report = await reports.daily(db, None, test_settings)
    assert {"kind": "RETURN_SESSION_ABANDONED", "count": 1} in report.attention


async def test_invalid_transition_is_coded_error(api: AsyncClient, db: AsyncSession) -> None:
    """SM-F4: InvalidTransition không thành 500."""
    from aicam.main import create_app
    from aicam.modules.orders.service import InvalidTransition

    app = create_app(Settings(app_env="test"))

    @app.get("/boom")
    async def boom() -> None:
        raise InvalidTransition("PACKING", "CANCELLED")

    from httpx import ASGITransport

    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://t") as c:
        res = await c.get("/boom")
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "TRANSITION_NOT_ALLOWED"
