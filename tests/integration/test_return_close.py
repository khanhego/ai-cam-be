"""Phiên RETURN đóng / hủy (T-108): API-102 (BR-22), đóng API-11 (BR-07, 23, 24), API-12, `camera_clock`,
API-15 — 02a §4, §4.1, §5; TC-04.16..04.25, 04.48, 04.50..04.52."""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.orders.models import OrderItem, Package
from aicam.modules.platforms.base import ReturnItem
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations.models import Camera

from .returns_helpers import ITEM, SOCK, Desk, buyer_return_case, make_desk, make_order, return_session

pytestmark = pytest.mark.integration


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    return await make_desk(api, db)


async def _save(desk: Desk, session_id: str, body: dict[str, Any]) -> Response:
    return await desk.api.put(
        f"/api/v1/station/sessions/{session_id}/inspection", headers=desk.headers, json=body
    )


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


async def _open(desk: Desk, code: str) -> dict[str, Any]:
    body = (await desk.scan(code)).json()
    assert body["outcome"] == "SESSION_OPENED", body
    return body["state"]["session"]  # type: ignore[no-any-return]


async def _conclude(desk: Desk, session: dict[str, Any], conclusion: str, **override: Any) -> Response:
    res = await _save(
        desk, session["id"], {"conclusion": conclusion, "note": "", "lines": _lines(session, **override)}
    )
    assert res.status_code == 200, res.text
    return res


# ---------------------------------------------------------------- API-102


async def test_save_inspection_draft(desk: Desk, db: AsyncSession) -> None:
    """TC-04.15 (API): giảm số nhận + "Thiếu hàng" → lưu, `saved_at`, state cập nhật."""
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")

    res = await _save(
        desk, session["id"],
        {"conclusion": "MISSING_ITEM", "note": " thiếu 1 ", "lines": _lines(session, quantity_received=1,
                                                                            condition="MISSING_ITEM")},
    )  # fmt: skip

    assert res.status_code == 200
    inspection = res.json()["inspection"]
    assert (inspection["conclusion"], inspection["note"]) == ("MISSING_ITEM", "thiếu 1")
    assert inspection["saved_at"] is not None
    assert inspection["lines"][0]["quantity_received"] == 1
    state = await desk.state()
    assert state["session"]["inspection"]["lines"][0]["condition"] == "MISSING_ITEM"


async def test_ok_inconsistent_rejected(desk: Desk, db: AsyncSession) -> None:
    """TC-04.16, BR-22: "Nguyên vẹn" khi nhận 1 / yêu cầu 2 → `CONCLUSION_INCONSISTENT`, không lưu."""
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")

    res = await _save(
        desk, session["id"], {"conclusion": "OK", "note": "", "lines": _lines(session, quantity_received=1)}
    )

    assert res.status_code == 422
    assert res.json()["error"]["code"] == "CONCLUSION_INCONSISTENT"
    assert (await desk.state())["session"]["inspection"]["conclusion"] is None


async def test_other_requires_note_and_lines_complete(desk: Desk, db: AsyncSession) -> None:
    """TC-04.17 + thiếu dòng / dòng lạ / số ngoài khoảng → `VALIDATION_ERROR` theo field."""
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")

    other = await _save(desk, session["id"], {"conclusion": "OTHER", "note": "", "lines": _lines(session)})
    assert other.status_code == 422
    assert "note" in other.json()["error"]["details"]["fields"]

    missing = await _save(desk, session["id"], {"conclusion": "DAMAGED", "note": "", "lines": []})
    assert missing.json()["error"]["details"]["fields"] == {"lines": "Thiếu dòng của phiên"}

    stranger = [*_lines(session), {"order_item_id": str(uuid.uuid4()), "quantity_received": 1}]
    bad = await _save(desk, session["id"], {"conclusion": "DAMAGED", "lines": stranger})
    assert "lines.1.order_item_id" in bad.json()["error"]["details"]["fields"]

    big = await _save(
        desk, session["id"], {"conclusion": "DAMAGED", "lines": _lines(session, quantity_received=1000)}
    )
    assert "lines.0.quantity_received" in big.json()["error"]["details"]["fields"]


async def test_reference_mode_only_checks_conclusion(desk: Desk, db: AsyncSession) -> None:
    """02 §6.3 #8: `REFERENCE` → "Nguyên vẹn" không bị khóa bởi dòng."""
    order, _ = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER")
    await returns.attach_or_create(db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="F:43"))
    session = await _open(desk, "SPXTST0000043-1")

    res = await _save(
        desk, session["id"], {"conclusion": "OK", "note": "", "lines": _lines(session, quantity_received=0)}
    )

    assert res.status_code == 200, res.text


async def test_save_errors_session_state(desk: Desk, db: AsyncSession) -> None:
    """API-102: phiên PACK → `NOT_RETURN_SESSION`; phiên lạ / đã đóng → `SESSION_NOT_OPEN`."""
    _, (package,) = await make_order(db, 12, status="READY_TO_SHIP", warehouse_status="PACKING")
    pack = PackSession(type="PACK", package_id=package.id, station_id=desk.station.id, status="OPEN",
                       open_code=package.tracking_number, flags=[])  # fmt: skip
    db.add(pack)
    await db.flush()

    res = await _save(desk, str(pack.id), {"conclusion": None, "lines": []})
    assert (res.status_code, res.json()["error"]["code"]) == (409, "NOT_RETURN_SESSION")

    res = await _save(desk, str(uuid.uuid4()), {"conclusion": None, "lines": []})
    assert (res.status_code, res.json()["error"]["code"]) == (409, "SESSION_NOT_OPEN")


# ---------------------------------------------------------------- đóng phiên


async def test_close_with_other_code_of_case(desk: Desk, db: AsyncSession, sent_jobs: list[Any]) -> None:
    """TC-04.19, BR-23, AC-36: mở bằng mã chiều về, kết luận OK, đóng bằng mã gốc → nhận OK."""
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)
    camera = Camera(station_id=desk.station.id, role="CAM1", rtsp_url="rtsp://x", mediamtx_path="cam-x",
                    clock_offset_ms=120)  # fmt: skip
    db.add(camera)
    await db.flush()
    session = await _open(desk, "SPXRTTST000041")
    await _conclude(desk, session, "OK")

    body = (await desk.scan("SPXTST0000041")).json()

    assert body["outcome"] == "SESSION_COMPLETED", body
    closed = body["closed_session"]
    assert closed == {
        "id": session["id"], "type": "RETURN", "tracking_number": "SPXRTTST000041", "flags": ["NO_PACK_CLIP"],
        "conclusion": "OK", "claim_code": None, "package_status": "RETURN_RECEIVED_OK",
        "return_case_status": "RECEIVED_OK",
    }  # fmt: skip
    assert body["state"]["state"] == "READY"
    assert (body["state"]["today_return_count"], body["state"]["today_return_issue_count"]) == (1, 0)
    await db.refresh(package)
    await db.refresh(case)
    assert package.warehouse_status == "RETURN_RECEIVED_OK"
    assert (case.status, case.conclusion) == ("RECEIVED_OK", "OK")
    assert case.received_at is not None
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    await db.refresh(pack)
    assert pack.close_code == "SPXTST0000041"
    assert pack.camera_clock == [{"camera_role": "CAM1", "clock_offset_ms": 120, "checked_at": None}]
    assert [j[0] for j in sent_jobs] == ["media.build_session_clips"]


async def test_close_issue(desk: Desk, db: AsyncSession) -> None:
    """TC-04.21: "Hộp rỗng" → kiện `RETURN_RECEIVED_ISSUE` + hồ sơ khiếu nại tự tạo (T-110)."""
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")
    await _conclude(desk, session, "EMPTY_BOX", quantity_received=0, condition="MISSING_ITEM")

    body = (await desk.scan("SPXRTTST000041")).json()

    assert body["closed_session"]["conclusion"] == "EMPTY_BOX"
    assert body["closed_session"]["return_case_status"] == "RECEIVED_ISSUE"
    assert body["state"]["today_return_issue_count"] == 1
    recent = (await desk.api.get("/api/v1/station/sessions/recent", headers=desk.headers)).json()
    item = recent["items"][0]
    assert (item["type"], item["conclusion"], item["tracking_number"]) == (
        "RETURN",
        "EMPTY_BOX",
        "SPXRTTST000041",
    )
    assert item["claim_code"] == body["closed_session"]["claim_code"]
    assert body["closed_session"]["claim_code"].startswith("KN-")


async def test_close_replay_keeps_closed_session(desk: Desk, db: AsyncSession) -> None:
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")
    await _conclude(desk, session, "OK")
    scan_id = str(uuid.uuid4())

    first = (await desk.scan("SPXRTTST000041", scan_id)).json()
    again = (await desk.scan("SPXRTTST000041", scan_id)).json()

    assert again["outcome"] == "SESSION_COMPLETED"
    assert again["closed_session"] == first["closed_session"]


async def test_failed_delivery_two_packages(desk: Desk, db: AsyncSession) -> None:
    """TC-04.48: giao thất bại 2 kiện → từng kiện; kiện 1 → `PARTIALLY_RECEIVED`, kiện 2 → `RECEIVED_OK`."""
    order, packages = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER")
    result = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="F:43")
    )
    case = result.case
    assert case is not None

    first = await _open(desk, "SPXTST0000043-1")
    await _conclude(desk, first, "OK")
    closed = (await desk.scan("SPXTST0000043-1")).json()["closed_session"]
    assert closed["return_case_status"] == "PARTIALLY_RECEIVED"
    await db.refresh(packages[1])
    assert packages[1].warehouse_status == "RETURN_EXPECTED"

    second = await _open(desk, "SPXTST0000043-2")
    await _conclude(desk, second, "OK")
    closed = (await desk.scan("2410TST00043")).json()["closed_session"]
    assert closed["return_case_status"] == "RECEIVED_OK"


async def test_unannounced_two_packages_two_sessions(desk: Desk, db: AsyncSession) -> None:
    """TC-04.50, DEC-265: đơn 2 kiện về trước khi sàn báo → mỗi kiện một phiên, kiện 2 mở được."""
    await make_order(db, 46, packages=2, warehouse_status="HANDED_OVER")

    first = await _open(desk, "SPXTST0000046-1")
    assert first["return_case"]["kind"] == "UNANNOUNCED"
    assert first["inspection"]["lines_mode"] == "REFERENCE"
    await _conclude(desk, first, "OK")
    closed = (await desk.scan("SPXTST0000046-1")).json()["closed_session"]
    assert closed["return_case_status"] == "PARTIALLY_RECEIVED"

    second = (await desk.scan("SPXTST0000046-2")).json()
    assert second["outcome"] == "SESSION_OPENED"
    assert second["state"]["session"]["return_case"]["id"] == first["return_case"]["id"]


async def test_buyer_return_whole_order(desk: Desk, db: AsyncSession) -> None:
    """TC-04.51, DEC-249: trả trọn đơn 2 kiện bằng 1 kiện chiều về → một phiên, cả 2 kiện nhận."""
    order, packages = await make_order(db, 47, packages=2)
    await buyer_return_case(db, order, 47)

    session = await _open(desk, "SPXRTTST000047")
    await _conclude(desk, session, "OK")
    closed = (await desk.scan("SPXRTTST000047")).json()["closed_session"]

    assert closed["return_case_status"] == "RECEIVED_OK"
    for package in packages:
        await db.refresh(package)
        assert package.warehouse_status == "RETURN_RECEIVED_OK"


async def test_buyer_return_partial_order(desk: Desk, db: AsyncSession) -> None:
    """TC-04.52, DEC-271, R3-3: trả một phần đơn 2 kiện → kiện còn lại rời hồ sơ, về `DELIVERED`."""
    order, packages = await make_order(db, 48, packages=2, items=(ITEM, SOCK))
    await buyer_return_case(db, order, 48, items=(ReturnItem(quantity=1, sku="AT-DEN-L"),))

    session = await _open(desk, "SPXRTTST000048")
    sock_line = next(line for line in session["inspection"]["lines"] if line["product_name"] == "Tất cổ ngắn")
    assert sock_line["quantity_requested"] == 0
    await _conclude(desk, session, "OK")
    closed = (await desk.scan("SPXRTTST000048")).json()["closed_session"]

    assert closed["package_status"] == "RETURN_RECEIVED_OK"
    await db.refresh(packages[1])
    assert packages[1].warehouse_status == "DELIVERED"


async def test_pending_merge_on_close(desk: Desk, db: AsyncSession) -> None:
    """R3-2: hồ sơ chưa xác định có đơn xuất hiện khi phiên còn mở → gộp lúc quét đóng."""
    case, placeholder = await returns.create_unidentified(db)
    pack = return_session(desk.station, placeholder, case, status="OPEN", conclusion="DAMAGED",
                          open_code="SPXTST0000046")  # fmt: skip
    pack.inspection_lines_mode = "FULL"
    placeholder.warehouse_status = "RETURN_INSPECTING"
    pack.package_status_before = "NEW"
    db.add(pack)
    await db.flush()
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")
    assert await returns.merge_unidentified_by_code(db, order) == []
    assert case.pending_merge_order_id == order.id

    body = (await desk.scan("SPXTST0000046")).json()

    assert body["outcome"] == "SESSION_COMPLETED", body
    await db.refresh(case)
    await db.refresh(package)
    assert (case.order_id, case.kind, case.status) == (order.id, "UNANNOUNCED", "RECEIVED_ISSUE")
    assert package.warehouse_status == "RETURN_RECEIVED_ISSUE"
    assert await db.get(Package, placeholder.id) is None


# ---------------------------------------------------------------- API-12


async def test_cancel_return_session(desk: Desk, db: AsyncSession, sent_jobs: list[Any]) -> None:
    """TC-04.24: "Không phải hàng hoàn" → kiện về `RETURN_EXPECTED`, hồ sơ về `EXPECTED`, clip vẫn cắt."""
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")

    res = await desk.api.post(
        f"/api/v1/station/sessions/{session['id']}/cancel",
        headers=desk.headers,
        json={"reason": "NOT_A_RETURN"},
    )

    assert res.status_code == 200, res.text
    assert res.json()["state"]["state"] == "READY"
    await db.refresh(package)
    await db.refresh(case)
    assert package.warehouse_status == "RETURN_EXPECTED"
    assert case.status == "EXPECTED"
    assert [j[0] for j in sent_jobs] == ["media.build_session_clips"]


async def test_cancel_unannounced_cancels_case(desk: Desk, db: AsyncSession) -> None:
    """TC-04.25 (dạng về trước khi sàn báo): hồ sơ do phiên tạo → `CANCELLED`, kiện về trạng thái trước."""
    await make_order(db, 46, warehouse_status="HANDED_OVER")
    session = await _open(desk, "SPXTST0000046")

    res = await desk.api.post(
        f"/api/v1/station/sessions/{session['id']}/cancel",
        headers=desk.headers,
        json={"reason": "WRONG_SCAN"},
    )

    assert res.status_code == 200
    case = await db.get(ReturnCase, uuid.UUID(session["return_case"]["id"]))
    assert case is not None
    await db.refresh(case)
    assert case.status == "CANCELLED"
    package = await db.scalar(select(Package).where(Package.tracking_number == "SPXTST0000046"))
    assert package is not None
    await db.refresh(package)
    assert package.warehouse_status == "HANDED_OVER"


async def test_cancel_reason_by_session_type(desk: Desk, db: AsyncSession) -> None:
    await make_order(db, 46, warehouse_status="HANDED_OVER")
    session = await _open(desk, "SPXTST0000046")

    res = await desk.api.post(
        f"/api/v1/station/sessions/{session['id']}/cancel",
        headers=desk.headers,
        json={"reason": "OUT_OF_STOCK"},
    )

    assert res.status_code == 422
    assert "reason" in res.json()["error"]["details"]["fields"]


async def test_lines_keep_order_item_after_resync(desk: Desk, db: AsyncSession) -> None:
    """DEC-307: đồng bộ lại đơn giữa phiên không làm dòng kiểm mất `order_item_id`."""
    from aicam.modules.orders import service as orders
    from aicam.modules.platforms.base import PlatformOrder

    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    session = await _open(desk, "SPXRTTST000041")
    await orders.upsert_platform_order(
        db, PlatformOrder("2410TST00041", "TO_RETURN", ("SPXTST0000041",), (ITEM,))
    )

    res = await _save(
        desk, session["id"], {"conclusion": "DAMAGED", "lines": _lines(session, condition="DAMAGED")}
    )

    assert res.status_code == 200, res.text
    assert await db.scalar(
        select(OrderItem.id).where(
            OrderItem.id == uuid.UUID(session["inspection"]["lines"][0]["order_item_id"])
        )
    )
