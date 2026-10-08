"""API-11 chế độ RETURN (mở phiên), API-10 khối RETURN, API-104 — 02a §4.1; TC-04.xx, TC-N2.02 (item 02)."""

import math
import time
import uuid
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.orders import service as orders
from aicam.modules.orders.models import OrderItem, Package
from aicam.modules.platforms.base import PlatformItem, PlatformOrder
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import InspectionLine, PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import make_station_account
from .returns_helpers import Desk, buyer_return_case, make_desk, make_order, return_session

pytestmark = pytest.mark.integration


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    return await make_desk(api, db)


def _alert(res: Response) -> dict[str, Any]:
    body = res.json()
    assert body["outcome"] == "ALERT", body
    return body["alert"]  # type: ignore[no-any-return]


# ---------------------------------------------------------------- mở phiên


async def test_operator_required(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> None:
    """TC-04.03, BR-28."""
    desk = await make_desk(api, db, operator=None)
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)

    alert = _alert(await desk.scan("SPXRTTST000041"))

    assert alert["code"] == "OPERATOR_REQUIRED"
    assert (await desk.state())["session"] is None


async def test_open_by_return_tracking(desk: Desk, db: AsyncSession) -> None:
    """TC-04.04, AC-22: mã chiều về → R2; dòng gửi 2 / yêu cầu 2 / nhận 2 Nguyên vẹn; kiện + hồ sơ kiểm."""
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)

    res = await desk.scan("spxrttst000041")

    body = res.json()
    assert body["outcome"] == "SESSION_OPENED", body
    state = body["state"]
    assert state["state"] == "INSPECTING"
    session = state["session"]
    assert session["type"] == "RETURN"
    assert session["operator_name"] == "Lan QA"
    assert session["package"]["tracking_number"] == "SPXTST0000041"
    assert session["package"]["items"][0]["order_item_id"]
    rc = session["return_case"]
    assert (rc["code"], rc["kind"], rc["status"]) == (case.code, "BUYER_RETURN", "INSPECTING")
    assert (rc["reason_label"], rc["reason_text"]) == ("Hàng bị hư", "Áo bị rách ở tay")
    assert (rc["package_count"], rc["received_count"]) == (1, 0)
    inspection = session["inspection"]
    assert inspection["conclusion"] is None
    assert inspection["note"] == ""
    assert inspection["lines_mode"] == "FULL"
    line = inspection["lines"][0]
    assert (line["quantity_sent"], line["quantity_requested"], line["quantity_received"]) == (2, 2, 2)
    assert line["condition"] == "OK"
    assert session["snapshots"] == []
    assert session["pack_reference"] is None
    assert "NO_PACK_CLIP" in session["flags"]
    assert session["mismatch"] is None
    await db.refresh(package)
    await db.refresh(case)
    assert package.warehouse_status == "RETURN_INSPECTING"
    assert case.status == "INSPECTING"
    assert case.single_session is True


@pytest.mark.parametrize("code", ["SPXTST0000041", "2410TST00041"])
async def test_open_by_original_or_order_code(desk: Desk, db: AsyncSession, code: str) -> None:
    """TC-04.05, TC-04.06: cùng hồ sơ, không tạo hồ sơ mới."""
    order, _ = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)

    body = (await desk.scan(code)).json()

    assert body["outcome"] == "SESSION_OPENED"
    assert body["state"]["session"]["return_case"]["id"] == str(case.id)
    count = len((await db.scalars(select(ReturnCase).where(ReturnCase.order_id == order.id))).all())
    assert count == 1


async def test_pack_reference_when_packed_before(desk: Desk, db: AsyncSession) -> None:
    """TC-04.43 (phần API): kiện có phiên PACK hiệu lực → `pack_reference`, không cờ `NO_PACK_CLIP`."""
    order, (package,) = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    _, other = await make_station_account(db, "tst_station09", "TST Station 09")
    db.add(
        PackSession(
            type="PACK", package_id=package.id, station_id=other.id, status="COMPLETED",
            open_code=package.tracking_number, flags=[], ended_at=package.created_at,
        )
    )  # fmt: skip
    await db.flush()

    session = (await desk.scan("SPXRTTST000041")).json()["state"]["session"]

    assert session["pack_reference"]["station_name"] == "TST Station 09"
    assert "NO_PACK_CLIP" not in session["flags"]


async def test_unknown_code_not_found(desk: Desk) -> None:
    """TC-04.08: mã lạ, sàn mock không có → `RETURN_NOT_FOUND`, `can_open_unidentified`."""
    alert = _alert(await desk.scan("SPXVN0000000000"))

    assert alert["code"] == "RETURN_NOT_FOUND"
    assert alert["data"] == {"code": "SPXVN0000000000", "can_open_unidentified": True}


async def test_tc_n2_02_slow_platform_lookup_return_mode(desk: Desk, adapter: MockAdapter) -> None:
    """TC-N2.02, NFR-01 (tra sàn): chế độ RETURN, mã lạ, sàn mock trễ 3 giây → cắt ở
    `PLATFORM_LOOKUP_TIMEOUT_S` (2 giây) → `RETURN_NOT_FOUND` ("sàn không trả lời", vẫn cho mở kiện chưa xác
    định) trong ≤ 3 giây. Đo 5 lần (mỗi lần một mã lạ khác)."""
    adapter.delay_s = 3.0
    durations: list[float] = []
    for n in range(1, 6):
        code = f"SPXVN99900000{n:02d}"
        started = time.perf_counter()
        res = await desk.scan(code)
        durations.append(time.perf_counter() - started)

        alert = _alert(res)
        assert alert["code"] == "RETURN_NOT_FOUND", alert
        assert "sàn không trả lời" in alert["message"]
        assert alert["data"] == {"code": code, "can_open_unidentified": True}
    assert (await desk.state())["session"] is None

    p95 = sorted(durations)[math.ceil(0.95 * len(durations)) - 1]
    print(f"TC-N2.02: n={len(durations)} min={min(durations):.3f}s p95={p95:.3f}s max={max(durations):.3f}s")
    assert min(durations) >= 1.9  # đã tra sàn thật (bị cắt ở 2 giây), không trả sớm
    assert max(durations) <= 3.0


async def test_platform_lookup_opens_unannounced(desk: Desk, db: AsyncSession) -> None:
    """EX-R1, EX-R3: mã chưa có trong DB → tra sàn (mock đơn 41 COMPLETED) → kiện NEW mở được, hồ sơ
    "Về trước khi sàn báo", cờ `UNANNOUNCED`."""
    body = (await desk.scan("SPXTST0000041")).json()

    assert body["outcome"] == "SESSION_OPENED", body
    session = body["state"]["session"]
    assert session["return_case"]["kind"] == "UNANNOUNCED"
    assert {"UNANNOUNCED", "NO_PACK_CLIP"} <= set(session["flags"])
    package = await orders.find_package(db, "SPXTST0000041")
    assert package is not None
    pack = await db.scalar(select(PackSession).where(PackSession.package_id == package.id))
    assert pack is not None
    assert pack.package_status_before == "NEW"


async def test_not_shipped(desk: Desk, db: AsyncSession) -> None:
    """TC-04.10, EX-R6: kiện `PACKED` → `NOT_SHIPPED`, không phiên."""
    await make_order(db, 10, status="READY_TO_SHIP", warehouse_status="PACKED")

    alert = _alert(await desk.scan("SPXTST0000010"))

    assert alert["code"] == "NOT_SHIPPED"
    assert alert["data"] == {"warehouse_status": "PACKED"}
    assert "Đã đóng gói" in alert["message"]
    assert (await desk.state())["session"] is None


async def test_new_package_of_unshipped_order_not_openable(desk: Desk, db: AsyncSession) -> None:
    await make_order(db, 12, status="READY_TO_SHIP", warehouse_status="NEW")

    assert _alert(await desk.scan("SPXTST0000012"))["code"] == "NOT_SHIPPED"


async def test_multiple_packages(desk: Desk, db: AsyncSession) -> None:
    """TC-04.13, DEC-229."""
    await make_order(db, 47, packages=2)

    alert = _alert(await desk.scan("2410TST00047"))

    assert alert["code"] == "RETURN_MULTIPLE_PACKAGES"
    assert alert["data"] == {"platform_order_sn": "2410TST00047"}


async def test_in_progress_elsewhere(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> None:
    """TC-04.14: kiện đang kiểm ở Station 02."""
    desk1 = await make_desk(api, db, 1)
    desk2 = await make_desk(api, db, 2, operator="Bình")
    await make_order(db, 42, warehouse_status="HANDED_OVER")
    assert (await desk2.scan("SPXTST0000042")).json()["outcome"] == "SESSION_OPENED"

    alert = _alert(await desk1.scan("SPXTST0000042"))

    assert alert["code"] == "RETURN_IN_PROGRESS_ELSEWHERE"
    assert alert["data"] == {"station_name": "TST Station 02"}


async def test_already_received(desk: Desk, db: AsyncSession) -> None:
    """TC-04.11 bước 1, EX-R11."""
    order, (package,) = await make_order(db, 41, warehouse_status="RETURN_RECEIVED_OK")
    case = await buyer_return_case(db, order, 41)
    case.status = "RECEIVED_OK"
    db.add(return_session(desk.station, package, case, conclusion="OK"))
    await db.flush()

    alert = _alert(await desk.scan("SPXTST0000041"))

    assert alert["code"] == "RETURN_ALREADY_RECEIVED"
    assert alert["data"]["station_name"] == "TST Station 01"
    assert alert["data"]["conclusion"] == "OK"
    assert alert["data"]["can_record_other"] is True
    assert "Nguyên vẹn" in alert["message"]


async def test_invalid_code(desk: Desk) -> None:
    assert _alert(await desk.scan("abc"))["code"] == "INVALID_CODE"


# ---------------------------------------------------------------- đang kiểm


async def test_code_different_and_inspection_required(desk: Desk, db: AsyncSession) -> None:
    """TC-04.20 (BR-23, EX-R9) + TC-04.18 (BR-07): phiên vẫn mở."""
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    await make_order(db, 42, warehouse_status="HANDED_OVER")
    await desk.scan("SPXRTTST000041")

    other = _alert(await desk.scan("SPXTST0000042"))
    assert other["code"] == "RETURN_CODE_DIFFERENT"
    assert set(other["data"]["expected_codes"]) == {"SPXRTTST000041", "SPXTST0000041", "2410TST00041"}

    same = _alert(await desk.scan("2410TST00041"))
    assert same["code"] == "INSPECTION_REQUIRED"
    state = await desk.state()
    assert state["state"] == "INSPECTING"
    assert state["session"]["status"] == "OPEN"


async def test_failed_delivery_multi_package_reference_lines(desk: Desk, db: AsyncSession) -> None:
    """TC-04.48 bước 1: giao thất bại đơn 2 kiện → `lines_mode = REFERENCE`."""
    from aicam.modules.returns import service as returns

    order, _ = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER")
    await returns.attach_or_create(db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="F:43"))

    session = (await desk.scan("SPXTST0000043-1")).json()["state"]["session"]

    assert session["inspection"]["lines_mode"] == "REFERENCE"
    assert session["return_case"]["kind"] == "FAILED_DELIVERY"
    assert session["return_case"]["package_count"] == 2


async def test_scan_replay_returns_same_outcome(desk: Desk, db: AsyncSession) -> None:
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    scan_id = str(uuid.uuid4())

    first = await desk.scan("SPXRTTST000041", scan_id)
    again = await desk.scan("SPXRTTST000041", scan_id)

    assert first.json()["outcome"] == again.json()["outcome"] == "SESSION_OPENED"
    count = len((await db.scalars(select(PackSession).where(PackSession.type == "RETURN"))).all())
    assert count == 1


async def test_pack_mode_counts_exclude_return(desk: Desk, db: AsyncSession) -> None:
    state = await desk.state()
    assert (state["today_count"], state["today_return_count"], state["today_return_issue_count"]) == (0, 0, 0)


# ---------------------------------------------------------------- API-104


async def test_lookup_prefix_and_can_open(desk: Desk, db: AsyncSession) -> None:
    """TC-04.45 bước 2: tiền tố mã đơn ≥ 6 ký tự."""
    order, _ = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)
    await make_order(db, 10, status="READY_TO_SHIP", warehouse_status="PACKED")

    res = await desk.api.get(
        "/api/v1/station/return-lookup", params={"q": "2410tst000"}, headers=desk.headers
    )

    assert res.status_code == 200
    body = res.json()
    assert body["platform_checked"] is False
    by_code = {i["tracking_number"]: i for i in body["items"]}
    assert by_code["SPXTST0000041"]["can_open"] is True
    assert by_code["SPXTST0000041"]["return_case"]["code"] == case.code
    assert by_code["SPXTST0000010"]["can_open"] is False
    assert by_code["SPXTST0000010"]["blocked_reason"] == "NOT_SHIPPED"


async def test_lookup_validation_and_mode(api: AsyncClient, db: AsyncSession, desk: Desk) -> None:
    """TC-04.46, API-104 lỗi."""
    short = await api.get("/api/v1/station/return-lookup", params={"q": "241"}, headers=desk.headers)
    assert short.status_code == 422
    assert short.json()["error"]["details"]["fields"]["q"] == "Nhập ít nhất 4 ký tự."

    desk.station.work_mode, desk.station.kind = "PACK", "PACK"
    await db.flush()
    res = await api.get("/api/v1/station/return-lookup", params={"q": "2410TST"}, headers=desk.headers)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "WRONG_WORK_MODE"


async def test_lookup_platform_checked(desk: Desk) -> None:
    res = await desk.api.get(
        "/api/v1/station/return-lookup", params={"q": "SPXTST0000044"}, headers=desk.headers
    )

    body = res.json()
    assert body["platform_checked"] is True
    assert [i["tracking_number"] for i in body["items"]] == ["SPXTST0000044"]


# ---------------------------------------------------------------- dòng đơn giữ id (DEC-307)


async def test_resync_keeps_order_item_ids(db: AsyncSession) -> None:
    order, _ = await make_order(db, 41)
    before = (await db.scalars(select(OrderItem.id).where(OrderItem.order_id == order.id))).all()
    data = PlatformOrder(
        platform_order_sn="2410TST00041",
        status="TO_RETURN",
        tracking_numbers=("SPXTST0000041",),
        items=(PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"), PlatformItem("Mũ", 1, "MU-1", None)),
        status_group=shopee_order_group("TO_RETURN"),
    )

    await orders.upsert_platform_order(db, data)

    after = (await db.scalars(select(OrderItem.id).where(OrderItem.order_id == order.id))).all()
    assert before[0] in after
    assert len(after) == 2


async def test_inspection_line_rows_created(desk: Desk, db: AsyncSession) -> None:
    order, (package,) = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    await desk.scan("SPXRTTST000041")

    pack = await db.scalar(select(PackSession).where(PackSession.package_id == package.id))
    assert pack is not None
    lines = (await db.scalars(select(InspectionLine).where(InspectionLine.session_id == pack.id))).all()
    assert [(line.position, line.quantity_requested) for line in lines] == [(1, 2)]
    assert pack.inspection_lines_mode == "FULL"
    assert isinstance(await db.get(Package, package.id), Package)
