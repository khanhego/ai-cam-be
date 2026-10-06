"""Module `returns` lõi (T-104): `attach_or_create`, `resolve_code`, `recompute`, kiện tạm, gộp hồ sơ chưa
xác định; API-110, API-111 — 02a §4.1, §5 BR-24, "Gắn tín hiệu hoàn"; AC-23, AC-24, AC-34."""

import uuid
from datetime import timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.orders.models import Package, StatusHistory
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import ITEM, SOCK, buyer_return_case, make_order, return_session

pytestmark = pytest.mark.integration


async def _case_count(db: AsyncSession, order_id: uuid.UUID) -> int:
    return int(await db.scalar(select(func.count()).where(ReturnCase.order_id == order_id)) or 0)


# ---------------------------------------------------------------- attach_or_create


async def test_platform_return_creates_expected_case(db: AsyncSession) -> None:
    """AC-23: yêu cầu trả có kiện về → hồ sơ `EXPECTED`, kiện → `RETURN_EXPECTED`, dòng yêu cầu ghép đơn."""
    order, (package,) = await make_order(db, 41)

    case = await buyer_return_case(db, order, 41)

    assert (case.kind, case.status, case.source) == ("BUYER_RETURN", "EXPECTED", "PLATFORM")
    assert case.code.startswith("HH-")
    assert len(case.code) == 9
    assert case.expected_since is not None
    assert case.reported_at is not None
    assert case.signal_keys == ["RETURN:2410RTTST041"]
    assert case.requested_items[0]["quantity"] == 2
    assert case.requested_items[0]["order_item_id"]
    assert package.warehouse_status == "RETURN_EXPECTED"


async def test_same_signal_key_is_idempotent(db: AsyncSession) -> None:
    """DEC-267: `TO_RETURN` / J-13 nhiều lần → một hồ sơ."""
    order, _ = await make_order(db, 41)
    first = await buyer_return_case(db, order, 41)

    again = await buyer_return_case(db, order, 41)
    failed = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:2410TST00041:1")
    )
    failed_again = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:2410TST00041:1")
    )

    assert again.id == first.id
    assert failed.case is not None
    assert failed.case.id == first.id
    assert failed_again.case is not None
    assert failed_again.case.id == first.id
    assert first.kind == "BUYER_RETURN"  # không hạ loại
    assert await _case_count(db, order.id) == 1


async def test_failed_delivery_then_buyer_return_upgrades_kind(db: AsyncSession) -> None:
    order, packages = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER", status="TO_RETURN")

    failed = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:2410TST00043:1")
    )
    assert failed.case is not None
    assert failed.created
    assert failed.case.kind == "FAILED_DELIVERY"
    assert [p.warehouse_status for p in packages] == ["RETURN_EXPECTED", "RETURN_EXPECTED"]

    upgraded = await buyer_return_case(db, order, 43)

    assert upgraded.id == failed.case.id
    assert upgraded.kind == "BUYER_RETURN"
    assert upgraded.platform_return_sn == "2410RTTST043"
    assert await _case_count(db, order.id) == 1


async def test_refund_only_is_separate_no_parcel(db: AsyncSession) -> None:
    """AC-23: chỉ hoàn tiền → hồ sơ `NO_PARCEL`, trạng thái kho không đổi."""
    order, (package,) = await make_order(db, 44)

    case = await buyer_return_case(db, order, 44, needs_parcel=False, tracking="")

    assert (case.kind, case.status) == ("REFUND_ONLY", "NO_PARCEL")
    assert package.warehouse_status == "DELIVERED"


async def test_late_platform_report_attaches_to_received_unannounced(db: AsyncSession) -> None:
    """AC-24: kiện về trước, đã nhận; sàn báo sau → gắn vào chính hồ sơ đó, không tạo mới, kiện giữ nguyên."""
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")
    scan = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_WAREHOUSE_SCAN, package_ids=(package.id,))
    )
    assert scan.case is not None
    assert (scan.case.kind, scan.case.source, scan.case.status) == ("UNANNOUNCED", "WAREHOUSE", "EXPECTED")
    assert package.warehouse_status == "HANDED_OVER"  # quét ở bàn hoàn không tự chuyển kiện
    scan.case.status = "RECEIVED_OK"
    package.warehouse_status = "RETURN_RECEIVED_OK"
    await db.flush()

    late = await buyer_return_case(db, order, 46)

    assert late.id == scan.case.id
    assert (late.kind, late.status, late.platform_return_sn) == (
        "BUYER_RETURN",
        "RECEIVED_OK",
        "2410RTTST046",
    )
    assert package.warehouse_status == "RETURN_RECEIVED_OK"
    assert await _case_count(db, order.id) == 1


async def test_cancelled_case_does_not_absorb_new_signal(db: AsyncSession) -> None:
    """R3-6, R3-7: hồ sơ `CANCELLED` không nhận tín hiệu mới; khóa của hồ sơ hủy không chặn đợt mới."""
    order, _ = await make_order(db, 43, warehouse_status="HANDED_OVER")
    signal = returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:2410TST00043:1")
    first = await returns.attach_or_create(db, order, signal)
    assert first.case is not None
    first.case.status = "CANCELLED"
    await db.flush()

    second = await returns.attach_or_create(db, order, signal)

    assert second.case is not None
    assert second.created
    assert second.case.id != first.case.id


async def test_known_return_sn_not_reattached(db: AsyncSession) -> None:
    """Mã yêu cầu sàn đã thuộc hồ sơ (kể cả đã hủy) → không tạo hồ sơ trùng (unique `platform_return_sn`)."""
    order, _ = await make_order(db, 45)
    first = await buyer_return_case(db, order, 45)
    first.status = "CANCELLED"
    await db.flush()

    again = await buyer_return_case(db, order, 45)

    assert again.id == first.id
    assert await _case_count(db, order.id) == 1


async def test_failed_signal_ignored_when_all_received(db: AsyncSession) -> None:
    order, _ = await make_order(db, 42, warehouse_status="RETURN_RECEIVED_OK")

    result = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:2410TST00042:9")
    )

    assert result.case is None
    assert await _case_count(db, order.id) == 0


# ---------------------------------------------------------------- resolve_code


async def test_resolve_code_three_sources(db: AsyncSession) -> None:
    """02a §4.1: mã chiều về, mã gốc, mã đơn sàn → cùng kiện + hồ sơ."""
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)

    for code in ("spxrttst000041", "SPXTST0000041", "2410TST00041", "2410RTTST041"):
        found = await returns.resolve_code(db, code)
        assert found.status == "FOUND", code
        assert found.package is not None
        assert found.package.id == package.id
        assert found.case is not None
        assert found.case.id == case.id


async def test_resolve_code_multiple_and_not_found(db: AsyncSession) -> None:
    """DEC-229: đơn > 1 kiện chưa có hồ sơ → MULTIPLE; mã lạ → NOT_FOUND."""
    await make_order(db, 47, packages=2)

    assert (await returns.resolve_code(db, "2410TST00047")).status == "MULTIPLE"
    assert (await returns.resolve_code(db, "SPXVN0000000000")).status == "NOT_FOUND"
    single = await returns.resolve_code(db, "SPXTST0000047-2")
    assert single.status == "FOUND"
    assert single.case is None


async def test_resolve_order_code_with_case_picks_unreceived(db: AsyncSession) -> None:
    order, packages = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER")
    result = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="F:1")
    )
    packages[0].warehouse_status = "RETURN_RECEIVED_OK"
    _, station = await make_station_account(db)
    assert result.case is not None
    db.add(return_session(station, packages[0], result.case))
    await db.flush()

    found = await returns.resolve_code(db, "2410TST00043")

    assert found.package is not None
    assert found.package.id == packages[1].id


# ---------------------------------------------------------------- recompute (BR-24)


async def test_recompute_multi_package(db: AsyncSession) -> None:
    """AC-34: giao thất bại 2 kiện → nhận kiện 1 `PARTIALLY_RECEIVED`, kiện 2 `RECEIVED_*`."""
    order, packages = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER")
    result = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="F:1")
    )
    case = result.case
    assert case is not None
    _, station = await make_station_account(db)

    db.add(return_session(station, packages[0], case, status="OPEN", conclusion=None))
    await db.flush()
    assert await returns.recompute(db, case)
    assert case.status == "INSPECTING"

    for s in await returns.return_sessions_of_case(db, case.id):
        s.status, s.inspection_conclusion = "COMPLETED", "OK"
    packages[0].warehouse_status = "RETURN_RECEIVED_OK"
    await db.flush()
    await returns.recompute(db, case)
    assert case.status == "PARTIALLY_RECEIVED"
    assert case.received_at is None

    db.add(return_session(station, packages[1], case, conclusion="DAMAGED"))
    packages[1].warehouse_status = "RETURN_RECEIVED_ISSUE"
    await db.flush()
    await returns.recompute(db, case)
    assert (case.status, case.conclusion) == ("RECEIVED_ISSUE", "DAMAGED")
    assert case.received_at is not None


async def test_recompute_missing_and_back_to_expected(db: AsyncSession) -> None:
    order, (package,) = await make_order(db, 49)
    case = await buyer_return_case(db, order, 49)
    package.warehouse_status = "RETURN_MISSING"
    await db.flush()

    await returns.recompute(db, case)
    assert case.status == "MISSING"

    package.warehouse_status = "RETURN_EXPECTED"
    await db.flush()
    await returns.recompute(db, case)
    assert case.status == "EXPECTED"


async def test_single_session_close_partial_return(db: AsyncSession) -> None:
    """DEC-271, R3-3: khách trả một phần đơn 2 kiện → kiện kia rời hồ sơ, về `DELIVERED`."""
    order, packages = await make_order(db, 48, packages=2, items=(ITEM, SOCK))
    case = await buyer_return_case(db, order, 48)  # yêu cầu chỉ áo
    case.single_session = True
    assert [p.warehouse_status for p in packages] == ["RETURN_EXPECTED", "RETURN_EXPECTED"]
    assert not await returns.covers_whole_order(db, case)

    changed = await returns.apply_close_to_packages(
        db, case, packages[0].id, "OK", source="WAREHOUSE", actor_label="TST Station 01"
    )

    assert changed == [packages[1].id]
    assert packages[1].warehouse_status == "DELIVERED"
    remaining = (
        await db.scalars(
            select(ReturnCasePackage.package_id).where(ReturnCasePackage.return_case_id == case.id)
        )
    ).all()
    assert list(remaining) == [packages[0].id]
    label = await db.scalar(
        select(StatusHistory.actor_label).where(
            StatusHistory.package_id == packages[1].id, StatusHistory.to_status == "DELIVERED"
        )
    )
    assert label == returns.PARTIAL_RETURN_LABEL


async def test_single_session_close_whole_order(db: AsyncSession) -> None:
    """DEC-249: yêu cầu trả bao trọn đơn 2 kiện → mọi kiện `RETURN_RECEIVED_*`."""
    order, packages = await make_order(db, 47, packages=2)
    case = await buyer_return_case(db, order, 47)
    case.single_session = True
    assert await returns.covers_whole_order(db, case)

    await returns.apply_close_to_packages(db, case, packages[0].id, "OK", source="WAREHOUSE", actor_label="x")

    assert packages[1].warehouse_status == "RETURN_RECEIVED_OK"


# ---------------------------------------------------------------- kiện tạm, gộp hồ sơ chưa xác định


async def test_create_unidentified_placeholder_code(db: AsyncSession) -> None:
    case, package = await returns.create_unidentified(db)

    assert package.tracking_number.startswith("TAM-")
    assert len(package.tracking_number) == 10
    assert package.is_placeholder
    assert not package.verified
    assert (case.kind, case.order_id, case.single_session) == ("UNIDENTIFIED", None, True)


async def test_merge_unidentified_when_sessions_ended(db: AsyncSession) -> None:
    """DEC-269: phiên chưa xác định đã đóng với mã thật → đơn xuất hiện → gộp, kiện tạm xóa."""
    _, station = await make_station_account(db)
    case, placeholder = await returns.create_unidentified(db)
    db.add(return_session(station, placeholder, case, conclusion="EMPTY_BOX", open_code="SPXTST0000046"))
    case.status = "RECEIVED_ISSUE"
    await db.flush()
    order, (package,) = await make_order(db, 46, warehouse_status="HANDED_OVER")

    merged = await returns.merge_unidentified_by_code(db, order)

    assert merged == [case.id]
    assert (case.order_id, case.kind, case.status) == (order.id, "UNANNOUNCED", "RECEIVED_ISSUE")
    assert package.warehouse_status == "RETURN_RECEIVED_ISSUE"
    sessions = await returns.return_sessions_of_case(db, case.id)
    assert [s.package_id for s in sessions] == [package.id]
    assert await db.get(Package, placeholder.id) is None


async def test_merge_unidentified_pending_while_session_active(db: AsyncSession) -> None:
    """R3-2: còn phiên hoạt động → đánh dấu `pending_merge_order_id`, chưa gộp."""
    _, station = await make_station_account(db)
    case, placeholder = await returns.create_unidentified(db)
    db.add(
        return_session(station, placeholder, case, status="OPEN", conclusion=None, open_code="SPXTST0000046")
    )
    await db.flush()
    order, _ = await make_order(db, 46, warehouse_status="HANDED_OVER")

    assert await returns.merge_unidentified_by_code(db, order) == []
    assert case.pending_merge_order_id == order.id
    assert case.order_id is None


async def test_merge_into_open_case_of_order(db: AsyncSession) -> None:
    _, station = await make_station_account(db)
    order, (package,) = await make_order(db, 41)
    open_case = await buyer_return_case(db, order, 41)
    case, placeholder = await returns.create_unidentified(db)
    db.add(return_session(station, placeholder, case, conclusion="OK", open_code="SPXTST0000041"))
    await db.flush()

    assert await returns.merge_unidentified_by_code(db, order) == [case.id]
    assert (case.status, case.merged_into_id) == ("CANCELLED", open_case.id)
    assert open_case.status == "RECEIVED_OK"
    assert package.warehouse_status == "RETURN_RECEIVED_OK"


# ---------------------------------------------------------------- API-110, API-111


async def _login(api: AsyncClient, username: str, client: str = "DASHBOARD") -> dict[str, str]:
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def test_api_110_tabs_counts_and_filters(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_cskh", "CSKH")
    o41, _ = await make_order(db, 41)
    o44, _ = await make_order(db, 44)
    o49, _ = await make_order(db, 49)
    c41 = await buyer_return_case(db, o41, 41)
    await buyer_return_case(db, o44, 44, needs_parcel=False, tracking="")
    c49 = await buyer_return_case(db, o49, 49)
    c49.status = "MISSING"
    c41.expected_since = clock.now() - timedelta(days=2)
    await db.flush()
    headers = await _login(api, "tst_cskh")

    res = await api.get("/api/v1/returns", params={"tab": "EXPECTED"}, headers=headers)

    assert res.status_code == 200
    body = res.json()
    assert body["tab_counts"] == {
        "EXPECTED": 1,
        "MISSING": 1,
        "RECEIVED": 0,
        "NO_PARCEL": 1,
        "UNIDENTIFIED": 0,
    }
    assert body["total"] == 1
    item = body["items"][0]
    assert item["code"] == c41.code
    assert item["order"]["platform_order_sn"] == "2410TST00041"
    assert item["packages"][0]["warehouse_status"] == "RETURN_EXPECTED"
    assert item["reason_label"] == "Hàng bị hư"
    assert item["waiting_days"] == 2
    assert item["claims"] == []
    assert item["merged_into"] is None

    by_code = await api.get("/api/v1/returns", params={"tab": "ALL", "q": "spxtst0000049"}, headers=headers)
    assert [i["id"] for i in by_code.json()["items"]] == [str(c49.id)]
    assert (await api.get("/api/v1/returns", params={"tab": "NO_PARCEL"}, headers=headers)).json()[
        "total"
    ] == 1


async def test_api_111_detail_and_permissions(api: AsyncClient, db: AsyncSession) -> None:
    await make_user(db, "tst_sup", "SUPERVISOR")
    user, station = await make_station_account(db)
    order, (package,) = await make_order(db, 41)
    case = await buyer_return_case(db, order, 41)
    db.add(return_session(station, package, case, conclusion="EMPTY_BOX"))
    await db.flush()
    sup = await _login(api, "tst_sup")

    res = await api.get(f"/api/v1/returns/{case.id}", headers=sup)

    assert res.status_code == 200
    body = res.json()
    assert (body["platform_return_sn"], body["source"], body["needs_parcel"]) == (
        "2410RTTST041",
        "PLATFORM",
        True,
    )
    assert body["requested_items"][0]["quantity"] == 2
    assert body["sessions"][0]["station_name"] == station.name
    assert body["sessions"][0]["conclusion"] == "EMPTY_BOX"
    assert "raw_payload" not in body
    assert (await api.get(f"/api/v1/returns/{uuid.uuid4()}", headers=sup)).status_code == 404
    st = await _login(api, user.username, "STATION")
    assert (await api.get("/api/v1/returns", headers=st)).status_code == 403
