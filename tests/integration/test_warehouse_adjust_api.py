"""API-122 điều chỉnh trạng thái kho thủ công (T-102; FR-06.05, L6).

TC-06.13 (API), TC-06.14, TC-06.19 (bước 1), TC-P2.08 (API-122); /me permissions (FR-10.02).
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.audit import AuditLog
from aicam.modules.orders.models import Order, Package, StatusHistory
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.modules.sessions.models import PackSession

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> dict[str, str]:
    if role == "STATION":
        user, _ = await make_station_account(db, "tst_station_adj", "TST Station ADJ")
    else:
        user = await make_user(db, f"tst_adj_{role.lower()}", role, display_name=f"QA {role}")
    client = "STATION" if role == "STATION" else "DASHBOARD"
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _package(db: AsyncSession, n: int, status: str) -> Package:
    order = Order(platform_order_sn=f"2410ADJ{n:05d}", platform_status="SHIPPED")
    db.add(order)
    await db.flush()
    package = Package(order_id=order.id, tracking_number=f"SPXADJ{n:07d}", warehouse_status=status)
    db.add(package)
    await db.flush()
    return package


async def _adjust(api: AsyncClient, headers: dict[str, str], package_id: uuid.UUID, **body: Any) -> Any:
    return await api.post(f"/api/v1/packages/{package_id}/warehouse-status", json=body, headers=headers)


async def test_adjust_new_to_handed_over_with_alert(api: AsyncClient, db: AsyncSession) -> None:
    """TC-06.13 (API): NEW → HANDED_OVER, cảnh báo RESOLVED ADJUST_STATUS, lịch sử MANUAL, audit."""
    headers = await _login(api, db, "SUPERVISOR")
    package = await _package(db, 1, "NEW")
    alert = ReconAlert(
        package_id=package.id, rule="SHIPPED_NOT_PACKED", severity="HIGH", context={}, context_key="SHIPPED"
    )
    db.add(alert)
    await db.flush()

    res = await _adjust(
        api,
        headers,
        package.id,
        to_status="HANDED_OVER",
        reason="Đã giao ĐVVC 04/10",
        recon_alert_id=str(alert.id),
    )

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["package"]["warehouse_status"] == "HANDED_OVER"
    assert body["recon_alert"]["status"] == "RESOLVED"
    assert body["recon_alert"]["br"] == "BR-10"
    assert body["recon_alert"]["resolution"]["action"] == "ADJUST_STATUS"
    assert body["recon_alert"]["resolution"]["to_status"] == "HANDED_OVER"
    assert body["recon_alert"]["resolution"]["by"]["display_name"] == "QA SUPERVISOR"
    assert body["recon_alert"]["resolution"]["at"].endswith("Z")
    assert body["recon_alert"]["allowed_status_targets"] == ["DELIVERED"]
    history = (await db.scalars(select(StatusHistory).where(StatusHistory.package_id == package.id))).all()
    assert [(h.source, h.from_status, h.to_status) for h in history] == [("MANUAL", "NEW", "HANDED_OVER")]
    await db.refresh(package)
    assert package.status_changed_at == history[0].at
    log = await db.scalar(select(AuditLog).where(AuditLog.action == "WAREHOUSE_STATUS_ADJUST"))
    assert log is not None
    assert log.data is not None
    assert (log.data["from"], log.data["to"], log.data["reason"]) == (
        "NEW",
        "HANDED_OVER",
        "Đã giao ĐVVC 04/10",
    )


async def test_transition_not_allowed_lists_targets(api: AsyncClient, db: AsyncSession) -> None:
    """TC-06.14: RETURN_EXPECTED → RETURN_RECEIVED_OK bị chặn, `details.allowed` = [DELIVERED]."""
    headers = await _login(api, db, "ADMIN")
    package = await _package(db, 2, "RETURN_EXPECTED")

    res = await _adjust(api, headers, package.id, to_status="RETURN_RECEIVED_OK", reason="Thử chuyển tay")

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "TRANSITION_NOT_ALLOWED"
    assert res.json()["error"]["details"]["allowed"] == ["DELIVERED"]
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"


async def test_cancelled_after_pack_to_handed_over(api: AsyncClient, db: AsyncSession) -> None:
    """TC-06.19 bước 1 (DEC-258)."""
    headers = await _login(api, db, "SUPERVISOR")
    package = await _package(db, 3, "CANCELLED_AFTER_PACK")

    res = await _adjust(
        api, headers, package.id, to_status="HANDED_OVER", reason="Kiện đã giao trước khi hủy"
    )

    assert res.status_code == 200
    assert res.json()["recon_alert"] is None


async def test_missing_to_expected_extends(api: AsyncClient, db: AsyncSession) -> None:
    """DEC-255: gia hạn RETURN_MISSING → RETURN_EXPECTED đặt lại mốc `status_changed_at`."""
    headers = await _login(api, db, "SUPERVISOR")
    package = await _package(db, 4, "RETURN_MISSING")
    before = package.status_changed_at

    res = await _adjust(
        api, headers, package.id, to_status="RETURN_EXPECTED", reason="ĐVVC xác nhận đang trả"
    )

    assert res.status_code == 200
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    assert package.status_changed_at >= before


async def test_session_active_blocks(api: AsyncClient, db: AsyncSession) -> None:
    headers = await _login(api, db, "SUPERVISOR")
    _, station = await make_station_account(db, "tst_station_busy", "TST Station Busy")
    package = await _package(db, 5, "NEW")
    db.add(
        PackSession(package_id=package.id, station_id=station.id, status="OPEN", open_code="SPXADJ0000005")
    )
    await db.flush()

    res = await _adjust(api, headers, package.id, to_status="HANDED_OVER", reason="Đã gửi thật rồi")

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "SESSION_ACTIVE"


async def test_validation_and_foreign_alert(api: AsyncClient, db: AsyncSession) -> None:
    headers = await _login(api, db, "SUPERVISOR")
    package = await _package(db, 6, "NEW")
    other = await _package(db, 7, "NEW")
    alert = ReconAlert(
        package_id=other.id, rule="UNVERIFIED_STALE", severity="LOW", context={}, context_key="x"
    )
    db.add(alert)
    await db.flush()

    short = await _adjust(api, headers, package.id, to_status="HANDED_OVER", reason="abc")
    blank = await _adjust(api, headers, package.id, to_status="HANDED_OVER", reason="     ")
    foreign = await _adjust(
        api, headers, package.id, to_status="HANDED_OVER", reason="Lý do đủ dài", recon_alert_id=str(alert.id)
    )
    missing = await _adjust(api, headers, uuid.uuid4(), to_status="HANDED_OVER", reason="Lý do đủ dài")

    assert short.status_code == 422
    assert blank.status_code == 422
    assert foreign.status_code == 422
    assert "recon_alert_id" in foreign.json()["error"]["details"]["fields"]
    assert missing.status_code == 404


async def test_already_closed_alert_kept(api: AsyncClient, db: AsyncSession) -> None:
    """DEC-303: cảnh báo đã tự hết → vẫn điều chỉnh kiện, không ghi đè kết quả xử lý cũ."""
    headers = await _login(api, db, "SUPERVISOR")
    package = await _package(db, 8, "PACKED")
    alert = ReconAlert(
        package_id=package.id,
        rule="PACKED_NOT_HANDED_OVER",
        severity="MEDIUM",
        status="AUTO_RESOLVED",
        context={},
        context_key="k",
    )
    db.add(alert)
    await db.flush()

    res = await _adjust(
        api,
        headers,
        package.id,
        to_status="HANDED_OVER",
        reason="Sàn đã lấy hàng",
        recon_alert_id=str(alert.id),
    )

    assert res.status_code == 200
    assert res.json()["recon_alert"]["status"] == "AUTO_RESOLVED"
    assert res.json()["recon_alert"]["resolution"] is None


async def test_return_to_delivered_cancels_case_without_active_packages(
    api: AsyncClient, db: AsyncSession
) -> None:
    """02a API-122: `RETURN_* → DELIVERED` → hồ sơ mở → CANCELLED (không còn kiện chờ / nhận — DEC-303)."""
    headers = await _login(api, db, "SUPERVISOR")
    package = await _package(db, 9, "RETURN_EXPECTED")
    received = await _package(db, 10, "RETURN_EXPECTED")
    lone = ReturnCase(order_id=package.order_id, kind="BUYER_RETURN", status="EXPECTED", source="PLATFORM")
    shared = ReturnCase(
        order_id=received.order_id, kind="FAILED_DELIVERY", status="EXPECTED", source="PLATFORM"
    )
    db.add_all([lone, shared])
    await db.flush()
    sibling = Package(
        order_id=received.order_id, tracking_number="SPXADJ0000011", warehouse_status="RETURN_EXPECTED"
    )
    db.add(sibling)
    await db.flush()
    db.add_all(
        [
            ReturnCasePackage(return_case_id=lone.id, package_id=package.id),
            ReturnCasePackage(return_case_id=shared.id, package_id=received.id),
            ReturnCasePackage(return_case_id=shared.id, package_id=sibling.id),
        ]
    )
    await db.flush()

    r1 = await _adjust(api, headers, package.id, to_status="DELIVERED", reason="Sàn hủy yêu cầu trả")
    r2 = await _adjust(api, headers, received.id, to_status="DELIVERED", reason="Khách không trả kiện này")

    assert (r1.status_code, r2.status_code) == (200, 200)
    await db.refresh(lone)
    await db.refresh(shared)
    assert lone.status == "CANCELLED"
    assert shared.status == "EXPECTED"  # còn kiện chị em đang về
    # G3 SM-F3: kiện đã xác nhận giao rời hồ sơ (không còn tính vào hồ sơ).
    links = (
        await db.scalars(
            select(ReturnCasePackage.package_id).where(ReturnCasePackage.return_case_id == shared.id)
        )
    ).all()
    assert set(links) == {sibling.id}


async def test_adjust_recomputes_case(api: AsyncClient, db: AsyncSession) -> None:
    """G3 SM-F3 / R12: (a) giao thất bại P1 đã nhận, P2 quá hạn → chỉnh P2 DELIVERED: P2 rời hồ sơ, hồ sơ
    `RECEIVED_OK` (không kẹt MISSING / PARTIALLY_RECEIVED); (b) gia hạn MISSING → EXPECTED: hồ sơ về
    `EXPECTED`."""
    headers = await _login(api, db, "SUPERVISOR")
    p1 = await _package(db, 21, "RETURN_RECEIVED_OK")
    p2 = Package(order_id=p1.order_id, tracking_number="SPXADJ0000022", warehouse_status="RETURN_MISSING")
    db.add(p2)
    case = ReturnCase(
        order_id=p1.order_id, kind="FAILED_DELIVERY", status="PARTIALLY_RECEIVED", source="PLATFORM",
        single_session=False,
    )  # fmt: skip
    db.add(case)
    await db.flush()
    db.add_all([ReturnCasePackage(return_case_id=case.id, package_id=p.id) for p in (p1, p2)])
    await db.flush()
    res = await _adjust(api, headers, p2.id, to_status="DELIVERED", reason="ĐVVC xác nhận đã giao lại")
    assert res.status_code == 200, res.text
    await db.refresh(case)
    assert case.status == "RECEIVED_OK"

    p3 = await _package(db, 23, "RETURN_MISSING")
    missing = ReturnCase(order_id=p3.order_id, kind="BUYER_RETURN", status="MISSING", source="PLATFORM")
    db.add(missing)
    await db.flush()
    db.add(ReturnCasePackage(return_case_id=missing.id, package_id=p3.id))
    await db.flush()
    res = await _adjust(api, headers, p3.id, to_status="RETURN_EXPECTED", reason="ĐVVC xác nhận đang trả")
    assert res.status_code == 200, res.text
    await db.refresh(missing)
    assert missing.status == "EXPECTED"


@pytest.mark.parametrize("role", ["CSKH", "STATION"])
async def test_permission(api: AsyncClient, db: AsyncSession, role: str) -> None:
    """TC-P2.08 (API-122): CSKH, STATION → 403."""
    headers = await _login(api, db, role)
    package = await _package(db, 12, "NEW")

    res = await _adjust(api, headers, package.id, to_status="HANDED_OVER", reason="Đã gửi thật rồi")

    assert res.status_code == 403


@pytest.mark.parametrize(
    ("role", "has", "lacks"),
    [
        ("ADMIN", {"returns.read", "returns.link", "warehouse_status.adjust", "claims.manage"}, set()),
        ("SUPERVISOR", {"recon.resolve", "inspection.correct", "recon.read"}, set()),
        (
            "CSKH",
            {"returns.read", "recon.read", "claims.manage"},
            {"recon.resolve", "warehouse_status.adjust"},
        ),
        ("STATION", set(), {"returns.read", "claims.manage"}),
    ],
)
async def test_me_permissions_phase2(
    api: AsyncClient, db: AsyncSession, role: str, has: set[str], lacks: set[str]
) -> None:
    """API-04 `permissions` mới theo 01 §5.10 (FR-10.02)."""
    headers = await _login(api, db, role)

    perms = set((await api.get("/api/v1/me", headers=headers)).json()["permissions"])

    assert has <= perms
    assert not (lacks & perms)
