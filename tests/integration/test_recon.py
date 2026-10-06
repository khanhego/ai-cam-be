"""Đối soát (T-113): J-14 `run_rules` 7 quy tắc + `context_key`, API-120 / 121 / 123 — FR-06.02, 06.03, 06.06.

TC-06.01, 06.03..06.11, 06.15, 06.17, 06.18, TC-P2.07, P2.08 (API-121 / 123); BR-26 không trùng / tự đóng /
không tạo lại sau xử lý tay; BR-12 không đè `PARTIALLY_RECEIVED`.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.redis import get_redis
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, StatusHistory
from aicam.modules.reconciliation import service as recon
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.settings.models import Setting
from aicam.realtime import publish

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 20, 3, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
async def _env(db: AsyncSession, redis_client: object) -> None:
    clock.freeze(NOW)
    row = await db.get(Setting, 1)
    assert row is not None
    row.recon_start_at = NOW - timedelta(days=30)
    await db.flush()


@pytest.fixture
def ws(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
    sent: list[tuple[str, dict[str, Any]]] = []

    async def _capture(event: str, data: dict[str, Any]) -> None:
        sent.append((event, data))

    monkeypatch.setattr(publish, "to_dashboard", _capture)
    return sent


async def _login(api: AsyncClient, db: AsyncSession, role: str, name: str = "") -> dict[str, str]:
    if role == "STATION":
        user, _ = await make_station_account(db, f"tst_recon_st{name}", f"TST Recon {name}")
    else:
        user = await make_user(db, f"tst_recon_{role.lower()}{name}", role, display_name=f"QA {role}{name}")
    client = "STATION" if role == "STATION" else "DASHBOARD"
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _alerts(db: AsyncSession, package: Package, rule: str | None = None) -> list[ReconAlert]:
    query = select(ReconAlert).where(ReconAlert.package_id == package.id)
    if rule:
        query = query.where(ReconAlert.rule == rule)
    rows = await db.scalars(query.order_by(ReconAlert.detected_at).execution_options(populate_existing=True))
    return list(rows.all())


async def _run(db: AsyncSession, settings: Settings) -> dict[str, Any]:
    return await recon.run_rules(db, settings)


async def _package(
    db: AsyncSession,
    n: int,
    status: str,
    *,
    platform: str = "READY_TO_SHIP",
    changed_ago: timedelta = timedelta(0),
    created_at_platform: datetime | None = None,
) -> Package:
    order = Order(
        platform_order_sn=f"2410REC{n:05d}", platform_status=platform,
        created_at_platform=created_at_platform or NOW - timedelta(days=2),
    )  # fmt: skip
    db.add(order)
    await db.flush()
    package = Package(
        order_id=order.id, tracking_number=f"SPXREC{n:07d}", warehouse_status=status,
        created_at=NOW - timedelta(days=2), status_changed_at=NOW - changed_ago,
    )  # fmt: skip
    db.add(package)
    await db.flush()
    return package


# ---------------------------------------------------------------- BR-12 (bước 1 + RETURN_OVERDUE)


async def test_br12_overdue_moves_to_missing_and_alerts(
    db: AsyncSession, test_settings: Settings, ws: list[tuple[str, dict[str, Any]]]
) -> None:
    """TC-06.01 + TC-06.03: kiện `RETURN_EXPECTED` 8 ngày → `RETURN_MISSING` (WAREHOUSE "Đối soát"), hồ sơ
    `MISSING`, cảnh báo `RETURN_OVERDUE` Cao; chạy 2 lần → đúng 1 cảnh báo mở; WS `recon.updated`."""
    order, (package,) = await make_order(db, 49)
    case = await buyer_return_case(db, order, 49)
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    package.status_changed_at = NOW - timedelta(days=8)
    await db.execute(
        update(StatusHistory)
        .where(StatusHistory.package_id == package.id, StatusHistory.to_status == "RETURN_EXPECTED")
        .values(at=NOW - timedelta(days=8))
    )
    await db.flush()

    out = await _run(db, test_settings)
    assert out["missing"] == 1
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_MISSING"
    case = await db.get(ReturnCase, case.id, populate_existing=True)  # type: ignore[assignment]
    assert case.status == "MISSING"
    (alert,) = await _alerts(db, package)
    assert (alert.rule, alert.severity, alert.status) == ("RETURN_OVERDUE", "HIGH", "OPEN")
    assert alert.context["days"] == 8
    assert any(e == "recon.updated" and d["summary"]["open"]["HIGH"] == 1 for e, d in ws)
    assert ("return.updated", {"return_case_id": str(case.id), "status": "MISSING"}) in ws

    await _run(db, test_settings)
    assert len(await _alerts(db, package)) == 1


async def test_br12_keeps_partially_received(db: AsyncSession, test_settings: Settings) -> None:
    """R-18 / 02 §6.3 #3: hồ sơ nhiều kiện đã nhận một kiện, kiện kia quá hạn → kiện `RETURN_MISSING`, hồ sơ
    giữ `PARTIALLY_RECEIVED`."""
    order, (first, second) = await make_order(db, 43, packages=2, warehouse_status="HANDED_OVER")
    result = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_FAILED_DELIVERY, key="FAILED:2410TST00043:1")
    )
    assert result.case is not None
    first.warehouse_status = "RETURN_RECEIVED_OK"
    second.status_changed_at = NOW - timedelta(days=8)
    result.case.status = "PARTIALLY_RECEIVED"
    result.case.single_session = False
    await db.flush()
    await _run(db, test_settings)
    await db.refresh(second)
    assert second.warehouse_status == "RETURN_MISSING"
    case = await db.get(ReturnCase, result.case.id, populate_existing=True)
    assert case is not None
    assert case.status == "PARTIALLY_RECEIVED"


async def test_br12_extend_then_overdue_again(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """TC-06.15 (DEC-255): API-122 `RETURN_MISSING → RETURN_EXPECTED` → J-14 ngay không `MISSING` lại (cảnh
    báo cũ đã xử lý); +8 ngày → `MISSING` + cảnh báo **mới** (đợt mới)."""
    package = await _package(db, 1, "RETURN_EXPECTED", changed_ago=timedelta(days=8))
    await _run(db, test_settings)
    (old,) = await _alerts(db, package)
    headers = await _login(api, db, "SUPERVISOR")
    res = await api.post(
        f"/api/v1/packages/{package.id}/warehouse-status",
        json={
            "to_status": "RETURN_EXPECTED",
            "reason": "ĐVVC xác nhận đang trả",
            "recon_alert_id": str(old.id),
        },
        headers=headers,
    )
    assert res.status_code == 200, res.text
    await _run(db, test_settings)
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    assert [a.status for a in await _alerts(db, package)] == ["RESOLVED"]

    clock.advance(timedelta(days=8))
    await _run(db, test_settings)
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_MISSING"
    assert [a.status for a in await _alerts(db, package)] == ["RESOLVED", "OPEN"]


# ---------------------------------------------------------------- BR-10, 11, 13, 14, 19, 20


async def test_br10_shipped_not_packed_and_old_orders(db: AsyncSession, test_settings: Settings) -> None:
    """TC-06.04 / 06.05 (DEC-254): kiện `NEW`, sàn `SHIPPED`, đơn tạo sau `recon_start_at` → Cao; đơn cũ →
    không; kiện tạm không vào."""
    fresh = await _package(db, 2, "NEW", platform="SHIPPED")
    old = await _package(db, 3, "NEW", platform="SHIPPED", created_at_platform=NOW - timedelta(days=60))
    await _run(db, test_settings)
    (alert,) = await _alerts(db, fresh)
    assert (alert.rule, alert.severity, alert.context_key) == ("SHIPPED_NOT_PACKED", "HIGH", "SHIPPED")
    assert await _alerts(db, old) == []


async def test_br11_resolved_not_recreated(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """TC-06.06 (DEC-226): `CANCELLED_AFTER_PACK` → cảnh báo TB; API-121 "Đã tháo kiện" → J-14 không tạo lại
    (cùng `context_key`); audit `RECON_RESOLVE`."""
    package = await _package(db, 4, "CANCELLED_AFTER_PACK", platform="CANCELLED")
    await _run(db, test_settings)
    (alert,) = await _alerts(db, package)
    assert (alert.rule, alert.severity) == ("CANCELLED_AFTER_PACK", "MEDIUM")
    headers = await _login(api, db, "SUPERVISOR")
    res = await api.post(
        f"/api/v1/recon-alerts/{alert.id}/resolve", json={"note": "Đã tháo kiện"}, headers=headers
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "RESOLVED"
    assert body["resolution"]["action"] == "RESOLVE"
    assert body["resolution"]["note"] == "Đã tháo kiện"
    assert body["resolution"]["by"]["display_name"] == "QA SUPERVISOR"
    audit = await db.scalar(select(AuditLog).where(AuditLog.action == "RECON_RESOLVE"))
    assert audit is not None
    await _run(db, test_settings)
    assert [a.status for a in await _alerts(db, package)] == ["RESOLVED"]


async def test_br13_unannounced_then_platform_reports(db: AsyncSession, test_settings: Settings) -> None:
    """TC-06.07, TC-05.38 bước 1 + 3: hồ sơ `UNANNOUNCED` quá 24 giờ → Thấp; sàn báo (gắn mã sàn) → tự
    đóng."""
    order, (package,) = await make_order(db, 46, warehouse_status="RETURN_RECEIVED_OK")
    scan = await returns.attach_or_create(
        db, order, returns.Signal(returns.SIGNAL_WAREHOUSE_SCAN, package_ids=(package.id,))
    )
    assert scan.case is not None
    scan.case.created_at = NOW - timedelta(hours=25)
    await db.flush()
    await _run(db, test_settings)
    (alert,) = await _alerts(db, package)
    assert (alert.rule, alert.severity, alert.context_key) == ("RETURN_UNANNOUNCED", "LOW", str(scan.case.id))
    scan.case.platform_return_sn, scan.case.kind = "2410RTTST046", "BUYER_RETURN"
    await db.flush()
    out = await _run(db, test_settings)
    assert out["auto_resolved"] == 1
    (alert,) = await _alerts(db, package)
    assert alert.status == "AUTO_RESOLVED"
    assert alert.closed_at == NOW


async def test_br14_packed_then_handed_over_auto_resolves(db: AsyncSession, test_settings: Settings) -> None:
    """TC-06.08: `PACKED` 25 giờ → TB; sàn lấy hàng (`HANDED_OVER`) → `AUTO_RESOLVED`. Dưới ngưỡng → không."""
    package = await _package(db, 52, "PACKED", changed_ago=timedelta(hours=25))
    recent = await _package(db, 53, "PACKED", changed_ago=timedelta(hours=2))
    await _run(db, test_settings)
    (alert,) = await _alerts(db, package)
    assert (alert.rule, alert.severity, alert.context["hours"]) == ("PACKED_NOT_HANDED_OVER", "MEDIUM", 25)
    assert await _alerts(db, recent) == []
    await orders.transition(db, package, "HANDED_OVER", source="PLATFORM")
    await db.flush()
    await _run(db, test_settings)
    (alert,) = await _alerts(db, package)
    assert alert.status == "AUTO_RESOLVED"


async def test_br19_refund_paid_not_received_and_closed(db: AsyncSession, test_settings: Settings) -> None:
    """TC-06.09 / 06.10 (DEC-262): sàn `REFUND_PAID` mà kiện còn đang về → Cao; `CLOSED` → không cảnh báo."""
    order, (package,) = await make_order(db, 51)
    case = await buyer_return_case(db, order, 51)
    case.platform_status = "REFUND_PAID"
    other_order, (other,) = await make_order(db, 54)
    closed = await buyer_return_case(db, other_order, 54)
    closed.platform_status = "CLOSED"
    await db.flush()
    await _run(db, test_settings)
    (alert,) = await _alerts(db, package)
    assert (alert.rule, alert.severity, alert.context["return_case"]) == (
        "RETURN_DONE_NOT_RECEIVED",
        "HIGH",
        case.code,
    )
    assert await _alerts(db, other, "RETURN_DONE_NOT_RECEIVED") == []


async def test_br20_unverified_not_placeholder(db: AsyncSession, test_settings: Settings) -> None:
    """TC-06.11: kiện chưa xác minh 25 giờ → Thấp; kiện tạm `TAM-` cùng tuổi → không."""
    unverified = Package(
        tracking_number="SPXVN0000000777", verified=False, created_at=NOW - timedelta(hours=25)
    )
    db.add(unverified)
    placeholder = await returns.create_placeholder_package(db)
    placeholder.created_at = NOW - timedelta(hours=25)
    await db.flush()
    await _run(db, test_settings)
    (alert,) = await _alerts(db, unverified)
    assert (alert.rule, alert.severity) == ("UNVERIFIED_STALE", "LOW")
    assert await _alerts(db, placeholder) == []


# ---------------------------------------------------------------- J-14 khóa / tắt


async def test_run_lock_and_disabled(db: AsyncSession, test_settings: Settings) -> None:
    """R-20: J-14 đang giữ `recon:run` → lượt khác bỏ; `RECON_ENABLED=false` → không làm gì."""
    await get_redis().set(recon.RUN_LOCK_KEY, "other", ex=60)
    assert await _run(db, test_settings) == {"skipped": "locked"}
    await get_redis().delete(recon.RUN_LOCK_KEY)
    test_settings.recon_enabled = False
    assert await _run(db, test_settings) == {"skipped": "disabled"}
    test_settings.recon_enabled = True
    assert "created" in await _run(db, test_settings)
    assert not await get_redis().exists(recon.RUN_LOCK_KEY)  # nhả khóa sau khi chạy


# ---------------------------------------------------------------- API-120 / 121 / 123


async def test_api120_list_filters_sort_summary(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """API-120: sắp mức (HIGH trước) rồi `detected_at` cũ trước; lọc; `summary.open`;
    `allowed_status_targets`; CSKH xem được (TC-P2.07)."""
    low = Package(tracking_number="SPXVN0000000778", verified=False, created_at=NOW - timedelta(hours=25))
    db.add(low)
    await db.flush()
    high = await _package(db, 5, "RETURN_EXPECTED", changed_ago=timedelta(days=9))
    medium = await _package(db, 6, "PACKED", changed_ago=timedelta(hours=30))
    await _run(db, test_settings)
    headers = await _login(api, db, "CSKH")
    res = await api.get("/api/v1/recon-alerts", params={"status": "OPEN"}, headers=headers)
    assert res.status_code == 200, res.text
    body = res.json()
    assert [i["package"]["id"] for i in body["items"]] == [str(high.id), str(medium.id), str(low.id)]
    assert body["summary"] == {"open": {"HIGH": 1, "MEDIUM": 1, "LOW": 1}}
    assert body["total"] == 3
    first = body["items"][0]
    assert (first["rule"], first["br"], first["package"]["warehouse_status"]) == (
        "RETURN_OVERDUE",
        "BR-12",
        "RETURN_MISSING",
    )
    assert first["allowed_status_targets"] == ["RETURN_EXPECTED", "DELIVERED"]
    assert first["resolution"] is None
    res = await api.get("/api/v1/recon-alerts", params={"severity": "MEDIUM"}, headers=headers)
    assert [i["package"]["id"] for i in res.json()["items"]] == [str(medium.id)]
    res = await api.get("/api/v1/recon-alerts", params={"package_id": str(low.id)}, headers=headers)
    assert [i["rule"] for i in res.json()["items"]] == ["UNVERIFIED_STALE"]
    day = NOW.date().isoformat()
    res = await api.get("/api/v1/recon-alerts", params={"date_from": day, "date_to": day}, headers=headers)
    assert res.json()["total"] == 3
    res = await api.get("/api/v1/recon-alerts", params={"date_from": "2026-01-01", "date_to": "2026-10-20"},
                        headers=headers)  # fmt: skip
    assert res.status_code == 422


async def test_api121_conflict_and_validation(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, ws: list[tuple[str, dict[str, Any]]]
) -> None:
    """TC-06.17: người sau → `409 ALREADY_RESOLVED` kèm `resolved_by`; ghi chú rỗng → 422; cảnh báo tự hết →
    409 `resolved_by = null`; không tồn tại → 404; WS `recon.updated`."""
    package = await _package(db, 7, "PACKED", changed_ago=timedelta(hours=30))
    await _run(db, test_settings)
    (alert,) = await _alerts(db, package)
    a = await _login(api, db, "SUPERVISOR", "a")
    b = await _login(api, db, "ADMIN", "b")
    res = await api.post(f"/api/v1/recon-alerts/{alert.id}/resolve", json={"note": "   "}, headers=a)
    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"]["note"]
    ws.clear()
    res = await api.post(f"/api/v1/recon-alerts/{alert.id}/resolve", json={"note": "Đã kiểm kệ"}, headers=a)
    assert res.status_code == 200
    assert ws[0] == ("recon.updated", {"summary": {"open": {"HIGH": 0, "MEDIUM": 0, "LOW": 0}}})
    res = await api.post(
        f"/api/v1/recon-alerts/{alert.id}/resolve", json={"note": "Tôi cũng xử lý"}, headers=b
    )
    assert res.status_code == 409
    err = res.json()["error"]
    assert err["code"] == "ALREADY_RESOLVED"
    assert err["details"]["status"] == "RESOLVED"
    assert err["details"]["resolved_by"]["display_name"] == "QA SUPERVISORa"
    assert err["details"]["closed_at"] == clock.iso_z(NOW)

    auto = await _package(db, 8, "PACKED", changed_ago=timedelta(hours=30))
    await _run(db, test_settings)
    (alert2,) = await _alerts(db, auto)
    await orders.transition(db, auto, "HANDED_OVER", source="PLATFORM")
    await db.flush()
    await _run(db, test_settings)
    res = await api.post(f"/api/v1/recon-alerts/{alert2.id}/resolve", json={"note": "Muộn"}, headers=a)
    assert res.status_code == 409
    assert res.json()["error"]["details"]["status"] == "AUTO_RESOLVED"
    assert res.json()["error"]["details"]["resolved_by"] is None
    res = await api.post(f"/api/v1/recon-alerts/{uuid.uuid4()}/resolve", json={"note": "x"}, headers=a)
    assert res.status_code == 404


async def test_api123_run_and_in_progress(
    api: AsyncClient, db: AsyncSession, sent_jobs: list[tuple[str, list[Any], str, float]]
) -> None:
    """TC-06.18: API-123 → 202 `{queued: true}` + đẩy J-14; J-14 đang giữ khóa → `409 RECON_IN_PROGRESS`."""
    headers = await _login(api, db, "SUPERVISOR")
    res = await api.post("/api/v1/recon/run", headers=headers)
    assert (res.status_code, res.json()) == (202, {"queued": True})
    assert sent_jobs[-1] == ("reconciliation.run_rules", [], "default", 0.0)
    await get_redis().set(recon.RUN_LOCK_KEY, "j14", ex=60)
    res = await api.post("/api/v1/recon/run", headers=headers)
    assert res.status_code == 409
    assert res.json()["error"]["code"] == "RECON_IN_PROGRESS"


@pytest.mark.parametrize(
    ("role", "list_status", "write_status"), [("CSKH", 200, 403), ("STATION", 403, 403), ("ADMIN", 200, 202)]
)
async def test_recon_permissions(
    api: AsyncClient, db: AsyncSession, role: str, list_status: int, write_status: int
) -> None:
    """TC-P2.07 / P2.08: xem cảnh báo ADMIN / SUPERVISOR / CSKH; xử lý / chạy ngay chỉ ADMIN / SUPERVISOR."""
    headers = await _login(api, db, role)
    assert (await api.get("/api/v1/recon-alerts", headers=headers)).status_code == list_status
    assert (await api.post("/api/v1/recon/run", headers=headers)).status_code == write_status
    res = await api.post(f"/api/v1/recon-alerts/{uuid.uuid4()}/resolve", json={"note": "x"}, headers=headers)
    assert res.status_code == (404 if write_status == 202 else 403)


async def test_sync_change_requests_recon_soon(
    db: AsyncSession, sent_jobs: list[tuple[str, list[Any], str, float]]
) -> None:
    """02a §7 J-14: sau J-04 / J-06 / J-13 có thay đổi → J-14 sau 30 giây; nhiều lần trong 30 giây gộp một."""
    from aicam.core.db import commit

    recon.request_run_soon(db)
    await commit(db)
    recon.request_run_soon(db)
    await commit(db)
    assert [j for j in sent_jobs if j[0] == "reconciliation.run_rules"] == [
        ("reconciliation.run_rules", [], "default", 30.0)
    ]
    assert int(await db.scalar(select(func.count()).select_from(ReconAlert)) or 0) == 0
