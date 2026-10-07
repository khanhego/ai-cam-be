"""T-285 — kiện hủy oan (BR-21 v0.4, DEC-519): `revert_candidates`, `revert_cancel`,
`aicam fix-cancel-requests`,
lưới an toàn khi đồng bộ; BR-11 theo nhóm. 02a §5 BR-21 `test_cancel_revert` (a)–(h); AC-41.

Dữ liệu "kiểu Phase 2": kiện bị hủy khi đơn chỉ đang yêu cầu hủy (`IN_CANCEL`) — dựng bằng `transition` nguồn
`PLATFORM` như J-04 / J-06 Phase 2 đã làm.
"""

from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.settings import Settings
from aicam.modules.orders import cancel_revert
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, StatusHistory
from aicam.modules.platforms.base import PlatformItem, PlatformOrder
from aicam.modules.platforms.shopee.mapping import order_group
from aicam.modules.reconciliation import service as recon
from aicam.modules.reconciliation.models import ReconAlert

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 2, 0, tzinfo=UTC)
ITEM = PlatformItem("Áo", 1, "SKU")


@pytest.fixture(autouse=True)
def _clock(redis_client: object) -> None:
    clock.freeze(NOW)


def _maker(db: AsyncSession) -> async_sessionmaker[AsyncSession]:
    """Phiên riêng trên cùng connection của test (savepoint): lệnh chạy nhiều transaction, test rollback."""
    return async_sessionmaker(bind=db.bind, expire_on_commit=False, join_transaction_mode="create_savepoint")


async def _cancelled(
    db: AsyncSession,
    n: int,
    *,
    packed: bool = False,
    now_status: str = "READY_TO_SHIP",
    source: str = "PLATFORM",
) -> Package:
    """Kiện bị Phase 2 hủy oan lúc đơn `IN_CANCEL`; đơn nay ở `now_status`."""
    sn, code = f"2410REV{n:05d}", f"SPXREV{n:06d}"
    result = await orders.upsert_platform_order(
        db, PlatformOrder(sn, "READY_TO_SHIP", (code,), (ITEM,), status_group=order_group("READY_TO_SHIP"))
    )
    package = result.packages[0]
    if packed:
        await orders.transition(db, package, "PACKING", source="WAREHOUSE")
        await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    target = "CANCELLED_AFTER_PACK" if packed else "CANCELLED"
    await orders.transition(db, package, target, source=source, actor_label="Sàn (Phase 2)")
    orders.set_platform_status(result.order, now_status, order_group(now_status))
    await db.flush()
    return package


async def _status(db: AsyncSession, package: Package) -> str:
    await db.refresh(package)
    return package.warehouse_status


async def test_fix_command_dry_run_then_apply_idempotent(db: AsyncSession, test_settings: Settings) -> None:
    a = await _cancelled(db, 1)  # (a) CANCELLED, đơn READY_TO_SHIP → NEW
    b = await _cancelled(db, 2, packed=True)  # (b) CANCELLED_AFTER_PACK, BR-11 OPEN → PACKED
    c = await _cancelled(db, 3, packed=True)  # (c) BR-11 đã xử lý tay → giữ
    d = await _cancelled(db, 4, source="MANUAL")  # (d) hủy tay → giữ
    e = await _cancelled(db, 5, now_status="CANCELLED")  # (e) đơn đã hủy thật → không phải ứng viên
    g = await _cancelled(db, 7, now_status="IN_CANCEL")  # (g) còn yêu cầu hủy → trả lại, quét vẫn bị chặn
    for package, status in ((b, "OPEN"), (c, "RESOLVED")):
        db.add(ReconAlert(package_id=package.id, rule="CANCELLED_AFTER_PACK", severity="MEDIUM",
                          status=status,
                          context={}, context_key="CANCELLED", detected_at=NOW,
                          closed_at=NOW if status == "RESOLVED" else None))  # fmt: skip
    await db.flush()
    maker = _maker(db)

    dry = await cancel_revert.fix_cancel_requests(maker, apply=False)  # (f) chạy thử không ghi

    assert [await _status(db, p) for p in (a, b, c, d, e, g)] == [
        "CANCELLED", "CANCELLED_AFTER_PACK", "CANCELLED_AFTER_PACK", "CANCELLED", "CANCELLED", "CANCELLED",
    ]  # fmt: skip
    text = "\n".join(dry.lines)
    assert f"SẼ TRẢ LẠI {a.tracking_number}" in text
    assert f"BỎ QUA {c.tracking_number}" in text
    assert "Cảnh báo BR-11 đã được xử lý tay — kiểm tay" in text
    assert "Hủy do người chỉnh tay — kiểm tay" in text
    assert e.tracking_number not in text
    assert dry.lines[-1].endswith("sẽ trả lại 3, bỏ qua 2.")

    applied = await cancel_revert.fix_cancel_requests(maker, apply=True)

    assert (applied.reverted, applied.skipped, applied.failed) == (3, 2, 0)
    assert [await _status(db, p) for p in (a, b, c, d, e, g)] == [
        "NEW", "PACKED", "CANCELLED_AFTER_PACK", "CANCELLED", "CANCELLED", "NEW",
    ]  # fmt: skip
    history = (
        await db.scalars(
            select(StatusHistory)
            .where(StatusHistory.package_id == a.id)
            .order_by(StatusHistory.at, StatusHistory.id)
        )
    ).all()
    assert (history[-1].from_status, history[-1].to_status, history[-1].source) == (
        "CANCELLED",
        "NEW",
        "PLATFORM",
    )
    audits = (await db.scalars(select(AuditLog).where(AuditLog.action == "PACKAGE_CANCEL_REVERT"))).all()
    assert sorted(x.object_id for x in audits) == sorted(str(p.id) for p in (a, b, g))
    assert {x.data["trigger"] for x in audits} == {"COMMAND"}
    # (g) BR-01: đơn còn yêu cầu hủy → quét vẫn bị chặn
    assert await orders.is_cancelled(db, g) is True
    # (b) lượt đối soát sau: BR-11 hết điều kiện → AUTO_RESOLVED
    await recon.run_rules(db, test_settings)
    alert_b = await db.scalar(select(ReconAlert).where(ReconAlert.package_id == b.id))
    assert alert_b is not None
    assert alert_b.status == "AUTO_RESOLVED"
    # (f) chạy lại idempotent
    again = await cancel_revert.fix_cancel_requests(maker, apply=True)
    assert (again.reverted, again.failed) == (0, 0)
    assert (
        await db.scalar(
            select(func.count()).select_from(AuditLog).where(AuditLog.action == "PACKAGE_CANCEL_REVERT")
        )
        == 3
    )


async def test_sync_safety_net_reverts_when_request_rejected(db: AsyncSession) -> None:
    """(h): J-04 thấy đơn `IN_CANCEL → READY_TO_SHIP` khi ops chưa chạy lệnh → lưới an toàn trả lại kiện."""
    package = await _cancelled(db, 8, now_status="IN_CANCEL")
    order = await db.get(Order, package.order_id)
    assert order is not None

    await orders.upsert_platform_order(
        db, PlatformOrder(order.platform_order_sn, "READY_TO_SHIP", (package.tracking_number,), (ITEM,),
                          status_group=order_group("READY_TO_SHIP"))
    )  # fmt: skip

    assert await _status(db, package) == "NEW"
    audit = await db.scalar(select(AuditLog).where(AuditLog.action == "PACKAGE_CANCEL_REVERT"))
    assert audit is not None
    assert audit.data["trigger"] == "SYNC"


async def test_sync_does_not_revert_when_request_accepted(db: AsyncSession) -> None:
    package = await _cancelled(db, 9, now_status="IN_CANCEL")
    order = await db.get(Order, package.order_id)
    assert order is not None
    await orders.upsert_platform_order(
        db,
        PlatformOrder(
            order.platform_order_sn, "CANCELLED", (), (ITEM,), status_group=order_group("CANCELLED")
        ),
    )
    assert await _status(db, package) == "CANCELLED"


async def test_reverse_transitions_only_through_revert_cancel(db: AsyncSession) -> None:
    package = await _cancelled(db, 10)
    with pytest.raises(orders.InvalidTransition):
        await orders.transition(db, package, "NEW", source="MANUAL")
    order = await db.get(Order, package.order_id)
    assert order is not None
    with pytest.raises(ValueError, match="trigger"):
        await cancel_revert.revert_cancel(db, package, order, trigger="HAND")


async def test_br11_only_cancelled_group_and_packed_cancel_requested_quiet(
    db: AsyncSession, test_settings: Settings
) -> None:
    """BR-11 / BR-14 theo nhóm: đơn `CANCEL_REQUESTED` + kiện `PACKED` quá ngưỡng → không BR-11, không BR-14;
    nhóm `CANCELLED` → BR-11."""
    from aicam.modules.reconciliation import rules

    result = await orders.upsert_platform_order(
        db, PlatformOrder("2410REV00011", "READY_TO_SHIP", ("SPXREV000011",), (ITEM,),
                          status_group=order_group("READY_TO_SHIP"))
    )  # fmt: skip
    package = result.packages[0]
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    package.status_changed_at = datetime(2026, 10, 1, tzinfo=UTC)
    orders.set_platform_status(result.order, "IN_CANCEL", order_group("IN_CANCEL"))
    await db.flush()
    params = rules.Params(now=NOW, recon_start_at=datetime(2026, 1, 1, tzinfo=UTC), return_missing_days=7,
                          handover_warn_hours=24)  # fmt: skip

    hits: list[Any] = await rules.cancelled_after_pack(db, params) + await rules.packed_not_handed_over(
        db, params
    )
    assert package.id not in {h.package_id for h in hits}

    orders.set_platform_status(result.order, "CANCELLED", order_group("CANCELLED"))
    await db.flush()
    assert package.id in {h.package_id for h in await rules.cancelled_after_pack(db, params)}


async def test_pending_revert_shown_in_d2_attention_admin_only(api: Any, db: AsyncSession) -> None:
    """G3-EV-3: còn kiện hủy oan trả lại được → D2 "Cần xử lý" `CANCEL_REVERT_PENDING` (chỉ ADMIN; kiện hủy
    tay không tính) + log cảnh báo lúc khởi động; chạy lệnh xong → hết."""
    from .factories import PASSWORD, make_user

    await _cancelled(db, 11)
    await _cancelled(db, 12, source="MANUAL")  # kiểm tay — không tính
    await db.flush()

    async def daily(role: str) -> list[dict[str, Any]]:
        user = await make_user(db, f"tst_rev_{role.lower()}", role)
        login = {"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
        res = await api.post("/api/v1/auth/login", json=login)
        headers = {"Authorization": f"Bearer {res.json()['access_token']}"}
        body = await api.get("/api/v1/reports/daily", headers=headers)
        assert body.status_code == 200, body.text
        return [a for a in body.json()["attention"] if a["kind"] == "CANCEL_REVERT_PENDING"]

    assert await daily("ADMIN") == [{"kind": "CANCEL_REVERT_PENDING", "count": 1}]
    assert await daily("SUPERVISOR") == []
    assert await cancel_revert.log_pending_on_startup(db) == 1
    await cancel_revert.fix_cancel_requests(_maker(db), apply=True)
    assert await cancel_revert.pending_count(db) == 0
