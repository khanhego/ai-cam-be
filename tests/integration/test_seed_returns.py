"""`aicam seed-demo` phần hàng hoàn (T-116; 04 §1, DEC-333): đủ loại mẫu, chạy lại không nhân đôi."""

from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.settings import Settings
from aicam.entrypoints.seed_returns import UNIDENTIFIED_NOTE, seed_returns
from aicam.modules.claims.models import Claim
from aicam.modules.orders.models import Package
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.returns.models import ReturnCase, ReturnCasePackage
from aicam.realtime import publish

from .factories import make_station_account, make_user

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _no_ws(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _drop(event: str, data: dict[str, Any]) -> None:
        return None

    monkeypatch.setattr(publish, "to_dashboard", _drop)


async def _state(db: AsyncSession) -> dict[str, Any]:
    cases = (
        await db.execute(
            select(ReturnCase.kind, ReturnCase.status, ReturnCase.return_tracking_number, func.count())
            .join(ReturnCasePackage, ReturnCasePackage.return_case_id == ReturnCase.id)
            .join(Package, Package.id == ReturnCasePackage.package_id)
            .where((Package.tracking_number.like("SPXTST00000%")) | Package.is_placeholder)
            .where(
                (ReturnCase.platform_return_sn.like("2410RTTST0%"))
                | (ReturnCase.force_note == UNIDENTIFIED_NOTE)
            )
            .group_by(ReturnCase.id)
            .order_by(ReturnCase.code)
            .execution_options(populate_existing=True)
        )
    ).all()
    statuses = dict(
        (
            await db.execute(
                select(Package.tracking_number, Package.warehouse_status).where(
                    Package.tracking_number.in_(
                        [
                            "SPXTST0000047-1",
                            "SPXTST0000048-2",
                            "SPXTST0000049",
                            "SPXTST0000052",
                            "SPXTST0000053",
                        ]
                    )
                )
            )
        ).all()
    )
    alerts = (
        await db.execute(
            select(Package.tracking_number, ReconAlert.rule, ReconAlert.severity, ReconAlert.status)
            .join(Package, Package.id == ReconAlert.package_id)
            .where(Package.tracking_number.in_(["SPXTST0000049", "SPXTST0000052"]))
            .order_by(Package.tracking_number)
        )
    ).all()
    claims = (
        await db.execute(
            select(Claim.type, Claim.counterparty, Claim.status)
            .join(Package, Package.id == Claim.package_id)
            .where(Package.tracking_number == "SPXTST0000049")
        )
    ).all()
    return {"cases": [tuple(c) for c in cases], "statuses": statuses, "alerts": [tuple(a) for a in alerts],
            "claims": [tuple(c) for c in claims]}  # fmt: skip


async def test_seed_returns_creates_every_kind_and_is_idempotent(
    db: AsyncSession, redis_client: object, test_settings: Settings
) -> None:
    _, station = await make_station_account(db, "tst_seed_st02", "TST Seed Station 02")
    sup = await make_user(db, "tst_seed_sup", "SUPERVISOR")

    lines = await seed_returns(db, test_settings, station, sup)
    first = await _state(db)

    assert first == {
        "cases": [
            ("BUYER_RETURN", "EXPECTED", "SPXRTTST000047", 2),  # trọn đơn: 2 kiện đang về
            # một phần: chưa biết kiện nào chứa áo → cả 2 kiện của đơn đang về (đóng khi nhận đủ / BR-24)
            ("BUYER_RETURN", "EXPECTED", "SPXRTTST000048", 2),
            ("BUYER_RETURN", "MISSING", "SPXRTTST000049", 1),  # 8 ngày chưa về → J-14 quá hạn (BR-12)
            ("REFUND_ONLY", "NO_PARCEL", None, 1),  # chỉ hoàn tiền
            ("UNIDENTIFIED", "EXPECTED", None, 1),  # chưa xác định, kiện tạm TAM-
        ],
        "statuses": {
            "SPXTST0000047-1": "RETURN_EXPECTED",
            "SPXTST0000048-2": "RETURN_EXPECTED",
            "SPXTST0000049": "RETURN_MISSING",
            "SPXTST0000052": "PACKED",
            "SPXTST0000053": "DELIVERED",  # chỉ hoàn tiền: kiện không đổi
        },
        "alerts": [
            ("SPXTST0000049", "RETURN_OVERDUE", "HIGH", "OPEN"),
            ("SPXTST0000052", "PACKED_NOT_HANDED_OVER", "MEDIUM", "OPEN"),
        ],
        "claims": [("LOST_IN_TRANSIT", "CARRIER", "NEW")],
    }
    assert any(line.startswith("= đối soát J-14") for line in lines)

    await seed_returns(db, test_settings, station, sup)  # chạy lại: không nhân đôi

    assert await _state(db) == first
