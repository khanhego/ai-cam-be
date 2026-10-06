"""BR-42 (L14, FR-08.10, AC-59; T-214): tạo hồ sơ khi hạn sàn đã qua → hạn = lúc tạo + hạn mặc định, nguồn
`DEFAULT_PLATFORM_PASSED`, ghi chú hệ thống; hạn sàn còn → `PLATFORM`; không có hạn sàn → `DEFAULT`."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.deps import Principal
from aicam.modules.claims import service as claims
from aicam.modules.claims.models import ClaimNote
from aicam.modules.claims.schemas import ClaimCreateIn

from .factories import make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order, return_session

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 6, 2, 0, tzinfo=UTC)  # 06/10 09:00 giờ VN
PASSED = datetime(2026, 10, 5, 10, 0, tzinfo=UTC)  # 05/10 17:00 giờ VN
NOTE = "Hạn sàn (05/10 17:00) đã qua khi tạo hồ sơ — dùng hạn mặc định. Kiểm hạn thật trên sàn."


async def _notes(db: AsyncSession, claim_id: object) -> list[str]:
    return list((await db.scalars(select(ClaimNote.text).where(ClaimNote.claim_id == claim_id))).all())


async def test_auto_claim_with_passed_platform_deadline(db: AsyncSession) -> None:
    """BR-42 ví dụ: tạo 06/10 09:00, hạn sàn 05/10 → hạn 13/10 09:00 (mặc định 7 ngày) + ghi chú hệ thống."""
    clock.freeze(NOW)
    _, station = await make_station_account(db)
    order, (package,) = await make_order(db, 41, warehouse_status="RETURN_EXPECTED")
    case = await buyer_return_case(db, order, 41)
    case.seller_due_at = PASSED
    ret = return_session(station, package, case, conclusion="EMPTY_BOX")
    db.add(ret)
    await db.flush()

    created = await claims.create_from_return(db, ret, case)

    assert created is not None
    claim = created.claim
    assert (claim.deadline_at, claim.deadline_source) == (NOW + timedelta(days=7), "DEFAULT_PLATFORM_PASSED")
    assert NOTE in await _notes(db, claim.id)


async def test_manual_claim_deadline_sources(db: AsyncSession) -> None:
    clock.freeze(NOW)
    user = await make_user(db, "tst_cskh_br42", "CSKH")
    p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)
    order, (package,) = await make_order(db, 44, warehouse_status="RETURN_EXPECTED")
    case = await buyer_return_case(db, order, 44)

    case.seller_due_at = NOW + timedelta(hours=30)
    future = await claims.create_manual(
        db,
        ClaimCreateIn(package_id=package.id, type="DAMAGED", counterparty="PLATFORM", return_case_id=case.id),
        p,
    )
    assert (future.deadline_at, future.deadline_source) == (NOW + timedelta(hours=30), "PLATFORM")
    assert NOTE not in await _notes(db, future.id)

    case.seller_due_at = PASSED
    passed = await claims.create_manual(
        db,
        ClaimCreateIn(
            package_id=package.id, type="WRONG_ITEM", counterparty="PLATFORM", return_case_id=case.id
        ),
        p,
    )
    assert (passed.deadline_at, passed.deadline_source) == (
        NOW + timedelta(days=7),
        "DEFAULT_PLATFORM_PASSED",
    )
    assert NOTE in await _notes(db, passed.id)

    _, (plain,) = await make_order(db, 45)
    default = await claims.create_manual(
        db, ClaimCreateIn(package_id=plain.id, type="OTHER", counterparty="CARRIER"), p
    )
    assert default.deadline_source == "DEFAULT"
