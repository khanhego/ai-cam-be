"""Migration 0001 và ràng buộc DB (02a §3, §5, §6)."""

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit
from aicam.core.db import after_commit, commit, rollback
from aicam.core.security import hash_password
from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.settings.models import Setting
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

pytestmark = pytest.mark.integration


async def _station(db: AsyncSession, name: str = "TST Station 01") -> Station:
    station = Station(name=name)
    db.add(station)
    await db.flush()
    return station


async def _package(db: AsyncSession, code: str) -> Package:
    package = Package(tracking_number=code)
    db.add(package)
    await db.flush()
    return package


async def test_setting_row_is_seeded(db: AsyncSession) -> None:
    setting = await db.get(Setting, 1)

    assert setting is not None
    assert (setting.retention_raw_days, setting.retention_clip_days) == (30, 90)
    assert (setting.session_warn_minutes, setting.session_abandon_minutes) == (15, 30)


async def test_audit_log_is_insert_only(db: AsyncSession) -> None:
    audit.record(db, "LOGIN", user_id=None, object_type="USER", object_id="x")
    await db.flush()

    with pytest.raises(DBAPIError, match="chỉ cho phép INSERT"):
        async with db.begin_nested():
            await db.execute(text("UPDATE audit_log SET action = 'X'"))
    with pytest.raises(DBAPIError, match="chỉ cho phép INSERT"):
        async with db.begin_nested():
            await db.execute(text("DELETE FROM audit_log"))


def test_audit_rejects_unknown_action() -> None:
    with pytest.raises(ValueError, match="không hợp lệ"):
        audit.record(None, "HACK", user_id=None)  # type: ignore[arg-type]


async def test_one_active_session_per_station(db: AsyncSession) -> None:
    """BR-02 ở mức DB."""
    station = await _station(db)
    p1, p2 = await _package(db, "SPXTST0000001"), await _package(db, "SPXTST0000002")
    db.add(PackSession(package_id=p1.id, station_id=station.id, status="OPEN", open_code="SPXTST0000001"))
    await db.flush()

    db.add(PackSession(package_id=p2.id, station_id=station.id, status="MISMATCH", open_code="SPXTST0000002"))
    with pytest.raises(IntegrityError, match="uq_session_active_station"):
        await db.flush()


async def test_finished_sessions_do_not_block_station(db: AsyncSession) -> None:
    station = await _station(db)
    p1, p2 = await _package(db, "SPXTST0000011"), await _package(db, "SPXTST0000012")
    db.add(
        PackSession(package_id=p1.id, station_id=station.id, status="COMPLETED", open_code="SPXTST0000011")
    )
    db.add(PackSession(package_id=p2.id, station_id=station.id, status="OPEN", open_code="SPXTST0000012"))

    await db.flush()

    count = await db.scalar(
        select(text("count(*)")).select_from(PackSession).where(PackSession.station_id == station.id)
    )
    assert count == 2


async def test_package_open_at_two_stations_is_rejected(db: AsyncSession) -> None:
    s1, s2 = await _station(db, "TST Station 01"), await _station(db, "TST Station 02")
    package = await _package(db, "SPXTST0000013")
    db.add(PackSession(package_id=package.id, station_id=s1.id, status="OPEN", open_code="SPXTST0000013"))
    await db.flush()

    db.add(PackSession(package_id=package.id, station_id=s2.id, status="OPEN", open_code="SPXTST0000013"))
    with pytest.raises(IntegrityError, match="uq_session_active_package"):
        await db.flush()


async def test_tracking_number_unique_case_insensitive(db: AsyncSession) -> None:
    await _package(db, "SPXTST0000020")

    db.add(Package(tracking_number="spxtst0000020"))
    with pytest.raises(IntegrityError, match="uq_package_tracking_number_upper"):
        await db.flush()


async def test_warehouse_status_check(db: AsyncSession) -> None:
    db.add(Package(tracking_number="SPXTST0000030", warehouse_status="MISMATCH"))

    with pytest.raises(IntegrityError, match="warehouse_status_enum"):
        await db.flush()


async def test_username_unique_case_insensitive(db: AsyncSession) -> None:
    db.add(
        User(username="tst_admin", display_name="A", role="ADMIN", password_hash=hash_password("12345678"))
    )
    await db.flush()

    db.add(User(username="TST_ADMIN", display_name="B", role="ADMIN", password_hash="x"))
    with pytest.raises(IntegrityError, match="uq_user_username_lower"):
        await db.flush()


async def test_after_commit_runs_only_after_successful_commit(db: AsyncSession) -> None:
    calls: list[str] = []

    async def publish() -> None:
        calls.append("published")

    # Luồng thật: có ghi dữ liệu, lỗi → rollback → callback bị bỏ.
    await _station(db, "TST Station rollback")
    after_commit(db, publish)
    await db.rollback()
    assert calls == []

    # Helper rollback bỏ callback cả khi chưa có transaction.
    after_commit(db, publish)
    await rollback(db)
    assert calls == []

    after_commit(db, publish)
    await commit(db)
    assert calls == ["published"]
