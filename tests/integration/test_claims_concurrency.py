"""Đồng thời thật (2 connection, dữ liệu commit thật, TRUNCATE sau) cho hồ sơ khiếu nại.

- TC-08.10 (BR-27): hai API-131 song song cùng (kiện, loại) → một tạo được, một `CLAIM_EXISTS`.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core import clock
from aicam.core.db import commit, dispose_engine, init_engine, sessionmaker
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.claims import service as claims
from aicam.modules.claims.models import Claim
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.orders.models import Package

from .factories import make_user

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)
TABLES = (
    "claim_note, claim_evidence, evidence_pack, claim, recon_alert, snapshot, clip, session_event, session, "
    'return_case_package, return_case, status_history, order_item, package, "order", station'
)


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings, tmp_path: Path
) -> AsyncIterator[AsyncEngine]:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    engine = init_engine(migrated_database_url)
    yield engine
    clock.reset()
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_cc_%'"))
    await dispose_engine()


async def test_parallel_create_same_type(committed: AsyncEngine) -> None:
    """TC-08.10: 2 request song song → 1 tạo, 1 `CLAIM_EXISTS` (kèm mã hồ sơ đã có)."""
    async with sessionmaker()() as db:
        user = await make_user(db, "tst_cc_cskh", "CSKH")
        package = Package(tracking_number="SPXTSTCC00001", warehouse_status="DELIVERED")
        db.add(package)
        await db.commit()
        package_id, user_id = package.id, user.id
    p = Principal(user_id=user_id, role="CSKH", station_id=None, ip=None)
    body = ClaimCreateIn(package_id=package_id, type="BUYER_CLAIM", counterparty="PLATFORM")
    gate = asyncio.Event()

    async def create() -> str:
        async with sessionmaker()() as db:
            await gate.wait()
            try:
                claim = await claims.create_manual(db, body, p)
                await asyncio.sleep(0.2)  # giữ transaction để bên kia phải chờ khóa
                await commit(db)
                return claim.code
            except AppError as exc:
                await db.rollback()
                return f"{exc.code}:{exc.details.get('code')}"

    tasks = [asyncio.create_task(create()) for _ in range(2)]
    gate.set()
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=15)

    codes = [r for r in results if r.startswith("KN-")]
    assert len(codes) == 1
    assert sorted(results) == sorted([codes[0], f"CLAIM_EXISTS:{codes[0]}"])
    async with sessionmaker()() as db:
        assert await db.scalar(select(func.count()).select_from(Claim)) == 1
