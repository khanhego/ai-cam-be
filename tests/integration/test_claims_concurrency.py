"""Đồng thời thật (2 connection, dữ liệu commit thật, TRUNCATE sau) cho hồ sơ khiếu nại.

- TC-08.10 (BR-27): hai API-131 song song cùng (kiện, loại) → một tạo được, một `CLAIM_EXISTS`.
- TC-02.43 (DEC-251, R-7): tạo hồ sơ ∥ J-02 → không có clip bị xóa sau khi đã là bằng chứng.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core import clock
from aicam.core.db import commit, dispose_engine, init_engine, sessionmaker
from aicam.core.deps import Principal
from aicam.core.errors import AppError
from aicam.core.settings import Settings
from aicam.modules.claims import service as claims
from aicam.modules.claims import views as claim_views
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession

from .factories import make_station_account, make_user

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


# ---------------------------------------------------------------- tạo hồ sơ ∥ J-02 (DEC-251, R-7, TC-02.43)

OLD = T0 - timedelta(days=100)


async def _old_packed(db: Any, settings: Settings, n: int) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID, Path]:
    """Kiện + phiên PACK đóng 100 ngày trước + clip Cam 1 READY có file (quá hạn giữ 90 ngày)."""
    _, station = await make_station_account(db, f"tst_cc_st{n}", f"TST CC {n}")
    package = Package(tracking_number=f"SPXTSTCC0{n:04d}", warehouse_status="DELIVERED")
    db.add(package)
    await db.flush()
    pack = PackSession(
        type="PACK", package_id=package.id, station_id=station.id, status="COMPLETED",
        started_at=OLD - timedelta(minutes=1), ended_at=OLD, open_code=package.tracking_number,
    )  # fmt: skip
    db.add(pack)
    await db.flush()
    rel = f"clips/{pack.id}-CAM1.mp4"
    path = settings.video_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00" * 64)
    clip = Clip(session_id=pack.id, camera_role="CAM1", status="READY", start_at=OLD - timedelta(minutes=1),
                end_at=OLD, path=rel, sha256="ab" * 32, flags=[])  # fmt: skip
    db.add(clip)
    await db.flush()
    return package.id, pack.id, clip.id, path


async def _cskh(db: Any, name: str) -> Principal:
    user = await make_user(db, name, "CSKH")
    return Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)


async def test_claim_locks_clip_before_retention(
    committed: AsyncEngine, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hồ sơ khóa clip trước (chưa commit) → J-02 (đã chọn clip làm ứng viên) phải chờ, kiểm lại dưới khóa
    thấy bằng chứng → bỏ qua; clip READY, file còn."""
    async with sessionmaker()() as db:
        package_id, _, clip_id, path = await _old_packed(db, test_settings, 1)
        p = await _cskh(db, "tst_cc_a")
        await db.commit()
    clock.freeze(T0)
    locked = asyncio.Event()
    release = asyncio.Event()
    original = media.retention_clip_candidates
    creator: asyncio.Task[Any] | None = None

    async def create_claim() -> None:
        async with sessionmaker()() as db:
            await claims.create_manual(
                db, ClaimCreateIn(package_id=package_id, type="BUYER_CLAIM", counterparty="PLATFORM"), p
            )  # khóa clip của phiên PACK hiệu lực (DEC-251)
            locked.set()
            await release.wait()
            await commit(db)

    async def candidates_then_claim(*args: Any, **kwargs: Any) -> list[Any]:
        nonlocal creator
        result = await original(*args, **kwargs)
        assert clip_id in result
        creator = asyncio.create_task(create_claim())
        await asyncio.wait_for(locked.wait(), timeout=10)
        return result

    monkeypatch.setattr(media, "retention_clip_candidates", candidates_then_claim)

    async def run_j02() -> dict[str, int]:
        async with sessionmaker()() as db:
            return await media.enforce_retention(db, test_settings)

    j02 = asyncio.create_task(run_j02())
    await asyncio.wait_for(locked.wait(), timeout=10)
    await asyncio.sleep(0.3)
    assert not j02.done()  # J-02 chờ khóa dòng clip
    release.set()
    out = await asyncio.wait_for(j02, timeout=15)
    assert creator is not None
    await creator

    assert out["clips"] == 0
    async with sessionmaker()() as db:
        clip = await db.get(Clip, clip_id)
        assert clip is not None
        assert clip.status == "READY"
    assert path.exists()


async def test_retention_deletes_first_then_claim_reports_missing(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """J-02 khóa + xóa clip trước (chưa commit) → tạo hồ sơ chờ khóa clip, sau đó thấy clip `DELETED`: hồ sơ
    ghi `PACK_CLIP_DELETED`; không có clip nào bị xóa **sau** khi đã là bằng chứng."""
    async with sessionmaker()() as db:
        package_id, _, clip_id, _ = await _old_packed(db, test_settings, 2)
        p = await _cskh(db, "tst_cc_b")
        await db.commit()
    clock.freeze(T0)

    async with sessionmaker()() as j02:
        clip = await j02.scalar(select(Clip).where(Clip.id == clip_id).with_for_update())
        assert clip is not None
        clip.status, clip.deleted_at = "DELETED", clock.now()
        await j02.flush()

        async def create() -> uuid.UUID:
            async with sessionmaker()() as db:
                claim = await claims.create_manual(
                    db, ClaimCreateIn(package_id=package_id, type="BUYER_CLAIM", counterparty="PLATFORM"), p
                )
                await commit(db)
                return claim.id

        task = asyncio.create_task(create())
        await asyncio.sleep(0.3)
        assert not task.done()  # tạo hồ sơ chờ khóa clip
        await j02.commit()
    claim_id = await asyncio.wait_for(task, timeout=10)

    async with sessionmaker()() as db:
        detail = await claim_views.claim_detail(db, claim_id, p.user_id, test_settings)
        assert detail.missing == ["PACK_CLIP_DELETED"]
        evidence = await db.scalar(select(ClaimEvidence).where(ClaimEvidence.claim_id == claim_id))
        clip = await db.get(Clip, clip_id)
        assert evidence is not None
        assert clip is not None
        assert clip.deleted_at is not None
        assert clip.deleted_at <= evidence.added_at


async def test_parallel_claim_and_retention_invariant(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """Chạy song song thật (2 connection) 5 lần: tạo hồ sơ ∥ J-02 trên clip vừa quá hạn. Bất biến: clip còn
    READY và là bằng chứng, hoặc đã bị xóa trước khi thành bằng chứng (hồ sơ ghi `PACK_CLIP_DELETED`)."""
    async with sessionmaker()() as db:
        cases = [await _old_packed(db, test_settings, 10 + i) for i in range(5)]
        p = await _cskh(db, "tst_cc_c")
        await db.commit()
    clock.freeze(T0)

    for package_id, _, clip_id, _ in cases:

        async def create(pid: uuid.UUID = package_id) -> uuid.UUID:
            async with sessionmaker()() as db:
                claim = await claims.create_manual(
                    db, ClaimCreateIn(package_id=pid, type="BUYER_CLAIM", counterparty="PLATFORM"), p
                )
                await commit(db)
                return claim.id

        async def j02() -> dict[str, int]:
            async with sessionmaker()() as db:
                return await media.enforce_retention(db, test_settings)

        claim_id, _ = await asyncio.wait_for(asyncio.gather(create(), j02()), timeout=20)
        async with sessionmaker()() as db:
            clip = await db.get(Clip, clip_id)
            evidence = await db.scalar(select(ClaimEvidence).where(ClaimEvidence.claim_id == claim_id))
            detail = await claim_views.claim_detail(db, claim_id, p.user_id, test_settings)
            assert clip is not None
            assert evidence is not None
            if clip.status == "DELETED":
                assert clip.deleted_at is not None
                assert clip.deleted_at <= evidence.added_at
                assert detail.missing == ["PACK_CLIP_DELETED"]
            else:
                assert clip.status == "READY"
                assert detail.missing == []
    async with sessionmaker()() as db:  # hồ sơ mở → J-02 sau đó không xóa clip nào đã là bằng chứng
        assert (await media.enforce_retention(db, test_settings))["clips"] == 0
