"""Dữ liệu link chia sẻ (M16): hồ sơ khiếu nại có đủ loại phiên / clip, kho link `MemoryStore` (không
versioning), client API + đăng nhập theo vai.

Hồ sơ `KN` của kiện `SPXTST0000061` (đơn `2410TST00061`, Shopee), bằng chứng đang dùng:
- `pack`: PACK `COMPLETED`, Cam 1 + Cam 2 `READY` (60 giây) + ảnh lúc đóng gói;
- `ret_a`: RETURN `ABANDONED` sớm nhất, có clip + 2 ảnh → phiên chính (BR-39);
- `ret_b`: RETURN `COMPLETED` (120 giây), chỉ Cam 1 `READY` (Cam 2 `FAILED`);
- `wrong`: RETURN `CANCELLED` lý do `WRONG_SCAN` (bị loại), thêm tay + 1 ảnh;
- `review`: RETURN `CANCELLED` `SUPERVISOR` không `cancel_cause` ("Cần soát");
- `failed`: RETURN `CANCELLED` `OTHER`, Cam 1 `FAILED`;
- `removed`: RETURN `ABANDONED` đã **bỏ** khỏi bằng chứng (BR-38) + 1 ảnh.
"""

import hashlib
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings, get_settings
from aicam.modules.claims.models import Claim, ClaimEvidence
from aicam.modules.cloud import config as cloud
from aicam.modules.cloud.store import MemoryStore
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Package
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .conftest import TEST_DATABASE_URL, TEST_REDIS_URL
from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order, pack_session_with_clips, return_session

NOW = datetime(2026, 10, 7, 3, 0, tzinfo=UTC)


@pytest.fixture
def share_store() -> Iterator[MemoryStore]:
    store = MemoryStore("test-share", versioning=False)
    cloud.use_store(cloud.SHARE, store)
    yield store
    cloud.use_store(cloud.SHARE, None)


def make_share_settings(tmp: Path, **kw: object) -> Settings:
    base: dict[str, object] = {
        "app_env": "test",
        "log_json": False,
        "database_url": TEST_DATABASE_URL,
        "redis_url": TEST_REDIS_URL,
        "video_root": tmp / "video",
        "s3_public_endpoint": "https://s3.test.vn",
    }
    base.update(kw)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture
def share_settings(tmp_path: Path) -> Settings:
    return make_share_settings(tmp_path)


@pytest.fixture
async def share_api(
    db: AsyncSession, redis_client: object, share_settings: Settings
) -> AsyncIterator[AsyncClient]:
    from aicam.core.db import get_session
    from aicam.main import create_app

    app = create_app(share_settings)
    app.dependency_overrides[get_session] = lambda: db
    app.dependency_overrides[get_settings] = lambda: share_settings
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver") as client:
        yield client


async def login(api: AsyncClient, db: AsyncSession, role: str = "CSKH") -> tuple[dict[str, str], uuid.UUID]:
    user = await make_user(db, f"tst_{role.lower()}_{uuid.uuid4().hex[:6]}", role, display_name=f"{role} QA")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, user.id


@dataclass
class ShareWorld:
    settings: Settings
    station: Station
    package: Package
    claim: Claim
    pack: PackSession
    ret_a: PackSession
    ret_b: PackSession
    wrong: PackSession
    review: PackSession
    failed: PackSession
    removed: PackSession
    snaps: dict[str, list[Snapshot]] = field(default_factory=dict)  # theo tên phiên
    files: dict[str, bytes] = field(default_factory=dict)  # rel path → nội dung


def _write(settings: Settings, rel: str, content: bytes) -> str:
    path = settings.video_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


async def add_clips(
    db: AsyncSession,
    settings: Settings,
    s: PackSession,
    *,
    cam1: str = "READY",
    cam2: str | None = "READY",
    seconds: int = 60,
) -> dict[str, Clip]:
    out: dict[str, Clip] = {}
    for role, status in (("CAM1", cam1), ("CAM2", cam2)):
        if status is None:
            continue
        rel = f"clips/{s.id}-{role}.mp4"
        sha = _write(settings, rel, f"clip-{s.id}-{role}".encode()) if status == "READY" else None
        clip = Clip(
            session_id=s.id,
            camera_role=role,
            status=status,
            start_at=s.started_at,
            end_at=s.started_at + timedelta(seconds=seconds),
            duration_s=Decimal(seconds),
            path=rel if status in ("READY", "MISSING") else None,
            sha256=sha or ("ee" * 32 if status == "MISSING" else None),
            flags=[],
            deleted_at=NOW - timedelta(days=1) if status == "DELETED" else None,
        )
        db.add(clip)
        out[role] = clip
    await db.flush()
    return out


async def add_snapshot(db: AsyncSession, settings: Settings, s: PackSession, n: int) -> Snapshot:
    rel = f"snapshots/{s.id}-{n}.jpg"
    snap = Snapshot(
        session_id=s.id,
        kind="MANUAL",
        camera_role="CAM1",
        taken_at=s.started_at + timedelta(seconds=10 + n),
        path=rel,
        sha256=_write(settings, rel, f"jpg-{s.id}-{n}".encode()),
        size_bytes=10,
        status="READY",
    )
    db.add(snap)
    await db.flush()
    return snap


async def make_share_world(db: AsyncSession, settings: Settings, n: int = 61) -> ShareWorld:
    clock.freeze(NOW)
    _, station = await make_station_account(db, f"tst_share_st{n}", f"TST Station {n}")
    order, (package,) = await make_order(db, n)
    pack = await pack_session_with_clips(db, station, package, NOW - timedelta(days=6), snapshot=False)
    await db.execute(delete(Clip).where(Clip.session_id == pack.id))
    await add_clips(db, settings, pack)
    case = await buyer_return_case(db, order, n)

    def ret(minutes_ago: int, status: str, **kw: object) -> PackSession:
        s = return_session(station, package, case, status=status, conclusion=None)
        s.started_at = NOW - timedelta(minutes=minutes_ago)
        s.ended_at = s.started_at + timedelta(minutes=2)
        for k, v in kw.items():
            setattr(s, k, v)
        db.add(s)
        return s

    ret_a = ret(300, "ABANDONED")
    ret_b = ret(200, "COMPLETED", inspection_conclusion="EMPTY_BOX")
    wrong = ret(250, "CANCELLED", cancel_reason="WRONG_SCAN")
    review = ret(240, "CANCELLED", cancel_reason="SUPERVISOR")
    failed = ret(230, "CANCELLED", cancel_reason="OTHER")
    removed = ret(220, "ABANDONED")
    await db.flush()
    await add_clips(db, settings, ret_a)
    await add_clips(db, settings, ret_b, cam2="FAILED", seconds=120)
    await add_clips(db, settings, wrong)
    await add_clips(db, settings, review)
    await add_clips(db, settings, failed, cam1="FAILED")
    await add_clips(db, settings, removed)
    snaps = {
        "pack": [],
        "ret_a": [await add_snapshot(db, settings, ret_a, 1), await add_snapshot(db, settings, ret_a, 2)],
        "wrong": [await add_snapshot(db, settings, wrong, 1)],
        "removed": [await add_snapshot(db, settings, removed, 1)],
    }
    claim = Claim(
        package_id=package.id,
        order_id=order.id,
        return_case_id=case.id,
        type="EMPTY_BOX",
        counterparty="PLATFORM",
        source="MANUAL",
    )
    db.add(claim)
    await db.flush()
    for s in (pack, ret_a, ret_b, wrong, review, failed):
        db.add(ClaimEvidence(claim_id=claim.id, kind="SESSION", session_id=s.id, auto=s is not wrong))
    for key in ("ret_a", "wrong"):
        for snap in snaps[key]:
            db.add(ClaimEvidence(claim_id=claim.id, kind="SNAPSHOT", snapshot_id=snap.id, auto=False))
    db.add(
        ClaimEvidence(
            claim_id=claim.id, kind="SESSION", session_id=removed.id, auto=True,
            removed_at=NOW - timedelta(hours=1), removed_reason="Nhầm kiện",
        )
    )  # fmt: skip
    db.add(
        ClaimEvidence(
            claim_id=claim.id, kind="SNAPSHOT", snapshot_id=snaps["removed"][0].id, auto=False,
            removed_at=NOW - timedelta(hours=1), removed_reason="Nhầm kiện",
        )
    )  # fmt: skip
    await db.flush()
    await db.refresh(claim)
    return ShareWorld(
        settings, station, package, claim, pack, ret_a, ret_b, wrong, review, failed, removed, snaps
    )
