"""J-02 bảo vệ bằng chứng theo hồ sơ (T-111; ADR-009, BR-09 a–d, BR-25, DEC-245, DEC-268), API-31.

TC-02.30..34, TC-R.04, AC-26: đồng hồ giả lập; clip / ảnh có file thật trong `VIDEO_ROOT` tạm.
"""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.deps import Principal
from aicam.core.settings import Settings
from aicam.modules.claims import service as claims
from aicam.modules.claims.models import Claim
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip, Snapshot
from aicam.modules.orders.models import Package
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.settings.models import Setting
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import (
    buyer_return_case,
    make_desk,
    make_order,
    pack_session_with_clips,
    platform_return,
    return_session,
)

pytestmark = pytest.mark.integration

T0 = datetime(2026, 1, 1, 3, 0, tzinfo=UTC)


@pytest.fixture
def media_settings(test_settings: Settings, tmp_path: Path) -> Settings:
    test_settings.video_root = tmp_path / "video"
    test_settings.video_root.mkdir()
    return test_settings


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


async def _retention(db: AsyncSession, days: int) -> None:
    await db.execute(
        update(Setting).where(Setting.id == 1).values(retention_raw_days=7, retention_clip_days=days)
    )


async def _packed(
    db: AsyncSession, settings: Settings, station: Station, package: Package, ended: datetime
) -> tuple[PackSession, list[Path]]:
    """Phiên PACK đã đóng + clip Cam 1 / Cam 2 + ảnh lúc đóng gói, có file thật."""
    pack = await pack_session_with_clips(db, station, package, ended)
    files = []
    rels = [c.path for c in (await db.scalars(select(Clip).where(Clip.session_id == pack.id))).all()]
    rels += [s.path for s in (await db.scalars(select(Snapshot).where(Snapshot.session_id == pack.id))).all()]
    for rel in rels:
        assert rel is not None
        path = settings.video_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"\x00" * 16)
        files.append(path)
    return pack, files


async def _states(db: AsyncSession, session_id: uuid.UUID) -> list[str]:
    clips = (
        await db.scalars(select(Clip).where(Clip.session_id == session_id).order_by(Clip.camera_role))
    ).all()
    snaps = (await db.scalars(select(Snapshot).where(Snapshot.session_id == session_id))).all()
    for row in (*clips, *snaps):
        await db.refresh(row)
    return [c.status for c in clips] + [s.status for s in snaps]


async def _staff(api: AsyncClient, db: AsyncSession, username: str = "tst_cskh_rc") -> dict[str, str]:
    from aicam.modules.users.models import User

    if await db.scalar(select(User.id).where(User.username == username)) is None:
        await make_user(db, username, "CSKH")
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _clips_of(
    api: AsyncClient, headers: dict[str, str], package_id: uuid.UUID, session_id: uuid.UUID
) -> Any:
    detail = (await api.get(f"/api/v1/packages/{package_id}", headers=headers)).json()
    session = next(s for s in detail["sessions"] if s["id"] == str(session_id))
    return session


async def _manual_claim(db: AsyncSession, package: Package, claim_type: str = "BUYER_CLAIM") -> Claim:
    user = await make_user(db, f"tst_rc_{uuid.uuid4().hex[:6]}", "CSKH")
    p = Principal(user_id=user.id, role="CSKH", station_id=None, ip=None)
    return await claims.create_manual(
        db, ClaimCreateIn(package_id=package.id, type=claim_type, counterparty="PLATFORM"), p
    )


# ---------------------------------------------------------------- (a) hồ sơ khiếu nại


async def test_open_claim_keeps_evidence_200_days(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-02.30, AC-26: clip + ảnh gắn hồ sơ `NEW` còn sau +200 ngày; API-31 `protection = CLAIM`, hạn null.
    TC-R.04: clip không thuộc hồ sơ / hàng hoàn bị xóa như cũ."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    _, (package,) = await make_order(db, 11)
    pack, files = await _packed(db, media_settings, station, package, T0)
    _, (other,) = await make_order(db, 12)
    loose, loose_files = await _packed(db, media_settings, station, other, T0)
    claim = await _manual_claim(db, package)

    clock.advance(timedelta(days=200))
    out = await media.enforce_retention(db, media_settings)

    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    assert all(f.exists() for f in files)
    assert await _states(db, loose.id) == ["DELETED", "DELETED", "DELETED"]
    assert not any(f.exists() for f in loose_files)
    assert (out["clips"], out["snapshots"]) == (2, 1)
    session = await _clips_of(api, await _staff(api, db), package.id, pack.id)
    assert session["protected_by_claims"] == [{"id": str(claim.id), "code": claim.code}]
    for clip in session["clips"]:
        assert clip["protection"] == {
            "reasons": ["CLAIM"],
            "claims": [claim.code],
            "return_cases": [],
            "until": None,
        }
        assert (clip["protected_by_claim"], clip["retention_until"]) == (True, None)


async def test_closed_claim_retention_from_close_date(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-02.31, BR-09: clip 01/01, hồ sơ đóng 15/05, giữ 90 ngày → còn ngày 12/08 (`retention_until` 13/08),
    bị xóa ngày 14/08."""
    clock.freeze(T0)
    _, station = await make_station_account(db)
    _, (package,) = await make_order(db, 11)
    pack, files = await _packed(db, media_settings, station, package, T0)
    claim = await _manual_claim(db, package)
    closed_at = datetime(2026, 5, 15, 3, 0, tzinfo=UTC)
    clock.freeze(closed_at)
    claim.status, claim.closed_at = "CLOSED", closed_at
    await db.flush()

    clock.freeze(datetime(2026, 8, 12, 3, 0, tzinfo=UTC))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    session = await _clips_of(api, await _staff(api, db), package.id, pack.id)
    assert {c["retention_until"] for c in session["clips"]} == {"2026-08-13T03:00:00Z"}
    assert all(c["protection"] is None for c in session["clips"])

    clock.freeze(datetime(2026, 8, 14, 3, 0, tzinfo=UTC))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["DELETED", "DELETED", "DELETED"]
    assert not any(f.exists() for f in files)


# ---------------------------------------------------------------- (b) hồ sơ hàng hoàn


async def test_return_overdue_62_days_keeps_pack_clip_then_claim(
    api: AsyncClient, db: AsyncSession, media_settings: Settings, adapter: MockAdapter
) -> None:
    """AC-26, TC-02.32 (BR-09 b, R-1): retention 60; kiện hoàn quá hạn về sau 62 ngày → clip + ảnh đóng gói
    còn (`protection = RETURN_CASE`); kiện về, kết luận "Hộp rỗng" → hồ sơ khiếu nại có clip đóng gói
    READY."""
    clock.freeze(T0)
    await _retention(db, 60)
    _, station = await make_station_account(db, "tst_pack_rc", "TST Pack RC")
    order, (package,) = await make_order(db, 41)
    pack, files = await _packed(db, media_settings, station, package, T0)
    clock.advance(timedelta(days=1))
    case = await buyer_return_case(db, order, 41)
    clock.advance(timedelta(days=8))
    package.warehouse_status = "RETURN_MISSING"  # J-14 BR-12 (T-113)
    case.status = "MISSING"
    await db.flush()

    clock.advance(timedelta(days=53))  # 62 ngày sau khi đóng gói
    await media.enforce_retention(db, media_settings)

    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    assert all(f.exists() for f in files)
    session = await _clips_of(api, await _staff(api, db), package.id, pack.id)
    for clip in session["clips"]:
        assert clip["protection"] == {
            "reasons": ["RETURN_CASE"], "claims": [], "return_cases": [case.code], "until": None
        }  # fmt: skip
        assert (clip["protected_by_claim"], clip["retention_until"]) == (False, None)

    desk = await make_desk(api, db)
    opened = (await desk.scan("SPXRTTST000041")).json()
    assert opened["outcome"] == "SESSION_OPENED", opened
    lines = [
        {
            "order_item_id": line["order_item_id"],
            "quantity_received": 0,
            "condition": "MISSING_ITEM",
            "note": None,
        }
        for line in opened["state"]["session"]["inspection"]["lines"]
    ]
    await desk.api.put(
        f"/api/v1/station/sessions/{opened['state']['session']['id']}/inspection",
        headers=desk.headers,
        json={"conclusion": "EMPTY_BOX", "note": "", "lines": lines},
    )
    closed = (await desk.scan("SPXTST0000041")).json()
    code = closed["closed_session"]["claim_code"]

    claim = await db.scalar(select(Claim).where(Claim.code == code))
    assert claim is not None
    headers = await _staff(api, db)
    detail = (await api.get(f"/api/v1/claims/{claim.id}", headers=headers)).json()
    pack_evidence = next(
        e["session"] for e in detail["evidence"] if e.get("session", {}) and e["session"]["type"] == "PACK"
    )
    assert pack_evidence["id"] == str(pack.id)
    assert {c["status"] for c in pack_evidence["clips"]} == {"READY"}
    assert "PACK_CLIP_DELETED" not in detail["missing"]

    clock.advance(timedelta(days=200))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]


async def _received(
    db: AsyncSession, station: Station, n: int, settings: Settings, packed_at: datetime
) -> tuple[ReturnCase, Package, PackSession, PackSession, list[Path]]:
    """Kiện `n` đóng gói lúc `packed_at`, hồ sơ "Khách trả hàng", vừa nhận "Nguyên vẹn" lúc hiện tại."""
    order, (package,) = await make_order(db, n)
    saved = clock.now()
    clock.freeze(packed_at)
    pack, files = await _packed(db, settings, station, package, packed_at)
    clock.freeze(saved)
    case = await buyer_return_case(db, order, n)
    ret = return_session(station, package, case, conclusion="OK")
    db.add(ret)
    package.warehouse_status = "RETURN_RECEIVED_OK"
    case.single_session = True
    await db.flush()
    await returns.recompute(db, case)
    await db.flush()
    assert (case.status, case.received_at) == ("RECEIVED_OK", clock.now())
    return case, package, pack, ret, files


async def test_received_keeps_7_days_then_correction_claim(
    api: AsyncClient, db: AsyncSession, media_settings: Settings
) -> None:
    """TC-02.33, DEC-268, AC-26: clip đóng gói quá tuổi, nhận "Nguyên vẹn" → còn thêm 7 ngày
    (`protection.until` = nhận + 7 ngày); ngày thứ 5 sửa thành "Hộp rỗng" (API-113 gọi `create_from_return`) →
    hồ sơ khiếu nại có clip đóng gói READY và giữ tiếp; kiện không sửa → bị xóa sau ngày thứ 7."""
    await _retention(db, 60)
    t_receive = T0 + timedelta(days=61)
    clock.freeze(t_receive)
    _, station = await make_station_account(db)
    case, package, pack, ret, _ = await _received(db, station, 41, media_settings, T0)
    _, _, plain_pack, _, plain_files = await _received(db, station, 42, media_settings, T0)

    clock.advance(timedelta(days=5))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    assert await _states(db, plain_pack.id) == ["READY", "READY", "READY"]
    session = await _clips_of(api, await _staff(api, db), package.id, pack.id)
    until = clock.iso_z(t_receive + timedelta(days=7))
    for clip in session["clips"]:
        assert clip["protection"]["reasons"] == ["RETURN_CASE"]
        assert (clip["protection"]["until"], clip["retention_until"]) == (until, until)

    # API-113 OK → ISSUE (T-115) dùng `claims.create_from_return` trong cửa sổ 7 ngày.
    ret.inspection_conclusion = "EMPTY_BOX"
    created = await claims.create_from_return(db, ret, case)
    assert created is not None
    detail = (await api.get(f"/api/v1/claims/{created.claim.id}", headers=await _staff(api, db))).json()
    pack_evidence = next(
        e["session"] for e in detail["evidence"] if e["kind"] == "SESSION" and e["session"]["type"] == "PACK"
    )
    assert {c["status"] for c in pack_evidence["clips"]} == {"READY"}

    clock.advance(timedelta(days=3))  # 8 ngày sau khi nhận
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]  # hồ sơ khiếu nại mở
    assert await _states(db, plain_pack.id) == ["DELETED", "DELETED", "DELETED"]
    assert not any(f.exists() for f in plain_files)


# ---------------------------------------------------------------- (c) chỉ hoàn tiền


async def test_no_parcel_keeps_30_days(api: AsyncClient, db: AsyncSession, media_settings: Settings) -> None:
    """TC-02.34, BR-09 c: hồ sơ "Chỉ hoàn tiền" giữ clip đóng gói 30 ngày từ lúc sàn báo (`until`)."""
    await _retention(db, 60)
    clock.freeze(T0)
    _, station = await make_station_account(db)
    order, (package,) = await make_order(db, 44)
    pack, _ = await _packed(db, media_settings, station, package, T0)
    clock.advance(timedelta(days=40))
    reported = clock.now()
    result = await returns.attach_or_create(
        db,
        order,
        returns.Signal(
            returns.SIGNAL_PLATFORM_RETURN, key="RETURN:44", ret=platform_return(44, needs_parcel=False)
        ),
    )
    assert result.case is not None
    assert result.case.status == "NO_PARCEL"
    await db.flush()

    clock.advance(timedelta(days=29))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["READY", "READY", "READY"]
    session = await _clips_of(api, await _staff(api, db), package.id, pack.id)
    assert {c["protection"]["until"] for c in session["clips"]} == {
        clock.iso_z(reported + timedelta(days=30))
    }

    clock.advance(timedelta(days=2))
    await media.enforce_retention(db, media_settings)
    assert await _states(db, pack.id) == ["DELETED", "DELETED", "DELETED"]


# ---------------------------------------------------------------- sàn retention, ảnh là bằng chứng


async def test_floor_and_snapshot_evidence(db: AsyncSession, media_settings: Settings) -> None:
    """BR-25 / DEC-257: setting 45 < sàn 60 → J-02 dùng 60 (clip 50 ngày còn, 61 ngày xóa). Ảnh tự là bằng
    chứng của hồ sơ mở → còn dù phiên không được bảo vệ."""
    clock.freeze(T0)
    await _retention(db, 45)
    _, station = await make_station_account(db)
    _, (young_pkg,) = await make_order(db, 21)
    young, _ = await _packed(db, media_settings, station, young_pkg, T0 - timedelta(days=50))
    _, (old_pkg,) = await make_order(db, 22)
    old, _ = await _packed(db, media_settings, station, old_pkg, T0 - timedelta(days=61))
    snap = await db.scalar(select(Snapshot).where(Snapshot.session_id == old.id))
    assert snap is not None
    claim = Claim(package_id=old_pkg.id, type="OTHER", counterparty="PLATFORM", status="NEW", source="MANUAL")
    db.add(claim)
    await db.flush()
    await claims.add_evidence(db, claim, [], [snap.id], auto=False, added_by=None)

    out = await media.enforce_retention(db, media_settings)

    assert await _states(db, young.id) == ["READY", "READY", "READY"]
    assert await _states(db, old.id) == ["DELETED", "DELETED", "READY"]
    assert (out["clips"], out["snapshots"]) == (2, 0)
