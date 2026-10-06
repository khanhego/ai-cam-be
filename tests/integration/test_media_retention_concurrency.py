"""G3-F14: API-42 giữ clip đua với J-02 song song thật (2 connection, dữ liệu commit thật, TRUNCATE sau).

J-02 đã lấy clip vào danh sách ứng viên; API-42 khóa dòng + giữ, chưa commit; J-02 khóa dòng → phải chờ,
đọc lại thấy `held` → bỏ qua (BR-09). File clip còn nguyên.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core import clock
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.deps import Principal
from aicam.core.settings import Settings
from aicam.modules.media import service as media
from aicam.modules.media.models import Clip

from .factories import make_station_account, make_user
from .media_fixtures import make_closed_session

pytestmark = pytest.mark.integration

T0 = datetime(2026, 10, 5, 3, 0, tzinfo=UTC)
TABLES = "clip, session, status_history, package, station"


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
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_rtc_%'"))
    await dispose_engine()


async def test_hold_during_retention_wins(
    committed: AsyncEngine, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    ended = T0 - timedelta(days=100)
    async with sessionmaker()() as db:
        cskh = await make_user(db, "tst_rtc_cskh", "CSKH")
        _, station = await make_station_account(db, "tst_rtc_station", "TST RTC")
        pack = await make_closed_session(db, station, "SPXTSTRTC0001", ended - timedelta(minutes=1), ended)
        rel = f"clips/{pack.id}-CAM1.mp4"
        (test_settings.video_root / "clips").mkdir()
        (test_settings.video_root / rel).write_bytes(b"\x00" * 64)
        clip = Clip(session_id=pack.id, camera_role="CAM1", status="READY", start_at=ended - timedelta(
            minutes=1), end_at=ended, path=rel, sha256="ab" * 32, flags=[])  # fmt: skip
        db.add(clip)
        await db.commit()
        clip_id, user_id = clip.id, cskh.id
    clock.freeze(T0)

    holder_locked = asyncio.Event()
    release_holder = asyncio.Event()
    original = media.retention_clip_candidates

    async def hold_in_other_connection() -> None:
        async with sessionmaker()() as db2:
            locked = await db2.scalar(select(Clip).where(Clip.id == clip_id).with_for_update())
            assert locked is not None
            locked.held, locked.held_by, locked.held_at = True, user_id, clock.now()
            await db2.flush()
            holder_locked.set()
            await release_holder.wait()
            await db2.commit()

    holder: asyncio.Task[Any] | None = None

    async def candidates_then_hold(*args: Any, **kwargs: Any) -> list[Any]:
        nonlocal holder
        result = await original(*args, **kwargs)
        assert clip_id in result  # J-02 đã chọn clip làm ứng viên
        holder = asyncio.create_task(hold_in_other_connection())
        await asyncio.wait_for(holder_locked.wait(), timeout=10)
        return result

    monkeypatch.setattr(media, "retention_clip_candidates", candidates_then_hold)

    async def run_j02() -> dict[str, int]:
        async with sessionmaker()() as db:
            return await media.enforce_retention(db, test_settings)

    j02 = asyncio.create_task(run_j02())
    await asyncio.wait_for(holder_locked.wait(), timeout=10)
    await asyncio.sleep(0.3)
    assert not j02.done()  # J-02 đang chờ khóa dòng mà API-42 giữ
    release_holder.set()
    out = await asyncio.wait_for(j02, timeout=10)
    assert holder is not None
    await holder

    assert out["clips"] == 0
    async with sessionmaker()() as db:
        reloaded = await db.get(Clip, clip_id)
        assert reloaded is not None
        assert (reloaded.status, reloaded.held) == ("READY", True)
    assert (test_settings.video_root / rel).exists()


async def test_set_hold_uses_row_lock(committed: AsyncEngine, test_settings: Settings) -> None:
    """Chiều ngược lại: J-02 đã khóa + xóa clip trước → API-42 chờ rồi trả 410 CLIP_DELETED."""
    ended = T0 - timedelta(days=100)
    async with sessionmaker()() as db:
        cskh = await make_user(db, "tst_rtc_cskh2", "CSKH")
        _, station = await make_station_account(db, "tst_rtc_station2", "TST RTC 2")
        pack = await make_closed_session(db, station, "SPXTSTRTC0002", ended - timedelta(minutes=1), ended)
        clip = Clip(session_id=pack.id, camera_role="CAM1", status="READY", start_at=ended - timedelta(
            minutes=1), end_at=ended, path=None, sha256="ab" * 32, flags=[])  # fmt: skip
        db.add(clip)
        await db.commit()
        clip_id, user_id = clip.id, cskh.id

    from aicam.core.errors import AppError

    async with sessionmaker()() as j02:
        locked = await j02.scalar(select(Clip).where(Clip.id == clip_id).with_for_update())
        assert locked is not None
        locked.status, locked.deleted_at = "DELETED", clock.now()
        await j02.flush()

        async def hold() -> Any:
            async with sessionmaker()() as api_db:
                p = Principal(user_id=user_id, role="ADMIN", station_id=None, ip=None)
                return await media.set_hold(api_db, clip_id, True, p, test_settings)

        api_call = asyncio.create_task(hold())
        await asyncio.sleep(0.3)
        assert not api_call.done()
        await j02.commit()
    with pytest.raises(AppError) as err:
        await asyncio.wait_for(api_call, timeout=10)
    assert err.value.code == "CLIP_DELETED"
