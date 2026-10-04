"""BR-02 với hai request thật sự song song (02a §6, §11): advisory lock tuần tự hóa theo station.

Dữ liệu được commit thật (không dùng transaction rollback), dọn bằng TRUNCATE sau test.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core import clock
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings
from aicam.main import create_app
from aicam.modules.orders import service as orders
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

pytestmark = pytest.mark.integration

TABLES = (
    "scan_dedup, session_event, approval_request, clip, export, session, status_history, "
    'order_item, package, "order", camera, station, refresh_token'
)


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings
) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_conc_%'"))
    await dispose_engine()


async def test_parallel_scans_open_exactly_one_session(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    async with sessionmaker()() as db:
        user = User(username="tst_conc_station", display_name="C", role="STATION", password_hash="x")
        db.add(user)
        await db.flush()
        station = Station(name="TST Conc", account_user_id=user.id)
        db.add(station)
        await db.commit()
        station_id, user_id = station.id, user.id

    app = create_app(test_settings)
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()
    token, _ = encode_access_token(test_settings.jwt_secret, user_id, "STATION", station_id, 15)
    headers = {"Authorization": f"Bearer {token}"}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver") as client:

        async def fire(code: str) -> str:
            res = await client.post(
                "/api/v1/station/scan",
                headers=headers,
                json={"code": code, "client_scan_id": str(uuid.uuid4())},
            )
            assert res.status_code == 200, res.text
            return str(res.json()["outcome"])

        outcomes = await asyncio.gather(*(fire(f"SPXTST00000{n:02d}") for n in range(1, 9)))

    async with sessionmaker()() as db:
        active = await db.scalar(
            select(func.count()).select_from(PackSession).where(PackSession.station_id == station_id)
        )
    assert active == 1
    assert sorted(outcomes).count("SESSION_OPENED") == 1
    assert set(outcomes) <= {"SESSION_OPENED", "MISMATCH"}


async def _station(name: str) -> tuple[uuid.UUID, uuid.UUID]:
    async with sessionmaker()() as db:
        user = User(username=f"tst_conc_{name}", display_name=name, role="STATION", password_hash="x")
        db.add(user)
        await db.flush()
        station = Station(name=f"TST {name}", account_user_id=user.id)
        db.add(station)
        await db.commit()
        return station.id, user.id


def _client(test_settings: Settings) -> AsyncClient:
    app = create_app(test_settings)
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()
    return AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver")


def _headers(test_settings: Settings, station_id: uuid.UUID, user_id: uuid.UUID) -> dict[str, str]:
    token, _ = encode_access_token(test_settings.jwt_secret, user_id, "STATION", station_id, 15)
    return {"Authorization": f"Bearer {token}"}


async def _post_scan(client: AsyncClient, headers: dict[str, str], code: str, scan_id: uuid.UUID) -> dict:  # type: ignore[type-arg]
    res = await client.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(scan_id)}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


@pytest.mark.parametrize("code", ["SPXTST0000021", "SPXZZZ9999999"])
async def test_two_stations_same_code_one_opens_other_alerts(
    committed: AsyncEngine, test_settings: Settings, code: str
) -> None:
    """Review M1 #5: hai station quét cùng mã (đơn sàn / mã lạ) cùng lúc → một phiên, bên kia ALERT."""
    a = _headers(test_settings, *await _station("a"))
    b = _headers(test_settings, *await _station("b"))

    async with _client(test_settings) as client:
        results = await asyncio.gather(
            _post_scan(client, a, code, uuid.uuid4()), _post_scan(client, b, code, uuid.uuid4())
        )

    outcomes = sorted(r["outcome"] for r in results)
    assert outcomes == ["ALERT", "SESSION_OPENED"]
    alert = next(r["alert"] for r in results if r["outcome"] == "ALERT")
    assert alert["code"] == "PACKED_ELSEWHERE_IN_PROGRESS"
    async with sessionmaker()() as db:
        assert await db.scalar(select(func.count()).select_from(PackSession)) == 1


async def test_same_client_scan_id_in_parallel_runs_once(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """Review M1 #4: retry chạy chồng với lần gửi đầu → cùng kết quả, chỉ một phiên, một dòng dedup."""
    headers = _headers(test_settings, *await _station("dedup"))
    scan_id = uuid.uuid4()

    async with _client(test_settings) as client:
        results = await asyncio.gather(
            *(_post_scan(client, headers, "SPXTST0000022", scan_id) for _ in range(3))
        )

    assert {r["outcome"] for r in results} == {"SESSION_OPENED"}
    async with sessionmaker()() as db:
        assert await db.scalar(select(func.count()).select_from(PackSession)) == 1
        assert await db.scalar(text("SELECT count(*) FROM scan_dedup")) == 1


async def test_j07_racing_closing_scan_keeps_session_and_package_consistent(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """Review M1 #2: J-07 chạy đúng lúc quét đóng → trạng thái phiên và kiện luôn khớp nhau."""
    ids = await _station("j07")
    async with _client(test_settings) as client:
        for n in range(5):
            code = f"SPXTST00000{23 + n}"
            await _post_scan(client, _headers(test_settings, *ids), code, uuid.uuid4())
            clock.advance(timedelta(minutes=31))
            headers = _headers(test_settings, *ids)  # token cũ đã hết hạn sau khi tua giờ

            async def run_j07() -> None:
                async with sessionmaker()() as db:
                    await sessions.check_timeouts(db, test_settings)

            await asyncio.gather(_post_scan(client, headers, code, uuid.uuid4()), run_j07())

            async with sessionmaker()() as db:
                statuses = (
                    await db.scalars(
                        select(PackSession.status)
                        .where(PackSession.open_code == code)
                        .order_by(PackSession.id.desc())
                    )
                ).all()
                package = await orders.find_package(db, code)
            assert package is not None
            # Quét đóng thắng → COMPLETED. J-07 thắng → ABANDONED rồi lần quét đó mở phiên mới.
            assert (statuses, package.warehouse_status) in (
                (["COMPLETED"], "PACKED"),
                (["OPEN", "ABANDONED"], "PACKING"),
            )
            if statuses[0] == "OPEN":  # dọn để vòng sau bắt đầu từ station rảnh
                async with sessionmaker()() as db:
                    reopened = await db.scalar(select(PackSession.id).where(PackSession.status == "OPEN"))
                res = await client.post(
                    f"/api/v1/station/sessions/{reopened}/cancel",
                    headers=headers,
                    json={"reason": "OUT_OF_STOCK"},
                )
                assert res.status_code == 200, res.text
