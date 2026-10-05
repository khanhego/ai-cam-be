"""API-30 tra cứu, API-31 chi tiết kiện (FR-07.01..03) — TC-07.02, TC-07.04, TC-07.06 (API), TC-P.03."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.media.models import Clip
from aicam.modules.orders.models import Order, OrderItem, Package, StatusHistory
from aicam.modules.sessions.models import PackSession

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 5, 5, 0, tzinfo=UTC)  # 12:00 giờ VN


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> dict[str, str]:
    user = await make_user(db, f"tst_{role.lower()}", role)
    client = "STATION" if role == "STATION" else "DASHBOARD"
    if role == "STATION":
        from aicam.modules.stations.models import Station

        db.add(Station(name="TST Station X", account_user_id=user.id))
        await db.flush()
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _package(
    db: AsyncSession, n: int, status: str = "PACKED", source: str = "API"
) -> tuple[Package, Order]:
    order = Order(platform_order_sn=f"2410TST{n:05d}", source=source, platform_status="READY_TO_SHIP")
    db.add(order)
    await db.flush()
    db.add(OrderItem(order_id=order.id, product_name="Áo thun basic", variation="Đen / L", quantity=2))
    package = Package(order_id=order.id, tracking_number=f"SPXTST{n:07d}", warehouse_status=status)
    db.add(package)
    await db.flush()
    return package, order


async def _session(
    db: AsyncSession, package: Package, station_id: uuid.UUID, ended: datetime, status: str = "COMPLETED",
    flags: list[str] | None = None,
) -> PackSession:  # fmt: skip
    pack = PackSession(
        package_id=package.id, station_id=station_id, status=status, started_at=ended - timedelta(minutes=2),
        ended_at=ended, open_code=package.tracking_number, package_status_before="NEW", flags=flags or [],
    )  # fmt: skip
    db.add(pack)
    await db.flush()
    return pack


async def test_search_by_tracking_or_order_sn(api: AsyncClient, db: AsyncSession) -> None:
    """TC-07.02: q khớp chính xác mã vận đơn hoặc mã đơn sàn, không phân biệt hoa thường."""
    clock.freeze(NOW)
    headers = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    p10, _ = await _package(db, 10)
    await _package(db, 11)
    pack = await _session(db, p10, station.id, NOW - timedelta(hours=1))
    db.add(Clip(session_id=pack.id, camera_role="CAM1", status="READY", start_at=NOW, end_at=NOW))
    await db.flush()

    by_sn = (await api.get("/api/v1/packages", headers=headers, params={"q": "2410tst00010"})).json()
    by_code = (await api.get("/api/v1/packages", headers=headers, params={"q": "spxtst0000010"})).json()

    assert by_sn["total"] == 1
    item = by_sn["items"][0]
    assert item["tracking_number"] == "SPXTST0000010"
    assert (item["platform_order_sn"], item["source"], item["has_clip"]) == ("2410TST00010", "API", True)
    assert item["last_session"]["station_name"] == "TST Station 01"
    assert by_code["items"][0]["id"] == item["id"]


async def test_search_filters_by_session_date_status_and_flag(api: AsyncClient, db: AsyncSession) -> None:
    """Bộ lọc khớp thẻ API-32: session_status theo ngày kết thúc, session_flag theo ngày bắt đầu (giờ VN)."""
    clock.freeze(NOW)
    headers = await _login(api, db, "SUPERVISOR")
    _, s1 = await make_station_account(db)
    _, s2 = await make_station_account(db, "tst_station02", "TST Station 02")
    today, yesterday = NOW - timedelta(hours=1), NOW - timedelta(days=1)
    a, _ = await _package(db, 1)
    b, _ = await _package(db, 2)
    c, _ = await _package(db, 3, status="NEW")
    await _session(db, a, s1.id, today, flags=["HAD_MISMATCH"])
    await _session(db, b, s2.id, yesterday)
    await _session(db, c, s1.id, today, status="ABANDONED")

    async def codes(**params: str) -> list[str]:
        res = await api.get("/api/v1/packages", headers=headers, params=params)
        assert res.status_code == 200, res.text
        return sorted(i["tracking_number"] for i in res.json()["items"])

    day = "2026-10-05"
    assert await codes(date_from=day, date_to=day) == ["SPXTST0000001", "SPXTST0000003"]
    assert await codes(date_from=day, date_to=day, session_status="COMPLETED") == ["SPXTST0000001"]
    assert await codes(date_from=day, date_to=day, session_flag="HAD_MISMATCH") == ["SPXTST0000001"]
    assert await codes(station_id=str(s2.id)) == ["SPXTST0000002"]
    assert await codes(warehouse_status="NEW") == ["SPXTST0000003"]
    assert await codes(source="CSV") == []


async def test_search_rejects_range_over_92_days(api: AsyncClient, db: AsyncSession) -> None:
    """TC-07.04."""
    headers = await _login(api, db, "CSKH")
    res = await api.get(
        "/api/v1/packages", headers=headers, params={"date_from": "2026-07-01", "date_to": "2026-10-02"}
    )
    assert (res.status_code, res.json()["error"]["code"]) == (422, "VALIDATION_ERROR")
    ok = await api.get(
        "/api/v1/packages", headers=headers, params={"date_from": "2026-07-03", "date_to": "2026-10-02"}
    )
    assert ok.status_code == 200


async def test_detail_sessions_clips_and_timeline(api: AsyncClient, db: AsyncSession) -> None:
    """TC-07.06 (API): sản phẩm, phiên mới nhất trước, clip có retention_until, dòng thời gian."""
    clock.freeze(NOW)
    headers = await _login(api, db, "CSKH")
    _, station = await make_station_account(db)
    package, _ = await _package(db, 10)
    old = await _session(db, package, station.id, NOW - timedelta(days=1), status="SUPERSEDED")
    new = await _session(db, package, station.id, NOW - timedelta(hours=1), flags=["REPACK"])
    end = NOW - timedelta(hours=1)
    db.add_all(
        [
            Clip(
                session_id=new.id, camera_role="CAM1", status="READY", start_at=end, end_at=end, sha256="9f2c"
            ),
            Clip(session_id=new.id, camera_role="CAM2", status="READY", start_at=end, end_at=end, held=True),
            StatusHistory(
                package_id=package.id,
                source="WAREHOUSE",
                from_status="NEW",
                to_status="PACKING",
                at=NOW - timedelta(days=1, minutes=2),
                actor_label="TST Station 01",
            ),
        ]
    )
    await db.flush()

    res = await api.get(f"/api/v1/packages/{package.id}", headers=headers)

    assert res.status_code == 200
    body = res.json()
    assert body["order"]["items"] == [
        {"product_name": "Áo thun basic", "variation": "Đen / L", "quantity": 2, "image_url": None}
    ]
    assert [s["id"] for s in body["sessions"]] == [str(new.id), str(old.id)]
    assert body["sessions"][0]["duration_s"] == 120
    cam1, cam2 = body["sessions"][0]["clips"]
    assert datetime.fromisoformat(cam1["retention_until"]) == end + timedelta(days=90)
    assert cam2["retention_until"] is None  # đang giữ
    assert body["timeline"][0]["actor"] == "TST Station 01"
    missing = await api.get(f"/api/v1/packages/{uuid.uuid4()}", headers=headers)
    assert missing.status_code == 404


async def test_station_cannot_search(api: AsyncClient, db: AsyncSession) -> None:
    """TC-P.03: Station ⛔ API-30, 31."""
    headers = await _login(api, db, "STATION")
    assert (await api.get("/api/v1/packages", headers=headers)).status_code == 403
    assert (await api.get(f"/api/v1/packages/{uuid.uuid4()}", headers=headers)).status_code == 403
