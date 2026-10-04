"""API-12, API-15, J-07 (T-20) — BR-03, BR-16; TC-03.18, 03.19, 03.27..03.29, 03.33, 03.52, 03.53."""

import json
import uuid
from datetime import timedelta

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.orders import service as orders
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_station_account

pytestmark = pytest.mark.integration


@pytest.fixture
async def ctx(api: AsyncClient, db: AsyncSession) -> tuple[dict[str, str], uuid.UUID]:
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()  # type: ignore[attr-defined]
    user, station = await make_station_account(db)
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, station.id


async def _scan(api: AsyncClient, headers: dict[str, str], code: str) -> dict:  # type: ignore[type-arg]
    res = await api.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    return res.json()  # type: ignore[no-any-return]


async def test_cancel_returns_package_to_new(
    api: AsyncClient, db: AsyncSession, ctx: tuple[dict[str, str], uuid.UUID]
) -> None:
    """TC-03.18."""
    headers, _ = ctx
    opened = await _scan(api, headers, "SPXTST0000001")
    session_id = opened["state"]["session"]["id"]

    res = await api.post(
        f"/api/v1/station/sessions/{session_id}/cancel", headers=headers, json={"reason": "OUT_OF_STOCK"}
    )

    assert res.status_code == 200
    assert res.json()["state"]["state"] == "READY"
    pack = await db.get(PackSession, uuid.UUID(session_id))
    assert pack is not None
    assert (pack.status, pack.cancel_reason) == ("CANCELLED", "OUT_OF_STOCK")
    package = await orders.find_package(db, "SPXTST0000001")
    assert package is not None
    assert package.warehouse_status == "NEW"


async def test_cancel_other_requires_note(api: AsyncClient, ctx: tuple[dict[str, str], uuid.UUID]) -> None:
    """TC-03.19."""
    headers, _ = ctx
    opened = await _scan(api, headers, "SPXTST0000002")

    res = await api.post(
        f"/api/v1/station/sessions/{opened['state']['session']['id']}/cancel",
        headers=headers,
        json={"reason": "OTHER", "note": "  "},
    )

    assert res.status_code == 422
    assert res.json()["error"]["details"]["fields"] == {"note": "Nhập lý do khi chọn Khác"}


async def test_cancel_closed_session(api: AsyncClient, ctx: tuple[dict[str, str], uuid.UUID]) -> None:
    headers, _ = ctx
    opened = await _scan(api, headers, "SPXTST0000003")
    await _scan(api, headers, "SPXTST0000003")

    res = await api.post(
        f"/api/v1/station/sessions/{opened['state']['session']['id']}/cancel",
        headers=headers,
        json={"reason": "WRONG_SCAN"},
    )

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "SESSION_NOT_OPEN"


async def test_cancel_unknown_reason_rejected(
    api: AsyncClient, ctx: tuple[dict[str, str], uuid.UUID]
) -> None:
    headers, _ = ctx
    opened = await _scan(api, headers, "SPXTST0000004")

    res = await api.post(
        f"/api/v1/station/sessions/{opened['state']['session']['id']}/cancel",
        headers=headers,
        json={"reason": "SUPERVISOR"},
    )

    assert res.status_code == 422


async def _repack_session(
    db: AsyncSession, station_id: uuid.UUID, code: str
) -> tuple[PackSession, PackSession]:
    """Dựng tay phiên đóng gói lại (API-21 APPROVE_REPACK là T-13)."""
    data = await MockAdapter().find_by_tracking(None, code)
    assert data is not None
    package = (await orders.upsert_platform_order(db, data)).packages[0]
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    old = PackSession(package_id=package.id, station_id=station_id, status="COMPLETED", open_code=code,
                      started_at=clock.now(), ended_at=clock.now(), package_status_before="NEW")  # fmt: skip
    db.add(old)
    await db.flush()
    await orders.transition(db, package, "PACKING", source="WAREHOUSE")
    new = PackSession(package_id=package.id, station_id=station_id, status="OPEN", open_code=code,
                      started_at=clock.now(), package_status_before="PACKED", flags=["REPACK"],
                      supersedes_session_id=old.id)  # fmt: skip
    db.add(new)
    await db.flush()
    return old, new


async def test_cancel_repack_keeps_old_session(
    api: AsyncClient, db: AsyncSession, ctx: tuple[dict[str, str], uuid.UUID]
) -> None:
    """TC-03.52, AC-21: hủy phiên đóng gói lại → kiện PACKED, phiên cũ vẫn COMPLETED."""
    headers, station_id = ctx
    old, new = await _repack_session(db, station_id, "SPXTST0000010")

    res = await api.post(
        f"/api/v1/station/sessions/{new.id}/cancel", headers=headers, json={"reason": "WRONG_SCAN"}
    )

    assert res.status_code == 200
    await db.refresh(old)
    assert old.status == "COMPLETED"
    package = await orders.find_package(db, "SPXTST0000010")
    assert package is not None
    assert package.warehouse_status == "PACKED"


async def test_complete_repack_supersedes_old(
    api: AsyncClient, db: AsyncSession, ctx: tuple[dict[str, str], uuid.UUID]
) -> None:
    """TC-03.51 (phần đóng phiên): phiên cũ SUPERSEDED khi phiên mới hoàn tất."""
    headers, station_id = ctx
    old, _ = await _repack_session(db, station_id, "SPXTST0000011")

    done = await _scan(api, headers, "SPXTST0000011")

    assert done["outcome"] == "SESSION_COMPLETED"
    await db.refresh(old)
    assert old.status == "SUPERSEDED"


async def test_recent_sessions_today(api: AsyncClient, ctx: tuple[dict[str, str], uuid.UUID]) -> None:
    """TC-03.33, API-15."""
    headers, _ = ctx
    for code in ("SPXTST0000005", "SPXTST0000006"):
        await _scan(api, headers, code)
        await _scan(api, headers, code)

    res = await api.get("/api/v1/station/sessions/recent", headers=headers)

    assert res.status_code == 200
    items = res.json()["items"]
    assert [i["tracking_number"] for i in items] == ["SPXTST0000006", "SPXTST0000005"]
    assert items[0]["status"] == "COMPLETED"
    assert items[0]["clips"] == []


async def test_recent_excludes_yesterday(api: AsyncClient, ctx: tuple[dict[str, str], uuid.UUID]) -> None:
    headers, _ = ctx
    await _scan(api, headers, "SPXTST0000007")
    await _scan(api, headers, "SPXTST0000007")

    clock.advance(timedelta(days=1))
    # Access token 15 phút đã hết hạn sau khi tua giờ → đăng nhập lại.
    login = await api.post(
        "/api/v1/auth/login", json={"username": "tst_station01", "password": PASSWORD, "client": "STATION"}
    )
    fresh = {"Authorization": f"Bearer {login.json()['access_token']}"}
    res = await api.get("/api/v1/station/sessions/recent", headers=fresh)

    assert res.status_code == 200, res.text
    assert res.json()["items"] == []


async def test_timeouts_warn_once_then_abandon(
    api: AsyncClient,
    db: AsyncSession,
    ctx: tuple[dict[str, str], uuid.UUID],
    redis_client: Redis,
    test_settings: Settings,
) -> None:
    """TC-03.27, TC-03.28, AC-16, BR-16."""
    headers, station_id = ctx
    await _scan(api, headers, "SPXTST0000008")
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{station_id}")
    await pubsub.get_message(timeout=1)

    clock.advance(timedelta(minutes=14, seconds=59))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}
    clock.advance(timedelta(seconds=1))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 1, "abandoned": 0}
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}
    warn = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)

    clock.advance(timedelta(minutes=15))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 1}
    after_abandon = []
    for _ in range(2):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
        if msg is not None:
            after_abandon.append(json.loads(msg["data"]))
    await pubsub.aclose()  # type: ignore[no-untyped-call]

    assert warn is not None
    assert json.loads(warn["data"])["data"]["code"] == "SESSION_WARN"
    assert [m["type"] for m in after_abandon] == ["station.state", "alert"]
    assert after_abandon[0]["data"]["state"] == "READY"
    assert after_abandon[1]["data"]["code"] == "SESSION_ABANDONED"
    pack = await db.scalar(select(PackSession).where(PackSession.open_code == "SPXTST0000008"))
    assert pack is not None
    assert pack.status == "ABANDONED"
    package = await orders.find_package(db, "SPXTST0000008")
    assert package is not None
    assert package.warehouse_status == "NEW"


async def test_waiting_approval_is_not_abandoned(
    api: AsyncClient, db: AsyncSession, ctx: tuple[dict[str, str], uuid.UUID], test_settings: Settings
) -> None:
    """TC-03.29."""
    headers, station_id = ctx
    opened = await _scan(api, headers, "SPXTST0000012")
    pack = await db.get(PackSession, uuid.UUID(opened["state"]["session"]["id"]))
    assert pack is not None
    pack.status = "WAITING_APPROVAL"
    db.add(
        ApprovalRequest(
            station_id=station_id, session_id=pack.id, tracking_number="SPXTST0000012", type="ASSIST"
        )
    )
    await db.flush()

    clock.advance(timedelta(minutes=31))
    result = await sessions.check_timeouts(db, test_settings)

    assert result == {"warned": 0, "abandoned": 0}
    await db.refresh(pack)
    assert pack.status == "WAITING_APPROVAL"
