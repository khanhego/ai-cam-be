"""T-117: J-07 phiên RETURN (tự hoàn tất / bỏ dở), ASSIST phiên RETURN, bỏ qua khay, PACK chặn kiện hoàn,
`closed_session` PACK, BR-21 `flag_order_cancelled` (02a §4, §5, §7); TC-04.26..32, 04.53,
03.72, 03.75."""

import json
import uuid
from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.platforms.base import PlatformOrder
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions import service as sessions
from aicam.modules.sessions.models import PackSession, SessionEvent
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.sessions.tray import tray_key

from .factories import PASSWORD, make_user
from .returns_helpers import ITEM, Desk, buyer_return_case, make_desk, make_order

pytestmark = pytest.mark.integration


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


@pytest.fixture
async def desk(api: AsyncClient, db: AsyncSession, adapter: MockAdapter) -> Desk:
    return await make_desk(api, db)


async def _open_41(desk: Desk, db: AsyncSession) -> dict[str, Any]:
    order, _ = await make_order(db, 41)
    await buyer_return_case(db, order, 41)
    body = (await desk.scan("SPXRTTST000041")).json()
    assert body["outcome"] == "SESSION_OPENED", body
    return body["state"]["session"]  # type: ignore[no-any-return]


async def _messages(pubsub: Any, n: int = 3) -> list[dict[str, Any]]:
    out = []
    for _ in range(n):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        if msg is not None:
            out.append(json.loads(msg["data"]))
    return out


# ---------------------------------------------------------------- J-07 phiên RETURN


async def test_j07_auto_closes_with_saved_conclusion(
    desk: Desk, db: AsyncSession, redis_client: Redis, test_settings: Settings
) -> None:
    """TC-04.26, EX-R15, DEC-253, AC-39: quá 45 phút + kết luận đã lưu → `COMPLETED` + `AUTO_CLOSED`."""
    session = await _open_41(desk, db)
    lines = [{"order_item_id": i["order_item_id"], "quantity_received": 0, "condition": "MISSING_ITEM"}
             for i in session["inspection"]["lines"]]  # fmt: skip
    res = await desk.api.put(
        f"/api/v1/station/sessions/{session['id']}/inspection",
        headers=desk.headers,
        json={"conclusion": "EMPTY_BOX", "lines": lines},
    )
    assert res.status_code == 200, res.text
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{desk.station.id}")
    await pubsub.get_message(timeout=1)

    clock.advance(timedelta(minutes=44))
    await sessions.check_timeouts(db, test_settings)
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    await db.refresh(pack)
    assert pack.status == "OPEN"  # 44 phút: mới cảnh báo (ngưỡng 20)
    assert pack.warn_notified

    clock.advance(timedelta(minutes=2))
    await sessions.check_timeouts(db, test_settings)
    messages = await _messages(pubsub, 4)
    await pubsub.aclose()  # type: ignore[no-untyped-call]

    await db.refresh(pack)
    assert pack.status == "COMPLETED"
    assert "AUTO_CLOSED" in pack.flags
    assert pack.close_code is None
    case = await db.get(ReturnCase, uuid.UUID(session["return_case"]["id"]))
    assert case is not None
    await db.refresh(case)
    assert case.status == "RECEIVED_ISSUE"
    alerts = [m["data"] for m in messages if m["type"] == "alert"]
    auto = next(a for a in alerts if a["code"] == "SESSION_AUTO_CLOSED")
    assert auto["session_id"] == session["id"]
    assert auto["closed_session"]["conclusion"] == "EMPTY_BOX"
    assert auto["closed_session"]["package_status"] == "RETURN_RECEIVED_ISSUE"
    assert "AUTO_CLOSED" in auto["closed_session"]["flags"]


async def test_j07_abandons_without_conclusion(
    desk: Desk, db: AsyncSession, redis_client: Redis, test_settings: Settings
) -> None:
    """TC-04.27: chưa có kết luận → `ABANDONED`, kiện về trạng thái trước, hồ sơ về `EXPECTED`."""
    session = await _open_41(desk, db)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{desk.station.id}")
    await pubsub.get_message(timeout=1)

    clock.advance(timedelta(minutes=46))
    await sessions.check_timeouts(db, test_settings)
    messages = await _messages(pubsub)
    await pubsub.aclose()  # type: ignore[no-untyped-call]

    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    await db.refresh(pack)
    assert pack.status == "ABANDONED"
    package = await orders.find_package(db, "SPXTST0000041")
    assert package is not None
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"
    case = await db.get(ReturnCase, uuid.UUID(session["return_case"]["id"]))
    assert case is not None
    await db.refresh(case)
    assert case.status == "EXPECTED"
    assert any(m["type"] == "alert" and m["data"]["code"] == "SESSION_ABANDONED" for m in messages)


async def test_j07_return_warn_uses_return_threshold(
    desk: Desk, db: AsyncSession, redis_client: Redis, test_settings: Settings
) -> None:
    await _open_41(desk, db)

    clock.advance(timedelta(minutes=19))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}
    clock.advance(timedelta(minutes=1))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 1, "abandoned": 0}


# ---------------------------------------------------------------- khay (DEC-246)


async def test_tray_other_code_does_not_change_return_session(
    desk: Desk, db: AsyncSession, redis_client: Redis, test_settings: Settings
) -> None:
    """TC-04.32, DEC-203, DEC-246: Cam 2 thấy mã khác khi kiểm hoàn → phiên vẫn `OPEN`, ghi `CAM2_DETECT`."""
    session = await _open_41(desk, db)
    await redis_client.set(
        tray_key(desk.station.id),
        json.dumps({"codes": ["SPXTST0000003"], "updated_at": clock.now().isoformat()}),
        ex=60,
    )

    assert await sessions.on_tray_changed(db, desk.station.id, test_settings) is None

    state = await desk.state()
    assert state["state"] == "INSPECTING"
    assert state["session"]["status"] == "OPEN"
    events = (
        await db.scalars(select(SessionEvent.type).where(SessionEvent.session_id == uuid.UUID(session["id"])))
    ).all()
    assert "CAM2_DETECT" in events


# ---------------------------------------------------------------- ASSIST (API-13 / 21)


async def test_assist_for_return_session(api: AsyncClient, desk: Desk, db: AsyncSession) -> None:
    """TC-04.30, TC-04.31: gọi quản lý từ phiên hoàn; `CLOSE_WITH_NOTE` → 422; `CONTINUE` → về R2."""
    await make_user(db, "tst_sup", "SUPERVISOR")
    session = await _open_41(desk, db)
    sup_login = await api.post(
        "/api/v1/auth/login", json={"username": "tst_sup", "password": PASSWORD, "client": "DASHBOARD"}
    )
    sup = {"Authorization": f"Bearer {sup_login.json()['access_token']}"}

    req = await api.post(
        "/api/v1/station/approval-requests",
        headers=desk.headers,
        json={"type": "ASSIST", "session_id": session["id"]},
    )
    assert req.status_code == 201, req.text
    assert req.json()["state"]["state"] == "WAITING_APPROVAL"
    approval_id = req.json()["approval_request"]["id"]
    item = (await api.get("/api/v1/approval-requests", headers=sup)).json()["items"][0]
    assert (item["session_type"], item["operator_name"]) == ("RETURN", "Lan QA")

    bad = await api.post(
        f"/api/v1/approval-requests/{approval_id}/decision",
        headers=sup,
        json={"action": "CLOSE_WITH_NOTE", "note": "x"},
    )
    assert (bad.status_code, bad.json()["error"]["code"]) == (422, "INVALID_ACTION")

    ok = await api.post(
        f"/api/v1/approval-requests/{approval_id}/decision", headers=sup, json={"action": "CONTINUE"}
    )
    assert ok.status_code == 200, ok.text
    state = await desk.state()
    assert state["state"] == "INSPECTING"


async def test_assist_cancel_return_session(api: AsyncClient, desk: Desk, db: AsyncSession) -> None:
    await make_user(db, "tst_sup", "SUPERVISOR")
    session = await _open_41(desk, db)
    sup_login = await api.post(
        "/api/v1/auth/login", json={"username": "tst_sup", "password": PASSWORD, "client": "DASHBOARD"}
    )
    sup = {"Authorization": f"Bearer {sup_login.json()['access_token']}"}
    req = await api.post(
        "/api/v1/station/approval-requests",
        headers=desk.headers,
        json={"type": "ASSIST", "session_id": session["id"]},
    )
    approval_id = req.json()["approval_request"]["id"]

    res = await api.post(
        f"/api/v1/approval-requests/{approval_id}/decision", headers=sup, json={"action": "CANCEL_SESSION"}
    )

    assert res.status_code == 200, res.text
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    await db.refresh(pack)
    assert (pack.status, pack.cancel_reason) == ("CANCELLED", "SUPERVISOR")
    package = await orders.find_package(db, "SPXTST0000041")
    assert package is not None
    await db.refresh(package)
    assert package.warehouse_status == "RETURN_EXPECTED"


# ---------------------------------------------------------------- bàn đóng gói (PACK)


async def _pack_desk(api: AsyncClient, db: AsyncSession) -> Desk:
    return await make_desk(api, db, 3, operator=None, kind="PACK", mode="PACK")


async def test_pack_scan_of_return_package_alerts(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter
) -> None:
    """TC-04.53, EX-R16, DEC-247: kiện `RETURN_EXPECTED` ở bàn đóng gói → `ALREADY_HANDED_OVER`."""
    desk = await _pack_desk(api, db)
    await make_order(db, 49, warehouse_status="RETURN_EXPECTED")

    body = (await desk.scan("SPXTST0000049")).json()

    assert body["outcome"] == "ALERT"
    assert body["alert"]["code"] == "ALREADY_HANDED_OVER"
    assert body["alert"]["data"] == {"is_return": True}
    assert "kiện hàng hoàn" in body["alert"]["message"]


async def test_pack_close_returns_closed_session(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter, redis_client: Redis
) -> None:
    """TC-03.72, FR-03.14: khay vẫn thấy mã phiên khi quét đóng → `closed_session` có `LABEL_ON_TRAY`."""
    desk = await _pack_desk(api, db)
    await desk.scan("SPXTST0000012")
    await redis_client.set(
        tray_key(desk.station.id),
        json.dumps({"codes": ["SPXTST0000012"], "updated_at": clock.now().isoformat()}),
        ex=60,
    )

    body = (await desk.scan("SPXTST0000012")).json()

    closed = body["closed_session"]
    assert (closed["type"], closed["tracking_number"], closed["package_status"]) == (
        "PACK",
        "SPXTST0000012",
        "PACKED",
    )
    assert "LABEL_ON_TRAY" in closed["flags"]
    assert (closed["conclusion"], closed["claim_code"], closed["return_case_status"]) == (None, None, None)


async def test_order_cancelled_during_session(
    api: AsyncClient,
    db: AsyncSession,
    adapter: MockAdapter,
    redis_client: Redis,
    test_settings: Settings,
    sent_jobs: list[Any],
) -> None:
    """TC-03.75, BR-21, AC-32: J-04 thấy đơn hủy khi kiện `PACKING` → task gắn cờ + WS; đóng → hủy sau đóng"""
    desk = await _pack_desk(api, db)
    await desk.scan("SPXTST0000012")
    package = await orders.find_package(db, "SPXTST0000012")
    assert package is not None
    sent_jobs.clear()

    await orders.upsert_platform_order(
        db, PlatformOrder("2410TST00012", "CANCELLED", ("SPXTST0000012",), (ITEM,))
    )
    await db.flush()
    assert package.warehouse_status == "PACKING"  # đồng bộ không đụng kiện đang đóng
    from aicam.core.db import commit

    await commit(db)
    assert sent_jobs == [("sessions.flag_order_cancelled", [str(package.id)], "default", 0.0)]
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{desk.station.id}")
    await pubsub.get_message(timeout=1)

    assert await sessions.flag_order_cancelled(db, package.id, test_settings) == "flagged"
    assert await sessions.flag_order_cancelled(db, package.id, test_settings) == "noop"
    messages = await _messages(pubsub)
    await pubsub.aclose()  # type: ignore[no-untyped-call]

    alert = next(m["data"] for m in messages if m["type"] == "alert")
    assert alert["code"] == "ORDER_CANCELLED_DURING_SESSION"
    assert alert["tracking_number"] == "SPXTST0000012"
    state = await desk.state()
    assert "ORDER_CANCELLED" in state["session"]["flags"]

    closed = (await desk.scan("SPXTST0000012")).json()["closed_session"]
    assert closed["package_status"] == "CANCELLED_AFTER_PACK"


async def test_order_cancelled_after_session_closed(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter, test_settings: Settings
) -> None:
    """TC-03.76, R3-8: phiên đã đóng trước khi task chạy → `PACKED → CANCELLED_AFTER_PACK`."""
    desk = await _pack_desk(api, db)
    await desk.scan("SPXTST0000012")
    await desk.scan("SPXTST0000012")
    package = await orders.find_package(db, "SPXTST0000012")
    assert package is not None

    assert await sessions.flag_order_cancelled(db, package.id, test_settings) == "cancelled_after_pack"

    await db.refresh(package)
    assert package.warehouse_status == "CANCELLED_AFTER_PACK"


async def test_j07_return_timer_restarts_after_assist_continue(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter, test_settings: Settings
) -> None:
    """TC-04.28, R-24, DEC-60 (BR-16) cho phiên RETURN: mở 08:00, ASSIST 08:10, duyệt "Cho tiếp tục" 08:50 →
    thời gian chờ duyệt không tính: J-07 08:49 (đang chờ) và 09:05 không làm gì; cảnh báo 20 phút lúc 09:10;
    bỏ dở (chưa kết luận) lúc 09:35 (45 phút sau lúc duyệt)."""
    from datetime import UTC, datetime

    t0 = datetime(2026, 10, 6, 1, 0, tzinfo=UTC)  # 08:00 giờ VN
    clock.freeze(t0)
    desk = await make_desk(api, db)
    session = await _open_41(desk, db)
    await make_user(db, "tst_sup", "SUPERVISOR")

    clock.freeze(t0 + timedelta(minutes=10))
    req = await api.post(
        "/api/v1/station/approval-requests",
        headers=desk.headers,
        json={"type": "ASSIST", "session_id": session["id"]},
    )
    assert req.status_code == 201, req.text
    clock.freeze(t0 + timedelta(minutes=49))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}

    clock.freeze(t0 + timedelta(minutes=50))
    sup_login = await api.post(
        "/api/v1/auth/login", json={"username": "tst_sup", "password": PASSWORD, "client": "DASHBOARD"}
    )
    sup = {"Authorization": f"Bearer {sup_login.json()['access_token']}"}
    ok = await api.post(
        f"/api/v1/approval-requests/{req.json()['approval_request']['id']}/decision",
        headers=sup,
        json={"action": "CONTINUE"},
    )
    assert ok.status_code == 200, ok.text
    resumed = t0 + timedelta(minutes=50)
    relogin = await api.post(  # token station cấp lúc 08:00 đã hết hạn
        "/api/v1/auth/login", json={"username": "tst_station01", "password": PASSWORD, "client": "STATION"}
    )
    desk.headers = {"Authorization": f"Bearer {relogin.json()['access_token']}"}
    state = await desk.state()
    assert state["state"] == "INSPECTING"
    assert datetime.fromisoformat(state["session"]["warn_at"]) == resumed + timedelta(minutes=20)
    assert datetime.fromisoformat(state["session"]["abandon_at"]) == resumed + timedelta(minutes=45)

    clock.freeze(t0 + timedelta(minutes=65))  # 09:05 — 65 phút từ lúc mở, 15 phút từ lúc duyệt
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}
    clock.freeze(t0 + timedelta(minutes=70))  # 09:10
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 1, "abandoned": 0}
    clock.freeze(t0 + timedelta(minutes=94))  # 09:34
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}
    clock.freeze(t0 + timedelta(minutes=95))  # 09:35
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 1}

    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    await db.refresh(pack)
    assert pack.status == "ABANDONED"
