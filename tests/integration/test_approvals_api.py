"""API-13, 14, 20, 21 + WS `approval.*` / `station.state` (T-13) — FR-03.10, 03.12, UC-08, BR-03, 06, 18.

TC-03.40..03.55 phần API (E2E qua giao diện ở T-38 / T-55).
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings
from aicam.main import create_app
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.orders import service as orders
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.sessions.tray import write_tray
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

Headers = dict[str, str]


async def _login(api: AsyncClient, username: str, client: str) -> Headers:
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture
async def ctx(api: AsyncClient, db: AsyncSession) -> dict[str, Any]:
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()  # type: ignore[attr-defined]
    _, station = await make_station_account(db)
    await make_station_account(db, "tst_station02", "TST Station 02")
    await make_user(db, "tst_sup", "SUPERVISOR", display_name="Nguyễn B")
    await make_user(db, "tst_admin", "ADMIN", display_name="Quản trị")
    await make_user(db, "tst_cskh", "CSKH", display_name="Lan")
    return {
        "station_id": station.id,
        "st": await _login(api, "tst_station01", "STATION"),
        "st2": await _login(api, "tst_station02", "STATION"),
        "sup": await _login(api, "tst_sup", "DASHBOARD"),
        "admin": await _login(api, "tst_admin", "DASHBOARD"),
        "cskh": await _login(api, "tst_cskh", "DASHBOARD"),
    }


async def _scan(api: AsyncClient, h: Headers, code: str) -> dict[str, Any]:
    res = await api.post(
        "/api/v1/station/scan", headers=h, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


async def _request(api: AsyncClient, h: Headers, **body: Any) -> Any:
    return await api.post("/api/v1/station/approval-requests", headers=h, json=body)


async def _decide(
    api: AsyncClient, h: Headers, approval_id: str, action: str, note: str | None = None
) -> Any:
    return await api.post(
        f"/api/v1/approval-requests/{approval_id}/decision", headers=h, json={"action": action, "note": note}
    )


async def _mismatch_pending(
    api: AsyncClient, ctx: dict[str, Any], code: str = "SPXTST0000001"
) -> dict[str, Any]:
    """Phiên `code` lệch mã do quét …99 → gửi MISMATCH. Trả body 201."""
    opened = await _scan(api, ctx["st"], code)
    await _scan(api, ctx["st"], "SPXTST0000029")
    res = await _request(api, ctx["st"], type="MISMATCH", session_id=opened["state"]["session"]["id"])
    assert res.status_code == 201, res.text
    return res.json()  # type: ignore[no-any-return]


async def _pack(db: AsyncSession, session_id: str) -> PackSession:
    pack = await db.scalar(
        select(PackSession)
        .where(PackSession.id == uuid.UUID(session_id))
        .execution_options(populate_existing=True)
    )
    assert pack is not None
    return pack


async def _drain(pubsub: Any, n: int = 6) -> list[dict[str, Any]]:
    out = []
    for _ in range(n):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.3)
        if msg is not None:
            out.append({"channel": msg["channel"], **json.loads(msg["data"])})
    return out


# ---------------------------------------------------------------- API-13 / API-20 / API-21 luồng chính


async def test_mismatch_request_then_continue(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any], redis_client: Redis
) -> None:
    """TC-03.40 (API), UC-08, AC-19: gửi MISMATCH → D13 thấy → Cho tiếp tục → PACKING; audit người duyệt."""
    station_id = ctx["station_id"]
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{station_id}", "ws:approvals")
    await pubsub.get_message(timeout=1)
    await pubsub.get_message(timeout=1)

    created = await _mismatch_pending(api, ctx)
    apr = created["approval_request"]
    assert (apr["type"], apr["status"], apr["tracking_number"]) == ("MISMATCH", "PENDING", "SPXTST0000001")
    assert created["state"]["state"] == "WAITING_APPROVAL"
    assert created["state"]["approval_request"]["id"] == apr["id"]
    assert created["state"]["session"]["status"] == "WAITING_APPROVAL"

    listed = await api.get("/api/v1/approval-requests?status=PENDING", headers=ctx["sup"])
    assert listed.status_code == 200
    body = listed.json()
    assert body["total"] == 1
    item = body["items"][0]
    assert item["station"] == {"id": str(station_id), "name": "TST Station 01"}
    assert item["session_id"] == created["state"]["session"]["id"]
    assert item["context"] == {
        "expected": "SPXTST0000001", "actual": "SPXTST0000029", "source": "SCAN", "tray_match": "UNAVAILABLE",
    }  # fmt: skip

    res = await _decide(api, ctx["sup"], apr["id"], "CONTINUE")
    assert res.status_code == 200, res.text
    out = res.json()["approval_request"]
    assert (out["status"], out["decision"], out["decided_by"]["display_name"]) == (
        "RESOLVED",
        "CONTINUE",
        "Nguyễn B",
    )

    state = (await api.get("/api/v1/station/state", headers=ctx["st"])).json()
    assert (state["state"], state["approval_request"], state["session"]["mismatch"]) == (
        "PACKING",
        None,
        None,
    )
    pack = await _pack(db, item["session_id"])
    assert (pack.status, pack.status_before_approval) == ("OPEN", None)
    assert "HAD_MISMATCH" in pack.flags
    log = await db.scalar(select(AuditLog).where(AuditLog.action == "APPROVAL_DECISION"))
    assert log is not None
    assert log.object_id == apr["id"]
    assert log.data is not None
    assert (log.data["action"], log.data["type"]) == ("CONTINUE", "MISMATCH")

    msgs = await _drain(pubsub, 8)
    await pubsub.aclose()  # type: ignore[no-untyped-call]
    approvals = [m for m in msgs if m["channel"] == "ws:approvals" and m["data"]["id"] == apr["id"]]
    assert [m["type"] for m in approvals] == ["approval.created", "approval.resolved"]
    assert approvals[1]["data"]["decided_by"]["display_name"] == "Nguyễn B"
    station_states = [m["data"]["state"] for m in msgs if m["type"] == "station.state"]
    assert station_states[-2:] == ["WAITING_APPROVAL", "PACKING"]


async def test_assist_from_open_and_eligibility(api: AsyncClient, ctx: dict[str, Any]) -> None:
    """TC-03.41 (API), DEC-25: ASSIST khi phiên OPEN; NOT_ELIGIBLE khi sai trạng thái; TC-03.49 gửi 2 lần."""
    opened = await _scan(api, ctx["st"], "SPXTST0000003")
    sid = opened["state"]["session"]["id"]

    wrong = await _request(api, ctx["st"], type="MISMATCH", session_id=sid)
    assert (wrong.status_code, wrong.json()["error"]["code"]) == (409, "NOT_ELIGIBLE")
    missing = await _request(api, ctx["st"], type="ASSIST")
    assert (missing.status_code, missing.json()["error"]["code"]) == (422, "VALIDATION_ERROR")

    ok = await _request(api, ctx["st"], type="ASSIST", session_id=sid)
    assert ok.status_code == 201, ok.text
    assert ok.json()["state"]["approval_request"]["type"] == "ASSIST"

    again = await _request(api, ctx["st"], type="ASSIST", session_id=sid)
    assert (again.status_code, again.json()["error"]["code"]) == (409, "APPROVAL_ALREADY_PENDING")
    listed = (await api.get("/api/v1/approval-requests", headers=ctx["admin"])).json()
    assert listed["total"] == 1


async def test_close_with_note(api: AsyncClient, db: AsyncSession, ctx: dict[str, Any]) -> None:
    """TC-03.42: Đóng phiên có ghi chú → COMPLETED, CLOSED_BY_SUPERVISOR, kiện PACKED, station READY."""
    created = await _mismatch_pending(api, ctx, "SPXTST0000004")
    apr_id = created["approval_request"]["id"]

    no_note = await _decide(api, ctx["sup"], apr_id, "CLOSE_WITH_NOTE", "   ")
    assert no_note.status_code == 422
    assert "note" in no_note.json()["error"]["details"]["fields"]
    res = await _decide(api, ctx["sup"], apr_id, "CLOSE_WITH_NOTE", "QA đóng tay")
    assert res.status_code == 200, res.text

    pack = await _pack(db, created["state"]["session"]["id"])
    assert pack.status == "COMPLETED"
    assert {"CLOSED_BY_SUPERVISOR", "HAD_MISMATCH", "CAM2_UNVERIFIED"} <= set(pack.flags)
    assert (pack.note, pack.mismatch, pack.close_code) == ("QA đóng tay", None, None)
    package = await orders.find_package(db, "SPXTST0000004")
    assert package is not None
    assert package.warehouse_status == "PACKED"
    state = (await api.get("/api/v1/station/state", headers=ctx["st"])).json()
    assert state["state"] == "READY"


async def test_close_with_note_blocked_while_tray_different(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any], redis_client: Redis
) -> None:
    """TC-03.43 (BR-06): khay còn phiếu sai → 409 TRAY_STILL_DIFFERENT; TC-03.44 Cho tiếp tục → MISMATCH."""
    station_id = ctx["station_id"]
    opened = await _scan(api, ctx["st"], "SPXTST0000014")
    await write_tray(redis_client, station_id, ("SPXTST0000015",), clock.now(), ttl_s=60)
    mism = await _scan(api, ctx["st"], "SPXTST0000014")
    assert mism["state"]["session"]["mismatch"]["source"] == "CAM2"
    created = await _request(api, ctx["st"], type="MISMATCH", session_id=opened["state"]["session"]["id"])
    apr_id = created.json()["approval_request"]["id"]
    item = (await api.get("/api/v1/approval-requests", headers=ctx["sup"])).json()["items"][0]
    assert item["context"]["tray_match"] == "DIFFERENT"
    assert item["context"]["source"] == "CAM2"

    blocked = await _decide(api, ctx["sup"], apr_id, "CLOSE_WITH_NOTE", "QA")
    assert (blocked.status_code, blocked.json()["error"]["code"]) == (409, "TRAY_STILL_DIFFERENT")
    pack = await _pack(db, opened["state"]["session"]["id"])
    assert pack.status == "WAITING_APPROVAL"
    apr = await db.get(ApprovalRequest, uuid.UUID(apr_id), populate_existing=True)
    assert apr is not None
    assert apr.status == "PENDING"

    cont = await _decide(api, ctx["sup"], apr_id, "CONTINUE")
    assert cont.status_code == 200
    state = (await api.get("/api/v1/station/state", headers=ctx["st"])).json()
    assert state["state"] == "MISMATCH"
    assert state["session"]["mismatch"] == {
        "source": "CAM2",
        "expected": "SPXTST0000014",
        "actual": "SPXTST0000015",
    }


async def test_cancel_session_by_supervisor(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any], redis_client: Redis
) -> None:
    """TC-03.45: Hủy phiên từ dashboard → CANCELLED lý do SUPERVISOR, kiện NEW, station READY + alert."""
    opened = await _scan(api, ctx["st"], "SPXTST0000001")
    sid = opened["state"]["session"]["id"]
    created = await _request(api, ctx["st"], type="ASSIST", session_id=sid)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"ws:station:{ctx['station_id']}")
    await pubsub.get_message(timeout=1)

    res = await _decide(api, ctx["sup"], created.json()["approval_request"]["id"], "CANCEL_SESSION")

    assert res.status_code == 200, res.text
    pack = await _pack(db, sid)
    assert (pack.status, pack.cancel_reason) == ("CANCELLED", "SUPERVISOR")
    package = await orders.find_package(db, "SPXTST0000001")
    assert package is not None
    assert package.warehouse_status == "NEW"
    msgs = await _drain(pubsub)
    await pubsub.aclose()  # type: ignore[no-untyped-call]
    assert [m["type"] for m in msgs] == ["station.state", "alert"]
    assert msgs[0]["data"]["state"] == "READY"
    assert msgs[1]["data"]["code"] == "SESSION_CANCELLED_BY_SUPERVISOR"


async def test_withdraw_restores_previous_state(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any]
) -> None:
    """TC-03.46: Rút yêu cầu → về LỆCH MÃ; TC-03.48: duyệt sau khi rút → ALREADY_RESOLVED WITHDRAWN."""
    created = await _mismatch_pending(api, ctx)
    apr_id = created["approval_request"]["id"]

    other = await api.post(f"/api/v1/station/approval-requests/{apr_id}/withdraw", headers=ctx["st2"])
    assert other.status_code == 404  # yêu cầu của station khác
    res = await api.post(f"/api/v1/station/approval-requests/{apr_id}/withdraw", headers=ctx["st"])
    assert res.status_code == 200, res.text
    state = res.json()["state"]
    assert (state["state"], state["approval_request"]) == ("MISMATCH", None)
    assert state["session"]["mismatch"]["actual"] == "SPXTST0000029"

    again = await api.post(f"/api/v1/station/approval-requests/{apr_id}/withdraw", headers=ctx["st"])
    assert (again.status_code, again.json()["error"]["code"]) == (409, "ALREADY_RESOLVED")
    late = await _decide(api, ctx["sup"], apr_id, "CONTINUE")
    assert late.status_code == 409
    assert late.json()["error"]["code"] == "ALREADY_RESOLVED"
    details = late.json()["error"]["details"]
    # DEC-60: rút yêu cầu cũng ghi mốc kết thúc chờ (`decided_at`), không có người duyệt.
    assert (details["status"], details["decided_by"]) == ("WITHDRAWN", None)
    assert details["decided_at"] is not None
    assert (await api.get("/api/v1/approval-requests", headers=ctx["sup"])).json()["total"] == 0
    withdrawn = await api.get("/api/v1/approval-requests?status=WITHDRAWN", headers=ctx["sup"])
    assert withdrawn.json()["items"][0]["status"] == "WITHDRAWN"


async def test_invalid_action_and_permissions(api: AsyncClient, ctx: dict[str, Any]) -> None:
    """API-21 INVALID_ACTION; API-20/21 chỉ ADMIN, SUPERVISOR; API-13 chỉ STATION (ma trận 01 §5.1)."""
    created = await _mismatch_pending(api, ctx)
    apr_id = created["approval_request"]["id"]

    bad = await _decide(api, ctx["sup"], apr_id, "APPROVE_REPACK")
    assert (bad.status_code, bad.json()["error"]["code"]) == (422, "INVALID_ACTION")
    for who in ("cskh", "st"):
        assert (await api.get("/api/v1/approval-requests", headers=ctx[who])).status_code == 403
        assert (await _decide(api, ctx[who], apr_id, "CONTINUE")).status_code == 403
    assert (await _request(api, ctx["sup"], type="ASSIST", session_id=str(uuid.uuid4()))).status_code == 403
    missing = await _decide(api, ctx["sup"], str(uuid.uuid4()), "CONTINUE")
    assert missing.status_code == 404


# ---------------------------------------------------------------- REPACK (BR-03, AC-14, AC-21)


async def _packed(api: AsyncClient, ctx: dict[str, Any], code: str) -> None:
    """Đóng gói xong `code` ở TST Station 02 (để station 01 yêu cầu đóng gói lại)."""
    await _scan(api, ctx["st2"], code)
    done = await _scan(api, ctx["st2"], code)
    assert done["outcome"] == "SESSION_COMPLETED"


async def test_repack_approve_then_complete(api: AsyncClient, db: AsyncSession, ctx: dict[str, Any]) -> None:
    """TC-03.51 (API), AC-14: ALREADY_PACKED → REPACK → duyệt → phiên REPACK; phiên cũ SUPERSEDED khi xong."""
    await _packed(api, ctx, "SPXTST0000010")
    old = await db.scalar(select(PackSession).where(PackSession.open_code == "SPXTST0000010"))
    assert old is not None
    alert = await _scan(api, ctx["st"], "SPXTST0000010")
    assert alert["alert"]["data"]["can_request_repack"] is True

    created = await _request(api, ctx["st"], type="REPACK", tracking_number="spxtst0000010")
    assert created.status_code == 201, created.text
    body = created.json()
    assert (body["state"]["state"], body["state"]["session"]) == ("WAITING_APPROVAL", None)
    assert body["state"]["approval_request"]["type"] == "REPACK"
    assert (await _scan(api, ctx["st"], "SPXTST0000002"))["outcome"] == "IGNORED"  # TC-03.50

    res = await _decide(api, ctx["sup"], body["approval_request"]["id"], "APPROVE_REPACK")
    assert res.status_code == 200, res.text
    state = (await api.get("/api/v1/station/state", headers=ctx["st"])).json()
    assert state["state"] == "PACKING"
    assert "REPACK" in state["session"]["flags"]
    package = await orders.find_package(db, "SPXTST0000010")
    assert package is not None
    assert package.warehouse_status == "PACKING"
    await db.refresh(old)
    assert old.status == "COMPLETED"  # chưa thay thế khi phiên mới chưa xong
    item = (await api.get("/api/v1/approval-requests?status=RESOLVED", headers=ctx["sup"])).json()["items"][0]
    assert item["session_id"] == state["session"]["id"]

    done = await _scan(api, ctx["st"], "SPXTST0000010")
    assert done["outcome"] == "SESSION_COMPLETED"
    await db.refresh(old)
    assert old.status == "SUPERSEDED"
    new = await _pack(db, state["session"]["id"])
    assert (new.status, new.supersedes_session_id) == ("COMPLETED", old.id)


async def test_repack_reject_and_not_eligible(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any]
) -> None:
    """TC-03.54: Từ chối → station READY, kiện vẫn PACKED; TC-03.55: kiện không PACKED → NOT_ELIGIBLE."""
    new_pkg = await _request(api, ctx["st"], type="REPACK", tracking_number="SPXTST0000011")
    assert (new_pkg.status_code, new_pkg.json()["error"]["code"]) == (
        409,
        "NOT_ELIGIBLE",
    )  # chưa có / chưa PACKED
    missing = await _request(api, ctx["st"], type="REPACK")
    assert missing.status_code == 422

    await _packed(api, ctx, "SPXTST0000012")
    created = await _request(api, ctx["st"], type="REPACK", tracking_number="SPXTST0000012")
    res = await _decide(api, ctx["admin"], created.json()["approval_request"]["id"], "REJECT")

    assert res.status_code == 200
    assert res.json()["approval_request"]["decided_by"]["display_name"] == "Quản trị"
    state = (await api.get("/api/v1/station/state", headers=ctx["st"])).json()
    assert (state["state"], state["approval_request"]) == ("READY", None)
    package = await orders.find_package(db, "SPXTST0000012")
    assert package is not None
    assert package.warehouse_status == "PACKED"


async def test_repack_approve_after_package_left_packed(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any]
) -> None:
    """API-21 NOT_ELIGIBLE: kiện đã bàn giao trong lúc chờ → không mở phiên, yêu cầu vẫn chờ."""
    await _packed(api, ctx, "SPXTST0000013")
    created = await _request(api, ctx["st"], type="REPACK", tracking_number="SPXTST0000013")
    package = await orders.find_package(db, "SPXTST0000013")
    assert package is not None
    await orders.transition(db, package, "HANDED_OVER", source="PLATFORM")
    await db.flush()

    res = await _decide(api, ctx["sup"], created.json()["approval_request"]["id"], "APPROVE_REPACK")

    assert (res.status_code, res.json()["error"]["code"]) == (409, "NOT_ELIGIBLE")
    assert (
        await _decide(api, ctx["sup"], created.json()["approval_request"]["id"], "REJECT")
    ).status_code == 200


async def test_repack_cancel_keeps_package_packed(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any]
) -> None:
    """TC-03.52, AC-21: duyệt đóng gói lại rồi hủy → kiện PACKED, phiên cũ vẫn COMPLETED."""
    await _packed(api, ctx, "SPXTST0000016")
    created = await _request(api, ctx["st"], type="REPACK", tracking_number="SPXTST0000016")
    await _decide(api, ctx["sup"], created.json()["approval_request"]["id"], "APPROVE_REPACK")
    state = (await api.get("/api/v1/station/state", headers=ctx["st"])).json()

    res = await api.post(
        f"/api/v1/station/sessions/{state['session']['id']}/cancel",
        headers=ctx["st"],
        json={"reason": "WRONG_SCAN"},
    )

    assert res.status_code == 200
    package = await orders.find_package(db, "SPXTST0000016")
    assert package is not None
    assert package.warehouse_status == "PACKED"
    olds = (
        await db.scalars(select(PackSession.status).where(PackSession.open_code == "SPXTST0000016"))
    ).all()
    assert sorted(olds) == ["CANCELLED", "COMPLETED"]


# ---------------------------------------------------------------- khay đổi khi đang chờ (DEC-112)


async def test_tray_change_while_waiting_updates_context(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any], redis_client: Redis, test_settings: Settings
) -> None:
    """D13 khóa / mở "Đóng phiên có ghi chú" theo `context.tray_match` hiện tại; WS `approval.updated`."""
    from aicam.modules.sessions import service as sessions

    station_id = ctx["station_id"]
    opened = await _scan(api, ctx["st"], "SPXTST0000005")
    await write_tray(redis_client, station_id, ("SPXTST0000006",), clock.now(), ttl_s=60)
    await sessions.on_tray_changed(db, station_id, test_settings)
    created = await _request(api, ctx["st"], type="MISMATCH", session_id=opened["state"]["session"]["id"])
    apr_id = created.json()["approval_request"]["id"]
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ws:approvals")
    await pubsub.get_message(timeout=1)

    await write_tray(redis_client, station_id, (), clock.now(), ttl_s=60)  # bỏ phiếu sai
    assert await sessions.on_tray_changed(db, station_id, test_settings) is None  # phiên vẫn chờ duyệt

    msgs = [m for m in await _drain(pubsub) if m["data"]["id"] == apr_id]
    await pubsub.aclose()  # type: ignore[no-untyped-call]
    assert [m["type"] for m in msgs] == ["approval.updated"]
    assert msgs[0]["data"]["context"]["tray_match"] == "NOT_SEEN"
    res = await _decide(api, ctx["sup"], apr_id, "CLOSE_WITH_NOTE", "Đã bỏ phiếu sai")
    assert res.status_code == 200, res.text


# ---------------------------------------------------------------- đồng thời (TC-03.47)

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
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_apr_%'"))
    await dispose_engine()


async def test_two_supervisors_decide_at_once(committed: AsyncEngine, test_settings: Settings) -> None:
    """TC-03.47: hai API-21 song song (CONTINUE vs CANCEL_SESSION) → 200 + 409 ALREADY_RESOLVED."""
    async with sessionmaker()() as db:
        st_user = User(username="tst_apr_station", display_name="S", role="STATION", password_hash="x")
        sup = User(username="tst_apr_sup", display_name="Nguyễn B", role="SUPERVISOR", password_hash="x")
        adm = User(username="tst_apr_admin", display_name="Quản trị", role="ADMIN", password_hash="x")
        db.add_all([st_user, sup, adm])
        await db.flush()
        station = Station(name="TST Apr", account_user_id=st_user.id)
        db.add(station)
        await db.commit()
        ids = (station.id, st_user.id, sup.id, adm.id)

    app = create_app(test_settings)
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()

    def bearer(user_id: uuid.UUID, role: str, station_id: uuid.UUID | None = None) -> Headers:
        token, _ = encode_access_token(test_settings.jwt_secret, user_id, role, station_id, 15)
        return {"Authorization": f"Bearer {token}"}

    st_h = bearer(ids[1], "STATION", ids[0])
    async with AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver") as client:
        opened = await _scan(client, st_h, "SPXTST0000001")
        created = await _request(client, st_h, type="ASSIST", session_id=opened["state"]["session"]["id"])
        apr_id = created.json()["approval_request"]["id"]
        results = await asyncio.gather(
            _decide(client, bearer(ids[2], "SUPERVISOR"), apr_id, "CONTINUE"),
            _decide(client, bearer(ids[3], "ADMIN"), apr_id, "CANCEL_SESSION"),
        )

    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409]
    winner = next(r for r in results if r.status_code == 200).json()["approval_request"]
    loser = next(r for r in results if r.status_code == 409).json()["error"]
    assert loser["code"] == "ALREADY_RESOLVED"
    assert loser["details"]["status"] == "RESOLVED"
    assert loser["details"]["decided_by"]["display_name"] == winner["decided_by"]["display_name"]
    assert loser["details"]["decided_at"] is not None
    async with sessionmaker()() as db:
        pack = await db.get(PackSession, uuid.UUID(opened["state"]["session"]["id"]))
        assert pack is not None
        assert pack.status == ("OPEN" if winner["decision"] == "CONTINUE" else "CANCELLED")


async def test_timer_restarts_after_approval_resolved(
    api: AsyncClient, db: AsyncSession, ctx: dict[str, Any], test_settings: Settings
) -> None:
    """DEC-60 (RB-14): chờ duyệt 40 phút rồi "Cho tiếp tục" → không bỏ dở ngay; tính giờ lại từ lúc duyệt."""
    from datetime import UTC, datetime, timedelta

    from aicam.modules.sessions import service as sessions

    t0 = datetime(2026, 10, 5, 2, 0, tzinfo=UTC)
    clock.freeze(t0)
    st = await _login(api, "tst_station01", "STATION")
    opened = await _scan(api, st, "SPXTST0000005")
    await _request(api, st, type="ASSIST", session_id=opened["state"]["session"]["id"])

    clock.freeze(t0 + timedelta(minutes=40))
    sup = await _login(api, "tst_sup", "DASHBOARD")
    pending = (await api.get("/api/v1/approval-requests", headers=sup)).json()["items"][0]
    res = await _decide(api, sup, pending["id"], "CONTINUE")
    assert res.status_code == 200, res.text

    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 0}
    st = await _login(api, "tst_station01", "STATION")
    state = (await api.get("/api/v1/station/state", headers=st)).json()
    resumed = t0 + timedelta(minutes=40)
    assert datetime.fromisoformat(state["session"]["abandon_at"]) == resumed + timedelta(minutes=30)

    clock.freeze(resumed + timedelta(minutes=15))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 1, "abandoned": 0}
    clock.freeze(resumed + timedelta(minutes=30))
    assert await sessions.check_timeouts(db, test_settings) == {"warned": 0, "abandoned": 1}
