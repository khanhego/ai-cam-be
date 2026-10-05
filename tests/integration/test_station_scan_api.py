"""API-10, API-11 — 02 §6.2, 02a §4.1; TC-03.xx trong 04-test-cases."""

import json
import math
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from httpx import AsyncClient, Response
from redis.asyncio import Redis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings, get_settings
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.orders import service as orders
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.sessions.models import PackSession, SessionEvent
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.sessions.tray import tray_key

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

Scan = Callable[..., Awaitable[Response]]


@pytest.fixture
def adapter(api: AsyncClient) -> MockAdapter:
    mock = MockAdapter()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


async def _station_headers(
    api: AsyncClient, db: AsyncSession, n: int = 1
) -> tuple[dict[str, str], uuid.UUID]:
    user, station = await make_station_account(db, f"tst_station0{n}", f"TST Station 0{n}")
    res = await api.post(
        "/api/v1/auth/login", json={"username": user.username, "password": PASSWORD, "client": "STATION"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}, station.id


@pytest.fixture
async def station(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter
) -> tuple[dict[str, str], uuid.UUID]:
    return await _station_headers(api, db)


@pytest.fixture
def scan(api: AsyncClient, station: tuple[dict[str, str], uuid.UUID]) -> Scan:
    headers, _ = station

    async def _scan(
        code: str, client_scan_id: str | None = None, hdrs: dict[str, str] | None = None
    ) -> Response:
        return await api.post(
            "/api/v1/station/scan",
            headers=hdrs or headers,
            json={"code": code, "client_scan_id": client_scan_id or str(uuid.uuid4())},
        )

    return _scan


async def _set_tray(redis: Redis, station_id: uuid.UUID, *codes: str) -> None:
    await redis.set(
        tray_key(station_id), json.dumps({"codes": list(codes), "updated_at": clock.now().isoformat()}), ex=60
    )


def _session(body: dict[str, Any]) -> dict[str, Any]:
    return body["state"]["session"]  # type: ignore[no-any-return]


# ---------------------------------------------------------------- trạng thái + luồng chính


async def test_initial_state_is_ready(api: AsyncClient, station: tuple[dict[str, str], uuid.UUID]) -> None:
    headers, station_id = station

    res = await api.get("/api/v1/station/state", headers=headers)

    assert res.status_code == 200
    body = res.json()
    assert body["station"] == {"id": str(station_id), "name": "TST Station 01"}
    assert body["state"] == "READY"
    assert body["session"] is None
    assert body["tray"]["match"] == "UNAVAILABLE"
    assert body["today_count"] == 0


async def test_open_then_close(scan: Scan, db: AsyncSession) -> None:
    """TC-03.01, TC-03.02, AC-01: mã chưa có → tra sàn (mock) → mở; quét lại → đóng."""
    opened = await scan("spxtst0000012")

    assert opened.status_code == 200
    body = opened.json()
    assert body["outcome"] == "SESSION_OPENED"
    assert body["state"]["state"] == "PACKING"
    session = _session(body)
    assert session["package"]["tracking_number"] == "SPXTST0000012"
    assert session["package"]["order"]["platform_order_sn"] == "2410TST00012"
    assert [i["quantity"] for i in session["package"]["items"]] == [2, 1, 1]
    assert session["flags"] == []

    closed = await scan("SPXTST0000012")

    assert closed.json()["outcome"] == "SESSION_COMPLETED"
    assert closed.json()["state"]["state"] == "READY"
    assert closed.json()["state"]["today_count"] == 1
    package = await orders.find_package(db, "SPXTST0000012")
    assert package is not None
    assert package.warehouse_status == "PACKED"
    pack = await db.scalar(select(PackSession).where(PackSession.package_id == package.id))
    assert pack is not None
    assert pack.status == "COMPLETED"
    assert "CAM2_UNVERIFIED" in pack.flags  # không có dữ liệu Cam 2 (BR-18)


async def test_scan_mismatch_then_fix(scan: Scan) -> None:
    """TC-03.04, TC-03.05, BR-05."""
    await scan("SPXTST0000001")

    wrong = await scan("SPXTST0000002")
    fixed = await scan("SPXTST0000001")

    assert wrong.json()["outcome"] == "MISMATCH"
    assert wrong.json()["state"]["state"] == "MISMATCH"
    assert _session(wrong.json())["mismatch"] == {
        "source": "SCAN",
        "expected": "SPXTST0000001",
        "actual": "SPXTST0000002",
    }
    assert fixed.json()["outcome"] == "SESSION_COMPLETED"


async def test_mismatch_does_not_open_new_session(scan: Scan, db: AsyncSession) -> None:
    """TC-03.07."""
    await scan("SPXTST0000001")
    await scan("SPXTST0000002")

    again = await scan("SPXTST0000003")

    assert again.json()["outcome"] == "MISMATCH"
    assert _session(again.json())["mismatch"]["actual"] == "SPXTST0000003"
    assert await db.scalar(select(func.count()).select_from(PackSession)) == 1


# ---------------------------------------------------------------- cảnh báo


async def test_cancelled_order(scan: Scan, db: AsyncSession) -> None:
    """TC-03.08, BR-01, AC-05."""
    res = await scan("SPXTST0000009")

    assert res.json()["outcome"] == "ALERT"
    assert res.json()["alert"]["code"] == "ORDER_CANCELLED"
    assert res.json()["state"]["state"] == "READY"
    assert await db.scalar(select(func.count()).select_from(PackSession)) == 0


async def test_already_packed(scan: Scan) -> None:
    """TC-03.09, BR-03."""
    await scan("SPXTST0000010")
    await scan("SPXTST0000010")

    res = await scan("SPXTST0000010")

    alert = res.json()["alert"]
    assert alert["code"] == "ALREADY_PACKED"
    assert alert["data"]["station_name"] == "TST Station 01"
    assert alert["data"]["can_request_repack"] is True
    assert alert["data"]["packed_at"]
    # Review M1 #18: kèm giờ đóng theo giờ VN.
    packed_at = clock.now().astimezone(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%H:%M")
    assert alert["message"] == f"SPXTST0000010 đã đóng gói lúc {packed_at} tại TST Station 01."


async def test_already_handed_over(scan: Scan, db: AsyncSession) -> None:
    """TC-03.10, AC-21."""
    await scan("SPXTST0000011")
    await scan("SPXTST0000011")
    package = await orders.find_package(db, "SPXTST0000011")
    assert package is not None
    await orders.transition(db, package, "HANDED_OVER", source="PLATFORM")
    await db.flush()

    res = await scan("SPXTST0000011")

    assert res.json()["alert"]["code"] == "ALREADY_HANDED_OVER"


async def test_cancelled_after_pack_is_order_cancelled(scan: Scan, db: AsyncSession) -> None:
    """TC-03.11, BR-01, EX-P10: kiện đã đóng rồi sàn hủy (CANCELLED_AFTER_PACK) → ALERT ORDER_CANCELLED,
    không phải ALREADY_PACKED / ALREADY_HANDED_OVER; không mở phiên mới."""
    await scan("SPXTST0000010")
    await scan("SPXTST0000010")
    package = await orders.find_package(db, "SPXTST0000010")
    assert package is not None
    assert await orders.apply_platform_cancel(db, package)
    await db.flush()
    assert package.warehouse_status == "CANCELLED_AFTER_PACK"

    res = await scan("SPXTST0000010")

    body = res.json()
    assert res.status_code == 200
    assert body["outcome"] == "ALERT"
    assert body["alert"]["code"] == "ORDER_CANCELLED"
    assert body["state"]["state"] == "READY"
    assert await db.scalar(select(func.count()).select_from(PackSession)) == 1


async def test_invalid_code(scan: Scan) -> None:
    """TC-03.14."""
    res = await scan("abc!!12345")

    assert res.json()["outcome"] == "ALERT"
    assert res.json()["alert"]["code"] == "INVALID_CODE"


async def test_package_open_at_other_station(api: AsyncClient, db: AsyncSession, scan: Scan) -> None:
    """TC-03.15."""
    await scan("SPXTST0000013")
    other_headers, _ = await _station_headers(api, db, 2)

    res = await scan("SPXTST0000013", hdrs=other_headers)

    assert res.json()["alert"]["code"] == "PACKED_ELSEWHERE_IN_PROGRESS"


# ---------------------------------------------------------------- BR-04 tra sàn


async def test_unknown_code_opens_unverified(scan: Scan, db: AsyncSession) -> None:
    """TC-03.12/13: sàn không có mã → mở phiên, cờ UNVERIFIED, không có sản phẩm."""
    res = await scan("SPXTST9990001")

    session = _session(res.json())
    assert res.json()["outcome"] == "SESSION_OPENED"
    assert session["flags"] == ["UNVERIFIED"]
    assert session["package"]["order"] is None
    assert session["package"]["items"] == []


async def test_slow_platform_times_out(
    api: AsyncClient, adapter: MockAdapter, scan: Scan, test_settings: Settings
) -> None:
    """BR-04: sàn chậm hơn ngưỡng → không chờ, mở phiên chưa xác minh."""
    adapter.delay_s = 0.5
    fast = test_settings.model_copy(update={"platform_lookup_timeout_s": 0.1})
    api._transport.app.dependency_overrides[get_settings] = lambda: fast  # type: ignore[attr-defined]

    res = await scan("SPXTST0000014")

    assert res.json()["outcome"] == "SESSION_OPENED"
    assert _session(res.json())["flags"] == ["UNVERIFIED"]


# ---------------------------------------------------------------- DEC-29


async def test_retry_same_client_scan_id(scan: Scan, db: AsyncSession) -> None:
    """TC-03.17."""
    scan_id = str(uuid.uuid4())

    first = await scan("SPXTST0000015", scan_id)
    retry = await scan("SPXTST0000015", scan_id)

    assert first.json()["outcome"] == retry.json()["outcome"] == "SESSION_OPENED"
    assert retry.json()["state"]["state"] == "PACKING"
    events = await db.scalar(
        select(func.count()).select_from(SessionEvent).where(SessionEvent.type == "SCAN_OPEN")
    )
    assert events == 1


# ---------------------------------------------------------------- Cam 2 (BR-06, BR-18)


async def test_tray_different_blocks_close_even_with_correct_scan(
    scan: Scan, redis_client: Redis, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """Review #2: khay 790, phiên 789, quét 789 hai lần → cả hai lần MISMATCH nguồn CAM2."""
    _, station_id = station
    await scan("SPXTST0000016")
    await _set_tray(redis_client, station_id, "SPXTST0000017")

    first = await scan("SPXTST0000016")
    second = await scan("SPXTST0000016")

    for res in (first, second):
        assert res.json()["outcome"] == "MISMATCH"
        assert _session(res.json())["mismatch"]["source"] == "CAM2"
        assert _session(res.json())["mismatch"]["actual"] == "SPXTST0000017"

    await _set_tray(redis_client, station_id)
    done = await scan("SPXTST0000016")
    assert done.json()["outcome"] == "SESSION_COMPLETED"


async def test_two_labels_on_tray_is_mismatch(
    scan: Scan, redis_client: Redis, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    _, station_id = station
    await scan("SPXTST0000018")
    await _set_tray(redis_client, station_id, "SPXTST0000018", "SPXTST0000019")

    res = await scan("SPXTST0000018")

    assert res.json()["outcome"] == "MISMATCH"
    assert res.json()["state"]["tray"]["match"] == "MULTIPLE"


async def test_cam2_verified_flow_has_no_cam2_flags(
    scan: Scan, redis_client: Redis, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """Luồng chuẩn: phiếu trên khay khớp, gỡ dán, khay trống → đóng không cờ Cam 2."""
    _, station_id = station
    await _set_tray(redis_client, station_id, "SPXTST0000020")
    opened = await scan("SPXTST0000020")
    assert opened.json()["state"]["tray"]["match"] == "MATCH"
    await _set_tray(redis_client, station_id)

    done = await scan("SPXTST0000020")

    assert done.json()["outcome"] == "SESSION_COMPLETED"
    pack = await db.scalar(select(PackSession).where(PackSession.open_code == "SPXTST0000020"))
    assert pack is not None
    assert "CAM2_UNVERIFIED" not in pack.flags
    assert "LABEL_ON_TRAY" not in pack.flags
    assert "HAD_MISMATCH" not in pack.flags


async def test_label_still_on_tray_flag(
    scan: Scan, redis_client: Redis, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """TC-03.26."""
    _, station_id = station
    await _set_tray(redis_client, station_id, "SPXTST0000021")
    await scan("SPXTST0000021")

    await scan("SPXTST0000021")

    pack = await db.scalar(select(PackSession).where(PackSession.open_code == "SPXTST0000021"))
    assert pack is not None
    assert "LABEL_ON_TRAY" in pack.flags
    assert "CAM2_UNVERIFIED" not in pack.flags


# ---------------------------------------------------------------- chờ duyệt, quyền


async def test_scan_ignored_while_waiting_approval(
    scan: Scan, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """TC-03.50."""
    _, station_id = station
    db.add(ApprovalRequest(station_id=station_id, tracking_number="SPXTST0000010", type="REPACK"))
    await db.flush()

    res = await scan("SPXTST0000022")

    assert res.json()["outcome"] == "IGNORED"
    assert res.json()["state"]["state"] == "WAITING_APPROVAL"
    assert res.json()["state"]["approval_request"]["tracking_number"] == "SPXTST0000010"


async def test_invalid_code_ignored_while_waiting_approval(
    scan: Scan, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """Review M1 #20: đang chờ duyệt thì mọi lần quét đều IGNORED, kể cả mã sai định dạng."""
    _, station_id = station
    db.add(ApprovalRequest(station_id=station_id, tracking_number="SPXTST0000010", type="REPACK"))
    await db.flush()

    res = await scan("abc!!12345")

    assert (res.json()["outcome"], res.json()["alert"]) == ("IGNORED", None)


async def test_station_moved_to_other_account_rejects_old_token(
    api: AsyncClient, db: AsyncSession, scan: Scan, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """Review M1 #13: Admin gán station cho tài khoản khác → token cũ bị chặn ngay, không chờ hết hạn."""
    headers, station_id = station
    from aicam.modules.stations.models import Station

    other = await make_user(db, "tst_station09", role="STATION")
    row = await db.get(Station, station_id)
    assert row is not None
    row.account_user_id = other.id
    await db.flush()

    res = await api.get("/api/v1/station/state", headers=headers)

    assert res.status_code == 403
    assert res.json()["error"]["code"] == "FORBIDDEN"


async def test_dashboard_account_cannot_scan(
    api: AsyncClient, db: AsyncSession, adapter: MockAdapter
) -> None:
    await make_user(db, "tst_admin")
    login = await api.post(
        "/api/v1/auth/login", json={"username": "tst_admin", "password": PASSWORD, "client": "DASHBOARD"}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    res = await api.post(
        "/api/v1/station/scan",
        headers=headers,
        json={"code": "SPXTST0000001", "client_scan_id": str(uuid.uuid4())},
    )

    assert res.status_code == 403


async def test_inactive_station_rejected(
    api: AsyncClient, db: AsyncSession, scan: Scan, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    headers, station_id = station
    from aicam.modules.stations.models import Station

    row = await db.get(Station, station_id)
    assert row is not None
    row.is_active = False
    await db.flush()

    res = await api.get("/api/v1/station/state", headers=headers)

    assert res.status_code == 409
    assert res.json()["error"]["code"] == "STATION_INACTIVE"


async def test_state_shows_warn_and_abandon_times(scan: Scan) -> None:
    res = await scan("SPXTST0000023")

    session = _session(res.json())
    assert session["warn_at"] > session["started_at"]
    assert session["abandon_at"] > session["warn_at"]


@pytest.fixture
def vn_after_midnight() -> None:
    """01:00 giờ VN ngày 05/10 = 18:00 UTC ngày 04/10. Freeze trước khi fixture `scan` đăng nhập."""
    from datetime import UTC, datetime

    clock.freeze(datetime(2026, 10, 4, 18, 0, tzinfo=UTC))


async def test_report_updated_uses_vietnam_date(
    vn_after_midnight: None, scan: Scan, redis_client: Redis
) -> None:
    """Review M1 #7: dashboard nhận ngày theo giờ VN, không theo UTC."""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("ws:dashboard")
    await pubsub.get_message(timeout=1)

    await scan("SPXTST0000014")
    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
    await pubsub.aclose()  # type: ignore[no-untyped-call]

    assert msg is not None
    event = json.loads(msg["data"])
    assert (event["type"], event["data"]) == ("report.updated", {"date": "2026-10-05"})


# ---------------------------------------------------------------- G3-P2-6: tra sàn lỗi không làm quét 500


async def test_unreadable_shop_token_does_not_break_scan(scan: Scan, db: AsyncSession) -> None:
    """Token shop mã hóa bằng FERNET_KEY khác (khôi phục sai `.env`) → quét mã lạ vẫn 200 SESSION_OPENED /
    UNVERIFIED; shop chuyển EXPIRED + last_error CREDENTIALS_UNREADABLE."""
    from cryptography.fernet import Fernet
    from sqlalchemy import select

    from aicam.core import clock
    from aicam.core.security import Cipher
    from aicam.modules.orders.models import Shop

    other = Cipher(Fernet.generate_key().decode())
    shop = Shop(platform="SHOPEE", platform_shop_id="990777", auth_status="CONNECTED",
                access_token_enc=other.encrypt("a"), refresh_token_enc=other.encrypt("r"),
                auth_expires_at=clock.now() + timedelta(hours=3))  # fmt: skip
    db.add(shop)
    await db.flush()

    res = await scan("SPXTST9990077")

    assert res.status_code == 200, res.text
    assert res.json()["outcome"] == "SESSION_OPENED"
    assert _session(res.json())["flags"] == ["UNVERIFIED"]
    reloaded = await db.scalar(
        select(Shop).where(Shop.id == shop.id).execution_options(populate_existing=True)
    )
    assert reloaded is not None
    assert reloaded.auth_status == "EXPIRED"
    assert reloaded.last_error is not None
    assert reloaded.last_error["code"] == "CREDENTIALS_UNREADABLE"


async def test_unexpected_lookup_error_falls_back_to_unverified(adapter: MockAdapter, scan: Scan) -> None:
    async def boom(*_: Any) -> Any:
        raise RuntimeError("adapter hỏng")

    adapter.find_by_tracking = boom  # type: ignore[method-assign]
    res = await scan("SPXTST0000014")
    assert res.status_code == 200, res.text
    assert _session(res.json())["flags"] == ["UNVERIFIED"]


# ---------------------------------------------------------------- NFR-01 (TC-N.02)


async def test_tc_n02_twenty_slow_platform_lookups(adapter: MockAdapter, scan: Scan) -> None:
    """TC-N.02, NFR-01, BR-04: 20 lần quét mã phải tra sàn, `MockAdapter(delay_s=1.5)` → p95 ≤ 3 giây.

    10 mã có trên sàn mock (tra xong trong 1,5 giây < ngưỡng cắt 2 giây → đã xác minh, có sản phẩm) và 10 mã
    không có trên sàn (→ cờ `UNVERIFIED`, không sản phẩm). Mỗi phiên được đóng ngay (quét lại, không tra sàn)
    để lần quét sau mở phiên mới.
    """
    adapter.delay_s = 1.5
    known = [f"SPXTST{n:07d}" for n in (1, 2, 3, 4, 5, 6, 7, 8, 12, 13)]
    unknown = [f"SPXTST999{n:04d}" for n in range(101, 111)]
    durations: list[float] = []
    for code in [c for pair in zip(known, unknown, strict=True) for c in pair]:
        started = time.perf_counter()
        res = await scan(code)
        durations.append(time.perf_counter() - started)

        assert res.status_code == 200, res.text
        body = res.json()
        assert body["outcome"] == "SESSION_OPENED", (code, body)
        session = _session(body)
        if code in known:
            assert "UNVERIFIED" not in session["flags"], code
            assert session["package"]["items"], code
        else:
            assert session["flags"] == ["UNVERIFIED"], code
            assert session["package"]["items"] == [], code
        assert (await scan(code)).json()["outcome"] == "SESSION_COMPLETED", code

    p95 = sorted(durations)[math.ceil(0.95 * len(durations)) - 1]
    print(f"TC-N.02: n={len(durations)} min={min(durations):.3f}s p95={p95:.3f}s max={max(durations):.3f}s")
    assert len(durations) == 20
    assert max(durations) <= 3.0
    assert p95 <= 3.0
