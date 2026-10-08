"""T-206: `lookup.find_everywhere` (BR-32, FR-05.19, DEC-435) + `AMBIGUOUS_SHOP`; nối PACK (API-11), RETURN
(`return_scan.platform_find`), J-05; đo AC-43 bằng mock có độ trễ (NFR-01).

Adapter mock theo shop (`delay_s_by_shop`, `fail_shop`, `orders_by_shop`). **Chưa test với sàn thật.**
"""

import os
import statistics
import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.platforms import lookup, registry, sync
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group
from aicam.modules.sessions import return_scan
from aicam.modules.sessions.models import PackSession, SessionEvent
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration

FAR = datetime.now(UTC) + timedelta(days=30)


class TikTokMock(MockAdapter):
    code = "TIKTOK"


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = False


def _order(sn: str, code: str) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=sn, status="READY_TO_SHIP", tracking_numbers=(code,),
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L"),), status_group=order_group("READY_TO_SHIP"),
    )  # fmt: skip


async def _shop(db: AsyncSession, settings: Settings, psid: str, name: str, platform: str = "SHOPEE") -> Shop:
    shop = Shop(platform=platform, platform_shop_id=psid, name=name)
    platforms.store_credentials(
        shop, ShopCredentials(psid, f"a-{psid}", f"r-{psid}", FAR), Cipher(settings.fernet_key)
    )
    db.add(shop)
    await db.flush()
    return shop


async def _three(db: AsyncSession, settings: Settings) -> tuple[MockAdapter, Shop, Shop, Shop]:
    """AC-43: A chậm 5 giây, B có đơn, C không có."""
    mock = MockAdapter()
    a = await _shop(db, settings, "LKA", "TST A")
    b = await _shop(db, settings, "LKB", "TST B")
    c = await _shop(db, settings, "LKC", "TST C")
    mock.orders_by_shop = {"LKA": {}, "LKB": {}, "LKC": {}}
    mock.put_for_shop("LKB", _order("2410LOOKB0001", "SPXLOOK0000001"))
    mock.delay_s_by_shop = {"LKA": 5.0}
    return mock, a, b, c


# ---------------------------------------------------------------- find_everywhere


async def test_parallel_one_slow_one_found(db: AsyncSession, test_settings: Settings) -> None:
    """BR-32 / AC-43: 3 shop song song, shop chậm bị cắt 2 giây → tổng ~2 giây (không 3 × 2), đúng shop B."""
    mock, _a, b, _c = await _three(db, test_settings)
    started = time.monotonic()
    found = await lookup.find_everywhere(db, "SPXLOOK0000001", test_settings, mock)
    elapsed = time.monotonic() - started
    assert found.shops == 3
    assert found.failed == 1  # A quá hạn
    hit = found.single
    assert hit is not None
    assert (hit.shop_id, hit.platform, hit.shop_name) == (b.id, "SHOPEE", "TST B")
    assert 1.9 <= elapsed < 2.6


async def test_ambiguous_two_shops(db: AsyncSession, test_settings: Settings) -> None:
    mock, a, b, _c = await _three(db, test_settings)
    mock.delay_s_by_shop = {}
    mock.put_for_shop("LKA", _order("2410LOOKA0001", "SPXLOOK0000001"))
    found = await lookup.find_everywhere(db, "spxlook0000001", test_settings, mock)
    assert found.ambiguous
    assert {h.shop_id for h in found.hits} == {a.id, b.id}
    assert found.shops_brief() == [
        {"platform": "SHOPEE", "name": "TST A"},
        {"platform": "SHOPEE", "name": "TST B"},
    ]


async def test_shop_error_does_not_hide_other_shop(db: AsyncSession, test_settings: Settings) -> None:
    """Lỗi sàn ở một shop (quá tần suất hết lượt thử…) chỉ làm shop đó "không thấy"."""
    mock, _a, b, _c = await _three(db, test_settings)
    mock.delay_s_by_shop = {}
    mock.fail_shop = {"LKA", "LKC"}
    found = await lookup.find_everywhere(db, "SPXLOOK0000001", test_settings, mock)
    assert found.failed == 2
    assert found.single is not None
    assert found.single.shop_id == b.id


async def test_tiktok_disabled_not_queried_and_enabled_queried(
    db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC-44: cờ TikTok tắt → quét không tra TikTok; bật → tra cả shop TikTok, chip đúng sàn."""
    tt = TikTokMock()
    tt.orders_by_shop = {"TTA": {}}
    tt.put_for_shop("TTA", _order("5761LOOK0001", "TTLOOK00000001"))
    shopee = MockAdapter()
    real = registry.adapter_for
    monkeypatch.setattr(
        registry, "adapter_for", lambda p, s: tt if p == "TIKTOK" else real(p, s)
    )  # fmt: skip
    shop = await _shop(db, test_settings, "TTA", "TST TikTok A (mock)", platform="TIKTOK")

    found = await lookup.find_everywhere(db, "TTLOOK00000001", test_settings, shopee)
    assert found.hits == []
    assert tt.shop_calls == []

    test_settings.tiktok_enabled = True
    found = await lookup.find_everywhere(db, "TTLOOK00000001", test_settings, shopee)
    assert found.single is not None
    assert (found.single.platform, found.single.shop_id) == ("TIKTOK", shop.id)


async def test_no_shop_mock_keeps_phase2_behavior(db: AsyncSession, test_settings: Settings) -> None:
    """Mock Shopee chưa có shop (dev / test Phase 1–2) → một đích không shop, kể cả `SHOPEE_ENABLED=false`."""
    test_settings.shopee_enabled = False
    mock = MockAdapter()
    found = await lookup.find_everywhere(db, "SPXTST0000001", test_settings, mock)
    assert found.single is not None
    assert found.single.shop_id is None


# ---------------------------------------------------------------- API-11 PACK


async def _login(api: AsyncClient, username: str, client: str = "DASHBOARD") -> dict[str, str]:
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _scan(api: AsyncClient, headers: dict[str, str], code: str) -> Response:
    return await api.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )


def _use(api: AsyncClient, mock: MockAdapter) -> None:
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]


async def test_scan_ambiguous_opens_unverified_with_flag_and_timeline(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """FR-05.19 / TC-05.93: mã có ở 2 shop → phiên mở chưa xác minh + cờ `AMBIGUOUS_SHOP`, không gắn đơn;
    sự kiện phiên + API-31 `timeline[].shops` liệt kê shop (DEC-561)."""
    mock, _a, _b, _c = await _three(db, test_settings)
    mock.delay_s_by_shop = {}
    mock.put_for_shop("LKA", _order("2410LOOKA0001", "SPXLOOK0000001"))
    _use(api, mock)
    user, _st = await make_station_account(db, "tst_lk1", "TST Station LK1")
    headers = await _login(api, user.username, "STATION")

    body = (await _scan(api, headers, "SPXLOOK0000001")).json()

    assert body["outcome"] == "SESSION_OPENED"
    assert body["state"]["session"]["package"]["order"] is None
    pack = await db.scalar(select(PackSession).order_by(PackSession.started_at.desc()).limit(1))
    assert pack is not None
    assert set(pack.flags) >= {"UNVERIFIED", "AMBIGUOUS_SHOP"}
    event = await db.scalar(
        select(SessionEvent).where(SessionEvent.session_id == pack.id, SessionEvent.type == "AMBIGUOUS_SHOP")
    )
    assert event is not None
    assert event.payload is not None
    assert [s["name"] for s in event.payload["shops"]] == ["TST A", "TST B"]
    assert await db.scalar(select(Order).where(Order.platform_order_sn.like("2410LOOK%"))) is None

    admin = await make_user(db, "tst_lk_admin", "ADMIN")
    detail = await api.get(f"/api/v1/packages/{pack.package_id}", headers=await _login(api, admin.username))
    assert detail.status_code == 200
    timeline = detail.json()["timeline"]
    rows = [t for t in timeline if t.get("shops")]
    assert len(rows) == 1
    assert rows[0]["shops"] == [
        {"platform": "SHOPEE", "name": "TST A"},
        {"platform": "SHOPEE", "name": "TST B"},
    ]
    assert (rows[0]["source"], rows[0]["to_status"], rows[0]["actor"]) == (
        "WAREHOUSE",
        "PACKING",
        "TST Station LK1",
    )
    assert sum(1 for t in timeline if t["shops"] is None) == len(timeline) - 1


async def test_scan_single_shop_attaches_right_shop_within_3s(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """AC-43: 3 shop (A chậm 5 giây, B có đơn) → mở phiên gắn đơn shop B, mỗi lần quét ≤ 3 giây."""
    mock, _a, b, _c = await _three(db, test_settings)
    for n in range(2, 4):
        mock.put_for_shop("LKB", _order(f"2410LOOKB000{n}", f"SPXLOOK000000{n}"))
    _use(api, mock)
    user, _st = await make_station_account(db, "tst_lk2", "TST Station LK2")
    headers = await _login(api, user.username, "STATION")
    durations = []
    for n in range(1, 4):
        started = time.monotonic()
        body = (await _scan(api, headers, f"SPXLOOK000000{n}")).json()
        durations.append(time.monotonic() - started)
        assert body["outcome"] == "SESSION_OPENED"
        assert body["state"]["session"]["package"]["order"]["shop_name"] == "TST B"
        assert body["state"]["session"]["flags"] == []
        pack = await db.scalar(select(PackSession).order_by(PackSession.started_at.desc()).limit(1))
        assert pack is not None
        pack.status = "CANCELLED"  # giải phóng station cho lần quét kế
        await db.flush()
    assert max(durations) <= 3.0
    order = await db.scalar(select(Order).where(Order.platform_order_sn == "2410LOOKB0001"))
    assert order is not None
    assert order.shop_id == b.id


# ---------------------------------------------------------------- RETURN + J-05


async def test_return_platform_find_order_sn_on_two_shops_upserts_both(
    db: AsyncSession, test_settings: Settings
) -> None:
    """Bàn hoàn quét mã đơn có ở 2 shop (BR-29): ghi cả hai đơn vào đúng shop (để `resolve_code` cho chọn —
    T-271 / T-288); trả đơn đầu tiên."""
    mock, a, b, _c = await _three(db, test_settings)
    mock.delay_s_by_shop = {}
    mock.put_for_shop("LKA", _order("2410LOOKDUP01", "SPXLOOKA000009"))
    mock.put_for_shop("LKB", _order("2410LOOKDUP01", "SPXLOOKB000009"))
    first = await return_scan.platform_find(db, "2410LOOKDUP01", mock, test_settings)
    assert first is not None
    rows = (await db.scalars(select(Order).where(Order.platform_order_sn == "2410LOOKDUP01"))).all()
    assert {o.shop_id for o in rows} == {a.id, b.id}


async def test_return_platform_find_ambiguous_tracking_writes_nothing(
    db: AsyncSession, test_settings: Settings
) -> None:
    mock, _a, _b, _c = await _three(db, test_settings)
    mock.delay_s_by_shop = {}
    mock.put_for_shop("LKA", _order("2410LOOKA0002", "SPXLOOK0000001"))
    assert await return_scan.platform_find(db, "SPXLOOK0000001", mock, test_settings) is None
    assert await db.scalar(select(Order).where(Order.platform_order_sn.like("2410LOOK%"))) is None


async def test_j05_uses_parallel_lookup(db: AsyncSession, test_settings: Settings) -> None:
    """J-05: mỗi kiện tra song song mọi shop; đúng 1 shop → gắn đơn shop đó; ≥ 2 → giữ chưa xác minh."""
    mock, _a, b, _c = await _three(db, test_settings)
    mock.delay_s_by_shop = {}
    mock.put_for_shop("LKA", _order("2410LOOKA0003", "SPXLOOK0000003"))
    mock.put_for_shop("LKB", _order("2410LOOKB0003", "SPXLOOK0000003"))
    one = await orders.create_unverified_package(db, "SPXLOOK0000001")
    two = await orders.create_unverified_package(db, "SPXLOOK0000003")
    out = await sync.verify_unverified(db, mock, test_settings)
    assert out == {"checked": 2, "verified": 1}
    await db.refresh(one)
    await db.refresh(two)
    assert one.verified is True
    order = await db.get(Order, one.order_id)
    assert order is not None
    assert order.shop_id == b.id
    assert two.verified is False


# ---------------------------------------------------------------- đo AC-43 (RUN_PERF=1)


@pytest.mark.skipif(os.environ.get("RUN_PERF") != "1", reason="đặt RUN_PERF=1 để đo AC-43 (100 lần quét)")
async def test_perf_ac43_100_scans(api: AsyncClient, db: AsyncSession, test_settings: Settings) -> None:
    """AC-43 / NFR-01: 100 lần quét mã lạ qua API-11 với 3 shop mock (1 chậm 5 giây, 1 có đơn) → p95 ≤ 3 giây.
    Máy dev, không phải máy kho (02a §8)."""
    mock, _a, _b, _c = await _three(db, test_settings)
    for n in range(1, 101):
        mock.put_for_shop("LKB", _order(f"2410PERF{n:05d}", f"SPXPERF{n:07d}"))
    _use(api, mock)
    user, _st = await make_station_account(db, "tst_lkp", "TST Station LKP")
    headers = await _login(api, user.username, "STATION")
    durations: list[float] = []
    for n in range(1, 101):
        started = time.monotonic()
        body = (await _scan(api, headers, f"SPXPERF{n:07d}")).json()
        durations.append(time.monotonic() - started)
        assert body["state"]["session"]["package"]["order"]["shop_name"] == "TST B"
        pack = await db.scalar(select(PackSession).order_by(PackSession.started_at.desc()).limit(1))
        assert pack is not None
        pack.status = "CANCELLED"
        await db.flush()
    durations.sort()
    p50, p95 = statistics.median(durations), durations[94]
    print(f"AC-43 100 scans: p50={p50:.3f}s p95={p95:.3f}s max={durations[-1]:.3f}s")
    assert p95 <= 3.0
    assert await db.scalar(select(Package).where(Package.tracking_number == "SPXPERF0000001")) is not None
