"""T-211: mock TikTok 2 shop (adapter thật + transport giả, fixture định dạng TikTok) + mock Shopee nhiều
shop + `seed_phase3` — AC-40 (4 shop, kết nối không ngắt nhau, dữ liệu đúng shop), AC-41 (9 trạng thái + lạ),
AC-42 (6 kịch bản trả, đồng hồ giả), AC-43 (thử lại 429 có log), NFR-39 (1 shop lỗi), EX-T2, EX-T5, kiện gộp.
"""

from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.settings import Settings
from aicam.entrypoints.seed_phase3 import seed_phase3
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package, PackageOrder, Shop
from aicam.modules.platforms import sync
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.mock.tiktok import MockTikTokAdapter
from aicam.modules.platforms.router import get_platform_adapter, get_tiktok_adapter
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.reconciliation.service import run_rules
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.settings.models import Setting

from .factories import PASSWORD, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 2, 1, 0, tzinfo=UTC)


async def _no_sleep(_: float) -> None:
    return None


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)  # gần mốc dữ liệu mock Shopee Phase 1–2 (01/10) để J-04 lần đầu lấy được
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = True
    test_settings.tiktok_returns_enabled = True
    test_settings.tiktok_adapter = "mock"


@pytest.fixture
def shopee() -> MockAdapter:
    return MockAdapter.multi_shop(["990001", "990002"])


@pytest.fixture
def tiktok() -> MockTikTokAdapter:
    return MockTikTokAdapter(sleep=_no_sleep)


async def _admin(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    await make_user(db, "tst_admin11", "ADMIN")
    res = await api.post(
        "/api/v1/auth/login", json={"username": "tst_admin11", "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _follow(api: AsyncClient, admin: dict[str, str], platform: str) -> str:
    res = await api.post(f"/api/v1/shops/{platform}/auth-url", headers=admin)
    assert res.status_code == 200, res.text
    url = urlparse(res.json()["url"])
    out = await api.get(f"{url.path}?{url.query}")
    return str(out.headers["location"])


async def _shop(db: AsyncSession, psid: str) -> Shop:
    shop = await db.scalar(select(Shop).where(Shop.platform_shop_id == psid))
    assert shop is not None
    return shop


async def _connect_all(
    api: AsyncClient, db: AsyncSession, shopee: MockAdapter, tiktok: MockTikTokAdapter
) -> dict[str, Shop]:
    app = api._transport.app  # type: ignore[attr-defined]
    app.dependency_overrides[get_platform_adapter] = lambda: shopee
    app.dependency_overrides[get_tiktok_adapter] = lambda: tiktok
    admin = await _admin(api, db)
    assert (await _follow(api, admin, "shopee")).endswith("platform=shopee&result=connected&count=1")
    assert (await _follow(api, admin, "shopee")).endswith("platform=shopee&result=connected&count=1")
    assert (await _follow(api, admin, "tiktok")).endswith("platform=tiktok&result=connected&count=2")
    return {p: await _shop(db, p) for p in ("990001", "990002", "TTMOCKA", "TTMOCKB")}


async def test_ac40_four_shops_connect_and_sync(
    api: AsyncClient,
    db: AsyncSession,
    shopee: MockAdapter,
    tiktok: MockTikTokAdapter,
    test_settings: Settings,
) -> None:
    """AC-40: 2 Shopee + 2 TikTok mock cùng `CONNECTED` (kết nối shop sau không ngắt shop trước); mỗi shop
    đồng bộ đơn của nó; mã đơn trùng `2410DUP00001` → 2 đơn ở 2 shop; EX-T2 → cảnh báo, kiện giữ shop cũ."""
    shops = await _connect_all(api, db, shopee, tiktok)
    assert {s.auth_status for s in shops.values()} == {"CONNECTED"}
    assert shops["990002"].name == "TST B"
    assert (shops["TTMOCKA"].name, shops["TTMOCKA"].grant_ref) == ("TST TikTok A (mock)", "MOCK-OPEN-1")
    assert shops["TTMOCKB"].shop_cipher == "MOCKCIPHER-TTMOCKB"
    for psid, adapter in (("990001", shopee), ("990002", shopee), ("TTMOCKA", tiktok), ("TTMOCKB", tiktok)):
        out = await sync.sync_orders(db, adapter, test_settings, shops[psid].id)
        assert out[str(shops[psid].id)]["status"] == "OK", (psid, out)
    dup = (await db.scalars(select(Order).where(Order.platform_order_sn == "2410DUP00001"))).all()
    assert {o.shop_id for o in dup} == {shops["990002"].id, shops["TTMOCKA"].id}
    tst10 = await orders.find_package(db, "SPXTST0000010")
    assert tst10 is not None
    owner = await db.get(Order, tst10.order_id)
    assert owner is not None
    assert owner.shop_id == shops["990001"].id  # EX-T2: kiện vẫn thuộc đơn shop 990001
    await db.refresh(shops["TTMOCKB"])
    assert shops["TTMOCKB"].sync_warnings[0]["code"] == "TRACKING_OWNED_BY_OTHER_SHOP"
    count_b = await db.scalar(
        select(func.count()).select_from(Order).where(Order.shop_id == shops["990002"].id)
    )
    assert count_b == 22
    # API-70: 4 shop, cấu hình TikTok mock.
    body = (await api.get("/api/v1/shops", headers=await _admin_headers(api))).json()
    assert len(body["items"]) == 4
    assert {p["platform"]: p["configured"] for p in body["platforms"]} == {"SHOPEE": True, "TIKTOK": True}


async def _admin_headers(api: AsyncClient) -> dict[str, str]:
    res = await api.post(
        "/api/v1/auth/login", json={"username": "tst_admin11", "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def test_ac41_tiktok_statuses_merged_fbt(
    api: AsyncClient,
    db: AsyncSession,
    shopee: MockAdapter,
    tiktok: MockTikTokAdapter,
    test_settings: Settings,
) -> None:
    """AC-41: 9 trạng thái TikTok → đúng nhóm, `XYZ` → `UNKNOWN`; kiện gộp `TTTST0000000077` 2 đơn; đơn kho
    TikTok `TTTST0000000098` bị bỏ qua (EX-T5)."""
    shops = await _connect_all(api, db, shopee, tiktok)
    out = await sync.sync_orders(db, tiktok, test_settings, shops["TTMOCKA"].id)
    assert out[str(shops["TTMOCKA"].id)]["skipped"] == 1
    rows = {
        o.platform_order_sn: o.platform_status_group
        for o in (await db.scalars(select(Order).where(Order.shop_id == shops["TTMOCKA"].id))).all()
    }
    assert [rows[f"5761TT00000000{i}"] for i in range(11, 20)] == [
        "UNPAID", "UNPAID", "AWAITING_SHIPMENT", "AWAITING_SHIPMENT", "AWAITING_SHIPMENT", "SHIPPED",
        "DELIVERED", "DELIVERED", "CANCELLED",
    ]  # fmt: skip
    assert rows["5761TT0000000099"] == "UNKNOWN"
    assert "5761TT0000000098" not in rows
    assert await orders.find_package(db, "TTTST0000000098") is None
    assert await orders.find_package(db, "TTTST0000000011") is None  # chưa thanh toán: chưa có kiện
    merged = await orders.find_package(db, "TTTST0000000077")
    assert merged is not None
    links = (await db.scalars(select(PackageOrder).where(PackageOrder.package_id == merged.id))).all()
    assert len(links) == 1
    cancelled = await orders.find_package(db, "TTTST0000000019")
    assert cancelled is not None
    assert cancelled.warehouse_status == "CANCELLED"


async def test_shopee_b_cancel_request_then_rejected(
    api: AsyncClient,
    db: AsyncSession,
    shopee: MockAdapter,
    tiktok: MockTikTokAdapter,
    test_settings: Settings,
) -> None:
    """Mock Shopee 990002: `2410TSTB0015` lượt 1 `IN_CANCEL` (chặn quét, kiện vẫn `NEW`) → lượt 2
    `READY_TO_SHIP` (từ chối hủy — BR-21, AC-41)."""
    shops = await _connect_all(api, db, shopee, tiktok)
    await sync.sync_orders(db, shopee, test_settings, shops["990002"].id)
    package = await orders.find_package(db, "SPXTSTB000000015")
    assert package is not None
    assert package.warehouse_status == "NEW"
    assert await orders.is_cancelled(db, package)
    clock.advance(timedelta(minutes=5))
    await sync.sync_orders(db, shopee, test_settings, shops["990002"].id)
    order = await db.get(Order, package.order_id)
    assert order is not None
    await db.refresh(order)
    assert order.platform_status_group == "AWAITING_SHIPMENT"
    assert not await orders.is_cancelled(db, package)


async def test_ac42_six_tiktok_return_scenarios_with_fake_clock(
    db: AsyncSession, tiktok: MockTikTokAdapter, test_settings: Settings
) -> None:
    """AC-42 (TC-05.85): 061 chỉ hoàn tiền, 062 trả hàng (kiện "Hoàn đang về"), 063 đổi hàng, 064 chờ duyệt 3
    ngày (đồng hồ BR-12 chưa chạy) rồi chấp nhận, 065 người mua hủy → hồ sơ Đã hủy, 066 hoàn tiền xong khi
    kiện chưa về → BR-19."""
    test_settings.recon_enabled = True
    row = await db.get(Setting, 1)
    assert row is not None
    row.recon_start_at = NOW - timedelta(days=30)  # hồ sơ "sau nâng cấp" (G3 C2)
    await db.flush()
    lines = await seed_phase3(db, test_settings)
    assert any("J-13 TIKTOK TTMOCKA" in line for line in lines)
    shop_a = await _shop(db, "TTMOCKA")
    cases = {c.platform_return_sn: c for c in (await db.scalars(select(ReturnCase))).all()}
    assert cases["RTTT0000000061"].kind == "REFUND_ONLY"
    p61 = await orders.find_package(db, "TTTST0000000061")
    assert p61 is not None
    assert p61.warehouse_status == "NEW"
    assert cases["RTTT0000000062"].kind == "BUYER_RETURN"
    p62 = await orders.find_package(db, "TTTST0000000062")
    assert p62 is not None
    assert p62.warehouse_status == "RETURN_EXPECTED"
    assert cases["RTTT0000000063"].reason == "EXCHANGE"
    c64 = cases["RTTT0000000064"]
    assert (c64.platform_status_group, returns.clock_started(c64)) == ("REQUESTED", False)

    async def j13_after(hours: int) -> None:
        clock.advance(timedelta(hours=hours))
        await sync.refresh_tokens(db, tiktok, test_settings)  # J-12 (J-13 không tự làm mới — DEC-316)
        await sync.sync_returns(db, tiktok, test_settings, shop_a.id)

    await j13_after(25)  # 065 người mua hủy (24 giờ)
    await j13_after(24)  # 066 hoàn tiền xong (48 giờ)
    await j13_after(24)  # 064 chấp nhận (72 giờ)
    for c in cases.values():
        await db.refresh(c)
    assert cases["RTTT0000000065"].status == "CANCELLED"
    assert cases["RTTT0000000066"].platform_status_group == "DONE"
    c64 = cases["RTTT0000000064"]
    assert (c64.platform_status_group, c64.return_tracking_number) == ("ACCEPTED", "TTRTTST000064")
    assert returns.clock_started(cases["RTTT0000000064"])
    await run_rules(db, test_settings)
    alert = await db.scalar(select(ReconAlert).where(ReconAlert.rule == "RETURN_DONE_NOT_RECEIVED"))
    assert alert is not None
    p66 = await orders.find_package(db, "TTTST0000000066")
    assert p66 is not None
    assert alert.package_id == p66.id


async def test_tiktok_429_retry_logged_and_failing_shop_isolated(
    api: AsyncClient,
    db: AsyncSession,
    shopee: MockAdapter,
    tiktok: MockTikTokAdapter,
    test_settings: Settings,
) -> None:
    """TC-05.63 / AC-43: TikTok A trả 429 `Retry-After: 1` hai lần → lần 3 xong, 3 dòng `tiktok_call` có
    `attempt`, `http_status`, `request_id`, không có token / sign. TC-05.64: `fail_shop = TTMOCKB` → B
    `SYNC_FAILED`, A vẫn đồng bộ."""
    shops = await _connect_all(api, db, shopee, tiktok)
    tiktok.data.fail_times = {"TTMOCKA": 2}
    tiktok.data.fail_shop = {"TTMOCKB"}
    tiktok.data.calls.clear()
    out_a = await sync.sync_orders(db, tiktok, test_settings, shops["TTMOCKA"].id)
    out_b = await sync.sync_orders(db, tiktok, test_settings, shops["TTMOCKB"].id)
    assert out_a[str(shops["TTMOCKA"].id)]["status"] == "OK"
    assert out_a[str(shops["TTMOCKA"].id)]["orders"] > 0
    assert out_b[str(shops["TTMOCKB"].id)]["status"] == "FAILED"
    search_a = [c for c in tiktok.data.calls if c == ("/order/202309/orders/search", "TTMOCKA")]
    assert len(search_a) == 3  # 2 lần 429 + lần 3 thành công (log `tiktok_call` mỗi lần — test_tiktok_client)
    await db.refresh(shops["TTMOCKB"])
    assert shops["TTMOCKB"].last_error is not None
    assert shops["TTMOCKB"].last_error["code"] == "SYNC_FAILED"


async def test_seed_phase3_idempotent(db: AsyncSession, test_settings: Settings) -> None:
    """`seed_phase3` chạy lại không nhân đôi shop / đơn / hồ sơ."""
    await seed_phase3(db, test_settings)
    counts = [
        await db.scalar(select(func.count()).select_from(t)) for t in (Shop, Order, Package, ReturnCase)
    ]
    lines = await seed_phase3(db, test_settings)
    again = [await db.scalar(select(func.count()).select_from(t)) for t in (Shop, Order, Package, ReturnCase)]
    assert counts == again
    assert counts[0] == 4
    assert any(line.startswith("= shop TikTok TTMOCKB") for line in lines)


async def test_seed_phase3_claims_phase1_orders_long_after_mock_date(
    db: AsyncSession, test_settings: Settings
) -> None:
    """T-229 (DEC-821): seed chạy lâu sau mốc dữ liệu mock (01/10) — J-04 của `990001` vẫn nhận đơn Phase 1
    (seed-demo ghi trước, chưa shop, nhóm `UNKNOWN`) → có shop + nhóm trạng thái; đơn hủy → kiện `CANCELLED`
    (BR-21) như QA live Phase 1 cần (TC-03.08)."""
    clock.freeze(NOW + timedelta(days=30))
    for order in MockAdapter().orders.values():  # như `aicam seed-demo` (chưa có shop, chưa nhóm)
        await orders.upsert_platform_order(db, order)
    await db.commit()
    await seed_phase3(db, test_settings)
    shop_a = await db.scalar(select(Shop).where(Shop.platform_shop_id == "990001"))
    assert shop_a is not None
    cancelled = await db.scalar(select(Order).where(Order.platform_order_sn == "2410TST00009"))
    assert cancelled is not None
    assert (cancelled.shop_id, cancelled.platform_status_group) == (shop_a.id, "CANCELLED")
    package = await orders.find_package(db, "SPXTST0000009")
    assert package is not None
    assert package.warehouse_status == "CANCELLED"
    unclaimed = await db.scalar(
        select(func.count()).select_from(Order).where(Order.platform_order_sn.like("2410TST000%"),
                                                       Order.shop_id.is_(None))
    )  # fmt: skip
    assert unclaimed == 0


async def test_seed_demo_rerun_keeps_phase1_orders_in_shop(db: AsyncSession, test_settings: Settings) -> None:
    """T-229 (DEC-821): `seed-demo` chạy lại sau khi `990001` đã nhận đơn Phase 1 → không tạo đơn trùng mã
    không shop, kiện không bị gắn sang đơn mới (trước đây +34 đơn ở lần chạy thứ hai)."""
    from aicam.entrypoints.seed_returns import upsert_seed_order

    clock.freeze(NOW + timedelta(days=30))
    for _ in range(2):
        for order in MockAdapter().orders.values():  # như `aicam seed-demo`
            await upsert_seed_order(db, order)
        await db.commit()
        await seed_phase3(db, test_settings)
        if _ == 0:
            counts = [await db.scalar(select(func.count()).select_from(t)) for t in (Shop, Order, Package)]
    again = [await db.scalar(select(func.count()).select_from(t)) for t in (Shop, Order, Package)]
    assert again == counts
    shop_a = await db.scalar(select(Shop).where(Shop.platform_shop_id == "990001"))
    assert shop_a is not None
    package = await orders.find_package(db, "SPXTST0000001")
    assert package is not None
    owner = await db.scalar(select(Order).where(Order.id == package.order_id))
    assert owner is not None
    assert owner.shop_id == shop_a.id
