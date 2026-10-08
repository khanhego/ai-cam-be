"""T-271: 02a §5.1 — mọi điểm tra theo mã khi mã đơn / mã yêu cầu trả **không còn unique toàn cục** (BR-29,
DEC-492, 493; EX-R20). Một case cho mỗi dòng của bảng (2 shop A, B trùng mã đơn + mã yêu cầu trả). Dòng #15
(mã chiều về trùng) ở T-288 (`test_return_code_ambiguous.py`).
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.imports import parser
from aicam.modules.imports import service as imports
from aicam.modules.imports.models import CsvImport
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.router import get_platform_adapter

from .factories import PASSWORD, make_user
from .returns_helpers import make_desk, platform_return

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 5, 0, tzinfo=UTC)
SN = "2410DUP00001"
RSN = "RSDUP0000001"


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True


def _order(sn: str, *codes: str, status: str = "COMPLETED") -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=sn, status=status, tracking_numbers=codes,
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L"),), created_at=NOW, updated_at=NOW,
        status_group=order_group(status),
    )  # fmt: skip


async def _shop(db: AsyncSession, settings: Settings, ext: str, name: str, platform: str = "SHOPEE") -> Shop:
    shop = Shop(platform=platform, platform_shop_id=ext, name=name)
    platforms.store_credentials(
        shop, ShopCredentials(ext, "a", "r", NOW + timedelta(hours=4)), Cipher(settings.fernet_key)
    )
    db.add(shop)
    await db.flush()
    return shop


async def _two(db: AsyncSession, settings: Settings) -> tuple[Shop, Shop, Order, Order]:
    """Shop A (Shopee) + B (TikTok) cùng mã đơn `2410DUP00001`, mỗi đơn một kiện đã giao."""
    a = await _shop(db, settings, "990002", "TST B")
    b = await _shop(db, settings, "TTMOCKA", "TST TikTok A (mock)", "TIKTOK")
    oa = (await orders.upsert_platform_order(db, _order(SN, "SPXTSTB000000021"), shop_id=a.id)).order
    ob = (await orders.upsert_platform_order(db, _order(SN, "TTTST0000000021"), shop_id=b.id)).order
    for code in ("SPXTSTB000000021", "TTTST0000000021"):
        package = await orders.find_package(db, code)
        assert package is not None
        package.warehouse_status = "DELIVERED"
    await db.flush()
    return a, b, oa, ob


def _ret(order: Order, *, tracking: str, status: str = "ACCEPTED") -> Any:
    return replace(
        platform_return(1, status=status, tracking=tracking), return_sn=RSN, order_sn=order.platform_order_sn
    )


# ---------------------------------------------------------------- #1 khóa `order:{sn}` (song song thật)


@pytest.fixture
async def committed(migrated_database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text('TRUNCATE status_history, order_item, package, "order", shop CASCADE'))
    await dispose_engine()


async def test_1_parallel_writes_same_sn_two_shops(committed: AsyncEngine, test_settings: Settings) -> None:
    """#1: A, B ghi cùng mã song song (2 transaction thật, × 10) → 2 đơn mỗi lượt, không deadlock, không
    `IntegrityError` (chung khóa `order:{sn}` — DEC-493)."""
    async with sessionmaker()() as db:
        a = await _shop(db, test_settings, "C1", "A")
        b = await _shop(db, test_settings, "C2", "B", "TIKTOK")
        await db.commit()
        ids = (a.id, b.id)

    async def write(shop_id: uuid.UUID, sn: str, code: str) -> None:
        async with sessionmaker()() as db:
            await orders.upsert_platform_order(db, _order(sn, code, status="READY_TO_SHIP"), shop_id=shop_id)
            await asyncio.sleep(0.01)
            await db.commit()

    for n in range(10):
        sn = f"2410PAR{n:05d}"
        await asyncio.wait_for(
            asyncio.gather(write(ids[0], sn, f"SPXPARA{n:06d}"), write(ids[1], sn, f"TTPARB{n:07d}")),
            timeout=10,
        )
    async with sessionmaker()() as db:
        rows = (
            await db.execute(select(Order.platform_order_sn, func.count()).group_by(Order.platform_order_sn))
        ).all()
    assert sorted(rows) == [(f"2410PAR{n:05d}", 2) for n in range(10)]


# ---------------------------------------------------------------- #2 upsert, #6 J-04 kiện theo đơn


async def test_2_upsert_and_6_packages_by_order_id(db: AsyncSession, test_settings: Settings) -> None:
    a, b, oa, ob = await _two(db, test_settings)
    assert oa.id != ob.id
    assert (oa.shop_id, ob.shop_id) == (a.id, b.id)
    # #6: kiện "trước upsert" của B chỉ là kiện đơn B (không thấy kiện của A cùng mã).
    seen = await sync._order_packages(db, ob.id)
    package_b = await orders.find_package(db, "TTTST0000000021")
    assert package_b is not None
    assert set(seen) == {package_b.id}


# ---------------------------------------------------------------- #3, #4, #5 nhập file


async def _classify(db: AsyncSession, *rows: tuple[str, str]) -> imports.Classified:
    parsed = parser.Parsed(
        rows=[
            parser.Row(row=i + 2, tracking_number=code, platform_order_sn=sn, product_name="Áo", quantity=1)
            for i, (sn, code) in enumerate(rows)
        ],
        errors=[],
    )
    return await imports.classify(db, parsed)


async def test_3_5_import_classify_two_shops(db: AsyncSession, test_settings: Settings) -> None:
    """#3: file mã trùng đơn A, cùng mã vận đơn → SKIP; mã trùng A + B, mã vận đơn mới → NEW đơn file; mã vận
    đơn của B → lỗi dòng nêu đúng shop (#5)."""
    await _two(db, test_settings)
    skip = await _classify(db, (SN, "SPXTSTB000000021"))
    assert skip.groups[SN].action == "SKIP"
    assert skip.errors == []
    new = await _classify(db, (SN, "SPXFILE000001"))
    assert new.groups[SN].action == "NEW"
    assert new.errors == []
    bad = await _classify(db, ("2410FILE0009", "TTTST0000000021"))
    assert [(e.column, e.message) for e in bad.errors] == [
        ("tracking_number", "Mã vận đơn đã thuộc đơn 2410DUP00001 (TST TikTok A (mock))")
    ]
    owners = await orders.packages_by_code(db, ["tttst0000000021"])
    assert (owners["TTTST0000000021"].shop_name, owners["TTTST0000000021"].platform) == (
        "TST TikTok A (mock)", "TIKTOK",
    )  # fmt: skip


async def test_4_csv_apply_after_shop_claimed_file_order(db: AsyncSession, test_settings: Settings) -> None:
    """#4: xem trước ra UPDATE đơn file; J-04 shop A vừa nhận đơn file cùng mã trước khi bấm Nhập → không ghi
    đè đơn A (bỏ qua); mã trùng đơn của shop giữ đủ mã vận đơn → bỏ qua dưới khóa."""
    a = await _shop(db, test_settings, "990002", "TST B")
    user = await make_user(db, "imp271", "ADMIN")
    imp = CsvImport(
        file_name="a.csv", created_by=user.id, expires_at=NOW + timedelta(minutes=30), status="COMMITTED"
    )
    db.add(imp)
    await db.flush()
    data = orders.CsvOrder("2410FILE0001", "từ file", (PlatformItem("Áo", 1),), ("SPXFILE0000001",))
    assert (
        await orders.apply_csv_order(db, data, import_id=imp.id, shop_id=None, actor_user_id=user.id) is True
    )
    await orders.upsert_platform_order(db, _order("2410FILE0001", "SPXFILE0000001"), shop_id=a.id)
    again = await orders.apply_csv_order(
        db, data, import_id=imp.id, shop_id=None, actor_user_id=user.id, expect_new=False
    )
    assert again is None
    rows = (await db.scalars(select(Order).where(Order.platform_order_sn == "2410FILE0001"))).all()
    assert [(o.shop_id, o.source) for o in rows] == [(a.id, "API")]


# ---------------------------------------------------------------- #7, #8, #9 yêu cầu trả


async def test_7_j13_return_of_b_attaches_order_b(db: AsyncSession, test_settings: Settings) -> None:
    """#7 / #21: J-13 của shop B tra đơn theo (shop B, mã) — yêu cầu trả của B gắn đơn B, không gắn A."""
    _a, b, oa, ob = await _two(db, test_settings)
    mock = MockAdapter()
    mock.returns = {}
    mock.returns_by_shop = {"TTMOCKA": {RSN: _ret(ob, tracking="RTB0001")}}

    class TikTokMock(MockAdapter):
        code = "TIKTOK"

    tt = TikTokMock()
    tt.returns_by_shop = mock.returns_by_shop
    test_settings.tiktok_enabled = True
    test_settings.tiktok_returns_enabled = True
    out = await sync.sync_returns(db, tt, test_settings, b.id)
    assert out[str(b.id)]["status"] == "OK"
    case = await db.scalar(select(ReturnCase).where(ReturnCase.platform_return_sn == RSN))
    assert case is not None
    assert (case.order_id, case.shop_id) == (ob.id, b.id)
    assert oa.id != ob.id


async def test_8_9_same_return_sn_two_shops_two_cases(db: AsyncSession, test_settings: Settings) -> None:
    """#8: mã yêu cầu trả trùng A / B → 2 hồ sơ (mỗi hồ sơ mang shop của đơn); #9: cập nhật yêu cầu của B
    không đụng hồ sơ A."""
    a, b, oa, ob = await _two(db, test_settings)
    ra = await returns.upsert_from_platform(db, oa, _ret(oa, tracking="RTA0001"))
    rb = await returns.upsert_from_platform(db, ob, _ret(ob, tracking="RTB0001"))
    assert ra.case is not None
    assert rb.case is not None
    assert ra.case.id != rb.case.id
    assert (ra.case.shop_id, rb.case.shop_id) == (a.id, b.id)
    upd = await returns.upsert_from_platform(db, ob, _ret(ob, tracking="RTB0002"))
    assert upd.case is not None
    assert upd.case.id == rb.case.id
    await db.refresh(ra.case)
    assert (ra.case.return_tracking_number, upd.case.return_tracking_number) == ("RTA0001", "RTB0002")


# ---------------------------------------------------------------- #10, #11 bàn hoàn


def _use_mock(api: AsyncClient) -> None:
    api._transport.app.dependency_overrides[get_platform_adapter] = MockAdapter  # type: ignore[attr-defined]


async def test_10_resolve_and_scan_dup_order_sn_alerts(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """#10: quét mã đơn trùng → ALERT `RETURN_MULTIPLE_ORDERS` 2 đơn có sàn · shop (không mở phiên); mã yêu
    cầu trả trùng → alert; mã chỉ ở A → mở phiên A."""
    a, _b, oa, ob = await _two(db, test_settings)
    await returns.upsert_from_platform(db, oa, _ret(oa, tracking="RTA0001"))
    await returns.upsert_from_platform(db, ob, _ret(ob, tracking="RTB0001"))
    res = await returns.resolve_code(db, SN)
    assert res.status == "MULTIPLE_ORDERS"
    assert {o.id for o in res.orders} == {oa.id, ob.id}
    assert (await returns.resolve_code(db, RSN)).status == "MULTIPLE_ORDERS"
    only_a = (
        await orders.upsert_platform_order(db, _order("2410ONLYA001", "SPXONLYA0001"), shop_id=a.id)
    ).order
    package = await orders.find_package(db, "SPXONLYA0001")
    assert package is not None
    package.warehouse_status = "DELIVERED"
    await db.flush()
    found = await returns.resolve_code(db, "2410ONLYA001")
    assert (found.status, found.order) == ("FOUND", only_a)

    _use_mock(api)
    desk = await make_desk(api, db, 7)
    body = (await desk.scan(SN)).json()
    assert body["outcome"] == "ALERT"
    alert = body["alert"]
    assert alert["code"] == "RETURN_MULTIPLE_ORDERS"
    assert alert["message"] == f"Mã {SN} có ở 2 đơn của các shop khác nhau. Chọn đúng đơn."
    assert alert["data"] == {
        "code": SN,
        "orders": [
            {"platform": "SHOPEE", "shop_name": "TST B", "platform_order_sn": SN},
            {"platform": "TIKTOK", "shop_name": "TST TikTok A (mock)", "platform_order_sn": SN},
        ],
    }
    assert body["state"]["session"] is None
    body = (await desk.scan(RSN)).json()
    assert body["alert"]["code"] == "RETURN_MULTIPLE_ORDERS"


async def test_11_return_lookup_two_rows_with_shop(
    api: AsyncClient, db: AsyncSession, test_settings: Settings
) -> None:
    """#11 (API-104): tìm mã trùng → 2 dòng, mỗi dòng sàn + tên shop đúng."""
    await _two(db, test_settings)
    _use_mock(api)
    desk = await make_desk(api, db, 8)
    res = await api.get("/api/v1/station/return-lookup", params={"q": SN}, headers=desk.headers)
    assert res.status_code == 200, res.text
    rows = sorted((i["platform"], i["shop_name"], i["tracking_number"]) for i in res.json()["items"])
    assert rows == [
        ("SHOPEE", "TST B", "SPXTSTB000000021"),
        ("TIKTOK", "TST TikTok A (mock)", "TTTST0000000021"),
    ]


# ---------------------------------------------------------------- #13, #14


async def test_14_lists_show_both_shops(api: AsyncClient, db: AsyncSession, test_settings: Settings) -> None:
    """#14: ô tìm API-30 `q` mã trùng → 2 dòng, mỗi dòng shop riêng (logic lọc không đổi)."""
    await _two(db, test_settings)
    await make_user(db, "tst_admin271", "ADMIN")
    res = await api.post(
        "/api/v1/auth/login", json={"username": "tst_admin271", "password": PASSWORD, "client": "DASHBOARD"}
    )
    headers = {"Authorization": f"Bearer {res.json()['access_token']}"}
    body = (await api.get("/api/v1/packages", params={"q": SN}, headers=headers)).json()
    assert sorted(i["shop"]["name"] for i in body["items"]) == ["TST B", "TST TikTok A (mock)"]
