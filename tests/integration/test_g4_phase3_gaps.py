"""G4 item 03 — test bổ sung cho các case `04-test-cases.md` chưa có bằng chứng (lượt QA chạy test G4).

Chỉ thêm test, không đổi code sản phẩm. Mỗi test ghi TC trong docstring:
- TC-05.88: bàn hoàn quét mã chiều về TikTok → mở đúng phiên kiện TikTok, chip TikTok · shop.
- TC-04.81 / 02a §5.1 #13: đóng phiên bằng mã đơn trùng shop khác → đóng đúng phiên đang mở.
- TC-07.44 / 02a §5.1 #14 (phần API-110, API-130): ô tìm mã đơn trùng → mọi kết quả, mỗi dòng có shop.
- TC-05.94: không shop nào trả lời kịp → phiên "chưa xác minh" ở ~2 giây.
- TC-05.52: kết nối lại TikTok cùng tài khoản → cập nhật token, không tạo shop trùng.
- TC-R3.09: quét bàn hoàn ∥ J-04 hai shop cùng mã đơn ∥ J-13 × 20 lượt (connection thật) → không deadlock.
"""

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from aicam.core import clock
from aicam.core.audit import AuditLog
from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.entrypoints.seed_phase3 import seed_phase3
from aicam.modules.claims.models import Claim
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms import sync
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.mock.tiktok import MockTikTokAdapter
from aicam.modules.platforms.router import get_platform_adapter, get_tiktok_adapter
from aicam.modules.platforms.shopee.mapping import order_group
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase
from aicam.modules.sessions.models import PackSession

from . import test_code_lookup_two_shops as dup
from . import test_lookup_parallel as lk
from . import test_return_concurrency as conc
from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import make_desk, platform_return
from .test_return_close import _conclude, _open

pytestmark = pytest.mark.integration


async def _no_sleep(_: float) -> None:
    return None


def _use(api: AsyncClient, shopee: MockAdapter, tiktok: MockTikTokAdapter | None = None) -> None:
    overrides = api._transport.app.dependency_overrides  # type: ignore[attr-defined]
    overrides[get_platform_adapter] = lambda: shopee
    if tiktok is not None:
        overrides[get_tiktok_adapter] = lambda: tiktok


async def _admin(api: AsyncClient, db: AsyncSession, name: str) -> dict[str, str]:
    await make_user(db, name, "ADMIN")
    res = await api.post(
        "/api/v1/auth/login", json={"username": name, "password": PASSWORD, "client": "DASHBOARD"}
    )
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


# ---------------------------------------------------------------- TC-05.88


async def test_tc_05_88_tiktok_return_tracking_opens_tiktok_session(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-05.88 (AC-42, FR-05.11): seed Phase 3 (J-13 TikTok mock tạo hồ sơ 062, mã chiều về `TTRTTST000062`)
    → Station nhận hoàn quét `TTRTTST000062` → R2 mở phiên RETURN của kiện `TTTST0000000062`, đơn TikTok,
    shop "TST TikTok A (mock)" (chip R2), hồ sơ chuyển "Đang kiểm"."""
    clock.freeze(datetime(2026, 10, 2, 1, 0, tzinfo=UTC))
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = True
    test_settings.tiktok_returns_enabled = True
    test_settings.tiktok_adapter = "mock"
    await seed_phase3(db, test_settings)
    case = await db.scalar(select(ReturnCase).where(ReturnCase.platform_return_sn == "RTTT0000000062"))
    assert case is not None
    assert (case.kind, case.return_tracking_number) == ("BUYER_RETURN", "TTRTTST000062")

    _use(api, MockAdapter.multi_shop(["990001", "990002"]), MockTikTokAdapter(sleep=_no_sleep))
    desk = await make_desk(api, db, 5)
    body = (await desk.scan("TTRTTST000062")).json()

    assert body["outcome"] == "SESSION_OPENED", body
    session = body["state"]["session"]
    package = session["package"]
    assert package["order"]["platform"] == "TIKTOK", package
    assert package["order"]["shop_name"] == "TST TikTok A (mock)"
    pack = await db.get(PackSession, uuid.UUID(session["id"]))
    assert pack is not None
    assert pack.type == "RETURN"
    p62 = await orders.find_package(db, "TTTST0000000062")
    assert p62 is not None
    assert pack.package_id == p62.id
    await db.refresh(case)
    assert case.status == "INSPECTING"


# ---------------------------------------------------------------- TC-04.81 (§5.1 #13), TC-07.44 (§5.1 #14)


async def test_tc_04_81_close_with_dup_order_sn_closes_open_session(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-04.81 / 02a §5.1 #13: hai shop cùng mã đơn `2410DUP00001`, mỗi đơn một hồ sơ trả hàng mở. Mở phiên
    hồ sơ shop A bằng mã chiều về, kết luận "Nguyên vẹn", quét `2410DUP00001` → đóng đúng phiên đang mở
    (không ALERT `RETURN_MULTIPLE_ORDERS`); hồ sơ shop B không đổi."""
    clock.freeze(dup.NOW)
    test_settings.shopee_enabled = True
    _a, _b, oa, ob = await dup._two(db, test_settings)
    ra = await returns.upsert_from_platform(db, oa, dup._ret(oa, tracking="SPXRTDUPA0001"))
    rb = await returns.upsert_from_platform(db, ob, dup._ret(ob, tracking="SPXRTDUPB0001"))
    assert ra.case is not None
    assert rb.case is not None
    _use(api, MockAdapter())
    desk = await make_desk(api, db, 6)
    session = await _open(desk, "SPXRTDUPA0001")
    await _conclude(desk, session, "OK")

    body = (await desk.scan(dup.SN)).json()

    assert body["outcome"] == "SESSION_COMPLETED", body
    assert body["closed_session"]["id"] == session["id"]
    assert body["closed_session"]["return_case_status"] == "RECEIVED_OK"
    await db.refresh(ra.case)
    await db.refresh(rb.case)
    assert ra.case.status == "RECEIVED_OK"
    assert rb.case.status == "EXPECTED"


async def test_tc_07_44_q_dup_order_sn_returns_and_claims(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-07.44 / 02a §5.1 #14 (phần API-110, API-130 — API-30 ở `test_code_lookup_two_shops::test_14`):
    `q=2410DUP00001` → mọi hồ sơ / khiếu nại khớp của cả hai shop, mỗi dòng có `shop` để phân biệt."""
    clock.freeze(dup.NOW)
    test_settings.shopee_enabled = True
    _a, _b, oa, ob = await dup._two(db, test_settings)
    await returns.upsert_from_platform(db, oa, dup._ret(oa, tracking="RTA0001"))
    await returns.upsert_from_platform(db, ob, dup._ret(ob, tracking="RTB0001"))
    for code in ("SPXTSTB000000021", "TTTST0000000021"):
        package = await orders.find_package(db, code)
        assert package is not None
        db.add(
            Claim(
                package_id=package.id, order_id=package.order_id, type="DAMAGED", counterparty="PLATFORM",
                source="MANUAL", status="NEW", deadline_at=dup.NOW + timedelta(days=3),
            )
        )  # fmt: skip
    await db.flush()
    headers = await _admin(api, db, "tst_admin_g4_744")

    res = await api.get("/api/v1/returns", params={"q": dup.SN}, headers=headers)
    assert res.status_code == 200, res.text
    assert sorted(i["shop"]["name"] for i in res.json()["items"]) == ["TST B", "TST TikTok A (mock)"]
    res = await api.get("/api/v1/claims", params={"q": dup.SN}, headers=headers)
    assert res.status_code == 200, res.text
    assert sorted(i["shop"]["name"] for i in res.json()["items"]) == ["TST B", "TST TikTok A (mock)"]


# ---------------------------------------------------------------- TC-05.94


async def test_tc_05_94_no_shop_answers_in_time_opens_unverified(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-05.94: 3 shop đều chậm 5 giây → API-11 PACK mã lạ `SPXVN0000000000` mở phiên "chưa xác minh" ở ~2
    giây (cắt `PLATFORM_LOOKUP_TIMEOUT_S`, song song — không 3 × 2 giây), như Phase 1."""
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = False
    mock, _a, _b, _c = await lk._three(db, test_settings)
    mock.delay_s_by_shop = {"LKA": 5.0, "LKB": 5.0, "LKC": 5.0}
    lk._use(api, mock)
    user, _st = await make_station_account(db, "tst_lk94", "TST Station LK94")
    headers = await lk._login(api, user.username, "STATION")

    started = time.monotonic()
    body = (await lk._scan(api, headers, "SPXVN0000000000")).json()
    elapsed = time.monotonic() - started

    assert body["outcome"] == "SESSION_OPENED", body
    assert "UNVERIFIED" in body["state"]["session"]["flags"]
    assert body["state"]["session"]["package"]["order"] is None
    assert 1.9 <= elapsed < 3.0, elapsed


# ---------------------------------------------------------------- TC-05.52


async def _follow(api: AsyncClient, admin: dict[str, str], platform: str) -> str:
    res = await api.post(f"/api/v1/shops/{platform}/auth-url", headers=admin)
    assert res.status_code == 200, res.text
    url = urlparse(res.json()["url"])
    out = await api.get(f"{url.path}?{url.query}")
    return str(out.headers["location"])


async def test_tc_05_52_reconnect_tiktok_updates_token_no_duplicate(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-05.52 (UC-10 ngoại lệ): kết nối lại TikTok (cùng tài khoản, 2 shop) → vẫn 2 dòng `shop` TikTok
    (không 4), `auth_expires_at` mới hơn lần trước, audit `SHOP_CONNECT` thêm 2 dòng."""
    clock.freeze(datetime(2026, 10, 2, 1, 0, tzinfo=UTC))
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = True
    test_settings.tiktok_returns_enabled = True
    test_settings.tiktok_adapter = "mock"
    _use(api, MockAdapter.multi_shop(["990001", "990002"]), MockTikTokAdapter(sleep=_no_sleep))
    admin = await _admin(api, db, "tst_admin_g4_552")

    first = await _follow(api, admin, "tiktok")
    assert "result=connected" in first, first
    assert "count=2" in first, first
    shops = (await db.scalars(select(Shop).where(Shop.platform == "TIKTOK"))).all()
    assert len(shops) == 2
    before = {s.id: s.auth_expires_at for s in shops}

    clock.advance(timedelta(minutes=10))
    again = await _follow(api, admin, "tiktok")
    assert "result=connected" in again, again
    assert "count=2" in again, again

    assert await db.scalar(select(func.count()).select_from(Shop).where(Shop.platform == "TIKTOK")) == 2
    for shop in (await db.scalars(select(Shop).where(Shop.platform == "TIKTOK"))).all():
        await db.refresh(shop)
        assert shop.auth_status == "CONNECTED"
        assert shop.id in before
        assert shop.auth_expires_at is not None
        assert before[shop.id] is not None
        assert shop.auth_expires_at > before[shop.id]  # type: ignore[operator]
    audits = (await db.scalars(select(AuditLog).where(AuditLog.action == "SHOP_CONNECT"))).all()
    assert len([a for a in audits if (a.data or {}).get("platform") == "TIKTOK"]) == 4


# ---------------------------------------------------------------- TC-R3.09


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings
) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {conc.TABLES} CASCADE"))
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_rconc_%'"))
    await dispose_engine()


def _porder(sn: str, code: str, status: str) -> PlatformOrder:
    now = datetime.now(UTC)
    return PlatformOrder(
        platform_order_sn=sn, status=status, tracking_numbers=(code,),
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L"),), created_at=now, updated_at=now,
        status_group=order_group(status),
    )  # fmt: skip


async def test_tc_r3_09_scan_j04_two_shops_j13_twenty_rounds(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """TC-R3.09 (02a §6, DEC-266, DEC-493): mỗi lượt cùng xuất phát 4 đường trên connection thật — API-11 bàn
    hoàn quét kiện của shop A ∥ J-04 shop A ∥ J-04 shop B (cùng mã đơn với A) ∥ J-13 shop A (yêu cầu trả của
    đơn đó) — × 20 lượt → không deadlock (≤ 10 giây / lượt), đúng 2 đơn cùng mã, đúng 1 hồ sơ mở cho đơn A."""
    test_settings.shopee_enabled = True
    far = datetime.now(UTC) + timedelta(hours=4)
    async with sessionmaker()() as db:
        shops = []
        for ext in ("R39A", "R39B"):
            shop = Shop(platform="SHOPEE", platform_shop_id=ext, name=f"TST {ext}", auth_status="CONNECTED")
            platforms.store_credentials(
                shop, ShopCredentials(ext, "a", "r", far), Cipher(test_settings.fernet_key)
            )
            db.add(shop)
            shops.append(shop)
        await db.commit()
        a_id, b_id = shops[0].id, shops[1].id
    headers = await conc._station("r39", mode="RETURN")
    mock = MockAdapter()
    mock.orders_by_shop = {"R39A": {}, "R39B": {}}
    mock.returns_by_shop = {"R39A": {}, "R39B": {}}

    async def j04(shop_id: uuid.UUID) -> Any:
        async with sessionmaker()() as db:
            return await sync.sync_orders(db, mock, test_settings, shop_id)

    async def j13() -> Any:
        async with sessionmaker()() as db:
            return await sync.sync_returns(db, mock, test_settings, a_id)

    async with conc._client(test_settings) as client:
        for n in range(20):
            sn, code_a, code_b = f"2410R39{n:05d}", f"SPXR39A{n:07d}", f"SPXR39B{n:07d}"
            async with sessionmaker()() as db:
                await orders.upsert_platform_order(db, _porder(sn, code_a, "COMPLETED"), shop_id=a_id)
                package = await orders.find_package(db, code_a)
                assert package is not None
                package.warehouse_status = "DELIVERED"
                await db.commit()
            mock.put_for_shop("R39A", _porder(sn, code_a, "COMPLETED"))
            mock.put_for_shop("R39B", _porder(sn, code_b, "READY_TO_SHIP"))
            ret = replace(
                platform_return(1, tracking=f"SPXRTR39{n:06d}"), return_sn=f"2410R39RT{n:03d}", order_sn=sn,
                updated_at=datetime.now(UTC),
            )  # fmt: skip
            mock.returns_by_shop["R39A"][ret.return_sn] = ret
            gate = asyncio.Event()

            async def go(coro: Any, gate: asyncio.Event = gate) -> Any:
                await gate.wait()
                return await coro

            tasks = [
                asyncio.create_task(go(conc._scan(client, headers, code_a))),
                asyncio.create_task(go(j04(a_id))),
                asyncio.create_task(go(j04(b_id))),
                asyncio.create_task(go(j13())),
            ]
            await asyncio.sleep(0.02)
            gate.set()
            scan, *_ = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
            assert scan["outcome"] == "SESSION_OPENED", (n, scan)

            async with sessionmaker()() as db:
                rows = (await db.scalars(select(Order).where(Order.platform_order_sn == sn))).all()
                assert sorted(str(o.shop_id) for o in rows) == sorted([str(a_id), str(b_id)]), n
                order_a = next(o for o in rows if o.shop_id == a_id)
                open_cases = await db.scalar(
                    select(func.count())
                    .select_from(ReturnCase)
                    .where(ReturnCase.order_id == order_a.id, ReturnCase.status.in_(OPEN_CASE_STATUSES))
                )
                assert open_cases == 1, n
                # Giải phóng bàn cho lượt sau (phiên đang mở của lượt này → hủy).
                await db.execute(
                    text("UPDATE session SET status = 'CANCELLED', ended_at = now() WHERE status = 'OPEN'")
                )
                await db.commit()


# ---------------------------------------------------------------- TC-05.79, TC-05.86 (TikTok lỗi tạm / cuối)


async def _seeded_tiktok(db: AsyncSession, settings: Settings) -> dict[str, Shop]:
    clock.freeze(datetime(2026, 10, 2, 1, 0, tzinfo=UTC))
    settings.shopee_enabled = True
    settings.tiktok_enabled = True
    settings.tiktok_returns_enabled = True
    settings.tiktok_adapter = "mock"
    await seed_phase3(db, settings)
    return {s.platform_shop_id: s for s in (await db.scalars(select(Shop))).all()}


async def test_tc_05_79_j06_tiktok_temporary_error_retried(
    db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-05.79: J-06 shop TikTok A gặp lỗi tạm (429 `Retry-After: 1` một lần) → client thử lại, lô thành
    công: kiện `PACKED` của đơn `IN_TRANSIT` → `HANDED_OVER`; lời gọi chi tiết đơn của A lặp 2 lần (lần 2
    thành công)."""
    shops = await _seeded_tiktok(db, test_settings)
    package = await orders.find_package(db, "TTTST0000000016")  # đơn IN_TRANSIT
    assert package is not None
    package.warehouse_status = "PACKED"
    await db.commit()
    tiktok = MockTikTokAdapter(sleep=_no_sleep)
    tiktok.data.fail_times = {"TTMOCKA": 1}
    tiktok.data.calls.clear()

    out = await sync.sync_shipping_status(db, tiktok, test_settings, shops["TTMOCKA"].id)

    assert out["changed"] >= 1, out
    await db.refresh(package)
    assert package.warehouse_status == "HANDED_OVER"
    calls_a = [c for c in tiktok.data.calls if c[1] == "TTMOCKA"]
    assert len(calls_a) >= 2  # lần 1 bị 429, client gửi lại
    assert calls_a[0] == calls_a[1]


async def test_tc_05_86_j13_tiktok_temporary_then_final_error_per_shop(
    db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-05.86: (1) J-13 TikTok A lỗi tạm một lần → thử lại, thành công, không `last_error`; (2) A luôn lỗi
    (`fail_shop`) → A `last_error.code = SYNC_FAILED` (`job = returns`), cursor J-13 không tiến; B và Shopee
    vẫn chạy (cursor tiến)."""
    shops = await _seeded_tiktok(db, test_settings)
    a_id, b_id, s_id = shops["TTMOCKA"].id, shops["TTMOCKB"].id, shops["990002"].id
    tiktok = MockTikTokAdapter(sleep=_no_sleep)
    shopee = MockAdapter.multi_shop(["990001", "990002"])

    async def fresh(shop_id: uuid.UUID) -> Shop:
        shop = await db.get(Shop, shop_id, populate_existing=True)
        assert shop is not None
        return shop

    clock.advance(timedelta(minutes=15))
    tiktok.data.fail_times = {"TTMOCKA": 1}
    out = await sync.sync_returns(db, tiktok, test_settings, a_id)
    assert out[str(a_id)]["status"] == "OK", out
    assert (await fresh(a_id)).last_error is None

    clock.advance(timedelta(minutes=15))
    tiktok.data.fail_shop = {"TTMOCKA"}
    cursor_a = (await fresh(a_id)).last_return_cursor
    cursor_b = (await fresh(b_id)).last_return_cursor
    out_a = await sync.sync_returns(db, tiktok, test_settings, a_id)
    out_b = await sync.sync_returns(db, tiktok, test_settings, b_id)
    out_s = await sync.sync_returns(db, shopee, test_settings, s_id)
    assert out_a[str(a_id)]["status"] == "FAILED", out_a
    assert out_b[str(b_id)]["status"] == "OK", out_b
    assert out_s[str(s_id)]["status"] == "OK", out_s
    a, b = await fresh(a_id), await fresh(b_id)
    assert a.last_error is not None
    assert (a.last_error["code"], a.last_error.get("job")) == ("SYNC_FAILED", "returns")
    assert a.last_return_cursor == cursor_a
    assert b.last_return_cursor is not None
    assert cursor_b is not None
    assert b.last_return_cursor > cursor_b


# ---------------------------------------------------------------- TC-07.42, TC-03.94, TC-06.71


async def test_tc_07_42_shop_not_of_platform_returns_empty(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-07.42: API-30 `platform=SHOPEE&shop_id=<shop TikTok>` → 200, danh sách rỗng (không lỗi); cùng sàn
    → có kiện của shop đó."""
    clock.freeze(dup.NOW)
    test_settings.shopee_enabled = True
    _a, b, _oa, _ob = await dup._two(db, test_settings)
    headers = await _admin(api, db, "tst_admin_g4_742")
    res = await api.get(
        "/api/v1/packages", params={"platform": "SHOPEE", "shop_id": str(b.id)}, headers=headers
    )
    assert res.status_code == 200, res.text
    assert res.json()["items"] == []
    res = await api.get(
        "/api/v1/packages", params={"platform": "TIKTOK", "shop_id": str(b.id)}, headers=headers
    )
    assert [i["tracking_number"] for i in res.json()["items"]] == ["TTTST0000000021"]


async def test_tc_03_94_packer_name_length(
    api: AsyncClient, db: AsyncSession, test_settings: Settings, redis_client: object
) -> None:
    """TC-03.94 (API-101 chế độ PACK): `"M"`, 41 ký tự → 422 `fields.name`; 2 ký tự, 40 ký tự → 200."""
    desk = await make_desk(api, db, 9, mode="PACK", operator=None)
    for bad in ("M", "x" * 41, "   M   "):
        res = await api.put("/api/v1/station/operator", headers=desk.headers, json={"name": bad})
        assert res.status_code == 422, (bad, res.text)
        assert "name" in res.json()["error"]["details"]["fields"]
    for ok in ("Mi", "y" * 40):
        res = await api.put("/api/v1/station/operator", headers=desk.headers, json={"name": ok})
        assert res.status_code == 200, (ok, res.text)


async def test_tc_06_71_notify_jobs_overlap_skip(db: AsyncSession, tmp_path: Any, redis_client: Any) -> None:
    """TC-06.71: khóa `notify:scan` đang bị giữ (J-26 lượt trước chưa xong) → J-26 lượt 2 bỏ lượt, không tạo
    sự kiện; `notify:dispatch` bị giữ → J-27 bỏ lượt, không gửi; nhả khóa → lượt sau chạy bình thường, không
    trùng."""
    from aicam.modules.notify import dispatch
    from aicam.modules.notify.models import NotifyEvent, NotifyMessage

    from . import test_notify_dispatch as nd
    from .notify_fixtures import make_channel, make_notify_settings, mock_sent

    clock.freeze(nd.DAY)
    (tmp_path / "video").mkdir(exist_ok=True)
    settings = make_notify_settings(tmp_path)
    await make_channel(db, "Kho", events=["N02"])
    await nd.recon_high(db, await nd.package(db, "SPXTSTG4L0001"))

    await redis_client.set(dispatch.SCAN_LOCK, "other", ex=60)
    assert await dispatch.scan(db, settings) == {"skipped": "locked"}
    assert await db.scalar(select(func.count()).select_from(NotifyEvent)) == 0
    await redis_client.delete(dispatch.SCAN_LOCK)
    out = await dispatch.scan(db, settings)
    assert out["new"] >= 1, out
    assert (await dispatch.scan(db, settings))["new"] == 0  # chạy lại không trùng

    clock.advance(timedelta(minutes=3))  # qua cửa sổ gom 2 phút
    await redis_client.set(dispatch.DISPATCH_LOCK, "other", ex=60)
    assert await dispatch.dispatch(db, settings) == {"skipped": "locked"}
    assert await mock_sent() == []
    await redis_client.delete(dispatch.DISPATCH_LOCK)
    await dispatch.dispatch(db, settings)
    assert len(await mock_sent()) == 1
    assert await db.scalar(select(func.count()).select_from(NotifyMessage)) == 1


# ---------------------------------------------------------------- TC-P3.10..13, 16, 17 (ma trận bổ sung)


async def test_tc_p3_matrix_endpoints_outside_t228(api: AsyncClient, db: AsyncSession) -> None:
    """Ma trận quyền 4 vai (04 §3) cho endpoint Phase 3 dùng lại API cũ, chưa có trong `test_phase3_authz`:
    TC-P3.16 API-134 bỏ / thêm bằng chứng (A, S, C); TC-P3.11 API-80 PUT (chỉ A; GET A, S); TC-P3.13 API-21
    quyết định (A, S); TC-P3.10 API-81 sức khỏe (A, S); TC-P3.12 API-12 tự hủy phiên, TC-P3.17 API-101 tên
    người đóng gói (chỉ STATION). Vai bị chặn → 403; vai được phép → không 401 / 403 (id ngẫu nhiên → 404,
    body sai → 422). Không token → 401."""
    from .test_phase3_authz import ROLES, A, C, S, _call, _tokens

    tokens = await _tokens(api, db)
    x = str(uuid.uuid4())
    st = "STATION"
    matrix: list[tuple[str, str, str, set[str], object]] = [
        ("P3.16 API-134", "PUT", f"/claims/{x}/evidence", {A, S, C}, {"bad": 1}),
        ("P3.11 API-80 PUT", "PUT", "/settings", {A}, {"retention_clip_days": -1}),
        ("P3.11 API-80 GET", "GET", "/settings", {A, S}, None),
        ("P3.13 API-21", "POST", f"/approval-requests/{x}/decision", {A, S}, {"action": "CONTINUE"}),
        ("P3.10 API-81", "GET", "/system/health", {A, S}, None),
        ("P3.12 API-12", "POST", f"/station/sessions/{x}/cancel", {st}, {"reason": "WRONG_SCAN"}),
        ("P3.17 API-101", "PUT", "/station/operator", {st}, {"name": "M"}),
    ]
    wrong: list[str] = []
    for label, method, path, allowed, body in matrix:
        if await _call(api, method, path, body, None) != 401:
            wrong.append(f"{label} không token phải 401")
        for role in ROLES:
            status = await _call(api, method, path, body, tokens[role])
            if role not in allowed and status != 403:
                wrong.append(f"{label} {role}: {status} (cần 403)")
            if role in allowed and status in (401, 403):
                wrong.append(f"{label} {role}: {status} (được phép)")
    assert not wrong, wrong
