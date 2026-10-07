"""T-205: fan-out một task / shop (J-04 / J-06 / J-13), ngân sách mỗi shop, J-12 theo grant (02a §6, §7;
NFR-39, DEC-433, 434, 503, 507). Adapter mock điều khiển theo shop (`fail_shop`, `delay_s_by_shop`).

Hàng đợi `sync_fast` / `worker-sync-long` + test 6 shop đồng hồ giả 1 giờ là T-276 (M17)."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.security import Cipher
from aicam.core.settings import Settings
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms import budget, dispatch, grants, sync
from aicam.modules.platforms import service as platforms
from aicam.modules.platforms.base import PlatformItem, PlatformOrder, ShopCredentials
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 7, 1, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _env(test_settings: Settings, redis_client: object) -> None:
    clock.freeze(NOW)
    test_settings.shopee_enabled = True
    test_settings.tiktok_enabled = False


async def _shop(
    db: AsyncSession,
    settings: Settings,
    psid: str,
    *,
    platform: str = "SHOPEE",
    status: str = "CONNECTED",
    grant: str | None = None,
    expires_in: timedelta = timedelta(hours=4),
) -> Shop:
    shop = Shop(platform=platform, platform_shop_id=psid, name=f"TST {psid}", grant_ref=grant)
    platforms.store_credentials(
        shop,
        ShopCredentials(psid, f"acc-{psid}", f"ref-{psid}", NOW + expires_in),
        Cipher(settings.fernet_key),
    )
    shop.auth_status = status
    db.add(shop)
    await db.flush()
    return shop


def _order(sn: str, code: str) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=sn, status="READY_TO_SHIP", tracking_numbers=(code,),
        items=(PlatformItem("Áo thun basic", 1, "AT-DEN-L", "Đen / L"),), created_at=NOW, updated_at=NOW,
        status_group=order_group("READY_TO_SHIP"),
    )  # fmt: skip


def _mock_three() -> MockAdapter:
    mock = MockAdapter()
    for psid, prefix in (("990001", "A"), ("990002", "B"), ("990003", "C")):
        for n in range(1, 4):
            mock.put_for_shop(psid, _order(f"2410FAN{prefix}{n:04d}", f"SPXFAN{prefix}{n:07d}"))
    return mock


# ---------------------------------------------------------------- phân phối


async def test_dispatch_one_task_per_connected_shop(
    db: AsyncSession, test_settings: Settings, sent_jobs: list[Any]
) -> None:
    """Beat → một task / shop `CONNECTED` của sàn bật; shop ngắt / hết hạn / sàn tắt không có task."""
    a = await _shop(db, test_settings, "990001")
    b = await _shop(db, test_settings, "990002")
    await _shop(db, test_settings, "990003", status="DISCONNECTED")
    await _shop(db, test_settings, "990004", status="EXPIRED")
    await _shop(db, test_settings, "TTX1", platform="TIKTOK")  # TikTok tắt (EX-T1)

    out = await dispatch.dispatch(db, test_settings, dispatch.ORDERS)

    assert out["queued"] == 2
    sent = sorted((t, args[0], q) for t, args, q, _ in sent_jobs)
    assert sent == sorted(
        [
            ("platforms.sync_shop_orders", str(a.id), "sync_fast"),
            ("platforms.sync_shop_orders", str(b.id), "sync_fast"),
        ]
    )
    sent_jobs.clear()
    await dispatch.dispatch(db, test_settings, dispatch.SHIPPING)
    assert {t for t, *_ in sent_jobs} == {"platforms.sync_shop_shipping"}
    assert len(sent_jobs) == 2

    test_settings.shopee_enabled = False
    sent_jobs.clear()
    assert await dispatch.dispatch(db, test_settings, dispatch.ORDERS) == {"skipped": "not_configured"}
    assert sent_jobs == []


async def test_dispatch_returns_follows_platform_flag(
    db: AsyncSession, test_settings: Settings, sent_jobs: list[Any]
) -> None:
    """J-13 chỉ phân phối cho sàn bật cờ trả hàng (Shopee mock luôn chạy — G3 F-11; TikTok cờ riêng)."""
    a = await _shop(db, test_settings, "990001")
    await dispatch.dispatch(db, test_settings, dispatch.RETURNS)
    assert [(t, args) for t, args, *_ in sent_jobs] == [("platforms.sync_shop_returns", [str(a.id)])]


async def test_dispatch_shipping_without_shopee_shop_keeps_file_orders(
    db: AsyncSession, test_settings: Settings, sent_jobs: list[Any]
) -> None:
    """Shopee bật (mock) nhưng chưa có shop → một task không shop để kiện đơn file vẫn được tra (Phase 2)."""
    await dispatch.dispatch(db, test_settings, dispatch.SHIPPING)
    assert [(t, args) for t, args, *_ in sent_jobs] == [("platforms.sync_shop_shipping", [None])]


# ---------------------------------------------------------------- cô lập 3 shop (NFR-39)


async def test_three_shops_one_failing_one_slow_isolated(db: AsyncSession, test_settings: Settings) -> None:
    """3 shop: B luôn lỗi sàn, C chậm hơn ngân sách → mỗi task shop độc lập: A ghi đủ đơn + cursor tiến;
    B `SYNC_FAILED` + `error_since`; C `FAILED` (hết ngân sách), cursor không tiến, A không bị ảnh hưởng."""
    mock = _mock_three()
    a = await _shop(db, test_settings, "990001")
    b = await _shop(db, test_settings, "990002")
    c = await _shop(db, test_settings, "990003")
    mock.fail_shop = {"990002"}
    mock.delay_s_by_shop = {"990003": 0.3}
    test_settings.sync_task_budget_s = 0.5

    results = {}
    for shop in (a, b, c):
        with budget.time_budget(test_settings.sync_task_budget_s):
            results[shop.platform_shop_id] = await sync.sync_orders(db, mock, test_settings, shop.id)

    assert results["990001"][str(a.id)]["status"] == "OK"
    assert results["990001"][str(a.id)]["orders"] == 3
    assert results["990002"][str(b.id)]["status"] == "FAILED"
    assert results["990003"][str(c.id)]["status"] == "FAILED"
    assert "Hết thời gian" in results["990003"][str(c.id)]["error"]
    for shop in (a, b, c):
        await db.refresh(shop)
    assert (a.last_sync_cursor, a.last_error) == (NOW, None)
    assert b.last_error is not None
    assert b.last_error["code"] == "SYNC_FAILED"
    assert b.error_since == NOW
    assert b.last_sync_cursor is None
    assert c.last_sync_cursor is None  # hết ngân sách: cursor không tiến, lượt sau làm lại
    assert c.auth_status == "CONNECTED"
    assert await orders_count(db, a.id) == 3
    # Lượt sau B hết lỗi → xóa lỗi + error_since; không đụng shop khác.
    mock.fail_shop = set()
    await sync.sync_orders(db, mock, test_settings, b.id)
    await db.refresh(b)
    assert (b.last_error, b.error_since, b.last_sync_cursor) == (None, None, NOW)


async def test_run_shop_picks_adapter_by_platform_and_isolates_crash(
    db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Task shop chọn adapter theo `shop.platform`; shop văng lỗi bất ngờ → task đó lỗi, shop khác chạy."""
    mock = _mock_three()
    a = await _shop(db, test_settings, "990001")
    b = await _shop(db, test_settings, "990002")
    monkeypatch.setattr(dispatch.registry, "adapter_for", lambda platform, settings: mock)
    original = sync._upsert

    async def boom(session: AsyncSession, order: PlatformOrder, shop_id: Any) -> bool:
        if shop_id == b.id:
            raise RuntimeError("bất ngờ")
        return await original(session, order, shop_id)

    monkeypatch.setattr(sync, "_upsert", boom)
    out_b = await dispatch.run_shop(db, test_settings, dispatch.ORDERS, b.id)
    out_a = await dispatch.run_shop(db, test_settings, dispatch.ORDERS, a.id)
    assert out_b[str(b.id)] == {"status": "FAILED", "orders": 0, "changed": 0, "error": "RuntimeError"}
    assert out_a[str(a.id)]["status"] == "OK"
    assert await platforms.acquire_sync_lock(b.id, "t")  # lock shop lỗi đã nhả
    assert await dispatch.run_shop(db, test_settings, dispatch.ORDERS, None) == {"skipped": "no_shop"}


async def test_disconnect_mid_run_stops_keeps_written_orders(
    db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ngắt shop khi J-04 đang chạy (02a §6, EX-T7): dừng ở lần kiểm kế, đơn đã ghi giữ, cursor không tiến."""
    a = await _shop(db, test_settings, "990001")
    a_id = a.id
    monkeypatch.setattr(sync, "DISCONNECT_CHECK_EVERY", 1)

    class Disconnecting(MockAdapter):
        async def list_updated_orders(self, creds: Any, since: datetime) -> Any:  # type: ignore[override]
            yield _grouped(_order("2410DISC00001", "SPXDISC0000001"))
            await db.execute(update(Shop).where(Shop.id == a_id).values(auth_status="DISCONNECTED"))
            yield _grouped(_order("2410DISC00002", "SPXDISC0000002"))

    out = await sync.sync_orders(db, Disconnecting(), test_settings, a_id)
    assert out[str(a_id)]["status"] == "SKIPPED"
    assert await orders_count(db, a_id) == 1
    await db.refresh(a)
    assert a.last_sync_cursor is None


def _grouped(order: PlatformOrder) -> PlatformOrder:
    return replace(order, status_group=order_group(order.status))


async def orders_count(db: AsyncSession, shop_id: Any) -> int:
    from sqlalchemy import func, select

    return int(await db.scalar(select(func.count()).select_from(Order).where(Order.shop_id == shop_id)) or 0)


async def test_shipping_per_shop_uses_own_token_only(db: AsyncSession, test_settings: Settings) -> None:
    """J-06 task shop A chỉ tra kiện đơn của A bằng token A; shop B lỗi không chặn A."""
    mock = _mock_three()
    a = await _shop(db, test_settings, "990001")
    b = await _shop(db, test_settings, "990002")
    await sync.sync_orders(db, mock, test_settings, a.id)
    await sync.sync_orders(db, mock, test_settings, b.id)
    from aicam.modules.orders import service as orders
    from aicam.modules.orders.models import Package

    for shop in (a, b):
        rows = (await db.scalars(select_packages(shop.id))).all()
        for package in rows:
            await orders.transition(db, package, "PACKING", source="WAREHOUSE")
            await orders.transition(db, package, "PACKED", source="WAREHOUSE")
    await db.flush()
    mock.fail_shop = {"990002"}
    for code in ("SPXFANA0000001", "SPXFANB0000001"):
        mock.shipping[code] = "PICKED_UP"
    mock.shop_calls.clear()
    out_b = await sync.sync_shipping_status(db, mock, test_settings, b.id)
    out_a = await sync.sync_shipping_status(db, mock, test_settings, a.id)
    assert out_b == {"checked": 0, "changed": 0}
    assert out_a["changed"] == 1
    assert [s for op, s in mock.shop_calls if op == "get_shipping_statuses"] == ["990002", "990001"]
    handed = await db.scalar(select_packages(a.id).where(Package.tracking_number == "SPXFANA0000001"))
    assert handed is not None
    assert handed.warehouse_status == "HANDED_OVER"


def select_packages(shop_id: Any) -> Any:
    from sqlalchemy import select

    from aicam.modules.orders.models import Package

    return select(Package).join(Order, Order.id == Package.order_id).where(Order.shop_id == shop_id)


# ---------------------------------------------------------------- grant (DEC-433, 507)


async def test_grant_refresh_writes_all_connected_shops_of_grant(
    db: AsyncSession, test_settings: Settings
) -> None:
    """Hai shop cùng grant (TikTok `open_id` giả lập bằng Shopee cùng `grant_ref`): J-04 shop A làm mới một
    lần → ghi token cho cả A, B; shop ngắt cùng grant không được ghi; shop khác grant không đụng."""
    mock = MockAdapter()
    a = await _shop(db, test_settings, "990001", grant="G1", expires_in=timedelta(minutes=20))
    b = await _shop(db, test_settings, "990002", grant="G1", expires_in=timedelta(minutes=20))
    d = await _shop(db, test_settings, "990003", grant="G1", status="DISCONNECTED")
    other = await _shop(db, test_settings, "990004", grant="G2", expires_in=timedelta(minutes=20))
    cipher = Cipher(test_settings.fernet_key)

    creds = await grants.ensure_fresh(db, a, mock, cipher)

    assert creds is not None
    assert creds.access_token.startswith("mock-access-")
    assert mock.calls.count("refresh") == 1
    for shop in (a, b, d, other):
        await db.refresh(shop)
    cb = platforms.credentials(b, cipher)
    assert cb is not None
    assert cb.access_token == creds.access_token
    assert b.auth_expires_at == NOW + timedelta(hours=4)
    cd = platforms.credentials(d, cipher)
    assert d.auth_status == "DISCONNECTED"
    assert cd is not None
    assert cd.access_token == "acc-990003"
    co = platforms.credentials(other, cipher)
    assert co is not None
    assert co.access_token == "acc-990004"
    # B dùng luôn token mới (còn hạn) — không đốt refresh token một lần lần nữa.
    again = await grants.ensure_fresh(db, b, mock, cipher)
    assert again is not None
    assert again.access_token == creds.access_token
    assert mock.calls.count("refresh") == 1


async def test_grant_forced_refresh_rereads_after_waiting(
    db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sàn từ chối token của B (force) đúng lúc A cùng grant đang làm mới: B chờ khóa, đọc lại thấy token
    mới → dùng luôn, không refresh lần hai."""
    mock = MockAdapter()
    a = await _shop(db, test_settings, "990001", grant="G1")
    b = await _shop(db, test_settings, "990002", grant="G1")
    cipher = Cipher(test_settings.fernet_key)
    original = grants.acquire

    async def a_refreshed_first(platform: str, ref: str, *, wait_s: float = 10.0) -> str | None:
        monkeypatch.setattr(grants, "acquire", original)
        await grants.ensure_fresh(db, a, mock, cipher, force=True)
        return await original(platform, ref, wait_s=wait_s)

    monkeypatch.setattr(grants, "acquire", a_refreshed_first)
    creds = await grants.ensure_fresh(db, b, mock, cipher, force=True)
    assert creds is not None
    assert creds.access_token.startswith("mock-access-")
    assert mock.calls.count("refresh") == 1


async def test_grant_lock_busy_raises_after_wait(db: AsyncSession, test_settings: Settings) -> None:
    """Khóa grant bận quá thời gian chờ và token chưa đổi → `GrantBusy` (lượt sau), không gọi sàn."""
    mock = MockAdapter()
    a = await _shop(db, test_settings, "990001", expires_in=timedelta(minutes=10))
    token = await grants.acquire("SHOPEE", "990001", wait_s=0)
    with pytest.raises(grants.GrantBusy):
        await grants.ensure_fresh(db, a, mock, Cipher(test_settings.fernet_key), wait_s=0.3)
    assert "refresh" not in mock.calls
    assert token is not None
    await grants.release("SHOPEE", "990001", token)


async def test_grant_auth_error_expires_every_live_shop_of_grant(
    db: AsyncSession, test_settings: Settings
) -> None:
    """Refresh bị từ chối → mọi shop chưa ngắt của grant `EXPIRED` + `AUTH_EXPIRED`; shop ngắt giữ nguyên."""
    mock = MockAdapter()
    mock.fail_refresh = True
    a = await _shop(db, test_settings, "990001", grant="G1", expires_in=timedelta(minutes=10))
    b = await _shop(db, test_settings, "990002", grant="G1")
    d = await _shop(db, test_settings, "990003", grant="G1", status="DISCONNECTED")
    out = await sync.refresh_tokens(db, mock, test_settings)
    assert out == {"refreshed": 0, "expired": 2, "failed": 0, "skipped": 0}
    for shop in (a, b, d):
        await db.refresh(shop)
    assert (a.auth_status, b.auth_status, d.auth_status) == ("EXPIRED", "EXPIRED", "DISCONNECTED")
    assert b.last_error is not None
    assert b.last_error["code"] == "AUTH_EXPIRED"


async def test_j12_skips_disconnected_and_counts_by_grant(db: AsyncSession, test_settings: Settings) -> None:
    """J-12: grant chỉ còn shop ngắt → bỏ cả grant; grant 2 shop sắp hết hạn → một lần refresh, đếm 2 shop."""
    mock = MockAdapter()
    await _shop(db, test_settings, "990001", grant="G1", expires_in=timedelta(minutes=10))
    await _shop(db, test_settings, "990002", grant="G1", expires_in=timedelta(minutes=10))
    await _shop(
        db, test_settings, "990003", grant="G9", status="DISCONNECTED", expires_in=timedelta(minutes=10)
    )
    out = await sync.refresh_tokens(db, mock, test_settings)
    assert out == {"refreshed": 2, "expired": 0, "failed": 0, "skipped": 0}
    assert mock.calls.count("refresh") == 1


async def test_refresh_all_isolates_platform_errors(
    db: AsyncSession, test_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """J-12 mọi sàn bật: sàn TikTok văng lỗi không chặn Shopee."""
    test_settings.tiktok_enabled = True
    calls: list[str] = []

    async def fake(session: AsyncSession, adapter: Any, settings: Settings) -> dict[str, int]:
        calls.append(adapter.code)
        if len(calls) == 2:
            raise RuntimeError("x")
        return {"refreshed": 0}

    monkeypatch.setattr(sync, "refresh_tokens", fake)
    out = await dispatch.refresh_all(db, test_settings)
    assert set(out) == {"SHOPEE", "TIKTOK"}
    assert out["TIKTOK"] == {"error": "RuntimeError"}


def test_celery_tasks_registered() -> None:
    """Tên task phân phối + task shop có trong Celery (beat giữ tên cũ không tham số)."""
    import aicam.workers.tasks  # noqa: F401
    from aicam.workers.celery_app import app

    for name in (
        "platforms.sync_orders", "platforms.sync_shop_orders", "platforms.sync_shipping_status",
        "platforms.sync_shop_shipping", "platforms.sync_returns", "platforms.sync_shop_returns",
        "platforms.refresh_tokens", "platforms.verify_unverified",
    ):  # fmt: skip
        assert name in app.tasks
    assert dispatch.SHOP_TASKS[dispatch.ORDERS] == platforms.SYNC_TASK
    assert dispatch.SHOP_TASKS[dispatch.RETURNS] == platforms.SYNC_RETURNS_TASK
