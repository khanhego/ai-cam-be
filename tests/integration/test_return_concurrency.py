# ruff: noqa: E501 — chuỗi tiếng Việt dài trong docstring / dữ liệu test
"""Đồng thời Phase 2 trên Postgres thật (02a §6, §11 "Đồng thời"; DEC-266, DEC-303 d).

Dữ liệu commit thật, dọn bằng TRUNCATE CASCADE sau test (như `test_scan_concurrency.py`).
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from aicam.core.db import dispose_engine, init_engine, sessionmaker
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings
from aicam.main import create_app
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Package
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

from .returns_helpers import make_order

pytestmark = pytest.mark.integration

TABLES = (
    "scan_dedup, session_event, approval_request, clip, export, inspection_line, session, status_history, "
    'return_case_package, return_case, order_item, package, "order", camera, station, refresh_token, shop'
)


@pytest.fixture
async def committed(
    migrated_database_url: str, redis_client: object, test_settings: Settings
) -> AsyncIterator[AsyncEngine]:
    engine = init_engine(migrated_database_url)
    yield engine
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
        await conn.execute(text("DELETE FROM \"user\" WHERE username LIKE 'tst_rconc_%'"))
    await dispose_engine()


async def _station(name: str, *, mode: str) -> dict[str, str]:
    settings = get_settings()
    async with sessionmaker()() as db:
        user = User(username=f"tst_rconc_{name}", display_name=name, role="STATION", password_hash="x")
        db.add(user)
        await db.flush()
        station = Station(
            name=f"TST {name}", account_user_id=user.id, kind="BOTH", work_mode=mode, operator_name="Lan QA"
        )
        db.add(station)
        await db.commit()
        token, _ = encode_access_token(settings.jwt_secret, user.id, "STATION", station.id, 15)
    return {"Authorization": f"Bearer {token}"}


def _client(test_settings: Settings) -> AsyncClient:
    app = create_app(test_settings)
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_platform_adapter] = lambda: MockAdapter()
    return AsyncClient(transport=ASGITransport(app=app), base_url="https://testserver")


async def _scan(client: AsyncClient, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = await client.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


async def test_two_return_desks_same_package(committed: AsyncEngine, test_settings: Settings) -> None:
    """02a §6: hai bàn hoàn quét cùng kiện → một phiên, bên kia `RETURN_IN_PROGRESS_ELSEWHERE`."""
    async with sessionmaker()() as db:
        await make_order(db, 42, warehouse_status="HANDED_OVER")
        await db.commit()
    a = await _station("a", mode="RETURN")
    b = await _station("b", mode="RETURN")

    async with _client(test_settings) as client:
        results = await asyncio.gather(_scan(client, a, "SPXTST0000042"), _scan(client, b, "SPXTST0000042"))

    assert sorted(r["outcome"] for r in results) == ["ALERT", "SESSION_OPENED"]
    alert = next(r["alert"] for r in results if r["outcome"] == "ALERT")
    assert alert["code"] == "RETURN_IN_PROGRESS_ELSEWHERE"
    async with sessionmaker()() as db:
        assert await db.scalar(select(func.count()).select_from(PackSession)) == 1


async def test_pack_scan_waits_for_manual_adjust(committed: AsyncEngine, test_settings: Settings) -> None:
    """DEC-303 d (ghi chú M6): API-122 giữ khóa kiện → quét PACK chờ, thấy trạng thái mới, không ghi đè."""
    async with sessionmaker()() as db:
        await make_order(db, 21, status="READY_TO_SHIP", warehouse_status="NEW")
        await db.commit()
    headers = await _station("pack", mode="PACK")

    from aicam.modules.orders import adjust as adjust_mod

    async with sessionmaker()() as db:
        package = await orders.find_package(db, "SPXTST0000021")
        assert package is not None
        package_id = package.id
        sup = User(username="tst_rconc_sup", display_name="Sup", role="SUPERVISOR", password_hash="x")
        db.add(sup)
        await db.commit()
        sup_id = sup.id
    locked, release = asyncio.Event(), asyncio.Event()
    real_commit = adjust_mod.commit

    async def held_commit(session: Any) -> None:  # API-122 thật giữ khóa kiện tới khi test cho commit
        locked.set()
        await release.wait()
        await real_commit(session)

    async def run_adjust() -> None:
        async with sessionmaker()() as db:
            await adjust_mod.adjust_status(
                db, package_id, adjust_mod.AdjustIn(to_status="HANDED_OVER", reason="ĐVVC đã lấy hàng"),
                actor=sup_id, ip=None, tz="Asia/Ho_Chi_Minh",
            )  # fmt: skip

    adjust_mod.commit = held_commit  # type: ignore[assignment]
    try:
        async with _client(test_settings) as client:
            adjusting = asyncio.create_task(run_adjust())
            await asyncio.wait_for(locked.wait(), timeout=5)
            scan = asyncio.create_task(_scan(client, headers, "SPXTST0000021"))
            await asyncio.sleep(0.5)
            assert not scan.done()  # bị chặn bởi khóa kiện
            release.set()
            await asyncio.wait_for(adjusting, timeout=5)
            result = await asyncio.wait_for(scan, timeout=5)
    finally:
        adjust_mod.commit = real_commit  # type: ignore[assignment]

    assert result["outcome"] == "ALERT"
    assert result["alert"]["code"] == "ALREADY_HANDED_OVER"
    async with sessionmaker()() as db:
        package = await orders.find_package(db, "SPXTST0000021")
        assert package is not None
        assert package.warehouse_status == "HANDED_OVER"


async def test_flag_order_cancelled_races_closing_scan(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """TC-03.77, DEC-266, R3-8: `flag_order_cancelled` ∥ quét đóng → không deadlock, kiện cuối luôn
    `CANCELLED_AFTER_PACK` (task trước → cờ rồi đóng; đóng trước → task chuyển từ `PACKED`)."""
    from aicam.modules.sessions import service as sessions

    headers = await _station("race", mode="PACK")
    async with _client(test_settings) as client:
        for n in range(23, 28):
            code = f"SPXTST{n:07d}"
            assert (await _scan(client, headers, code))["outcome"] == "SESSION_OPENED"
            async with sessionmaker()() as db:
                package = await orders.find_package(db, code)
                assert package is not None
                package_id = package.id

            async def run_task(pid: uuid.UUID = package_id) -> str:
                async with sessionmaker()() as db:
                    return await sessions.flag_order_cancelled(db, pid, test_settings)

            await asyncio.wait_for(asyncio.gather(_scan(client, headers, code), run_task()), timeout=5)
            async with sessionmaker()() as db:
                package = await orders.find_package(db, code)
            assert package is not None
            assert package.warehouse_status == "CANCELLED_AFTER_PACK", code


async def test_scan_j13_j06_same_order_no_deadlock(committed: AsyncEngine, test_settings: Settings) -> None:
    """02a §11 "Đồng thời" (R3-4, DEC-266): API-11 RETURN mở hồ sơ mới ∥ J-13 cùng đơn ∥ J-06 cùng kiện →
    không deadlock (≤ 5 giây), đúng một hồ sơ mở mỗi đơn, kiện ở trạng thái hợp lệ."""
    from dataclasses import replace
    from datetime import timedelta

    from aicam.core import clock
    from aicam.core.security import Cipher
    from aicam.modules.orders.models import Shop
    from aicam.modules.platforms import service as platforms
    from aicam.modules.platforms import sync
    from aicam.modules.platforms.base import ShopCredentials
    from aicam.modules.returns.models import OPEN_CASE_STATUSES, ReturnCase

    from .returns_helpers import platform_return

    test_settings.shopee_enabled = True
    async with sessionmaker()() as db:
        shop = Shop(platform="SHOPEE", platform_shop_id="990001", name="TST Shop", auth_status="CONNECTED")
        platforms.store_credentials(
            shop, ShopCredentials("990001", "a", "r", clock.now() + timedelta(hours=4)),
            Cipher(test_settings.fernet_key),
        )  # fmt: skip
        db.add(shop)
        for n in range(61, 66):
            await make_order(db, n, status="SHIPPED", warehouse_status="HANDED_OVER")
        await db.commit()
    headers = await _station("sync", mode="RETURN")
    mock = MockAdapter()
    mock.returns = {}
    for n in range(61, 66):
        code = f"SPXTST{n:07d}"
        mock.orders[f"2410TST{n:05d}"] = replace(
            mock.orders["2410TST00001"], platform_order_sn=f"2410TST{n:05d}", status="SHIPPED",
            tracking_numbers=(code,),
        )  # fmt: skip
        mock.shipping[code] = "DELIVERY_FAILED"
        mock.put_return(replace(platform_return(n), updated_at=clock.now()))

    gate = asyncio.Event()  # G3 BB-17 (a): ba đường cùng xuất phát — ép chồng nhau

    async def j13() -> object:
        await gate.wait()
        async with sessionmaker()() as db:
            return await sync.sync_returns(db, mock, test_settings)

    async def j06() -> object:
        await gate.wait()
        async with sessionmaker()() as db:
            return await sync.sync_shipping_status(db, mock, test_settings)

    async def scan61(client: AsyncClient) -> object:
        await gate.wait()
        return await _scan(client, headers, "SPXTST0000061")

    async with _client(test_settings) as client:
        tasks = [asyncio.create_task(c) for c in (scan61(client), j13(), j06())]
        await asyncio.sleep(0.05)
        gate.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

    async with sessionmaker()() as db:
        for n in range(61, 66):
            order_id = await db.scalar(select(text("id")).select_from(text('"order"')).where(
                text("platform_order_sn = :sn")).params(sn=f"2410TST{n:05d}"))  # fmt: skip
            open_cases = await db.scalar(
                select(func.count())
                .select_from(ReturnCase)
                .where(ReturnCase.order_id == order_id, ReturnCase.status.in_(OPEN_CASE_STATUSES))
            )
            assert open_cases == 1, n
            package = await orders.find_package(db, f"SPXTST{n:07d}")
            assert package is not None
            assert package.warehouse_status in ("RETURN_EXPECTED", "RETURN_INSPECTING"), n


async def test_j14_skips_locked_package(committed: AsyncEngine, test_settings: Settings) -> None:
    """02a §6 (DEC-256 / 266): J-14 dùng `SKIP LOCKED` — kiện (hoặc hồ sơ) đang bị API / job khác khóa thì
    bỏ qua lượt này, không chờ, không deadlock; lượt sau chuyển `RETURN_MISSING`."""
    from datetime import timedelta

    from aicam.core import clock
    from aicam.modules.reconciliation import service as recon

    async with sessionmaker()() as db:
        _, (package,) = await make_order(db, 71, warehouse_status="RETURN_EXPECTED")
        package.status_changed_at = clock.now() - timedelta(days=8)
        await db.commit()
        package_id = package.id

    async with sessionmaker()() as holder, sessionmaker()() as job:
        locked = await orders.find_package(holder, "SPXTST0000071", for_update=True)
        assert locked is not None
        out = await asyncio.wait_for(recon.run_rules(job, test_settings), timeout=5)
        assert out["missing"] == 0
        await holder.rollback()
        out = await asyncio.wait_for(recon.run_rules(job, test_settings), timeout=5)
        assert out["missing"] == 1
    async with sessionmaker()() as db:
        package = await db.get(Package, package_id)
        assert package is not None
        assert package.warehouse_status == "RETURN_MISSING"


# ---------------------------------------------------------------- G3 (SM-F4, SM-F5, SM-F8, BB-17 c)


async def test_platform_cancel_waits_for_pack_scan(committed: AsyncEngine, test_settings: Settings) -> None:
    """SM-F4: quét PACK đang giữ kiện (NEW → PACKING, chưa commit) ∥ J-04 đơn hủy → J-04 chờ khóa kiện, đọc
    lại
    thấy PACKING → đẩy task gắn cờ phiên, KHÔNG ghi đè thành CANCELLED."""
    from aicam.modules.media import jobs
    from aicam.modules.platforms.base import PlatformItem, PlatformOrder

    async with sessionmaker()() as db:
        await make_order(db, 31, status="READY_TO_SHIP", warehouse_status="NEW")
        await db.commit()
    sent: list[Any] = []
    jobs.set_sender(lambda task, args, queue, countdown: sent.append((task, args)))
    data = PlatformOrder("2410TST00031", "CANCELLED", ("SPXTST0000031",),
                         (PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"),), status_group=shopee_order_group("CANCELLED"))  # fmt: skip
    try:
        async with sessionmaker()() as scan:
            package = await orders.find_package(scan, "SPXTST0000031", for_update=True)
            assert package is not None
            await orders.transition(scan, package, "PACKING", source="WAREHOUSE")

            async def j04() -> None:
                async with sessionmaker()() as db:
                    await orders.lock_orders(db, [data.platform_order_sn])
                    await orders.upsert_platform_order(db, data)
                    from aicam.core.db import commit

                    await commit(db)

            task = asyncio.create_task(j04())
            await asyncio.sleep(0.4)
            assert not task.done()  # chờ khóa kiện
            await scan.commit()
        await asyncio.wait_for(task, timeout=5)
    finally:
        jobs.set_sender(None)
    async with sessionmaker()() as db:
        package = await orders.find_package(db, "SPXTST0000031")
        assert package is not None
        assert package.warehouse_status == "PACKING"
    assert ("sessions.flag_order_cancelled", [str(package.id)]) in sent


async def test_two_desks_open_same_unknown_code(committed: AsyncEngine, test_settings: Settings) -> None:
    """SM-F8: hai bàn cùng mở "chưa xác định" cho một mã lạ → một hồ sơ, bên kia thấy đang kiểm nơi khác."""
    from aicam.modules.returns.models import ReturnCase

    a = await _station("ua", mode="RETURN")
    b = await _station("ub", mode="RETURN")

    async def open_unknown(client: AsyncClient, headers: dict[str, str]) -> dict[str, Any]:
        body = {"client_scan_id": str(uuid.uuid4()), "unidentified_code": "SPXVN0000000999"}
        res = await client.post("/api/v1/station/return-sessions", headers=headers, json=body)
        assert res.status_code == 200, res.text
        return res.json()  # type: ignore[no-any-return]

    async with _client(test_settings) as client:
        results = await asyncio.wait_for(
            asyncio.gather(open_unknown(client, a), open_unknown(client, b)), timeout=5
        )
    assert sorted(r["outcome"] for r in results) == ["ALERT", "SESSION_OPENED"]
    async with sessionmaker()() as db:
        n = await db.scalar(
            select(func.count()).select_from(ReturnCase).where(ReturnCase.kind == "UNIDENTIFIED")
        )
        assert n == 1


async def test_claim_from_alert_vs_manual_resolve(committed: AsyncEngine, test_settings: Settings) -> None:
    """BB-17 (c): tạo hồ sơ từ cảnh báo ∥ API-121 xử lý cùng cảnh báo → không lỗi 500 / không deadlock; cảnh
    báo
    đóng đúng một lần (một bên thắng)."""
    from datetime import UTC, datetime

    from aicam.core.deps import Principal
    from aicam.core.errors import AppError
    from aicam.modules.claims import service as claims
    from aicam.modules.claims.schemas import ClaimCreateIn
    from aicam.modules.reconciliation import service as recon
    from aicam.modules.reconciliation.models import ReconAlert

    async with sessionmaker()() as db:
        _, (package,) = await make_order(db, 32, warehouse_status="NEW")
        user = User(username="tst_rconc_cskh", display_name="CSKH", role="SUPERVISOR", password_hash="x")
        db.add(user)
        now = datetime.now(UTC)
        alert = ReconAlert(package_id=package.id, rule="SHIPPED_NOT_PACKED", severity="HIGH", status="OPEN",
                           context={}, context_key="SHIPPED", detected_at=now, last_seen_at=now)  # fmt: skip
        db.add(alert)
        await db.commit()
        ids = (package.id, user.id, alert.id)
    package_id, user_id, alert_id = ids
    gate = asyncio.Event()

    async def create() -> str:
        await gate.wait()
        async with sessionmaker()() as db:
            try:
                body = ClaimCreateIn(package_id=package_id, type="OTHER", counterparty="PLATFORM",
                                     recon_alert_id=alert_id)  # fmt: skip
                await claims.create_manual(db, body, Principal(user_id=user_id, role="SUPERVISOR",
                                                                station_id=None, ip=None))  # fmt: skip
                from aicam.core.db import commit

                await commit(db)
                return "claim"
            except AppError as exc:
                await db.rollback()
                return exc.code

    async def resolve() -> str:
        await gate.wait()
        async with sessionmaker()() as db:
            try:
                await recon.resolve(db, alert_id, "đã kiểm", actor=user_id, ip=None, tz="Asia/Ho_Chi_Minh")
                return "resolved"
            except AppError as exc:
                await db.rollback()
                return exc.code

    tasks = [asyncio.create_task(create()), asyncio.create_task(resolve())]
    gate.set()
    results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)
    async with sessionmaker()() as db:
        row = await db.get(ReconAlert, alert_id)
        assert row is not None
        assert row.status == "RESOLVED"
        assert row.resolution_action in ("OPEN_CLAIM", "RESOLVE")
    assert "INTERNAL" not in results
    await asyncio.sleep(0)
    async with committed.begin() as conn:
        await conn.execute(text("TRUNCATE recon_alert, claim CASCADE"))


async def test_close_with_pending_merge_while_order_locked(
    committed: AsyncEngine, test_settings: Settings
) -> None:
    """SM-F5: đóng phiên chưa xác định có đơn chờ gộp trong lúc J-13 / J-04 giữ `order:{sn}` → không chờ khóa
    (không khóa chéo), phiên đóng xong, hồ sơ vẫn chờ gộp (lượt đồng bộ sau gộp)."""
    from aicam.modules.returns.models import ReturnCase

    async with sessionmaker()() as db:
        order, _ = await make_order(db, 33)
        await db.commit()
        order_id, sn = order.id, order.platform_order_sn
    headers = await _station("pm", mode="RETURN")
    code = "SPXVN0000000333"
    async with _client(test_settings) as client:
        res = await client.post("/api/v1/station/return-sessions", headers=headers,
                                json={"client_scan_id": str(uuid.uuid4()), "unidentified_code": code})  # fmt: skip
        session = res.json()["state"]["session"]
        lines = [{"order_item_id": x["order_item_id"], "quantity_received": x["quantity_received"],
                  "condition": "OK", "note": None} for x in session["inspection"]["lines"]]  # fmt: skip
        saved = await client.put(f"/api/v1/station/sessions/{session['id']}/inspection", headers=headers,
                                 json={"conclusion": "OK", "note": "", "lines": lines})  # fmt: skip
        assert saved.status_code == 200, saved.text
        case_id = uuid.UUID(session["return_case"]["id"])
        async with sessionmaker()() as db:
            await db.execute(text("UPDATE return_case SET pending_merge_order_id = :o WHERE id = :c"),
                             {"o": order_id, "c": case_id})  # fmt: skip
            await db.commit()
        # Đơn chờ gộp được gắn SAU bước khóa ngoài station (J-13 vừa thấy mã) và J-13 vẫn đang giữ đơn: đóng phiên
        # (người gọi đã giữ station + hồ sơ) không chờ khóa đơn — để chờ gộp.
        from aicam.modules.sessions import return_scan
        from aicam.modules.sessions.models import PackSession as PS

        async with sessionmaker()() as holder:
            await orders.lock_orders(holder, [sn])  # J-13 đang xử lý đơn
            async with sessionmaker()() as db:
                pack = await db.get(PS, uuid.UUID(session["id"]))
                case = await db.get(ReturnCase, case_id)
                assert pack is not None
                assert case is not None
                closed = await asyncio.wait_for(
                    return_scan.close_return_session(db, pack, case, code=code, actor_label="TST"), timeout=5
                )
                await db.commit()
            await holder.rollback()
    assert closed.conclusion == "OK"
    async with sessionmaker()() as db:
        case = await db.get(ReturnCase, case_id)
        assert case is not None
        assert (case.order_id, case.pending_merge_order_id) == (None, order_id)
