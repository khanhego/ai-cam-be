"""T-212 — station Phase 3: API-10 sàn / shop / kiện gộp / `operator_required`, API-11 `OPERATOR_REQUIRED`
ở PACK, `ORDER_CANCEL_REQUESTED`, chép `operator_name`;
API-80 `packer_name_required`; API-31 trường đơn.

FR-03.03, 03.16, 05.17, 05.22; DEC-455, DEC-541 (5). Adapter mock.
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Shop
from aicam.modules.platforms.base import PlatformItem, PlatformOrder
from aicam.modules.platforms.mock.adapter import MockAdapter
from aicam.modules.platforms.shopee.mapping import order_group as shopee_order_group
from aicam.modules.sessions.models import PackSession
from aicam.modules.sessions.router import get_platform_adapter
from aicam.modules.settings.models import Setting
from aicam.realtime import publish

from .factories import PASSWORD, make_station_account, make_user

pytestmark = pytest.mark.integration


class _CountingMock(MockAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.lookups: list[str] = []

    async def find_by_tracking(self, creds: Any, tracking_number: str) -> PlatformOrder | None:
        self.lookups.append(tracking_number)
        return await super().find_by_tracking(creds, tracking_number)


@pytest.fixture
def adapter(api: AsyncClient) -> _CountingMock:
    mock = _CountingMock()
    api._transport.app.dependency_overrides[get_platform_adapter] = lambda: mock  # type: ignore[attr-defined]
    return mock


async def _login(api: AsyncClient, username: str, client: str = "DASHBOARD") -> dict[str, str]:
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": client}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@pytest.fixture
async def station(
    api: AsyncClient, db: AsyncSession, adapter: _CountingMock
) -> tuple[dict[str, str], uuid.UUID]:
    user, st = await make_station_account(db, "tst_st212", "TST Station 212")
    return await _login(api, user.username, "STATION"), st.id


async def _scan(api: AsyncClient, headers: dict[str, str], code: str) -> Response:
    return await api.post(
        "/api/v1/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )


def _order(
    sn: str, *codes: str, status: str = "READY_TO_SHIP", merged: tuple[str, ...] = (), name: str = "Áo"
) -> PlatformOrder:
    return PlatformOrder(
        platform_order_sn=sn, status=status, tracking_numbers=codes,
        items=(PlatformItem(name, 1, f"SKU-{sn}"),), status_group=shopee_order_group(status),
        merged_order_sns=merged,
    )  # fmt: skip


async def _shop(db: AsyncSession, platform: str = "TIKTOK", name: str = "Áo Đẹp Official") -> Shop:
    shop = Shop(
        platform=platform, platform_shop_id=f"ext-{uuid.uuid4().hex[:6]}", name=name, auth_status="CONNECTED"
    )
    db.add(shop)
    await db.flush()
    return shop


async def _require_packer(db: AsyncSession, on: bool = True) -> None:
    row = await db.get(Setting, 1)
    assert row is not None
    row.packer_name_required = on
    await db.flush()


# ---------------------------------------------------------------- API-10 sàn / shop / kiện gộp


async def test_state_order_platform_shop_and_merged_items(
    api: AsyncClient, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    headers, _ = station
    shop = await _shop(db)
    await orders.upsert_platform_order(db, _order("5761A", "581234567890", name="Áo thun"), shop_id=shop.id)
    await orders.upsert_platform_order(
        db, _order("5761B", "581234567890", merged=("5761A",), name="Tất"), shop_id=shop.id
    )

    body = (await _scan(api, headers, "581234567890")).json()

    assert body["outcome"] == "SESSION_OPENED"
    package = body["state"]["session"]["package"]
    assert package["order"] == {
        "platform": "TIKTOK", "shop_name": "Áo Đẹp Official", "platform_order_sn": "5761A",
        "buyer_note": None, "merged_orders": [{"platform_order_sn": "5761B"}],
    }  # fmt: skip
    assert [(i["product_name"], i["platform_order_sn"]) for i in package["items"]] == [
        ("Áo thun", "5761A"),
        ("Tất", "5761B"),
    ]


async def test_file_order_platform_is_null(
    api: AsyncClient, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    """DEC-541 (5): đơn file chưa gắn shop → `platform = null` (FE chip "Chưa rõ sàn")."""
    headers, _ = station
    order = Order(platform_order_sn="FILE212", source="CSV")
    db.add(order)
    await db.flush()
    await orders.sync_items(db, order.id, (PlatformItem("Hàng file", 1),), with_image=False)
    await orders.create_unverified_package(db, "SPXFILE212")
    package = await orders.find_package(db, "SPXFILE212")
    assert package is not None
    package.order_id, package.verified = order.id, True
    await db.flush()

    body = (await _scan(api, headers, "SPXFILE212")).json()

    assert body["state"]["session"]["package"]["order"]["platform"] is None
    assert body["state"]["session"]["package"]["order"]["shop_name"] is None
    res = await api.get(f"/api/v1/packages/{package.id}", headers=await _admin(api, db))
    assert res.status_code == 200
    assert (res.json()["order"]["platform"], res.json()["order"]["shop"]) == (None, None)


async def _admin(api: AsyncClient, db: AsyncSession) -> dict[str, str]:
    username = f"tst_admin_{uuid.uuid4().hex[:6]}"
    await make_user(db, username, "ADMIN")
    return await _login(api, username)


async def test_package_detail_order_shop_group_merged(api: AsyncClient, db: AsyncSession) -> None:
    """API-31 (02 §6.2): `order.{platform, shop, platform_status_group, merged_orders[]}`."""
    shop = await _shop(db, "SHOPEE", "Áo Đẹp")
    main = await orders.upsert_platform_order(db, _order("2410M", "SPX31M"), shop_id=shop.id)
    await orders.upsert_platform_order(db, _order("2410N", "SPX31M", merged=("2410M",)), shop_id=shop.id)

    res = await api.get(f"/api/v1/packages/{main.packages[0].id}", headers=await _admin(api, db))

    order = res.json()["order"]
    assert order["platform"] == "SHOPEE"
    assert order["shop"] == {"id": str(shop.id), "name": "Áo Đẹp"}
    assert order["platform_status_group"] == "AWAITING_SHIPMENT"
    assert order["merged_orders"] == [{"platform_order_sn": "2410N"}]


# ---------------------------------------------------------------- FR-03.16 người đóng gói


async def test_operator_required_blocks_pack_scan_before_lookup(
    api: AsyncClient, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID], adapter: _CountingMock
) -> None:
    headers, _ = station
    await _require_packer(db)

    state = (await api.get("/api/v1/station/state", headers=headers)).json()
    assert state["station"]["operator_required"] is True
    blocked = (await _scan(api, headers, "SPXTST0000012")).json()

    assert blocked["outcome"] == "ALERT"
    assert blocked["alert"]["code"] == "OPERATOR_REQUIRED"
    assert blocked["alert"]["data"] == {"mode": "PACK"}
    assert blocked["alert"]["message"] == "Nhập tên người đóng gói trước khi đóng gói."
    assert adapter.lookups == []  # chưa tra sàn
    assert await db.scalar(select(func.count()).select_from(PackSession)) == 0

    put = await api.put("/api/v1/station/operator", headers=headers, json={"name": "  Minh  "})
    assert put.status_code == 200, put.text
    opened = (await _scan(api, headers, "SPXTST0000012")).json()

    assert opened["outcome"] == "SESSION_OPENED"
    assert opened["state"]["session"]["operator_name"] == "Minh"
    pack = await db.scalar(select(PackSession))
    assert pack is not None
    assert pack.operator_name == "Minh"


async def test_operator_not_required_when_setting_off_or_return_mode(
    api: AsyncClient, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    headers, _ = station
    state = (await api.get("/api/v1/station/state", headers=headers)).json()
    assert state["station"]["operator_required"] is False
    opened = (await _scan(api, headers, "SPXTST0000003")).json()
    assert opened["outcome"] == "SESSION_OPENED"
    assert opened["state"]["session"]["operator_name"] is None  # không bật → phiên không có tên (Phase 2)


async def test_return_operator_required_alert_has_mode(
    api: AsyncClient, db: AsyncSession, adapter: _CountingMock
) -> None:
    from aicam.modules.stations.models import Station

    user, st = await make_station_account(db, "tst_st212r", "TST Return 212")
    st.kind, st.work_mode = "BOTH", "RETURN"
    await db.flush()
    await _require_packer(db)  # setting bật không ảnh hưởng bàn hoàn
    headers = await _login(api, user.username, "STATION")
    state = (await api.get("/api/v1/station/state", headers=headers)).json()
    assert state["station"]["operator_required"] is False
    body = (await _scan(api, headers, "SPXTST0000041")).json()
    assert (body["alert"]["code"], body["alert"]["data"]) == ("OPERATOR_REQUIRED", {"mode": "RETURN"})
    assert isinstance(await db.get(Station, st.id), Station)


async def test_settings_packer_name_required_put_get_and_ws(
    api: AsyncClient,
    db: AsyncSession,
    station: tuple[dict[str, str], uuid.UUID],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, station_id = station
    sent: list[tuple[uuid.UUID, str, Any]] = []

    async def _to_station(sid: uuid.UUID, event: str, data: Any) -> None:
        sent.append((sid, event, data))

    monkeypatch.setattr(publish, "to_station", _to_station)
    admin = await _admin(api, db)
    body = {"retention_raw_days": 30, "retention_clip_days": 90, "session_warn_minutes": 10,
            "session_abandon_minutes": 30}  # fmt: skip
    assert (await api.get("/api/v1/settings", headers=admin)).json()["packer_name_required"] is False

    res = await api.put("/api/v1/settings", headers=admin, json={**body, "packer_name_required": True})

    assert res.status_code == 200, res.text
    assert res.json()["packer_name_required"] is True
    states = [d for sid, ev, d in sent if sid == station_id and ev == "station.state"]
    assert states
    assert states[-1]["station"]["operator_required"] is True
    sent.clear()
    keep = await api.put("/api/v1/settings", headers=admin, json=body)  # thiếu = giữ; không đổi → không đẩy
    assert keep.json()["packer_name_required"] is True
    assert sent == []
    bad = await api.put("/api/v1/settings", headers=admin, json={**body, "packer_name_required": "x"})
    assert bad.status_code == 422


# ---------------------------------------------------------------- BR-01 theo nhóm


async def test_cancel_requested_and_cancelled_alerts(
    api: AsyncClient, db: AsyncSession, station: tuple[dict[str, str], uuid.UUID]
) -> None:
    headers, _ = station
    shop = await _shop(db, "SHOPEE", "Áo Đẹp")
    ic = await orders.upsert_platform_order(db, _order("2410IC", "SPXIC212"), shop_id=shop.id)
    # Đơn vào nhóm CANCEL_REQUESTED khi kiện còn NEW (đường đồng bộ không hủy kiện — T-278).
    ic.order.platform_status, ic.order.platform_status_group = "IN_CANCEL", "CANCEL_REQUESTED"
    await db.flush()
    await orders.upsert_platform_order(db, _order("2410CC", "SPXCC212", status="CANCELLED"), shop_id=shop.id)

    requested = (await _scan(api, headers, "SPXIC212")).json()
    cancelled = (await _scan(api, headers, "SPXCC212")).json()

    assert requested["alert"]["code"] == "ORDER_CANCEL_REQUESTED"
    assert requested["alert"]["data"] == {"platform": "SHOPEE"}
    assert cancelled["alert"]["code"] == "ORDER_CANCELLED"
    assert cancelled["alert"]["message"] == "SPXCC212 đã bị hủy trên sàn. Không đóng gói."
    assert "Shopee" not in cancelled["alert"]["message"]
    assert await db.scalar(select(func.count()).select_from(PackSession)) == 0
