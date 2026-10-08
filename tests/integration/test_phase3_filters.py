"""T-215 — L13 + D2 + lọc sàn / shop: API-110 (`pending_only` BR-40, `sort`, `response_due_*`, `claim`),
API-80 `refund_only_default_hours`, API-32 counts / attention mới + lọc vai, API-30 / 110 / 120 / 130
`platform`,
`shop_id`, API-30 `session_status` nhiều giá trị + `return_dropped` (BR-39). FR-08.08, 09.01, 07.01; AC-57.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.claims.models import Claim
from aicam.modules.orders.models import Order, Package, Shop
from aicam.modules.reconciliation.models import ReconAlert
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_station_account, make_user
from .returns_helpers import buyer_return_case, make_order, return_session

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 6, 2, 0, tzinfo=UTC)  # 09:00 VN


@pytest.fixture(autouse=True)
def _clock(redis_client: object) -> None:
    clock.freeze(NOW)


async def _login(api: AsyncClient, db: AsyncSession, role: str) -> dict[str, str]:
    username = f"tst_{role.lower()}_{uuid.uuid4().hex[:6]}"
    await make_user(db, username, role)
    res = await api.post(
        "/api/v1/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
    )
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


async def _shop(db: AsyncSession, platform: str, name: str) -> Shop:
    shop = Shop(platform=platform, platform_shop_id=uuid.uuid4().hex[:8], name=name, auth_status="CONNECTED")
    db.add(shop)
    await db.flush()
    return shop


async def _refund_case(
    db: AsyncSession, n: int, shop: Shop | None, *, due: datetime | None, reported: datetime | None = None
) -> tuple[ReturnCase, Order, Package]:
    order, packages = await make_order(db, n)
    order.shop_id = shop.id if shop else None
    case = await buyer_return_case(db, order, n, needs_parcel=False, status="REQUESTED")
    case.seller_due_at = due
    case.reported_at = reported or NOW - timedelta(hours=2)
    await db.flush()
    assert case.kind == "REFUND_ONLY"
    return case, order, packages[0]


def _claim(package: Package, order: Order | None, status: str = "NEW", **kw: Any) -> Claim:
    return Claim(
        package_id=package.id, order_id=order.id if order else None, type="OTHER", counterparty="PLATFORM",
        status=status, source="MANUAL", **kw,
    )  # fmt: skip


# ---------------------------------------------------------------- API-110 (BR-40, FR-08.08)


async def test_refund_pending_due_sort_claim_and_shop(api: AsyncClient, db: AsyncSession) -> None:
    """AC-57: chưa xử lý = REFUND_ONLY, yêu cầu sàn còn mở, đơn chưa có KN chưa đóng; hạn = hạn sàn / báo +
    48 giờ."""
    a = await _shop(db, "SHOPEE", "Áo Đẹp")
    b = await _shop(db, "TIKTOK", "Áo Đẹp Official")
    c1, _, _ = await _refund_case(db, 51, a, due=NOW + timedelta(hours=30))  # hạn sàn
    c2, _, _ = await _refund_case(db, 52, b, due=None, reported=NOW - timedelta(hours=40))  # mặc định: +8 giờ
    c3, o3, p3 = await _refund_case(
        db, 53, a, due=NOW + timedelta(hours=5)
    )  # có KN chưa đóng → không pending
    db.add(_claim(p3, o3))
    c4, o4, p4 = await _refund_case(db, 54, a, due=NOW + timedelta(hours=1))  # KN đã đóng → vẫn pending
    db.add(_claim(p4, o4, status="CLOSED", closed_at=NOW))
    c5, _, _ = await _refund_case(db, 55, a, due=NOW + timedelta(hours=2))
    c5.platform_status_group = "DONE"  # sàn đã xử lý → không pending
    await db.flush()
    sup = await _login(api, db, "SUPERVISOR")

    res = await api.get("/api/v1/returns", headers=sup, params={"tab": "NO_PARCEL", "pending_only": "true"})

    assert res.status_code == 200, res.text
    items = res.json()["items"]
    # sort mặc định tab NO_PARCEL = hạn gần nhất trước
    assert [i["code"] for i in items] == [c4.code, c2.code, c1.code]
    by_code = {i["code"]: i for i in items}
    assert by_code[c1.code]["response_due_source"] == "PLATFORM"
    assert by_code[c1.code]["response_due_at"] == clock.iso_z(NOW + timedelta(hours=30))
    assert by_code[c2.code]["response_due_source"] == "DEFAULT"
    assert by_code[c2.code]["response_due_at"] == clock.iso_z(NOW + timedelta(hours=8))
    assert (by_code[c2.code]["platform"], by_code[c2.code]["shop"]["name"]) == ("TIKTOK", "Áo Đẹp Official")
    assert by_code[c1.code]["platform_status_group"] == "REQUESTED"
    assert by_code[c4.code]["claim"] is None

    all_items = (await api.get("/api/v1/returns", headers=sup, params={"tab": "NO_PARCEL"})).json()["items"]
    with_claim = next(i for i in all_items if i["code"] == c3.code)
    assert with_claim["claim"]["code"].startswith("KN-")
    assert {i["code"] for i in all_items} == {c1.code, c2.code, c3.code, c4.code, c5.code}

    shop_b = (
        await api.get("/api/v1/returns", headers=sup, params={"tab": "ALL", "shop_id": str(b.id)})
    ).json()
    assert [i["code"] for i in shop_b["items"]] == [c2.code]
    tiktok = (
        await api.get("/api/v1/returns", headers=sup, params={"tab": "ALL", "platform": "TIKTOK"})
    ).json()
    assert [i["code"] for i in tiktok["items"]] == [c2.code]
    created = (
        await api.get("/api/v1/returns", headers=sup, params={"tab": "NO_PARCEL", "sort": "created_desc"})
    ).json()
    assert len(created["items"]) == 5
    bad = await api.get("/api/v1/returns", headers=sup, params={"platform": "LAZADA"})
    assert bad.status_code == 422


async def test_refund_default_hours_setting_applies_immediately(api: AsyncClient, db: AsyncSession) -> None:
    """DEC-451: hạn mặc định tính lúc đọc — đổi API-80 áp ngay; ngoài 1..168 → 422."""
    case, _, _ = await _refund_case(db, 56, None, due=None, reported=NOW)
    admin = await _login(api, db, "ADMIN")
    body = {"retention_raw_days": 30, "retention_clip_days": 90, "session_warn_minutes": 10,
            "session_abandon_minutes": 30}  # fmt: skip
    assert (await api.get("/api/v1/settings", headers=admin)).json()["refund_only_default_hours"] == 48

    res = await api.put("/api/v1/settings", headers=admin, json={**body, "refund_only_default_hours": 24})
    assert res.status_code == 200
    assert res.json()["refund_only_default_hours"] == 24
    item = (await api.get("/api/v1/returns", headers=admin, params={"tab": "NO_PARCEL"})).json()["items"][0]
    assert (item["code"], item["response_due_at"]) == (case.code, clock.iso_z(NOW + timedelta(hours=24)))
    for bad in (0, 169):
        r = await api.put("/api/v1/settings", headers=admin, json={**body, "refund_only_default_hours": bad})
        assert r.status_code == 422
        assert "refund_only_default_hours" in r.json()["error"]["details"]["fields"]


# ---------------------------------------------------------------- API-30 lọc + BR-39 return_dropped


async def _return_session(
    db: AsyncSession, station: Station, package: Package, case: ReturnCase, status: str, **kw: Any
) -> PackSession:
    s = return_session(station, package, case, status=status, conclusion=None)
    s.started_at, s.ended_at = NOW - timedelta(hours=1), NOW - timedelta(minutes=50)
    for k, v in kw.items():
        setattr(s, k, v)
    db.add(s)
    await db.flush()
    return s


async def test_packages_platform_shop_session_status_and_return_dropped(
    api: AsyncClient, db: AsyncSession
) -> None:
    a = await _shop(db, "SHOPEE", "Áo Đẹp")
    b = await _shop(db, "TIKTOK", "Áo Đẹp Official")
    _, station = await make_station_account(db, "tst_st215", "TST 215")
    rows: dict[str, Package] = {}
    sessions: dict[str, PackSession] = {}
    specs: list[tuple[str, Shop | None, str, dict[str, Any]]] = [
        ("abandoned", a, "ABANDONED", {}),
        ("wrong_scan", a, "CANCELLED", {"cancel_reason": "WRONG_SCAN"}),
        ("not_return", b, "CANCELLED", {"cancel_reason": "NOT_A_RETURN"}),
        ("sup_wrong", b, "CANCELLED", {"cancel_reason": "SUPERVISOR", "cancel_cause": "WRONG_SCAN"}),
        ("sup_other", b, "CANCELLED", {"cancel_reason": "SUPERVISOR", "cancel_cause": "OTHER"}),
        ("review", a, "CANCELLED", {"cancel_reason": "SUPERVISOR"}),
        ("marked", a, "ABANDONED", {"wrong_scan_at": NOW, "wrong_scan_code": "WRONG_SCAN"}),
        ("confirmed", None, "CANCELLED", {"cancel_reason": "WRONG_SCAN", "review_confirmed_at": NOW}),
    ]
    for i, (name, shop, status, kw) in enumerate(specs):
        order, packages = await make_order(db, 60 + i)
        order.shop_id = shop.id if shop else None
        case = await buyer_return_case(db, order, 60 + i)
        rows[name] = packages[0]
        sessions[name] = await _return_session(db, station, packages[0], case, status, **kw)
    sup = await _login(api, db, "SUPERVISOR")

    async def codes(**params: Any) -> set[str]:
        res = await api.get("/api/v1/packages", headers=sup, params=params)
        assert res.status_code == 200, res.text
        return {i["tracking_number"] for i in res.json()["items"]}

    def tn(*names: str) -> set[str]:
        return {rows[n].tracking_number for n in names}

    assert await codes(return_dropped="true") == tn("abandoned", "sup_other", "review", "confirmed")
    assert await codes(session_status="CANCELLED,ABANDONED", session_type="RETURN") == tn(*rows)
    assert await codes(session_status="ABANDONED") == tn("abandoned", "marked")
    assert await codes(platform="TIKTOK") == tn("not_return", "sup_wrong", "sup_other")
    assert await codes(shop_id=str(a.id), return_dropped="true") == tn("abandoned", "review")
    item = (
        await api.get("/api/v1/packages", headers=sup, params={"q": rows["not_return"].tracking_number})
    ).json()
    assert (item["items"][0]["platform"], item["items"][0]["shop"]) == (
        "TIKTOK",
        {"id": str(b.id), "name": b.name},
    )
    nulls = (
        await api.get("/api/v1/packages", headers=sup, params={"q": rows["confirmed"].tracking_number})
    ).json()
    assert (nulls["items"][0]["platform"], nulls["items"][0]["shop"]) == (None, None)
    for bad in ("CANCELLED,NOPE", "OPEN,MISMATCH,CANCELLED,ABANDONED,COMPLETED"):
        res = await api.get("/api/v1/packages", headers=sup, params={"session_status": bad})
        assert res.status_code == 422
        assert "session_status" in res.json()["error"]["details"]["fields"]

    # API-32: cùng luật (BR-39 v0.4) — không trừ khi kiện có phiên sau hoàn tất (L11).
    report = (await api.get("/api/v1/reports/daily", headers=sup)).json()
    assert report["counts"]["returns_dropped_7d"] == 4
    assert {"kind": "RETURN_SESSION_DROPPED", "count": 4} in report["attention"]
    assert not any(a["kind"] == "RETURN_SESSION_ABANDONED" for a in report["attention"])


# ---------------------------------------------------------------- API-120 / API-130 lọc


async def test_recon_and_claims_filter_by_platform_shop(api: AsyncClient, db: AsyncSession) -> None:
    a = await _shop(db, "SHOPEE", "Áo Đẹp")
    b = await _shop(db, "TIKTOK", "Áo Đẹp Official")
    created: dict[str, tuple[Package, Claim]] = {}
    for n, shop in ((71, a), (72, b), (73, None)):
        order, packages = await make_order(db, n, warehouse_status="PACKED")
        order.shop_id = shop.id if shop else None
        db.add(ReconAlert(package_id=packages[0].id, rule="PACKED_NOT_HANDED_OVER", severity="MEDIUM",
                          status="OPEN", context={}, context_key=f"t{n}", detected_at=NOW))  # fmt: skip
        claim = _claim(packages[0], order if n != 73 else None)
        db.add(claim)
        created[str(n)] = (packages[0], claim)
    await db.flush()
    sup = await _login(api, db, "SUPERVISOR")

    recon = (await api.get("/api/v1/recon-alerts", headers=sup, params={"platform": "TIKTOK"})).json()
    assert [i["package"]["tracking_number"] for i in recon["items"]] == [created["72"][0].tracking_number]
    assert (recon["items"][0]["platform"], recon["items"][0]["shop"]["name"]) == ("TIKTOK", b.name)
    recon_a = (await api.get("/api/v1/recon-alerts", headers=sup, params={"shop_id": str(a.id)})).json()
    assert [i["package"]["tracking_number"] for i in recon_a["items"]] == [created["71"][0].tracking_number]

    claims = (await api.get("/api/v1/claims", headers=sup, params={"shop_id": str(b.id)})).json()
    assert [i["code"] for i in claims["items"]] == [created["72"][1].code]
    assert claims["items"][0]["shop"] == {"id": str(b.id), "name": b.name}
    shopee = (await api.get("/api/v1/claims", headers=sup, params={"platform": "SHOPEE"})).json()
    assert [i["code"] for i in shopee["items"]] == [created["71"][1].code]
    everything = (await api.get("/api/v1/claims", headers=sup)).json()
    no_shop = next(i for i in everything["items"] if i["code"] == created["73"][1].code)
    # KN không có `order_id`: sàn / shop theo đơn của kiện (đơn 73 chưa gắn shop → null)
    assert (no_shop["platform"], no_shop["shop"]) == (None, None)


# ---------------------------------------------------------------- API-32 (FR-09.01)


async def test_daily_counts_attention_and_role_filter(api: AsyncClient, db: AsyncSession) -> None:
    shop = await _shop(db, "TIKTOK", "Áo Đẹp Official")
    shop.last_error = {"code": "AUTH_EXPIRED", "message": "x", "at": clock.iso_z(NOW)}
    await _refund_case(db, 81, shop, due=NOW + timedelta(hours=10))
    await _refund_case(db, 82, None, due=NOW + timedelta(hours=3))
    order, packages = await make_order(db, 83)
    db.add(_claim(packages[0], order, deadline_at=NOW - timedelta(minutes=1)))  # NEW quá hạn
    order2, packages2 = await make_order(db, 84)
    db.add(_claim(packages2[0], order2, status="SUBMITTED", deadline_at=NOW - timedelta(days=1)))  # đã gửi
    await db.flush()

    sup = (await api.get("/api/v1/reports/daily", headers=await _login(api, db, "SUPERVISOR"))).json()
    cskh = (await api.get("/api/v1/reports/daily", headers=await _login(api, db, "CSKH"))).json()
    admin = (await api.get("/api/v1/reports/daily", headers=await _login(api, db, "ADMIN"))).json()

    assert sup["counts"]["refund_only_pending"] == 2
    assert sup["counts"]["claims_overdue_unsent"] == 1
    assert {"kind": "REFUND_ONLY_PENDING", "count": 2,
            "nearest_due_at": clock.iso_z(NOW + timedelta(hours=3))} in sup["attention"]  # fmt: skip
    assert {"kind": "CLAIM_OVERDUE", "count": 1} in sup["attention"]
    for out in (sup, cskh):
        assert not any(a["kind"] == "SYNC_ERROR" for a in out["attention"])
    sync = [a for a in admin["attention"] if a["kind"] == "SYNC_ERROR"]
    assert sync == [
        {"kind": "SYNC_ERROR", "shop_id": str(shop.id), "at": clock.iso_z(NOW),
         "shop_name": "Áo Đẹp Official", "platform": "TIKTOK", "code": "AUTH_EXPIRED"}
    ]  # fmt: skip
