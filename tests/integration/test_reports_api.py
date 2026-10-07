"""T-216 — API-150..152 báo cáo M09 (FR-09.02..05, BR-41, EX-B1/B2; AC-45, 46, 47).

Bộ dữ liệu cố định = ví dụ số của 01 BR-41: 1.000 kiện bàn giao, 25 Khách trả + 15 Giao thất bại → 4,0 %;
6 Chỉ hoàn tiền → 0,6 %; 30 đã nhận (6 có vấn đề) → 20,0 %; 12 Thắng (2.350.000 đ) / 4 Thua → 75 %;
3 phiên 60, 90, 150 giây (30 giây chờ duyệt) → TB 90.
"""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import audit, clock
from aicam.modules.approvals.models import ApprovalRequest
from aicam.modules.claims.models import Claim
from aicam.modules.orders.models import Order, OrderItem, Package, Shop, StatusHistory
from aicam.modules.reports import analytics
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station

from .factories import PASSWORD, make_user

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 6, 2, 0, tzinfo=UTC)  # 09:00 VN 06/10
PERIOD = {"from": "2026-09-06", "to": "2026-10-05"}  # 30 ngày, giờ VN
IN = datetime(2026, 9, 20, 3, 0, tzinfo=UTC)  # giữa kỳ
START = datetime(2026, 9, 5, 17, 0, tzinfo=UTC)  # 00:00 VN 06/09 — đầu kỳ (tính)
END = datetime(2026, 10, 5, 17, 0, tzinfo=UTC)  # 00:00 VN 06/10 — sau kỳ (không tính)


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


async def _bulk(db: AsyncSession, model: Any, rows: Sequence[dict[str, Any]]) -> None:
    if rows:
        await db.execute(insert(model), list(rows))


async def _orders(
    db: AsyncSession,
    shop: Shop | None,
    n: int,
    *,
    prefix: str,
    handed_at: datetime | None = IN,
    items: Sequence[tuple[str | None, str, str | None]] = (("AT-DEN-L", "Áo thun basic", "Đen / L"),),
) -> list[tuple[uuid.UUID, uuid.UUID]]:
    """`n` đơn một kiện (+ dòng hàng); `handed_at` → kiện chuyển `HANDED_OVER` lúc đó.

    Trả [(order_id, package_id)]."""
    out = [(uuid.uuid4(), uuid.uuid4()) for _ in range(n)]
    await _bulk(
        db,
        Order,
        [
            {"id": oid, "shop_id": shop.id if shop else None, "platform_order_sn": f"{prefix}{i:06d}"}
            for i, (oid, _) in enumerate(out)
        ],
    )
    await _bulk(
        db,
        OrderItem,
        [
            {
                "id": uuid.uuid4(),
                "order_id": oid,
                "sku": sku,
                "product_name": name,
                "variation": var,
                "quantity": 1,
            }
            for oid, _ in out
            for sku, name, var in items
        ],
    )
    await _bulk(
        db,
        Package,
        [
            {
                "id": pid,
                "order_id": oid,
                "tracking_number": f"{prefix}TN{i:06d}",
                "warehouse_status": "HANDED_OVER",
            }
            for i, (oid, pid) in enumerate(out)
        ],
    )
    if handed_at is not None:
        await _bulk(
            db,
            StatusHistory,
            [
                {
                    "id": uuid.uuid4(),
                    "package_id": pid,
                    "source": "PLATFORM",
                    "from_status": "PACKED",
                    "to_status": "HANDED_OVER",
                    "at": handed_at,
                }
                for _, pid in out
            ],
        )
    return out


def _case(order_id: uuid.UUID | None, kind: str, status: str = "EXPECTED", **kw: Any) -> dict[str, Any]:
    return {
        "id": uuid.uuid4(),
        "order_id": order_id,
        "kind": kind,
        "status": status,
        "source": "PLATFORM",
        "created_at": kw.pop("created_at", IN),
        **kw,
    }


# ---------------------------------------------------------------- API-150 (AC-46)


async def _returns_dataset(db: AsyncSession) -> tuple[Shop, Shop]:
    a = await _shop(db, "SHOPEE", "Áo Đẹp")
    b = await _shop(db, "TIKTOK", "Áo Đẹp Official")
    a_orders = await _orders(db, a, 699, prefix="RPA")
    a_orders += await _orders(db, a, 1, prefix="RPS", handed_at=START)  # đúng 00:00 VN đầu kỳ → tính
    sock = (("", "Tất cổ ngắn", "Trắng"),)  # không SKU → gộp theo tên + phân loại
    b_orders = await _orders(db, b, 300, prefix="RPB", items=sock)
    await _orders(db, a, 5, prefix="RPX", handed_at=END)  # 00:00 VN 06/10 — ngoài kỳ
    await _orders(db, a, 5, prefix="RPY", handed_at=START - timedelta(seconds=1))  # ngoài kỳ

    rows: list[dict[str, Any]] = []
    # Shop A: 16 Khách trả + 10 Giao thất bại; shop B: 9 + 5 → 25 + 15 = 40 (AC-46).
    for i, (oid, _) in enumerate(a_orders[:26]):
        rows.append(_case(oid, "BUYER_RETURN" if i < 16 else "FAILED_DELIVERY", reason="ITEM_DAMAGED"))
    for i, (oid, _) in enumerate(b_orders[:14]):
        rows.append(_case(oid, "BUYER_RETURN" if i < 9 else "FAILED_DELIVERY", reason="CHANGE_MIND"))
    # 6 Chỉ hoàn tiền (riêng, không vào tỷ lệ hoàn).
    rows += [_case(oid, "REFUND_ONLY", "NO_PARCEL") for oid, _ in a_orders[100:106]]
    # Không tính: đã hủy / gộp, tạo ngoài kỳ, chưa xác định (không vào tỷ lệ hoàn).
    rows.append(_case(a_orders[200][0], "BUYER_RETURN", "CANCELLED"))
    rows.append(_case(a_orders[201][0], "BUYER_RETURN", created_at=START - timedelta(days=3)))
    rows.append(
        _case(
            None,
            "UNIDENTIFIED",
            "RECEIVED_OK",
            conclusion="OK",
            received_at=IN,
            created_at=START - timedelta(days=2),
        )
    )
    # 30 đã nhận trong kỳ (6 có vấn đề) — hồ sơ tạo trước kỳ, nhận trong kỳ.
    for i, (oid, _) in enumerate(a_orders[300:329]):
        issue = i < 6
        rows.append(
            _case(
                oid,
                "BUYER_RETURN",
                "RECEIVED_ISSUE" if issue else "RECEIVED_OK",
                reason="ITEM_DAMAGED" if i < 10 else None,
                conclusion=("DAMAGED" if i < 3 else "EMPTY_BOX") if issue else "OK",
                created_at=START - timedelta(days=20),
                received_at=IN,
            )
        )
    # Đã nhận ngoài kỳ → không tính.
    rows.append(
        _case(
            a_orders[400][0],
            "BUYER_RETURN",
            "RECEIVED_ISSUE",
            conclusion="DAMAGED",
            created_at=START - timedelta(days=20),
            received_at=START - timedelta(hours=1),
        )
    )
    # 3 "đang về" hiện tại (tạo trước kỳ).
    rows += [
        _case(oid, "BUYER_RETURN", "EXPECTED", created_at=START - timedelta(days=9))
        for oid, _ in a_orders[500:503]
    ]
    await _bulk(db, ReturnCase, rows)
    return a, b


async def test_returns_report_br41_example(api: AsyncClient, db: AsyncSession) -> None:
    """AC-46: 4,0 % / 0,6 % / 20,0 %; theo loại, lý do × kết luận, top sản phẩm, theo shop; mốc kỳ giờ VN."""
    a, b = await _returns_dataset(db)
    cskh = await _login(api, db, "CSKH")

    res = await api.get("/api/v1/reports/returns", headers=cskh, params=PERIOD)

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["period"] == PERIOD
    assert body["filters"] == {"platform": None, "shop_id": None}
    cards = body["cards"]
    assert cards["return_rate"] == {"numerator": 40, "denominator": 1000, "value": 0.04}
    assert cards["refund_only"] == {"count": 6, "rate_of_handed_over": 0.006}
    # 29 hồ sơ có đơn + 1 hồ sơ kiện tạm = 30 đã nhận, 6 có vấn đề → 20,0 %
    assert cards["issue_rate"] == {"numerator": 6, "denominator": 30, "value": 0.2}
    assert cards["expected_now"] == 40 + 3 + 1  # hiện tại: 40 tạo trong kỳ + 3 + 1 tạo trước kỳ còn "Đang về"
    assert body["by_kind"] == [
        {"kind": "BUYER_RETURN", "count": 25, "share": 0.625},
        {"kind": "FAILED_DELIVERY", "count": 15, "share": 0.375},
        {"kind": "REFUND_ONLY", "count": 6, "share": None},
    ]
    rbc = body["reason_by_conclusion"]
    assert rbc["conclusions"] == ["OK", "DAMAGED", "MISSING_ITEM", "WRONG_ITEM", "EMPTY_BOX", "OTHER"]
    by_reason = {r["reason"]: r for r in rbc["rows"]}
    assert by_reason["ITEM_DAMAGED"]["reason_label"] == "Hàng bị hư"
    assert by_reason["ITEM_DAMAGED"]["counts"] == {
        "OK": 4,
        "DAMAGED": 3,
        "MISSING_ITEM": 0,
        "WRONG_ITEM": 0,
        "EMPTY_BOX": 3,
        "OTHER": 0,
    }
    assert by_reason[None]["reason_label"] == "Không có lý do"
    assert by_reason[None]["total"] == 19 + 1  # 19 hồ sơ không lý do + kiện tạm
    assert rbc["rows"][-1]["reason"] is None  # dòng "không có lý do" cuối

    top = body["top_products"]
    assert top[0] == {
        "sku": "AT-DEN-L",
        "product_name": "Áo thun basic",
        "variation": "Đen / L",
        "shipped": 700,
        "return_requests": 32,
        "rate": 0.0457,
        "issue": 0,
    }
    assert top[1] == {
        "sku": None,
        "product_name": "Tất cổ ngắn",
        "variation": "Trắng",
        "shipped": 300,
        "return_requests": 14,
        "rate": 0.0467,
        "issue": 0,
    }

    assert body["by_shop"] == [
        {
            "platform": "SHOPEE",
            "shop_id": str(a.id),
            "shop_name": "Áo Đẹp",
            "handed_over": 700,
            "return_cases": 26,
            "rate": 0.0371,
        },
        {
            "platform": "TIKTOK",
            "shop_id": str(b.id),
            "shop_name": "Áo Đẹp Official",
            "handed_over": 300,
            "return_cases": 14,
            "rate": 0.0467,
        },
    ]


async def test_returns_report_filters_and_empty_period(api: AsyncClient, db: AsyncSession) -> None:
    """FR-09.05 lọc sàn / shop; shop không thuộc sàn → rỗng; EX-B2 kỳ không có kiện → `value = null`."""
    a, b = await _returns_dataset(db)
    sup = await _login(api, db, "SUPERVISOR")

    tiktok = (
        await api.get("/api/v1/reports/returns", headers=sup, params={**PERIOD, "platform": "TIKTOK"})
    ).json()
    assert tiktok["cards"]["return_rate"] == {"numerator": 14, "denominator": 300, "value": 0.0467}
    assert tiktok["cards"]["issue_rate"]["value"] is None  # kiện tạm không có shop → không vào khi lọc
    assert [r["shop_id"] for r in tiktok["by_shop"]] == [str(b.id)]
    by_shop = (
        await api.get("/api/v1/reports/returns", headers=sup, params={**PERIOD, "shop_id": str(a.id)})
    ).json()
    assert by_shop["cards"]["return_rate"]["numerator"] == 26
    assert by_shop["cards"]["issue_rate"] == {"numerator": 6, "denominator": 29, "value": 0.2069}
    mismatch = (
        await api.get(
            "/api/v1/reports/returns",
            headers=sup,
            params={**PERIOD, "platform": "TIKTOK", "shop_id": str(a.id)},
        )
    ).json()
    assert mismatch["cards"]["return_rate"] == {"numerator": 0, "denominator": 0, "value": None}
    assert mismatch["by_kind"] == mismatch["top_products"] == mismatch["by_shop"] == []

    empty = (
        await api.get(
            "/api/v1/reports/returns", headers=sup, params={"from": "2026-01-01", "to": "2026-01-31"}
        )
    ).json()
    assert empty["cards"]["return_rate"]["value"] is None
    assert empty["cards"]["refund_only"] == {"count": 0, "rate_of_handed_over": None}
    assert empty["reason_by_conclusion"]["rows"] == []


# ---------------------------------------------------------------- API-151 (AC-47)


async def test_claims_report_br41_example(api: AsyncClient, db: AsyncSession) -> None:
    """AC-47: 12 Thắng (2.350.000 đ), 4 Thua, 5 đang chờ → 75 %; gửi trước hạn; quá hạn chưa gửi; LEGACY_HOLD
    không tính; hồ sơ Thắng rồi đóng vẫn là Thắng (DEC-572)."""
    a = await _shop(db, "SHOPEE", "Áo Đẹp")
    b = await _shop(db, "TIKTOK", "Áo Đẹp Official")
    pa = await _orders(db, a, 30, prefix="CLA", handed_at=None)
    pb = await _orders(db, b, 10, prefix="CLB", handed_at=None)
    amounts = [200_000] * 11 + [150_000]  # 12 Thắng = 2.350.000 đ
    rows: list[dict[str, Any]] = []

    def claim(pkg: tuple[uuid.UUID, uuid.UUID], status: str, **kw: Any) -> dict[str, Any]:
        row = {
            "id": uuid.uuid4(),
            "package_id": pkg[1],
            "order_id": None,
            "type": kw.pop("type", "EMPTY_BOX"),
            "counterparty": kw.pop("counterparty", "PLATFORM"),
            "status": status,
            "source": "MANUAL",
            "created_at": kw.pop("created_at", IN),
            **kw,
        }
        rows.append(row)
        return row

    won_ids = []
    for i in range(12):
        pkg = pa[i] if i < 9 else pb[i - 9]
        status = "CLOSED" if i == 0 else "WON"  # hồ sơ đầu: Thắng rồi đóng → kết quả lấy từ audit
        won_ids.append(
            claim(
                pkg,
                status,
                recovered_amount=amounts[i],
                result_at=IN,
                submitted_at=IN,
                deadline_at=IN + timedelta(days=1 if i < 10 else -1),
                counterparty="CARRIER" if i >= 10 else "PLATFORM",
            )["id"]
        )
    for i in range(4):
        claim(
            pa[12 + i],
            "LOST",
            type="DAMAGED",
            result_at=IN,
            submitted_at=IN,
            deadline_at=IN + timedelta(days=1),
        )
    claim(pa[16], "NEW", deadline_at=NOW - timedelta(hours=1))  # quá hạn chưa gửi
    claim(pa[17], "NEW", deadline_at=NOW - timedelta(days=2))  # quá hạn chưa gửi
    claim(pa[18], "NEW", deadline_at=NOW + timedelta(days=2))
    claim(
        pa[19], "SUBMITTED", deadline_at=NOW + timedelta(days=2)
    )  # `submitted_at` trống (dữ liệu cũ) → không tính
    claim(pa[20], "WAITING", deadline_at=NOW + timedelta(days=2))
    # Không tính: LEGACY_HOLD; hồ sơ có kết quả ngoài kỳ (tạo trước kỳ).
    claim(pa[21], "WON", source="LEGACY_HOLD", recovered_amount=999_000, result_at=IN)
    claim(
        pa[22],
        "WON",
        recovered_amount=500_000,
        created_at=START - timedelta(days=40),
        result_at=START - timedelta(days=1),
    )
    await _bulk(db, Claim, rows)
    audit.record(
        db,
        "CLAIM_UPDATE",
        user_id=None,
        object_type="CLAIM",
        object_id=won_ids[0],
        data={"before": {"status": "WAITING"}, "after": {"status": "WON"}},
    )
    audit.record(
        db,
        "CLAIM_UPDATE",
        user_id=None,
        object_type="CLAIM",
        object_id=won_ids[0],
        data={"before": {"status": "WON"}, "after": {"status": "CLOSED"}},
    )
    await db.flush()
    cskh = await _login(api, db, "CSKH")

    body = (await api.get("/api/v1/reports/claims", headers=cskh, params=PERIOD)).json()

    cards = body["cards"]
    assert cards["created"] == 12 + 4 + 5
    assert cards["win_rate"] == {"numerator": 12, "denominator": 16, "value": 0.75}
    assert cards["recovered_amount"] == 2_350_000
    assert cards["submitted_before_deadline"] == {"numerator": 14, "denominator": 16, "value": 0.875}
    assert cards["overdue_unsent_now"] == 2
    assert body["by_status"] == [
        {"status": "NEW", "count": 3},
        {"status": "SUBMITTED", "count": 1},
        {"status": "WAITING", "count": 1},
        {"status": "WON", "count": 11},
        {"status": "LOST", "count": 4},
        {"status": "CLOSED", "count": 1},
    ]
    assert body["by_type_result"] == [
        {"type": "DAMAGED", "won": 0, "lost": 4, "pending": 0},
        {"type": "EMPTY_BOX", "won": 12, "lost": 0, "pending": 5},
    ]
    assert body["by_counterparty"] == [
        {"counterparty": "PLATFORM", "count": 19, "won": 10, "lost": 4, "recovered_amount": 2_000_000},
        {"counterparty": "CARRIER", "count": 2, "won": 2, "lost": 0, "recovered_amount": 350_000},
    ]
    assert body["by_shop"] == [
        {
            "platform": "SHOPEE",
            "shop_id": str(a.id),
            "shop_name": "Áo Đẹp",
            "count": 18,
            "won": 9,
            "lost": 4,
            "recovered_amount": 1_800_000,
        },
        {
            "platform": "TIKTOK",
            "shop_id": str(b.id),
            "shop_name": "Áo Đẹp Official",
            "count": 3,
            "won": 3,
            "lost": 0,
            "recovered_amount": 550_000,
        },
    ]

    tiktok = (
        await api.get("/api/v1/reports/claims", headers=cskh, params={**PERIOD, "platform": "TIKTOK"})
    ).json()
    assert tiktok["cards"]["win_rate"] == {"numerator": 3, "denominator": 3, "value": 1.0}
    assert tiktok["cards"]["overdue_unsent_now"] == 0


async def test_claim_transitions_stamp_submitted_and_result(api: AsyncClient, db: AsyncSession) -> None:
    """DEC-570: API-133 ghi `submitted_at` (lần đầu) và `result_at` (lần cuối sang Thắng / Thua) cho BR-41."""
    shop = await _shop(db, "SHOPEE", "Áo Đẹp")
    ((_, pid),) = await _orders(db, shop, 1, prefix="CLT", handed_at=None)
    c = Claim(
        package_id=pid,
        type="OTHER",
        counterparty="PLATFORM",
        status="NEW",
        source="MANUAL",
        platform_claim_ref="SP-1",
    )
    db.add(c)
    await db.flush()
    sup = await _login(api, db, "SUPERVISOR")

    res = await api.patch(f"/api/v1/claims/{c.id}", headers=sup, json={"version": 1, "status": "SUBMITTED"})
    assert res.status_code == 200, res.text
    clock.freeze(NOW + timedelta(minutes=5))
    res = await api.patch(
        f"/api/v1/claims/{c.id}",
        headers=sup,
        json={"version": 2, "status": "WON", "recovered_amount": 120000},
    )
    assert res.status_code == 200, res.text

    await db.refresh(c)
    assert (c.submitted_at, c.result_at) == (NOW, NOW + timedelta(minutes=5))


# ---------------------------------------------------------------- API-152 (AC-45)


async def test_productivity_report_br41_example(api: AsyncClient, db: AsyncSession) -> None:
    """AC-45: TB 90 giây (trừ thời gian chờ duyệt); lệch mã, bỏ dở, hủy, đóng gói lại theo station; tên gộp
    không phân biệt hoa thường; "(Không ghi tên)" cuối; bàn hoàn theo người kiểm; lọc station / sàn;
    CSKH 403."""
    shop = await _shop(db, "SHOPEE", "Áo Đẹp")
    pkgs = [pid for _, pid in await _orders(db, shop, 12, prefix="PRD", handed_at=None)]
    unverified = Package(tracking_number="PRDUNVERIFIED", verified=False, warehouse_status="PACKED")
    db.add(unverified)
    s1, s2 = Station(name="Station 01"), Station(name="Station 02")
    db.add_all([s1, s2])
    await db.flush()

    def sess(
        pkg: uuid.UUID, station: Station, seconds: int, status: str = "COMPLETED", **kw: Any
    ) -> PackSession:
        started = kw.pop("started_at", IN)
        return PackSession(
            type=kw.pop("type", "PACK"),
            package_id=pkg,
            station_id=station.id,
            status=status,
            started_at=started,
            ended_at=started + timedelta(seconds=seconds),
            open_code="X",
            flags=kw.pop("flags", []),
            **kw,
        )

    waited = sess(pkgs[2], s1, 150, operator_name="Minh", started_at=IN + timedelta(minutes=3))
    sessions = [
        sess(pkgs[0], s1, 60, operator_name="Minh"),
        sess(pkgs[1], s1, 90, operator_name="  minh ", started_at=IN + timedelta(minutes=2)),
        waited,
        sess(pkgs[3], s2, 100, flags=["HAD_MISMATCH"]),
        sess(pkgs[4], s2, 80, flags=["REPACK"]),
        sess(pkgs[5], s2, 50, "ABANDONED"),
        sess(pkgs[6], s2, 20, "CANCELLED", cancel_reason="WRONG_SCAN"),
        sess(unverified.id, s2, 30, operator_name="Hà"),  # kiện chưa xác minh — chỉ khi không lọc sàn
        sess(pkgs[7], s1, 40, started_at=START - timedelta(minutes=5)),  # kết thúc trước kỳ → không tính
        # Bàn hoàn: Lan 3 phiên (1 có vấn đề), phiên hủy quét nhầm không tính.
        sess(pkgs[8], s2, 200, type="RETURN", operator_name="Lan", inspection_conclusion="OK"),
        sess(
            pkgs[9],
            s2,
            220,
            type="RETURN",
            operator_name="LAN",
            inspection_conclusion="DAMAGED",
            started_at=IN + timedelta(minutes=10),
        ),
        sess(
            pkgs[10],
            s2,
            180,
            type="RETURN",
            operator_name="Lan",
            inspection_conclusion="OK",
            started_at=IN + timedelta(minutes=20),
        ),
        sess(pkgs[11], s2, 25, "CANCELLED", type="RETURN", operator_name="Lan", cancel_reason="WRONG_SCAN"),
    ]
    db.add_all(sessions)
    await db.flush()
    db.add_all(
        [
            ApprovalRequest(
                station_id=s1.id,
                session_id=waited.id,
                tracking_number="X",
                type="MISMATCH",
                status="RESOLVED",
                decision="CONTINUE",
                created_at=IN + timedelta(minutes=3, seconds=10),
                decided_at=IN + timedelta(minutes=3, seconds=30),
            ),
            ApprovalRequest(
                station_id=s1.id,
                session_id=waited.id,
                tracking_number="X",
                type="MISMATCH",
                status="RESOLVED",
                decision="CONTINUE",
                created_at=IN + timedelta(minutes=3, seconds=40),
                decided_at=IN + timedelta(minutes=3, seconds=50),
            ),
        ]
    )
    await db.flush()
    admin = await _login(api, db, "ADMIN")

    res = await api.get("/api/v1/reports/productivity", headers=admin, params=PERIOD)

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["filters"] == {"platform": None, "shop_id": None, "station_id": None}
    # PACK hoàn tất: 60, 90, 120 (150 − 30 chờ duyệt), 100, 80, 30 → 6 kiện, TB 80
    assert body["cards"] == {
        "packed": 6,
        "pack_avg_seconds": 80,
        "returns_inspected": 3,
        "return_avg_seconds": 200,
    }
    assert body["by_station"] == [
        {
            "station_id": str(s1.id),
            "station_name": "Station 01",
            "packed": 3,
            "avg_seconds": 90,
            "mismatch": 0,
            "abandoned": 0,
            "cancelled": 0,
            "repacked": 0,
        },
        {
            "station_id": str(s2.id),
            "station_name": "Station 02",
            "packed": 3,
            "avg_seconds": 70,
            "mismatch": 1,
            "abandoned": 1,
            "cancelled": 1,
            "repacked": 1,
        },
    ]
    assert body["by_operator"] == [
        {
            "operator_name": "Minh",
            "packed": 3,
            "avg_seconds": 90,
            "mismatch": 0,
            "abandoned": 0,
            "cancelled": 0,
            "repacked": 0,
        },
        {
            "operator_name": "Hà",
            "packed": 1,
            "avg_seconds": 30,
            "mismatch": 0,
            "abandoned": 0,
            "cancelled": 0,
            "repacked": 0,
        },
        {
            "operator_name": None,
            "packed": 2,
            "avg_seconds": 90,
            "mismatch": 1,
            "abandoned": 1,
            "cancelled": 1,
            "repacked": 1,
        },
    ]
    assert body["return_by_operator"] == [
        {
            "operator_name": "Lan",
            "inspected": 3,
            "avg_seconds": 200,
            "issue_rate": {"numerator": 1, "denominator": 3, "value": 0.3333},
        },
    ]

    s1_only = (
        await api.get(
            "/api/v1/reports/productivity", headers=admin, params={**PERIOD, "station_id": str(s1.id)}
        )
    ).json()
    assert s1_only["cards"] == {
        "packed": 3,
        "pack_avg_seconds": 90,
        "returns_inspected": 0,
        "return_avg_seconds": None,
    }
    shopee = (
        await api.get("/api/v1/reports/productivity", headers=admin, params={**PERIOD, "platform": "SHOPEE"})
    ).json()
    assert shopee["cards"]["packed"] == 5  # kiện chưa xác minh bị loại khi lọc sàn
    assert [o["operator_name"] for o in shopee["by_operator"]] == ["Minh", None]

    cskh = await _login(api, db, "CSKH")
    denied = await api.get("/api/v1/reports/productivity", headers=cskh, params=PERIOD)
    assert denied.status_code == 403
    assert denied.json()["error"]["code"] == "FORBIDDEN"


# ---------------------------------------------------------------- lỗi · cache · timeout


async def test_report_validation_errors(api: AsyncClient, db: AsyncSession) -> None:
    """EX-B1 + 02 §6 "Kỳ báo cáo": 422 `fields` tiếng Việt."""
    sup = await _login(api, db, "SUPERVISOR")
    cases = [
        ({"from": "2026-10-05", "to": "2026-10-01"}, {"to": "Ngày đến phải sau ngày từ."}),
        ({"from": "2025-10-01", "to": "2026-10-05"}, {"from": "Chọn tối đa 366 ngày."}),
        ({"from": "2026-10-01", "to": "2026-10-07"}, {"to": "Không chọn ngày trong tương lai."}),
    ]
    for params, fields in cases:
        for path in ("returns", "claims", "productivity"):
            res = await api.get(f"/api/v1/reports/{path}", headers=sup, params=params)
            assert res.status_code == 422, (path, params)
            assert res.json()["error"]["details"]["fields"] == fields
    bad = await api.get("/api/v1/reports/returns", headers=sup, params={**PERIOD, "platform": "LAZADA"})
    assert bad.status_code == 422
    assert "platform" in bad.json()["error"]["details"]["fields"]


async def test_report_cached_60s_per_filter(api: AsyncClient, db: AsyncSession) -> None:
    """02a §8: cache Redis theo bộ lọc — lần hai trả số cũ, bộ lọc khác tính lại."""
    shop = await _shop(db, "SHOPEE", "Áo Đẹp")
    await _orders(db, shop, 10, prefix="CCH")
    sup = await _login(api, db, "SUPERVISOR")
    first = (await api.get("/api/v1/reports/returns", headers=sup, params=PERIOD)).json()
    await _orders(db, shop, 5, prefix="CCI")

    again = (await api.get("/api/v1/reports/returns", headers=sup, params=PERIOD)).json()
    other = (
        await api.get("/api/v1/reports/returns", headers=sup, params={**PERIOD, "platform": "SHOPEE"})
    ).json()

    assert first["cards"]["return_rate"]["denominator"] == 10
    assert again == first
    assert other["cards"]["return_rate"]["denominator"] == 15


async def test_report_timeout_returns_503(
    api: AsyncClient, db: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """02a §4: `statement_timeout` → 503 `REPORT_TIMEOUT`; savepoint rollback, phiên DB dùng tiếp được."""

    async def slow(session: AsyncSession, f: analytics.ReportFilters, tz: str) -> Any:
        await session.execute(text("SELECT pg_sleep(1)"))
        raise AssertionError("không tới đây")

    monkeypatch.setattr(analytics, "STATEMENT_TIMEOUT", "50ms")
    monkeypatch.setitem(analytics.REPORTS, "claims", slow)
    sup = await _login(api, db, "SUPERVISOR")

    res = await api.get("/api/v1/reports/claims", headers=sup, params=PERIOD)

    assert res.status_code == 503
    assert res.json()["error"] == {
        "code": "REPORT_TIMEOUT",
        "message": "Không tải được báo cáo.",
        "details": {},
    }
    assert (await db.execute(text("SELECT 1"))).scalar() == 1
