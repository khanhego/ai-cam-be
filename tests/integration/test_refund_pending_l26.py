"""L26 (06-business-qa E2) — BR-40 "Chỉ hoàn tiền chưa xử lý" xét theo **yêu cầu** (hồ sơ hàng
hoàn), không theo đơn: hồ sơ khiếu nại `LEGACY_HOLD`, hồ sơ của yêu cầu trả khác, hồ sơ tạo trước
khi sàn báo yêu cầu này **không** làm yêu cầu mới rời D2 (API-32) / N04 (J-26) / chip "Chỉ chưa
xử lý" (API-110 `pending_only`). Hồ sơ gắn đúng yêu cầu, hoặc hồ sơ chưa gắn yêu cầu nào tạo sau
khi sàn báo → đã xử lý (AC-57 "tạo KN → rời D2"). DEC-1001.
"""

import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.modules.claims.models import Claim
from aicam.modules.notify import conditions
from aicam.modules.orders.models import Order, Package
from aicam.modules.returns.models import ReturnCase

from .factories import PASSWORD, make_user
from .notify_fixtures import make_notify_settings
from .returns_helpers import buyer_return_case, make_order

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 6, 2, 0, tzinfo=UTC)  # 09:00 VN
REPORTED = NOW - timedelta(hours=2)


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


async def _refund_case(db: AsyncSession, n: int) -> tuple[ReturnCase, Order, Package]:
    order, packages = await make_order(db, n)
    case = await buyer_return_case(db, order, n, needs_parcel=False, status="REQUESTED")
    case.seller_due_at = NOW + timedelta(hours=n % 50)
    case.reported_at = REPORTED
    await db.flush()
    assert case.kind == "REFUND_ONLY"
    return case, order, packages[0]


def _claim(package: Package, order: Order, **kw: Any) -> Claim:
    base: dict[str, Any] = {
        "package_id": package.id, "order_id": order.id, "type": "DAMAGED", "counterparty": "PLATFORM",
        "status": "NEW", "source": "MANUAL",
    }  # fmt: skip
    return Claim(**{**base, **kw})


async def test_refund_pending_by_request_not_by_order(
    api: AsyncClient, db: AsyncSession, tmp_path: Path
) -> None:
    # Vẫn chưa xử lý (L26): đơn chỉ có hồ sơ LEGACY_HOLD (cờ giữ Phase 1) đang mở.
    legacy, o1, p1 = await _refund_case(db, 61)
    db.add(_claim(p1, o1, type="OTHER", source="LEGACY_HOLD", created_at=NOW - timedelta(days=20)))
    # Vẫn chưa xử lý: hồ sơ "Hư hỏng" của lần trả trước (gắn hồ sơ hàng hoàn khác), đang chờ sàn.
    other, o2, p2 = await _refund_case(db, 62)
    prev = ReturnCase(order_id=o2.id, kind="BUYER_RETURN", status="RECEIVED_ISSUE", source="PLATFORM")
    db.add(prev)
    await db.flush()
    db.add(_claim(p2, o2, status="WAITING", source="AUTO_RETURN", return_case_id=prev.id,
                  created_at=NOW - timedelta(days=10)))  # fmt: skip
    # Vẫn chưa xử lý: hồ sơ khác loại tạo trước khi sàn báo yêu cầu này, chưa gắn yêu cầu nào.
    older, o3, p3 = await _refund_case(db, 63)
    db.add(_claim(p3, o3, type="EMPTY_BOX", created_at=REPORTED - timedelta(days=3)))
    # Đã xử lý: hồ sơ gắn đúng yêu cầu (tạo từ D14).
    linked, o4, p4 = await _refund_case(db, 64)
    db.add(_claim(p4, o4, type="OTHER", return_case_id=linked.id))
    # Đã xử lý: hồ sơ chưa gắn yêu cầu, tạo sau khi sàn báo (tạo từ D4 / D16).
    after, o5, p5 = await _refund_case(db, 65)
    db.add(_claim(p5, o5, type="OTHER", created_at=REPORTED + timedelta(minutes=30)))
    await db.flush()
    pending = {legacy.code, other.code, older.code}
    sup = await _login(api, db, "SUPERVISOR")

    # D14 chip "Chỉ chưa xử lý" (API-110 pending_only)
    res = await api.get("/api/v1/returns", headers=sup, params={"tab": "NO_PARCEL", "pending_only": "true"})
    assert res.status_code == 200, res.text
    assert {i["code"] for i in res.json()["items"]} == pending
    # Cột "Hồ sơ khiếu nại" D14 (`claim`) cùng luật → hiện nút "Tạo hồ sơ khiếu nại",
    # không hiện KN không liên quan.
    assert [i["claim"] for i in res.json()["items"]] == [None, None, None]
    everything = (await api.get("/api/v1/returns", headers=sup, params={"tab": "NO_PARCEL"})).json()["items"]
    handled = {i["code"]: i["claim"] for i in everything if i["code"] in (linked.code, after.code)}
    assert all(c is not None and c["code"].startswith("KN-") for c in handled.values()), handled

    # D2 "Cần xử lý" (API-32)
    daily = (await api.get("/api/v1/reports/daily", headers=sup)).json()
    assert daily["counts"]["refund_only_pending"] == 3
    attention = next(a for a in daily["attention"] if a["kind"] == "REFUND_ONLY_PENDING")
    assert attention["count"] == 3

    # N04 (J-26)
    (tmp_path / "video").mkdir(exist_ok=True)
    drafts = await conditions.n04_refund_only(db, make_notify_settings(tmp_path), NOW)
    new_keys = {d.dedupe_key for d in drafts if d.dedupe_key.endswith(":new")}
    assert new_keys == {f"refund:{c.id}:new" for c in (legacy, other, older)}
