"""Dữ liệu hàng hoàn mẫu cho `aicam seed-demo` (T-116; 04-test-cases §1, DEC-333).

Dải mã riêng `SPXTST00000(47..53)`, mã chiều về `SPXRTTST0000xx` — không đụng kiện mà adapter mock / QA live
dùng (`…41`, `…43`, `…44`, `…45` do J-13 / J-06 mock tạo hồ sơ), để luồng J-13 thật vẫn chạy như trước:

- `SPXTST0000047-1`, `-2` (đơn `2410TST00047`): khách trả **trọn đơn**, hồ sơ "Đang về", `SPXRTTST000047`.
- `SPXTST0000048-1`, `-2` (đơn `2410TST00048`): khách trả **một phần** (1 áo), `SPXRTTST000048`.
- `SPXTST0000049`: đang về từ 8 ngày trước → J-14 "Quá hạn chưa về" + cảnh báo Cao + hồ sơ khiếu nại ĐVVC.
- `SPXTST0000052`: `PACKED` 25 giờ, sàn chưa lấy → J-14 cảnh báo "Đã đóng gói chưa giao ĐVVC".
- `SPXTST0000053` (đơn `2410TST00053`): **chỉ hoàn tiền** (không có kiện về) — hồ sơ `NO_PARCEL`.
- `TAM-…`: hồ sơ **chưa xác định** (kiện tạm, chờ gắn đơn).

Idempotent: chạy lại không nhân đôi (đơn upsert theo mã, yêu cầu trả theo `platform_return_sn`, kiện chỉ
chuyển trạng thái khi còn `NEW`, hồ sơ chưa xác định / khiếu nại mẫu kiểm trước khi tạo, J-14 không tạo trùng
cảnh báo).
"""

import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from aicam.core import clock
from aicam.core.deps import Principal
from aicam.core.settings import Settings
from aicam.modules.claims import service as claims
from aicam.modules.claims.schemas import ClaimCreateIn
from aicam.modules.orders import service as orders
from aicam.modules.orders.models import Order, Package
from aicam.modules.platforms.base import PlatformItem, PlatformOrder
from aicam.modules.platforms.shopee import mapping as shopee_mapping
from aicam.modules.platforms.shopee import returns_mapping
from aicam.modules.reconciliation import service as recon
from aicam.modules.returns import service as returns
from aicam.modules.returns.models import ReturnCase
from aicam.modules.sessions.models import PackSession
from aicam.modules.stations.models import Station
from aicam.modules.users.models import User

FIXTURES = Path(__file__).resolve().parents[1] / "modules" / "platforms" / "mock" / "fixtures" / "returns"
UNIDENTIFIED_NOTE = "Dữ liệu mẫu seed-demo: kiện về không có mã đọc được"
_SHIRT = PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L")
_SHORTS = PlatformItem("Quần short kaki", 1, "QS-KAKI-M", "Kaki / M")
_SHIRT_ITEM = {"item_id": 9001, "model_id": 90011, "name": "Áo thun basic", "model_name": "Đen / L",
               "item_sku": "AT-DEN-L"}  # fmt: skip
_SHORTS_ITEM = {"item_id": 9002, "model_id": 90021, "name": "Quần short kaki", "model_name": "Kaki / M",
                "item_sku": "QS-KAKI-M"}  # fmt: skip


@dataclass(frozen=True)
class _Demo:
    n: int
    packages: int
    items: tuple[PlatformItem, ...]
    final: str  # trạng thái kho trước khi có tín hiệu hoàn


DEMO = (
    _Demo(47, 2, (_SHIRT,), "DELIVERED"),
    _Demo(48, 2, (_SHIRT, _SHORTS), "DELIVERED"),
    _Demo(49, 1, (_SHIRT,), "DELIVERED"),
    _Demo(52, 1, (_SHIRT,), "PACKED"),
    _Demo(53, 1, (_SHIRT,), "DELIVERED"),
)


def _codes(demo: _Demo) -> tuple[str, ...]:
    base = f"SPXTST{demo.n:07d}"
    return (base,) if demo.packages == 1 else tuple(f"{base}-{k}" for k in range(1, demo.packages + 1))


def _return(fixture: str, n: int, *, items: list[dict[str, Any]], tracking: bool = True) -> Any:
    """Payload Shopee v2 từ fixture mock (đổi mã) → `returns_mapping` như J-13 (adapter mock / thật)."""
    data = {
        k: v
        for k, v in json.loads((FIXTURES / fixture).read_text(encoding="utf-8")).items()
        if not k.startswith("_")
    }
    offset = data.pop("return_seller_due_date_offset_hours", None)
    now = clock.now()
    data.update(return_sn=f"2410RTTST{n:03d}", order_sn=f"2410TST{n:05d}", item=items,
                create_time=int(now.timestamp()), update_time=int(now.timestamp()))  # fmt: skip
    if tracking:
        data["tracking_number"] = f"SPXRTTST{n:06d}"
    if offset is not None:
        data["return_seller_due_date"] = int((now + timedelta(hours=offset)).timestamp())
    return returns_mapping.to_platform_return(data)


async def _pack_and_ship(session: AsyncSession, package: Package, station: Station, final: str) -> None:
    """Kiện mới → phiên PACK hoàn tất (không video) → `final` (như seed Phase 1 cho `…010`, `…011`)."""
    now = clock.now()
    session.add(
        PackSession(package_id=package.id, station_id=station.id, status="COMPLETED", started_at=now,
                    ended_at=now, open_code=package.tracking_number, close_code=package.tracking_number,
                    package_status_before="NEW", flags=["CAM2_UNVERIFIED"])
    )  # fmt: skip
    label = station.name
    await orders.transition(session, package, "PACKING", source="WAREHOUSE", actor_label=label)
    await orders.transition(session, package, "PACKED", source="WAREHOUSE", actor_label=label)
    if final in ("HANDED_OVER", "DELIVERED"):
        await orders.transition(session, package, "HANDED_OVER", source="PLATFORM", actor_label="Sàn")
    if final == "DELIVERED":
        await orders.transition(session, package, "DELIVERED", source="PLATFORM", actor_label="Sàn")


async def seed_returns(
    session: AsyncSession, settings: Settings, station: Station, supervisor: User
) -> list[str]:
    lines: list[str] = []
    base = clock.now() - timedelta(days=10)
    for demo in DEMO:
        order_sn = f"2410TST{demo.n:05d}"
        status = "COMPLETED" if demo.final == "DELIVERED" else "READY_TO_SHIP"  # chữ Shopee (dữ liệu mock)
        data = PlatformOrder(
            platform_order_sn=order_sn, status=status, tracking_numbers=_codes(demo), items=demo.items,
            created_at=base, updated_at=base, raw={"seed": "demo-returns", "n": demo.n},
            status_group=shopee_mapping.order_group(status),
        )  # fmt: skip
        await orders.upsert_platform_order(session, data)
        for code in _codes(demo):
            package = await orders.find_package(session, code, for_update=True)
            if package is not None and package.warehouse_status == "NEW":
                await _pack_and_ship(session, package, station, demo.final)
    await session.flush()
    # Kiện 52 đóng gói 25 giờ trước, sàn chưa lấy (BR-14, 04 §1).
    await session.execute(
        update(Package)
        .where(Package.tracking_number == "SPXTST0000052", Package.warehouse_status == "PACKED")
        .values(status_changed_at=clock.now() - timedelta(hours=25))
    )
    lines.append("= hàng hoàn mẫu: đơn 2410TST00047..49, 52, 53")

    shirt2, shirt1 = {**_SHIRT_ITEM, "amount": 2}, {**_SHIRT_ITEM, "amount": 1}
    requests = (
        (47, _return("buyer_return.json", 47, items=[shirt2])),  # trọn đơn (2 kiện)
        (48, _return("buyer_return.json", 48, items=[shirt1])),  # một phần: 1 áo, không có quần
        (49, _return("buyer_return.json", 49, items=[shirt2])),
        (53, _return("refund_only.json", 53, items=[shirt1], tracking=False)),  # chỉ hoàn tiền
    )
    for n, ret in requests:
        order = await session.scalar(select(Order).where(Order.platform_order_sn == ret.order_sn))
        if order is None:
            continue
        result = await returns.upsert_from_platform(session, order, ret)
        if result.created and result.case is not None and n == 49:
            # Đang về từ 8 ngày trước (> `return_missing_days` 7) → J-14 dưới đây chuyển "Quá hạn" (BR-12).
            past = clock.now() - timedelta(days=8)
            result.case.expected_since = past
            result.case.reported_at = past
            await session.execute(
                update(Package)
                .where(Package.tracking_number == "SPXTST0000049")
                .values(status_changed_at=past)
            )
        if result.created and result.case is not None:
            lines.append(
                f"+ hồ sơ {result.case.code} ({result.case.kind} {result.case.status}) đơn {ret.order_sn}"
            )

    unidentified = await session.scalar(
        select(ReturnCase.id).where(
            ReturnCase.kind == "UNIDENTIFIED", ReturnCase.force_note == UNIDENTIFIED_NOTE
        )
    )
    if unidentified is None:
        case, package = await returns.create_unidentified(session, force_note=UNIDENTIFIED_NOTE)
        lines.append(f"+ hồ sơ {case.code} chưa xác định, kiện tạm {package.tracking_number}")
    await session.commit()

    # J-14 thật trên dữ liệu trên: 49 → "Quá hạn" + cảnh báo Cao; 52 → cảnh báo chưa giao ĐVVC.
    run = await recon.run_rules(session, settings)
    lines.append(f"= đối soát J-14: {run}")

    missing = await orders.find_package(session, "SPXTST0000049")
    if missing is not None and await claims.find_open(session, missing.id, "LOST_IN_TRANSIT") is None:
        principal = Principal(user_id=supervisor.id, role=supervisor.role, station_id=None, ip=None)
        claim = await claims.create_manual(
            session,
            ClaimCreateIn(
                package_id=missing.id,
                type="LOST_IN_TRANSIT",
                counterparty="CARRIER",
                note="Hồ sơ mẫu: kiện hoàn quá hạn chưa về — đòi ĐVVC",
            ),
            principal,
        )
        await session.commit()
        lines.append(f"+ hồ sơ khiếu nại {claim.code} (ĐVVC, kiện SPXTST0000049)")
    return lines
