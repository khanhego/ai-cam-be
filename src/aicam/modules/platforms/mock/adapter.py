"""Adapter sàn giả (02a §7): dev, test, và khi chưa có quyền Shopee (`PLATFORM_ADAPTER=mock`).

Dữ liệu khớp seed `--prefix TST` (04-test-cases §1): SPXTST0000001..30, `…09` hủy, `…12` có 3 sản phẩm.
Phase 2 (02a §7): 4 fixture `fixtures/returns/*.json` — yêu cầu trả 41 (khách trả), 44 (chỉ hoàn tiền),
45 (hủy) theo định dạng Shopee v2 (qua cùng `returns_mapping` như adapter thật); đơn 43 giao thất bại.
"""

import asyncio
import json
import secrets
from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from aicam.core import clock
from aicam.modules.platforms.base import (
    PlatformAuthError,
    PlatformError,
    PlatformItem,
    PlatformOrder,
    PlatformReturn,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.shopee import mapping, returns_mapping

MOCK_SHOP_ID = "990001"
MOCK_SHOP_NAME = "TST Shop (mock)"
MOCK_SHOP_B_NAME = "TST B"

_BASE_TIME = datetime(2026, 10, 1, tzinfo=UTC)
MOCK_DATA_SINCE = _BASE_TIME  # mốc `updated_at` cố định của dữ liệu mock Shopee Phase 1–2 (seed — DEC-821)
RETURN_FIXTURES = Path(__file__).parent / "fixtures" / "returns"
_ITEM = PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L")
# Hint vận chuyển giả (khóa = `self.shipping[mã]`), gồm tín hiệu hoàn (DEC-259).
_SHIPPING_HINTS = {
    "PICKED_UP": "HANDED_OVER",
    "IN_TRANSIT": "HANDED_OVER",
    "DELIVERED": "DELIVERED",
    "DELIVERY_FAILED": "RETURN_EXPECTED",
    "COD_REJECTED": "RETURN_EXPECTED",
}


def _grouped(order: PlatformOrder) -> PlatformOrder:
    """Mock Shopee trả nhóm như adapter thật (`shopee/mapping.order_group`); test đặt chữ trạng thái."""
    return replace(order, status_group=mapping.order_group(order.status))


def _load(name: str) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((RETURN_FIXTURES / name).read_text(encoding="utf-8"))
    return data


def _fixture_return(name: str, loaded_at: datetime) -> PlatformReturn:
    """Fixture → payload Shopee (hạn người bán tương đối lúc nạp) → `returns_mapping` như adapter thật."""
    data = {k: v for k, v in _load(name).items() if not k.startswith("_")}
    offset = data.pop("return_seller_due_date_offset_hours", None)
    if offset is not None:
        data["return_seller_due_date"] = int((loaded_at + timedelta(hours=offset)).timestamp())
    data.setdefault("create_time", int(loaded_at.timestamp()))
    data.setdefault("update_time", int(loaded_at.timestamp()))
    return returns_mapping.to_platform_return(data)


def _return_orders() -> list[PlatformOrder]:
    """Đơn gốc của các fixture hàng hoàn (04 §1): 41, 44, 45 một kiện đã giao; 43 hai kiện giao thất bại."""
    out = [
        PlatformOrder(
            platform_order_sn=f"2410TST{n:05d}", status="COMPLETED", tracking_numbers=(f"SPXTST{n:07d}",),
            items=(_ITEM,), created_at=_BASE_TIME, updated_at=_BASE_TIME, raw={"mock": True, "n": n},
        )
        for n in (41, 44, 45)
    ]  # fmt: skip
    failed = _load("failed_delivery_order.json")
    out.append(
        PlatformOrder(
            platform_order_sn=failed["order_sn"],
            status=failed["order_status"],
            tracking_numbers=tuple(failed["tracking_numbers"]),
            items=tuple(
                PlatformItem(i["name"], int(i["amount"]), i.get("item_sku"), i.get("model_name"))
                for i in failed["items"]
            ),
            created_at=_BASE_TIME,
            updated_at=_BASE_TIME,
            raw={"mock": True, "fixture": "failed_delivery_order"},
        )
    )
    return out


def _order(n: int) -> PlatformOrder:
    code = f"SPXTST{n:07d}"
    items: tuple[PlatformItem, ...] = (PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"),)
    if n == 12:
        items = (
            PlatformItem("Áo thun basic", 2, "AT-DEN-L", "Đen / L"),
            PlatformItem("Tất cổ ngắn", 1, "TAT-TRANG", "Trắng"),
            PlatformItem("Túi vải", 1, "TUI-01", None),
        )
    return PlatformOrder(
        platform_order_sn=f"2410TST{n:05d}",
        status="CANCELLED" if n == 9 else "READY_TO_SHIP",
        tracking_numbers=(code,),
        items=items,
        buyer_note="Gói kỹ giúp em" if n % 5 == 0 else None,
        created_at=_BASE_TIME,
        updated_at=_BASE_TIME,
        raw={"mock": True, "n": n},
    )


class MockAdapter:
    code = "SHOPEE"
    is_mock = True  # tra không cần token (lookup.targets)

    def __init__(self, delay_s: float = 0.0) -> None:
        """Một bộ dữ liệu chung cho mọi shop (Phase 1–2, test). Nhiều shop (dev — `MOCK_SHOPEE_SHOP_IDS`):
        `MockAdapter.multi_shop(ids)`. Chỉ một tham số: test dùng class làm dependency FastAPI."""
        self.delay_s = delay_s
        self.shop_ids: tuple[str, ...] = (MOCK_SHOP_ID,)
        self.orders: dict[str, PlatformOrder] = {o.platform_order_sn: o for o in map(_order, range(1, 31))}
        self.shipping: dict[str, str] = {}
        for order in _return_orders():
            self.orders[order.platform_order_sn] = order
        failed = _load("failed_delivery_order.json")
        for code in failed["tracking_numbers"]:
            self.shipping[code.upper()] = "DELIVERY_FAILED"
        loaded_at = clock.now()
        self.returns: dict[str, PlatformReturn] = {
            r.return_sn: r
            for r in (
                _fixture_return(name, loaded_at)
                for name in ("buyer_return.json", "refund_only.json", "cancelled.json")
            )
        }
        self.fail_returns_times = 0  # số lần list_returns ném PlatformError (TC-05.43)
        # Điều khiển lỗi trong test (TC-05.08..05.10).
        self.fail_list_times = 0  # số lần list_updated_orders ném PlatformError trước khi chạy được
        self.fail_list_auth = False  # list_updated_orders ném PlatformAuthError (token bị thu hồi)
        self.fail_refresh = False  # refresh ném PlatformAuthError
        self.calls: list[str] = []
        # Phase 3 (02a §7.2, NFR-39, AC-43): điều khiển theo shop (`creds.shop_id` = `platform_shop_id`).
        self.delay_s_by_shop: dict[str, float] = {}  # chờ mỗi lời gọi (tra khi quét) / mỗi đơn (J-04)
        self.fail_shop: set[str] = set()  # shop luôn lỗi `PlatformError`
        self.orders_by_shop: dict[str, dict[str, PlatformOrder]] = {}  # dữ liệu riêng shop (không có → chung)
        self.returns_by_shop: dict[str, dict[str, PlatformReturn]] = {}
        self.shop_calls: list[tuple[str, str | None]] = []  # (thao tác, shop)
        # Trạng thái đổi theo lượt J-04 của shop: {shop: {mã đơn: [lượt 1, lượt 2, …]}} (giữ trạng thái cuối).
        self.scripted: dict[str, dict[str, list[str]]] = {}
        self._script_step: dict[str, int] = {}

    @classmethod
    def multi_shop(cls, shop_ids: Sequence[str]) -> "MockAdapter":
        """02a §7.2: shop đầu giữ dữ liệu Phase 1–2, shop thứ hai có bộ đơn riêng `SPXTSTB…` (04 §1)."""
        mock = cls()
        mock.shop_ids = tuple(shop_ids) or (MOCK_SHOP_ID,)
        if len(mock.shop_ids) >= 2:
            _second_shop(mock, mock.shop_ids[1], clock.now())
        return mock

    # ----- điều khiển trong test
    def put(self, order: PlatformOrder) -> None:
        self.orders[order.platform_order_sn] = order

    def set_status(self, order_sn: str, status: str, at: datetime) -> None:
        self.orders[order_sn] = replace(self.orders[order_sn], status=status, updated_at=at)

    def put_return(self, ret: PlatformReturn) -> None:
        self.returns[ret.return_sn] = ret

    def set_return_status(self, return_sn: str, status: str, at: datetime) -> None:
        current = self.returns[return_sn]
        self.returns[return_sn] = replace(
            current, status=status, status_group=returns_mapping.status_group(status), updated_at=at
        )

    # ----- PlatformAdapter: ủy quyền (luồng giả — chuyển thẳng về callback với code giả)
    def prepare_auth(self, connected: set[str]) -> None:
        """API-71 (02a §7.2): lần lượt trả shop **chưa** kết nối (hết → shop đầu, như kết nối lại)."""
        self._next_shop = next((s for s in self.shop_ids if s not in connected), self.shop_ids[0])

    def build_auth_url(self, redirect_url: str, state: str) -> str:
        sep = "&" if "?" in redirect_url else "?"
        shop = getattr(self, "_next_shop", MOCK_SHOP_ID)
        return f"{redirect_url}{sep}{urlencode({'code': 'MOCK-CODE', 'shop_id': shop})}"

    def _creds(self, shop_id: str) -> ShopCredentials:
        return ShopCredentials(
            shop_id=shop_id,
            access_token=f"mock-access-{secrets.token_hex(4)}",
            refresh_token=f"mock-refresh-{secrets.token_hex(4)}",
            expires_at=clock.now() + timedelta(hours=4),
        )

    async def exchange_code(self, code: str, shop_id: str | None) -> list[ShopCredentials]:
        self.calls.append("exchange_code")
        if code != "MOCK-CODE" or not shop_id:
            raise PlatformAuthError("error_auth: code không hợp lệ")
        return [replace(self._creds(shop_id), grant_ref=shop_id)]

    async def refresh(self, creds: ShopCredentials) -> ShopCredentials:
        self.calls.append("refresh")
        if self.fail_refresh:
            raise PlatformAuthError("error_auth: refresh token không hợp lệ")
        return self._creds(creds.shop_id)

    async def shop_name(self, creds: ShopCredentials) -> str | None:
        if len(self.shop_ids) >= 2 and creds.shop_id == self.shop_ids[1]:
            return MOCK_SHOP_B_NAME
        return MOCK_SHOP_NAME

    # ----- PlatformAdapter: đơn
    async def _wait(self, creds: ShopCredentials | None = None) -> None:
        delay = self.delay_s
        if creds is not None:
            delay = self.delay_s_by_shop.get(creds.shop_id, delay)
        if delay:
            await asyncio.sleep(delay)

    def _check_shop(self, op: str, creds: ShopCredentials | None) -> None:
        shop = creds.shop_id if creds is not None else None
        self.shop_calls.append((op, shop))
        if shop is not None and shop in self.fail_shop:
            raise PlatformError(f"HTTP 503 (mock fail_shop {shop})")

    def _orders_of(self, creds: ShopCredentials | None) -> dict[str, PlatformOrder]:
        if creds is not None and creds.shop_id in self.orders_by_shop:
            return self.orders_by_shop[creds.shop_id]
        return self.orders

    def put_for_shop(self, shop: str, order: PlatformOrder) -> None:
        self.orders_by_shop.setdefault(shop, {})[order.platform_order_sn] = order

    def _run_script(self, creds: ShopCredentials | None) -> None:
        """Lượt J-04 thứ n của shop: đơn trong `scripted` lấy trạng thái thứ n (`updated_at` = bây giờ)."""
        shop = creds.shop_id if creds is not None else None
        if shop is None or shop not in self.scripted:
            return
        step = self._script_step.get(shop, 0)
        self._script_step[shop] = step + 1
        orders = self._orders_of(creds)
        for sn, seq in self.scripted[shop].items():
            if sn in orders:
                status = seq[min(step, len(seq) - 1)]
                orders[sn] = replace(orders[sn], status=status, updated_at=clock.now())

    def _returns_of(self, creds: ShopCredentials | None) -> dict[str, PlatformReturn]:
        if creds is not None and creds.shop_id in self.returns_by_shop:
            return self.returns_by_shop[creds.shop_id]
        return self.returns

    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None:
        self._check_shop("get_order", creds)
        await self._wait(creds)
        order = self._orders_of(creds).get(order_sn)
        return _grouped(order) if order else None

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None:
        self._check_shop("find_by_tracking", creds)
        await self._wait(creds)
        code = tracking_number.upper()
        found = next((o for o in self._orders_of(creds).values() if code in o.tracking_numbers), None)
        return _grouped(found) if found else None

    async def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]:
        self.calls.append("list_updated_orders")
        self._check_shop("list_updated_orders", creds)
        if self.fail_list_auth:
            raise PlatformAuthError("invalid_access_token")
        if self.fail_list_times > 0:
            self.fail_list_times -= 1
            raise PlatformError("HTTP 503")
        self._run_script(creds)
        slow = creds is not None and creds.shop_id in self.delay_s_by_shop
        for order in sorted(self._orders_of(creds).values(), key=lambda o: o.updated_at or _BASE_TIME):
            if (order.updated_at or _BASE_TIME) >= since:
                if slow:
                    await self._wait(creds)
                yield _grouped(order)

    async def get_shipping_statuses(
        self, creds: ShopCredentials | None, refs: Sequence[ShipmentRef]
    ) -> list[ShippingStatus]:
        self._check_shop("get_shipping_statuses", creds)
        out = []
        orders = self._orders_of(creds)
        for ref in refs:
            order = orders.get(ref.platform_order_sn)
            raw = self.shipping.get(ref.tracking_number.upper())
            if raw is None and order is None:
                continue
            hint = _SHIPPING_HINTS.get(raw or "")
            if order is not None and order.status == "TO_RETURN":
                hint = "RETURN_EXPECTED"
            out.append(
                ShippingStatus(
                    ref.tracking_number,
                    raw or "",
                    hint,
                    order.status if order else None,
                    order.updated_at if order else None,
                    mapping.order_group(order.status) if order else None,
                )
            )
        return out

    # ----- PlatformAdapter: yêu cầu trả (Phase 2)
    async def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]:
        self.calls.append("list_returns")
        self._check_shop("list_returns", creds)
        if self.fail_returns_times > 0:
            self.fail_returns_times -= 1
            raise PlatformError("HTTP 503")
        for ret in sorted(self._returns_of(creds).values(), key=lambda r: r.updated_at or _BASE_TIME):
            if (ret.updated_at or _BASE_TIME) >= since:
                yield ret

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None:
        await self._wait()
        return self._returns_of(creds).get(return_sn)


def _second_shop(mock: MockAdapter, shop: str, loaded_at: datetime) -> None:
    """Shop Shopee thứ hai "TST B" (02a §7.2; 04 §1): `SPXTSTB000000001..20` (đơn `2410TSTB0001..20`), đơn
    **trùng mã** `2410DUP00001` (kiện `SPXTSTB000000021`, cùng mã đơn với `TTMOCKA`), mã lạ có ở cả `TTMOCKB`
    `SPXTSTX0000001`, `SPXTSTB000000015` lượt 1 `IN_CANCEL` → lượt 2 `READY_TO_SHIP` (từ chối hủy — BR-21) và
    yêu cầu trả `RSDUP0000001` mã chiều về `RTTST-DUP-1` (trùng `TTMOCKA` — BR-29, EX-R20)."""
    orders: dict[str, PlatformOrder] = {}

    def add(sn: str, code: str, status: str = "READY_TO_SHIP", item: PlatformItem = _ITEM) -> None:
        orders[sn] = PlatformOrder(
            platform_order_sn=sn, status=status, tracking_numbers=(code,), items=(item,),
            created_at=loaded_at, updated_at=loaded_at, raw={"mock": True, "shop": shop},
        )  # fmt: skip

    for n in range(1, 21):
        add(f"2410TSTB{n:04d}", f"SPXTSTB{n:09d}", "COMPLETED" if n == 20 else "READY_TO_SHIP")
    add("2410DUP00001", "SPXTSTB000000021")
    add("2410TSTBX001", "SPXTSTX0000001")
    mock.orders_by_shop[shop] = orders
    mock.scripted[shop] = {"2410TSTB0015": ["IN_CANCEL", "READY_TO_SHIP"]}
    dup = _fixture_return("dup_return_shop_b.json", loaded_at)
    mock.returns_by_shop[shop] = {dup.return_sn: dup}
