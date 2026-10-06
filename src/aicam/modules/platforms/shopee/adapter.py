"""Adapter Shopee Open Platform v2 (FR-05.01..04, 05.06, 05.08; ADR-007).

Endpoint dùng (tài liệu công khai Shopee v2):

| Việc | Endpoint |
|---|---|
| URL ủy quyền shop | `GET /api/v2/shop/auth_partner` (`redirect`) → Shopee về `redirect?code=&shop_id=` |
| Đổi code → token | `POST /api/v2/auth/token/get` body `{code, shop_id, partner_id}` |
| Làm mới token | `POST /api/v2/auth/access_token/get` body `{refresh_token, shop_id, partner_id}` |
| Tên shop | `GET /api/v2/shop/get_shop_info` |
| Đơn cập nhật | `GET /api/v2/order/get_order_list` (`update_time`, ≤ 15 ngày, ≤ 100 / trang, `cursor`) |
| Chi tiết đơn | `GET /api/v2/order/get_order_detail` (`order_sn_list` ≤ 50, `response_optional_fields`) |
| Mã vận đơn | `GET /api/v2/logistics/get_tracking_number` (`order_sn`, `package_number`) |
| Yêu cầu trả | `GET /api/v2/returns/get_return_list` (`page_no`, `page_size`, `update_time_*` ≤ 15 ngày) |
| Chi tiết yêu cầu trả | `GET /api/v2/returns/get_return_detail` (`return_sn`) |

Shopee không có API công khai "tìm đơn theo mã vận đơn" → `find_by_tracking` dò các đơn cập nhật gần đây
(`SHOPEE_LOOKUP_LOOKBACK_MIN`) và nhớ cặp mã vận đơn → mã đơn đã thấy (DEC-123, cần xác nhận ở T-3).
**Chưa test với Shopee thật — thiếu tài khoản partner.**
"""

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from aicam.core import clock
from aicam.modules.platforms.base import (
    PlatformError,
    PlatformItem,
    PlatformOrder,
    PlatformReturn,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.shopee import mapping, returns_mapping
from aicam.modules.platforms.shopee.client import ShopeeClient, ShopeeRequestError

MAX_WINDOW = timedelta(days=15)  # get_order_list: time_to − time_from ≤ 15 ngày
DETAIL_BATCH = 50
DETAIL_FIELDS = "item_list,package_list,cancel_reason,pickup_done_time"
CACHE_SIZE = 5000
# Tra khi quét (G3-P2-12, cần xác nhận ở T-3): tối đa 1 trang danh sách (100 đơn) và 10 lần hỏi mã vận đơn mỗi
# lần quét — người gọi chỉ chờ 2 giây, không đốt hạn mức API của shop cho một mã lạ.
LOOKUP_MAX_PAGES = 1
LOOKUP_MAX_CANDIDATES = 10


def _ts(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=UTC) if value else None
    except (TypeError, ValueError):
        return None


def _body(data: dict[str, Any]) -> dict[str, Any]:
    inner = data.get("response")
    return inner if isinstance(inner, dict) else data


class ShopeeAdapter:
    code = "SHOPEE"

    def __init__(
        self,
        client: ShopeeClient,
        *,
        lookup_lookback: timedelta = timedelta(minutes=60),
        returns_page_size: int = 50,
        returns_window: timedelta = MAX_WINDOW,
    ) -> None:
        self.client = client
        self.lookup_lookback = lookup_lookback
        self.returns_page_size = returns_page_size
        self.returns_window = min(returns_window, MAX_WINDOW)
        # Mã vận đơn → mã đơn đã thấy (giới hạn kích thước) — tra khi quét khỏi dò lại.
        self._tracking_cache: OrderedDict[str, str] = OrderedDict()

    # ------------------------------------------------------------ ủy quyền (FR-05.01)
    def build_auth_url(self, redirect_url: str) -> str:
        return self.client.auth_partner_url(redirect_url)

    def _creds(self, data: dict[str, Any], shop_id: str) -> ShopCredentials:
        access, refresh = data.get("access_token"), data.get("refresh_token")
        if not access or not refresh:
            raise PlatformError("Shopee không trả access_token / refresh_token")
        expire_in = int(data.get("expire_in") or 14400)  # tài liệu: access token 4 giờ
        return ShopCredentials(
            str(shop_id), str(access), str(refresh), clock.now() + timedelta(seconds=expire_in)
        )

    async def exchange_code(self, code: str, shop_id: str) -> ShopCredentials:
        body = {"code": code, "shop_id": int(shop_id), "partner_id": self.client.partner_id}
        data = await self.client.call("POST", "/api/v2/auth/token/get", body=body)
        return self._creds(data, shop_id)

    async def refresh(self, creds: ShopCredentials) -> ShopCredentials:
        body = {
            "refresh_token": creds.refresh_token,
            "shop_id": int(creds.shop_id),
            "partner_id": self.client.partner_id,
        }
        data = await self.client.call("POST", "/api/v2/auth/access_token/get", body=body)
        return self._creds(data, creds.shop_id)

    async def shop_name(self, creds: ShopCredentials) -> str | None:
        data = await self._shop_call("GET", "/api/v2/shop/get_shop_info", creds)
        name = data.get("shop_name") or _body(data).get("shop_name")
        return str(name) if name else None

    async def _shop_call(
        self, method: str, path: str, creds: ShopCredentials, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self.client.call(
            method, path, params=params, access_token=creds.access_token, shop_id=creds.shop_id
        )

    # ------------------------------------------------------------ đơn (FR-05.02, 05.03)
    async def _order_sns(
        self, creds: ShopCredentials, since: datetime, until: datetime, *, max_pages: int | None = None
    ) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        start = since
        pages = 0
        while start < until:
            end = min(start + MAX_WINDOW, until)
            cursor = ""
            while True:
                params = {
                    "time_range_field": "update_time", "time_from": int(start.timestamp()),
                    "time_to": int(end.timestamp()), "page_size": 100, "cursor": cursor,
                    "response_optional_fields": "order_status",
                }  # fmt: skip
                body = _body(await self._shop_call("GET", "/api/v2/order/get_order_list", creds, params))
                for o in body.get("order_list") or []:
                    out.append((str(o["order_sn"]), str(o.get("order_status") or "")))
                pages += 1
                if max_pages is not None and pages >= max_pages:
                    return out
                cursor = str(body.get("next_cursor") or "")
                if not body.get("more") or not cursor:
                    break
            start = end
        return out

    async def _details(self, creds: ShopCredentials, sns: Sequence[str]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for i in range(0, len(sns), DETAIL_BATCH):
            params = {
                "order_sn_list": ",".join(sns[i : i + DETAIL_BATCH]),
                "response_optional_fields": DETAIL_FIELDS,
            }
            body = _body(await self._shop_call("GET", "/api/v2/order/get_order_detail", creds, params))
            out.extend(body.get("order_list") or [])
        return out

    async def _tracking_number(
        self, creds: ShopCredentials, order_sn: str, package_number: str | None
    ) -> str | None:
        params: dict[str, Any] = {"order_sn": order_sn}
        if package_number:
            params["package_number"] = package_number
        try:
            body = _body(await self._shop_call("GET", "/api/v2/logistics/get_tracking_number", creds, params))
        except ShopeeRequestError:
            return None  # đơn chưa có mã vận đơn (chưa sắp xếp vận chuyển); lỗi token / hết lượt thử báo lên
        code = str(body.get("tracking_number") or "").strip().upper()
        return code or None

    async def _to_order(self, creds: ShopCredentials, detail: dict[str, Any]) -> PlatformOrder:
        sn = str(detail["order_sn"])
        status = str(detail.get("order_status") or "")
        packages = [p for p in detail.get("package_list") or [] if isinstance(p, dict)]
        by_package: dict[str, str] = {}
        tracking: list[str] = []
        if status not in mapping.NO_TRACKING_STATUSES:
            numbers = [str(p.get("package_number") or "") or None for p in packages] or [None]
            for package_number in numbers:
                code = await self._tracking_number(creds, sn, package_number)
                if code and code not in tracking:
                    tracking.append(code)
                    if package_number:
                        by_package[package_number] = code
                    self._remember(code, sn)
        items = tuple(
            PlatformItem(
                product_name=str(i.get("item_name") or ""),
                quantity=int(i.get("model_quantity_purchased") or 0),
                sku=(i.get("model_sku") or i.get("item_sku") or None),
                variation=(i.get("model_name") or None),
                image_url=((i.get("image_info") or {}).get("image_url") or None),
            )
            for i in detail.get("item_list") or []
            if int(i.get("model_quantity_purchased") or 0) > 0
        )
        return PlatformOrder(
            platform_order_sn=sn,
            status=status,
            tracking_numbers=tuple(tracking),
            items=items,
            buyer_note=(detail.get("message_to_seller") or None),
            created_at=_ts(detail.get("create_time")),
            updated_at=_ts(detail.get("update_time")),
            raw={"detail": detail, "tracking_by_package": by_package},
        )

    def _remember(self, tracking: str, order_sn: str) -> None:
        self._tracking_cache[tracking] = order_sn
        self._tracking_cache.move_to_end(tracking)
        while len(self._tracking_cache) > CACHE_SIZE:
            self._tracking_cache.popitem(last=False)

    async def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]:
        if creds is None:
            return
        sns = [sn for sn, _ in await self._order_sns(creds, since, clock.now())]
        for i in range(0, len(sns), DETAIL_BATCH):
            for detail in await self._details(creds, sns[i : i + DETAIL_BATCH]):
                yield await self._to_order(creds, detail)

    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None:
        if creds is None:
            return None
        details = await self._details(creds, [order_sn])
        return await self._to_order(creds, details[0]) if details else None

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None:
        """FR-05.06: tra một mã vận đơn chưa có trong hệ thống (người gọi giới hạn 2 giây — BR-04)."""
        if creds is None:
            return None
        code = tracking_number.strip().upper()
        known = self._tracking_cache.get(code)
        if known is not None:
            order = await self.get_order(creds, known)
            if order is not None and code in order.tracking_numbers:
                return order
        now = clock.now()
        # Ưu tiên đơn chưa biết mã vận đơn: đơn đã có trong bộ nhớ (đã hỏi) chắc chắn không phải mã này.
        resolved = set(self._tracking_cache.values())
        candidates = [
            sn
            for sn, status in await self._order_sns(
                creds, now - self.lookup_lookback, now, max_pages=LOOKUP_MAX_PAGES
            )
            if status not in mapping.NO_TRACKING_STATUSES and sn not in resolved
        ][:LOOKUP_MAX_CANDIDATES]
        for i in range(0, len(candidates), 5):  # dò song song từng nhóm 5 đơn, dừng khi thấy
            chunk = candidates[i : i + 5]
            found = await asyncio.gather(*(self._tracking_number(creds, sn, None) for sn in chunk))
            for sn, number in zip(chunk, found, strict=True):
                if number:
                    self._remember(number, sn)
                if number == code:
                    return await self.get_order(creds, sn)
        return None

    # ------------------------------------------------------------ vận chuyển (FR-05.04)
    async def get_shipping_statuses(
        self, creds: ShopCredentials | None, refs: Sequence[ShipmentRef]
    ) -> list[ShippingStatus]:
        if creds is None or not refs:
            return []
        by_order: dict[str, list[str]] = {}
        for ref in refs:
            by_order.setdefault(ref.platform_order_sn, []).append(ref.tracking_number.upper())
        out: list[ShippingStatus] = []
        for detail in await self._details(creds, list(by_order)):
            sn = str(detail["order_sn"])
            status = str(detail.get("order_status") or "")
            packages = [p for p in detail.get("package_list") or [] if isinstance(p, dict)]
            codes = by_order.get(sn, [])
            logistics: dict[str, str] = {}
            if len(packages) == 1:
                logistics = dict.fromkeys(codes, str(packages[0].get("logistics_status") or ""))
            elif packages:  # đơn nhiều kiện: hỏi mã vận đơn từng kiện để ghép đúng
                for p in packages:
                    number = await self._tracking_number(creds, sn, str(p.get("package_number") or ""))
                    if number in codes:
                        logistics[number] = str(p.get("logistics_status") or "")
            for code in codes:
                raw = logistics.get(code, "")
                out.append(
                    ShippingStatus(
                        code,
                        raw or status,
                        mapping.warehouse_hint(status, raw),
                        status,
                        _ts(detail.get("update_time")),
                    )
                )
        return out

    # ------------------------------------------------------- yêu cầu trả (FR-05.05, 05.12 — chưa test, T-3)
    async def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]:
        """J-13: yêu cầu trả cập nhật từ `since`, chia cửa sổ ≤ 15 ngày, phân trang `page_no` (02a §7)."""
        if creds is None:
            return
        start, until = since, clock.now()
        while start < until:
            end = min(start + self.returns_window, until)
            page = 1
            while True:
                params = {
                    "page_no": page, "page_size": self.returns_page_size,
                    "update_time_from": int(start.timestamp()), "update_time_to": int(end.timestamp()),
                }  # fmt: skip
                body = _body(await self._shop_call("GET", "/api/v2/returns/get_return_list", creds, params))
                for detail in body.get("return") or []:
                    if isinstance(detail, dict) and detail.get("return_sn") and detail.get("order_sn"):
                        yield returns_mapping.to_platform_return(detail)
                if not body.get("more"):
                    break
                page += 1
            start = end

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None:
        if creds is None:
            return None
        body = _body(
            await self._shop_call("GET", "/api/v2/returns/get_return_detail", creds, {"return_sn": return_sn})
        )
        if not body.get("return_sn"):
            return None
        return returns_mapping.to_platform_return(body)
