"""Adapter TikTok Shop Partner API (02a §7.1; FR-05.07, 05.08, 05.13, 05.15..05.18, 05.22; AS-12, AS-13).

**Giả định theo tài liệu công khai — chưa test với TikTok thật, thiếu tài khoản đối tác (Q18, Q19).** Mọi
ánh xạ trạng thái đi qua `mapping.py` / `returns_mapping.py` (lõi chỉ đọc nhóm — NFR-28).

| Việc | Gọi (giả định, version `202309`) |
|---|---|
| Trang ủy quyền | `{TIKTOK_AUTHORIZE_URL}?service_id={TIKTOK_SERVICE_ID}&state=…` |
| Đổi code / làm mới | `GET {AUTH_BASE}/api/v2/token/get` / `/api/v2/token/refresh` |
| Shop được ủy quyền | `GET /authorization/202309/shops` → `[{id, name, region, cipher}]` |
| Đơn đổi | `POST /order/202309/orders/search` (`update_time_ge/lt`, `page_token`) → chi tiết `GET
…/orders?ids=` |
| Yêu cầu hủy | `POST /return_refund/202309/cancellations/search` → **luôn** đọc chi tiết đơn (DEC-502) |
| Kiện gộp | đơn `split_or_combine_tag = COMBINED` → `GET /fulfillment/202309/packages/{id}` → `orders[]` |
| Vận chuyển | chi tiết đơn theo lô → `status` + `line_items[].package_status` → gợi ý kho |
| Tra khi quét | 1 trang đơn cập nhật `TIKTOK_LOOKUP_LOOKBACK_MIN` phút + bộ nhớ mã vận đơn (AS-12, DEC-469) |
"""

from collections import OrderedDict
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

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
from aicam.modules.platforms.tiktok import mapping
from aicam.modules.platforms.tiktok.client import VERSION, TikTokClient, TikTokRequestError, Token

log = structlog.get_logger()

PAGE_SIZE = 50
DETAIL_BATCH = 50  # `orders?ids=` ≤ 50
SEARCH_MAX_PAGES = 200  # phòng `next_page_token` không dứt (lỗi phía sàn)
CACHE_SIZE = 5000  # mã vận đơn → mã đơn (tra khi quét — DEC-469)


def _ts(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=UTC) if value else None
    except (TypeError, ValueError):
        return None


def _tracking_numbers(detail: dict[str, Any]) -> tuple[str, ...]:
    """Mã vận đơn của đơn: `line_items[].tracking_number` (mỗi kiện một mã — giả định Q19), khử trùng."""
    codes: list[str] = []
    for li in detail.get("line_items") or []:
        code = str((li or {}).get("tracking_number") or "").strip().upper()
        if code and code not in codes:
            codes.append(code)
    return tuple(codes)


def _package_ids(detail: dict[str, Any]) -> list[str]:
    ids = [str(p["id"]) for p in detail.get("packages") or [] if isinstance(p, dict) and p.get("id")]
    for li in detail.get("line_items") or []:
        pid = str((li or {}).get("package_id") or "")
        if pid and pid not in ids:
            ids.append(pid)
    return ids


def _items(lines: list[dict[str, Any]]) -> tuple[PlatformItem, ...]:
    """TikTok trả mỗi đơn vị hàng một `line_item` (giả định) → gộp theo (sku, tên, phân loại) đếm số lượng."""
    grouped: dict[tuple[str, str, str], list[Any]] = {}
    for li in lines:
        if str(li.get("display_status") or "").upper() == "CANCELLED":
            continue
        key = (
            str(li.get("seller_sku") or li.get("sku_id") or ""),
            str(li.get("product_name") or ""),
            str(li.get("sku_name") or ""),
        )
        qty = int(li.get("quantity") or 1)
        if key in grouped:
            grouped[key][0] += qty
        else:
            grouped[key] = [qty, li.get("sku_image") or None]
    return tuple(
        PlatformItem(
            product_name=name, quantity=qty, sku=sku or None, variation=variation or None, image_url=image
        )
        for (sku, name, variation), (qty, image) in grouped.items()
    )


class TikTokAdapter:
    code = "TIKTOK"

    def __init__(
        self,
        client: TikTokClient,
        *,
        authorize_url: str,
        service_id: str,
        lookup_lookback: timedelta = timedelta(minutes=60),
    ) -> None:
        self.client = client
        self.authorize_url = authorize_url
        self.service_id = service_id
        self.lookup_lookback = lookup_lookback
        self._tracking_cache: OrderedDict[str, str] = OrderedDict()
        self._package_cache: dict[str, list[str]] = {}

    # ------------------------------------------------------------ ủy quyền (FR-05.13)
    def build_auth_url(self, redirect_url: str, state: str) -> str:
        """URL về đăng ký sẵn ở Partner Center (RK-26) — `redirect_url` không gửi đi."""
        return self.client.authorize_url(self.authorize_url, self.service_id, state)

    @staticmethod
    def _creds(token: Token, shop: dict[str, Any]) -> ShopCredentials:
        return ShopCredentials(
            shop_id=str(shop["id"]),
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            expires_at=token.expires_at,
            shop_cipher=str(shop["cipher"]) if shop.get("cipher") else None,
            grant_ref=token.open_id,
            shop_name=str(shop["name"]) if shop.get("name") else None,
            region=str(shop["region"]) if shop.get("region") else None,
        )

    async def exchange_code(self, code: str, shop_id: str | None) -> list[ShopCredentials]:
        """Một lần ủy quyền → mọi shop người bán chọn (token chung của grant `open_id` — DEC-433)."""
        token = await self.client.token_get(code)
        shops = await self.client.authorized_shops(token.access_token)
        if not shops:
            raise PlatformError("TikTok không trả shop nào trong lần ủy quyền")
        return [self._creds(token, s) for s in shops]

    async def refresh(self, creds: ShopCredentials) -> ShopCredentials:
        token = await self.client.token_refresh(creds.refresh_token)
        return ShopCredentials(
            shop_id=creds.shop_id,
            access_token=token.access_token,
            refresh_token=token.refresh_token,
            expires_at=token.expires_at,
            shop_cipher=creds.shop_cipher,
            grant_ref=token.open_id or creds.grant_ref,
            shop_name=creds.shop_name,
            region=creds.region,
        )

    async def shop_name(self, creds: ShopCredentials) -> str | None:
        return creds.shop_name

    # ------------------------------------------------------------ đơn (FR-05.15, 05.16, 05.17, 05.22)
    async def _shop_call(
        self,
        method: str,
        path: str,
        creds: ShopCredentials,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self.client.call(
            method, path, params=params, body=body, access_token=creds.access_token,
            shop_cipher=creds.shop_cipher,
        )  # fmt: skip

    async def _search(
        self,
        path: str,
        key: str,
        creds: ShopCredentials,
        body: dict[str, Any],
        *,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """`POST …/search?page_size&page_token` → mọi phần tử `data[key]` (dừng khi hết `next_page_token`)."""
        out: list[dict[str, Any]] = []
        token = ""
        for page in range(1, (max_pages or SEARCH_MAX_PAGES) + 1):
            params: dict[str, Any] = {"page_size": PAGE_SIZE}
            if token:
                params["page_token"] = token
            data = await self._shop_call("POST", path, creds, params=params, body=body)
            out.extend(x for x in data.get(key) or [] if isinstance(x, dict))
            token = str(data.get("next_page_token") or "")
            if not token:
                break
            if page == (max_pages or SEARCH_MAX_PAGES) and max_pages is None:
                log.error("tiktok_search_max_pages", path=path, pages=page)
        return out

    async def _details(self, creds: ShopCredentials, ids: Sequence[str]) -> list[dict[str, Any]]:
        """`GET /order/202309/orders?ids=` ≤ 50 / lần."""
        out: list[dict[str, Any]] = []
        for i in range(0, len(ids), DETAIL_BATCH):
            chunk = ",".join(ids[i : i + DETAIL_BATCH])
            data = await self._shop_call("GET", f"/order/{VERSION}/orders", creds, params={"ids": chunk})
            out.extend(o for o in data.get("orders") or [] if isinstance(o, dict) and o.get("id"))
        return out

    async def _cancellations(
        self, creds: ShopCredentials, body: dict[str, Any]
    ) -> dict[str, list[dict[str, Any]]]:
        """`POST /return_refund/202309/cancellations/search` → {order_id: [yêu cầu hủy]} (DEC-468, 502)."""
        rows = await self._search(
            f"/return_refund/{VERSION}/cancellations/search", "cancellations", creds, body
        )
        out: dict[str, list[dict[str, Any]]] = {}
        for c in rows:
            if c.get("order_id"):
                out.setdefault(str(c["order_id"]), []).append(c)
        return out

    async def _package_orders(self, creds: ShopCredentials, package_id: str) -> list[str]:
        """Kiện gộp (FR-05.22): `GET /fulfillment/202309/packages/{id}` → `orders[].id` (giả định — Q19)."""
        if package_id in self._package_cache:
            return self._package_cache[package_id]
        data = await self._shop_call("GET", f"/fulfillment/{VERSION}/packages/{package_id}", creds)
        ids = [str(o["id"]) for o in data.get("orders") or [] if isinstance(o, dict) and o.get("id")]
        self._package_cache[package_id] = ids
        while len(self._package_cache) > CACHE_SIZE:
            self._package_cache.pop(next(iter(self._package_cache)))
        return ids

    async def _to_orders(
        self,
        creds: ShopCredentials,
        details: list[dict[str, Any]],
        cancels: dict[str, list[dict[str, Any]]] | None = None,
    ) -> list[PlatformOrder]:
        """Chi tiết đơn → `PlatformOrder` (nhóm qua `mapping.order_group`); kiện gộp: mã vận đơn chung với đơn
        khác trong lô, hoặc đơn gắn cờ `COMBINED` → hỏi kiện."""
        by_tracking: dict[str, set[str]] = {}
        for d in details:
            for code in _tracking_numbers(d):
                by_tracking.setdefault(code, set()).add(str(d["id"]))
        out = []
        for d in details:
            sn = str(d["id"])
            merged: set[str] = set()
            for code in _tracking_numbers(d):
                merged |= by_tracking.get(code, set())
            if str(d.get("split_or_combine_tag") or "").upper() == "COMBINED":
                for package_id in _package_ids(d):
                    try:
                        merged |= set(await self._package_orders(creds, package_id))
                    except TikTokRequestError as exc:
                        log.warning("tiktok_package_lookup_failed", order_id=sn, error=str(exc))
            merged.discard(sn)
            out.append(self._order(d, (cancels or {}).get(sn, []), tuple(sorted(merged))))
        return out

    def _order(
        self, detail: dict[str, Any], cancels: list[dict[str, Any]], merged: tuple[str, ...]
    ) -> PlatformOrder:
        status = str(detail.get("status") or "")
        latest = mapping.latest_cancel(cancels)
        lines = [li for li in detail.get("line_items") or [] if isinstance(li, dict)]
        group = mapping.order_group(
            status, latest, buyer_request_flag=bool(detail.get("is_buyer_request_cancel")),
            package_statuses=[str(li.get("package_status") or "") for li in lines],
        )  # fmt: skip
        tracking = _tracking_numbers(detail)
        for code in tracking:
            self._remember(code, str(detail["id"]))
        return PlatformOrder(
            platform_order_sn=str(detail["id"]),
            status=status,
            tracking_numbers=tracking,
            items=_items(lines),
            buyer_note=(detail.get("buyer_message") or None),
            created_at=_ts(detail.get("create_time")),
            updated_at=_ts(detail.get("update_time")),
            raw={"detail": detail, "latest_cancel": latest},
            status_group=group,
            merged_order_sns=merged,
            fulfilled_by_platform=mapping.fulfilled_by_platform(detail),
        )

    def _remember(self, tracking: str, order_id: str) -> None:
        self._tracking_cache[tracking] = order_id
        self._tracking_cache.move_to_end(tracking)
        while len(self._tracking_cache) > CACHE_SIZE:
            self._tracking_cache.popitem(last=False)

    async def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]:
        """J-04: đơn cập nhật (`orders/search`) ∪ đơn có yêu cầu hủy cập nhật (`cancellations/search`)
        trong cửa sổ → **luôn** đọc chi tiết `orders?ids=` (DEC-502), khử trùng theo `order_id`."""
        if creds is None:
            return
        window = {"update_time_ge": int(since.timestamp()), "update_time_lt": int(clock.now().timestamp())}
        found = await self._search(f"/order/{VERSION}/orders/search", "orders", creds, window)
        cancels = await self._cancellations(creds, window)
        ids = list(dict.fromkeys([str(o["id"]) for o in found if o.get("id")] + list(cancels)))
        for i in range(0, len(ids), DETAIL_BATCH):
            details = await self._details(creds, ids[i : i + DETAIL_BATCH])
            for order in await self._to_orders(creds, details, cancels):
                yield order

    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None:
        if creds is None:
            return None
        details = await self._details(creds, [order_sn])
        if not details:
            return None
        cancels = await self._cancellations(creds, {"order_ids": [order_sn]})
        return (await self._to_orders(creds, details, cancels))[0]

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None:
        """Tra khi quét (AS-12 — không có API tra theo mã vận đơn, giả định): đơn trong bộ nhớ → đọc lại;
        không thì 1 trang đơn cập nhật `TIKTOK_LOOKUP_LOOKBACK_MIN` phút gần nhất (DEC-469). Người gọi cắt
        2 giây."""
        if creds is None:
            return None
        code = tracking_number.strip().upper()
        known = self._tracking_cache.get(code)
        if known is not None:
            order = await self.get_order(creds, known)
            if order is not None and code in order.tracking_numbers:
                return order
        now = clock.now()
        window = {
            "update_time_ge": int((now - self.lookup_lookback).timestamp()),
            "update_time_lt": int(now.timestamp()),
        }
        found = await self._search(f"/order/{VERSION}/orders/search", "orders", creds, window, max_pages=1)
        with_lines = [o for o in found if o.get("line_items")]
        ids = [str(o["id"]) for o in found if o.get("id") and not o.get("line_items")]
        details = with_lines + (await self._details(creds, ids) if ids else [])
        for d in details:
            if code in _tracking_numbers(d):
                return await self.get_order(creds, str(d["id"]))
            for other in _tracking_numbers(d):
                self._remember(other, str(d["id"]))
        return None

    # ------------------------------------------------------------ vận chuyển (FR-05.04, J-06)
    async def get_shipping_statuses(
        self, creds: ShopCredentials | None, refs: Sequence[ShipmentRef]
    ) -> list[ShippingStatus]:
        """Đọc lại chi tiết đơn theo lô (`orders?ids=`) → nhóm + `package_status` → gợi ý kho (02a §7.1)."""
        if creds is None or not refs:
            return []
        wanted: dict[str, list[str]] = {}
        for ref in refs:
            wanted.setdefault(ref.platform_order_sn, []).append(ref.tracking_number.upper())
        out: list[ShippingStatus] = []
        for d in await self._details(creds, list(wanted)):
            sn = str(d["id"])
            status = str(d.get("status") or "")
            lines = [li for li in d.get("line_items") or [] if isinstance(li, dict)]
            group = mapping.order_group(
                status, None, buyer_request_flag=bool(d.get("is_buyer_request_cancel")),
                package_statuses=[str(li.get("package_status") or "") for li in lines],
            )  # fmt: skip
            for code in wanted.get(sn, []):
                package_status = next(
                    (
                        str(li.get("package_status") or "")
                        for li in lines
                        if str(li.get("tracking_number") or "").upper() == code
                    ),
                    "",
                )
                out.append(
                    ShippingStatus(
                        code, package_status or status, mapping.warehouse_hint(status, package_status),
                        status, _ts(d.get("update_time")), group,
                    )
                )  # fmt: skip
        return out

    # ------------------------------------------------------------ yêu cầu trả (T-210)
    async def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]:
        raise PlatformError("TikTok: yêu cầu trả chưa hỗ trợ (T-210)")
        yield  # pragma: no cover

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None:
        raise PlatformError("TikTok: yêu cầu trả chưa hỗ trợ (T-210)")
