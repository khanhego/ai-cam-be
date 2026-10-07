"""Adapter TikTok Shop Partner API (02a §7.1; FR-05.07, 05.08, 05.13, 05.15..05.18, 05.22; AS-12, AS-13).

**Giả định theo tài liệu công khai — chưa test với TikTok thật, thiếu tài khoản đối tác (Q18, Q19).** Mọi ánh
xạ
trạng thái đi qua `mapping.py` / `returns_mapping.py` (lõi chỉ đọc nhóm — NFR-28).

| Việc | Gọi (giả định, version `202309`) |
|---|---|
| Trang ủy quyền | `{TIKTOK_AUTHORIZE_URL}?service_id={TIKTOK_SERVICE_ID}&state=…` |
| Đổi code / làm mới | `GET {AUTH_BASE}/api/v2/token/get` / `/api/v2/token/refresh` |
| Shop được ủy quyền | `GET /authorization/202309/shops` → `[{id, name, region, cipher}]` |
"""

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from typing import Any

import structlog

from aicam.modules.platforms.base import (
    PlatformError,
    PlatformOrder,
    PlatformReturn,
    ShipmentRef,
    ShippingStatus,
    ShopCredentials,
)
from aicam.modules.platforms.tiktok.client import TikTokClient, Token

log = structlog.get_logger()


class TikTokAdapter:
    code = "TIKTOK"

    def __init__(self, client: TikTokClient, *, authorize_url: str, service_id: str) -> None:
        self.client = client
        self.authorize_url = authorize_url
        self.service_id = service_id

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

    # ------------------------------------------------------------ đơn / kiện / trả (T-209, T-210)
    async def get_order(self, creds: ShopCredentials | None, order_sn: str) -> PlatformOrder | None:
        raise PlatformError("TikTok: đơn chưa hỗ trợ (T-209)")

    async def find_by_tracking(
        self, creds: ShopCredentials | None, tracking_number: str
    ) -> PlatformOrder | None:
        raise PlatformError("TikTok: tra khi quét chưa hỗ trợ (T-209)")

    async def list_updated_orders(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformOrder]:
        raise PlatformError("TikTok: đồng bộ đơn chưa hỗ trợ (T-209)")
        yield  # pragma: no cover

    async def get_shipping_statuses(
        self, creds: ShopCredentials | None, refs: Sequence[ShipmentRef]
    ) -> list[ShippingStatus]:
        raise PlatformError("TikTok: vận chuyển chưa hỗ trợ (T-209)")

    async def list_returns(
        self, creds: ShopCredentials | None, since: datetime
    ) -> AsyncIterator[PlatformReturn]:
        raise PlatformError("TikTok: yêu cầu trả chưa hỗ trợ (T-210)")
        yield  # pragma: no cover

    async def get_return(self, creds: ShopCredentials | None, return_sn: str) -> PlatformReturn | None:
        raise PlatformError("TikTok: yêu cầu trả chưa hỗ trợ (T-210)")
