"""Adapter TikTok giả 2 shop (02a §7.2, DEC-474): **adapter TikTok thật** (`tiktok/adapter.py` + `mapping.py`
+ `returns_mapping.py` + client ký / thử lại) chạy trên một `httpx.MockTransport` trong tiến trình phục vụ
fixture JSON **định dạng TikTok** (`fixtures/tiktok/{orders,returns}/*.json`) — như mock Shopee đi qua
`returns_mapping`.

- Ủy quyền: trang ủy quyền giả chuyển thẳng về callback với `code = MOCK-TT-CODE` → một grant (`open_id`
  `MOCK-OPEN-1`) gồm 2 shop `TTMOCKA` "TST TikTok A (mock)", `TTMOCKB` "TST TikTok B (mock)".
- Dữ liệu theo shop (`_shop` trong fixture, nhận diện qua `shop_cipher`); `create_time` / `update_time` = lúc
  nạp; `_status_after` (yêu cầu trả) đổi trạng thái sau N giờ theo `clock` (AC-42 với đồng hồ giả).
- Điều khiển test (NFR-39, AC-43): `delay_s_by_shop`, `fail_shop` (luôn 503), `fail_times` (429 `Retry-After:
  1` N lần), `fail_auth` (mã token), `calls[]` `(path, shop)`.
"""

import asyncio
import json
import secrets
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode

import httpx

from aicam.core import clock
from aicam.modules.platforms.tiktok.adapter import TikTokAdapter
from aicam.modules.platforms.tiktok.client import TikTokClient

FIXTURES = Path(__file__).parent / "fixtures" / "tiktok"
MOCK_TT_CODE = "MOCK-TT-CODE"
MOCK_OPEN_ID = "MOCK-OPEN-1"
MOCK_TT_SHOPS = {"TTMOCKA": "TST TikTok A (mock)", "TTMOCKB": "TST TikTok B (mock)"}
_BASE = "https://mock.tiktok.invalid"


def cipher_of(shop: str) -> str:
    return f"MOCKCIPHER-{shop}"


def _load(kind: str) -> list[dict[str, Any]]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted((FIXTURES / kind).glob("*.json"))]


def _ok(data: dict[str, Any]) -> httpx.Response:
    return httpx.Response(
        200, json={"code": 0, "message": "Success", "request_id": secrets.token_hex(6), "data": data}
    )


def _window(body: dict[str, Any], item: dict[str, Any]) -> bool:
    t = int(item.get("update_time") or 0)
    return int(body.get("update_time_ge") or 0) <= t < int(body.get("update_time_lt") or 1 << 62)


class MockTikTokData:
    """Kho dữ liệu giả theo shop + điều khiển test."""

    def __init__(self, loaded_at: datetime | None = None) -> None:
        self.loaded_at = loaded_at or clock.now()
        # Cửa sổ TikTok `update_time_lt` loại trừ: dữ liệu nạp "1 phút trước" để lượt J-04 đầu (cùng giây)
        # thấy.
        stamp = int(self.loaded_at.timestamp()) - 60
        self.orders: dict[str, dict[str, dict[str, Any]]] = {s: {} for s in MOCK_TT_SHOPS}
        for o in _load("orders"):
            shop = o.pop("_shop")
            o.pop("_note", None)
            o.setdefault("create_time", stamp)
            o.setdefault("update_time", stamp)
            self.orders[shop][str(o["id"])] = o
        self.returns: dict[str, dict[str, dict[str, Any]]] = {s: {} for s in MOCK_TT_SHOPS}
        for r in _load("returns"):
            shop = r.pop("_shop")
            r.pop("_note", None)
            r.setdefault("create_time", stamp)
            r.setdefault("update_time", stamp)
            self.returns[shop][str(r["return_id"])] = r
        self.cancellations: dict[str, list[dict[str, Any]]] = {s: [] for s in MOCK_TT_SHOPS}
        # 4 kịch bản yêu cầu hủy (DEC-502, T-277): `steps[k]` áp ở lượt `orders/search` thứ k+1 của shop.
        self.cancel_scripts: dict[str, list[tuple[str, list[list[dict[str, Any]]]]]] = {
            s: [] for s in MOCK_TT_SHOPS
        }
        self._search_count: dict[str, int] = {}
        for c in _load("cancellations"):
            shop = c["_shop"]
            detail = dict(c["order"])
            detail.setdefault("create_time", stamp)
            detail.setdefault("update_time", stamp)
            self.orders[shop][str(detail["id"])] = detail
            self.cancel_scripts[shop].append((str(detail["id"]), c["steps"]))
        self.delay_s_by_shop: dict[str, float] = {}
        self.fail_shop: set[str] = set()
        self.fail_times: dict[str, int] = {}
        self.fail_auth: set[str] = set()
        self.calls: list[tuple[str, str | None]] = []
        self.tokens_issued = 0

    # ----- điều khiển trong test
    def put_order(self, shop: str, detail: dict[str, Any]) -> None:
        detail.setdefault("create_time", int(clock.now().timestamp()) - 1)
        detail["update_time"] = int(clock.now().timestamp()) - 1
        self.orders[shop][str(detail["id"])] = detail

    def set_status(self, shop: str, order_id: str, status: str) -> None:
        self.orders[shop][order_id]["status"] = status
        self.orders[shop][order_id]["update_time"] = int(clock.now().timestamp()) - 1

    def _return_now(self, r: dict[str, Any]) -> dict[str, Any]:
        """Áp `_status_after` theo đồng hồ (giờ kể từ lúc nạp) — `update_time` = mốc đổi."""
        out = {k: v for k, v in r.items() if k != "_status_after"}
        for step in r.get("_status_after") or []:
            at = self.loaded_at + timedelta(hours=float(step["after_hours"]))
            if clock.now() >= at:
                out.update({k: v for k, v in step.items() if k != "after_hours"})
                out["update_time"] = int(at.timestamp())
        return out

    def _advance_cancellations(self, shop: str) -> None:
        """Lượt J-04 thứ n: áp bước n của từng kịch bản (giữ bước cuối). Yêu cầu hủy đổi → `update_time` mới
        của **yêu cầu** (đơn có thể không đổi — 02a §7.1: vẫn phải đọc chi tiết đơn); đổi trạng thái đơn khi
        bước có `order_status`. Cờ `is_buyer_request_cancel` theo yêu cầu `PENDING` mới nhất."""
        step = self._search_count.get(shop, 0)
        self._search_count[shop] = step + 1
        now = int(clock.now().timestamp()) - 1
        for order_id, steps in self.cancel_scripts[shop]:
            if not steps:
                continue
            for change in steps[min(step, len(steps) - 1)]:
                rows = self.cancellations[shop]
                current = next((c for c in rows if c["cancel_id"] == change["cancel_id"]), None)
                status = change["cancel_status"]
                if current is None:
                    rows.append(
                        {"cancel_id": change["cancel_id"], "order_id": order_id, "cancel_status": status,
                         "create_time": now, "update_time": now}
                    )  # fmt: skip
                elif current["cancel_status"] != status:
                    current["cancel_status"] = status
                    current["update_time"] = now
                order = self.orders[shop][order_id]
                if change.get("order_status") and order["status"] != change["order_status"]:
                    order["status"] = change["order_status"]
                    order["update_time"] = now
                order["is_buyer_request_cancel"] = status == "PENDING"

    # ----- HTTP giả
    def shop_of(self, request: httpx.Request) -> str | None:
        cipher = parse_qs(request.url.query.decode()).get("shop_cipher", [""])[0]
        return next((s for s in MOCK_TT_SHOPS if cipher_of(s) == cipher), None)

    async def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        shop = self.shop_of(request)
        self.calls.append((path, shop))
        if shop is not None and shop in self.delay_s_by_shop:
            await asyncio.sleep(self.delay_s_by_shop[shop])
        if shop is not None and shop in self.fail_shop:
            return httpx.Response(503, json={"code": 50001, "message": "mock fail_shop"})
        if shop is not None and self.fail_times.get(shop, 0) > 0:
            self.fail_times[shop] -= 1
            return httpx.Response(
                429, headers={"Retry-After": "1"}, json={"code": 36009004, "message": "limit"}
            )
        if shop is not None and shop in self.fail_auth:
            return httpx.Response(200, json={"code": 105002, "message": "Expired credentials (mock)"})
        q = {k: v[0] for k, v in parse_qs(request.url.query.decode()).items()}
        body = json.loads(request.content) if request.content else {}
        if path in ("/api/v2/token/get", "/api/v2/token/refresh"):
            return self._token(path, q)
        if path == "/authorization/202309/shops":
            shops = [
                {"id": s, "name": n, "region": "VN", "cipher": cipher_of(s), "code": s}
                for s, n in MOCK_TT_SHOPS.items()
            ]
            return _ok({"shops": shops})
        if shop is None:
            return httpx.Response(200, json={"code": 36004004, "message": "missing shop_cipher (mock)"})
        if path == "/order/202309/orders/search":
            self._advance_cancellations(shop)
            rows = [{"id": i} for i, o in self.orders[shop].items() if _window(body, o)]
            return _ok({"orders": rows, "next_page_token": "", "total_count": len(rows)})
        if path == "/order/202309/orders":
            ids = [i for i in q.get("ids", "").split(",") if i]
            return _ok({"orders": [self.orders[shop][i] for i in ids if i in self.orders[shop]]})
        if path == "/return_refund/202309/cancellations/search":
            rows = self.cancellations[shop]
            if body.get("order_ids"):
                rows = [c for c in rows if c["order_id"] in body["order_ids"]]
            else:
                rows = [c for c in rows if _window(body, c)]
            return _ok({"cancellations": rows, "next_page_token": ""})
        if path == "/return_refund/202309/returns/search":
            rows = [self._return_now(r) for r in self.returns[shop].values()]
            if body.get("return_ids"):
                rows = [r for r in rows if r["return_id"] in body["return_ids"]]
            else:
                rows = [r for r in rows if _window(body, r)]
            return _ok({"return_orders": rows, "next_page_token": ""})
        if path.startswith("/fulfillment/202309/packages/"):
            package_id = path.rsplit("/", 1)[-1]
            ids = [
                i
                for i, o in self.orders[shop].items()
                if any(li.get("package_id") == package_id for li in o.get("line_items") or [])
            ]
            return _ok({"id": package_id, "orders": [{"id": i} for i in ids]})
        return httpx.Response(404, json={"code": 40400, "message": f"mock: không có {path}"})

    def _token(self, path: str, q: dict[str, str]) -> httpx.Response:
        if path.endswith("/get") and q.get("auth_code") != MOCK_TT_CODE:
            return httpx.Response(200, json={"code": 105003, "message": "invalid auth_code (mock)"})
        if path.endswith("/refresh") and MOCK_OPEN_ID in self.fail_auth:
            return httpx.Response(200, json={"code": 105003, "message": "invalid refresh_token (mock)"})
        self.tokens_issued += 1
        now = int(clock.now().timestamp())
        return _ok(
            {
                "access_token": f"mock-tt-access-{secrets.token_hex(4)}",
                "access_token_expire_in": now + 7 * 86400,
                "refresh_token": f"mock-tt-refresh-{secrets.token_hex(4)}",
                "refresh_token_expire_in": now + 365 * 86400,
                "open_id": MOCK_OPEN_ID,
                "seller_name": "TST TikTok (mock)",
            }
        )


Sleep = Callable[[float], Awaitable[None]]


class MockTikTokAdapter(TikTokAdapter):
    """Adapter TikTok thật + transport giả (`code = "TIKTOK"`, `is_mock`)."""

    is_mock = True

    def __init__(
        self, *, sleep: Sleep = asyncio.sleep, max_attempts: int = 5, backoff_s: float = 0.5
    ) -> None:
        self.data = MockTikTokData()
        client = TikTokClient(
            "mock-app-key", "mock-app-secret", _BASE, _BASE, timeout_s=10.0, max_attempts=max_attempts,
            backoff_s=backoff_s, sleep=sleep, transport=httpx.MockTransport(self.data.handle),
        )  # fmt: skip
        super().__init__(client, authorize_url=f"{_BASE}/open/authorize", service_id="mock")

    def build_auth_url(self, redirect_url: str, state: str) -> str:
        """Trang ủy quyền giả: về thẳng callback API-155 với `code` giả + `state`."""
        sep = "&" if "?" in redirect_url else "?"
        return f"{redirect_url}{sep}{urlencode({'code': MOCK_TT_CODE, 'state': state})}"
