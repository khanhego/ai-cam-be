"""Telegram Bot API (02a §7.5) — `POST {TELEGRAM_API_BASE}/bot{token}/sendMessage`.

Theo tài liệu công khai https://core.telegram.org/bots/api#sendmessage: trả `{"ok": true, "result": …}` hoặc
`{"ok": false, "error_code": 400, "description": "Bad Request: chat not found"}`; 429 kèm
`parameters.retry_after` (giây). **Chưa test với bot thật — thiếu tài nguyên (Q21)**; test dùng server giả
(respx).
"""

from typing import Any

import httpx
import structlog

from aicam.modules.notify.providers.base import HTTP_TIMEOUT_S, SendError

log = structlog.get_logger()

TEXT_MAX = 4096  # giới hạn độ dài tin của Telegram

MSG_CHAT = "Telegram không nhận Chat ID này. Kiểm tra bot đã vào nhóm."
MSG_TOKEN = "Bot Telegram trên máy chủ không hợp lệ. Liên hệ IT."  # noqa: S105 — thông điệp, không phải bí mật
MSG_RATE = "Telegram tạm giới hạn tần suất gửi — sẽ thử lại."
MSG_UNREACHABLE = "Không kết nối được Telegram từ máy chủ (mạng chặn?)."
MSG_OTHER = "Telegram từ chối tin."
MSG_SERVER = "Máy chủ Telegram đang lỗi — sẽ thử lại."


class TelegramProvider:
    type = "TELEGRAM"

    def __init__(
        self, token: str, api_base: str, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self._token = token
        self._base = api_base.rstrip("/")
        self._transport = transport

    async def send(self, target: str, text: str) -> None:
        body = {"chat_id": target, "text": text[:TEXT_MAX], "disable_web_page_preview": True}
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S, transport=self._transport) as client:
                resp = await client.post(f"{self._base}/bot{self._token}/sendMessage", json=body)
        except httpx.TransportError as exc:  # timeout, DNS, TLS, kết nối bị chặn
            log.warning("telegram_unreachable", error=type(exc).__name__)
            raise SendError(MSG_UNREACHABLE, provider_code=type(exc).__name__, timeout=True) from None
        try:
            data: dict[str, Any] = resp.json()
        except ValueError:
            data = {}
        if resp.status_code == 200 and data.get("ok") is True:
            return
        code = data.get("error_code") or resp.status_code
        description = str(data.get("description") or "")
        log.warning("telegram_rejected", http_status=resp.status_code, provider_code=code)
        if resp.status_code == 429 or code == 429:
            retry_after = (data.get("parameters") or {}).get("retry_after")
            raise SendError(
                MSG_RATE,
                provider_code="429",
                retry_after_s=float(retry_after) if isinstance(retry_after, (int, float)) else None,
            )
        if code in (401, 404) and "chat" not in description.lower():
            raise SendError(MSG_TOKEN, provider_code=str(code))  # token sai → 401 / 404 không phải chat
        if code in (400, 403):
            raise SendError(MSG_CHAT, provider_code=str(code))
        if resp.status_code >= 500:
            raise SendError(MSG_SERVER, provider_code=str(code))
        raise SendError(MSG_OTHER, provider_code=str(code))
