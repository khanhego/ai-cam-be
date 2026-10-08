"""Giao diện nhà cung cấp gửi tin (02a §7.5 "Nhà cung cấp").

Không bao giờ đưa URL gọi ra (Telegram có bot token trong đường dẫn), header hay chuỗi lỗi thô của httpx vào
`SendError.message` / log — chỉ thông điệp tiếng Việt cố định + mã của nhà cung cấp.
"""

from typing import Protocol

# HTTP timeout mỗi lần gửi (02a J-27: 8 giây; API-174 bọc thêm `asyncio.timeout(10)`).
HTTP_TIMEOUT_S = 8.0


class SendError(Exception):
    """Nhà cung cấp từ chối / không kết nối được. `timeout` → API-174 504 `NOTIFY_TIMEOUT`, còn lại 502."""

    def __init__(
        self,
        message: str,
        *,
        provider_code: str | None = None,
        timeout: bool = False,
        retry_after_s: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.provider_code = provider_code
        self.timeout = timeout
        self.retry_after_s = retry_after_s


class Provider(Protocol):
    type: str

    async def send(self, target: str, text: str) -> None:
        """Gửi một tin; lỗi → `SendError`."""
        ...
