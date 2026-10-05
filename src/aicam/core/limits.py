"""Giới hạn kích thước body trước khi đọc (G3-N2).

Starlette ghi toàn bộ multipart ra file tạm **trước** khi chạy dependency xác thực: không chặn sớm thì một
request nhiều GB từ LAN (kể cả chưa đăng nhập) làm đầy ổ `/tmp` (có thể chung ổ video). Middleware kiểm
`Content-Length` trước khi đọc body và đếm byte thật khi đọc (phòng client khai sai / chunked). Caddy chặn
thêm ở lớp proxy (`request_body max_size`, docker/Caddyfile).
"""

import json
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from aicam.core.errors import error_body

IMPORT_PREFIX = "/api/v1/imports"
IMPORT_MAX_BYTES = 6 * 1024 * 1024  # file ≤ 5 MB (parser.MAX_BYTES) + phần bao multipart
DEFAULT_MAX_BYTES = 1024 * 1024  # mọi API JSON khác
_METHODS = frozenset({"POST", "PUT", "PATCH"})


def limit_for(path: str) -> int:
    return IMPORT_MAX_BYTES if path.startswith(IMPORT_PREFIX) else DEFAULT_MAX_BYTES


class _TooLarge(Exception):
    pass


class BodySizeLimitMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] not in _METHODS:
            await self.app(scope, receive, send)
            return
        limit = limit_for(scope["path"])
        declared = _content_length(scope)
        if declared is not None and declared > limit:
            await _reject(send, limit)
            return
        received = 0
        exceeded = False
        replied = False

        async def counted_receive() -> Message:
            nonlocal received, exceeded
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    exceeded = True
                    raise _TooLarge
            return message

        async def guarded_send(message: Message) -> None:
            # Vượt giới hạn giữa chừng: FastAPI bọc lỗi đọc form thành 400 → thay bằng 413 thống nhất.
            nonlocal replied
            if exceeded:
                if not replied:
                    replied = True
                    await _reject(send, limit)
                return
            replied = replied or message["type"] == "http.response.start"
            await send(message)

        try:
            await self.app(scope, counted_receive, guarded_send)
        except _TooLarge:
            if not replied:
                await _reject(send, limit)


def _content_length(scope: Scope) -> int | None:
    for name, value in scope.get("headers", []):
        if name == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


async def _reject(send: Send, limit: int) -> None:
    body: dict[str, Any] = error_body(
        "PAYLOAD_TOO_LARGE",
        f"Dữ liệu gửi lên quá lớn (tối đa {limit // (1024 * 1024)} MB).",
        {"max_bytes": limit},
    )
    raw = json.dumps(body, ensure_ascii=False).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(raw)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": raw})
