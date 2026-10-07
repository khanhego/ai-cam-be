"""Log JSON có ngữ cảnh (02a §10)."""

import logging
import re
import sys
from collections.abc import MutableMapping
from typing import Any

import structlog

_SENSITIVE_KEYS = re.compile(r"pass(word)?|secret|token|authorization|cookie|key", re.IGNORECASE)
_URL_CREDENTIALS = re.compile(r"(\w+://)[^/@\s:]+:[^/@\s]+@")
# Tham số query mang bí mật (G3-F3, G3-N1): URL ký media (`sig`, `exp`, `uid`), token WS, OAuth (`code`,
# `state`), Shopee (`access_token`, `refresh_token`, `sign`), TikTok Shop (`app_secret`, `auth_code` ở
# `/api/v2/token/*` — 02a §2 core/logging.py), URL ký S3 (`X-Amz-Signature`, `X-Amz-Credential` — ADR-010).
SENSITIVE_QUERY = (
    "token", "sig", "exp", "uid", "code", "state", "access_token", "refresh_token", "sign", "app_secret",
    "auth_code", "X-Amz-Signature", "X-Amz-Credential", "X-Amz-Security-Token",
)  # fmt: skip
_QUERY_SECRET = re.compile(r"([?&])(" + "|".join(SENSITIVE_QUERY) + r")=[^&\s\"'#]*", re.IGNORECASE)
# Bot token Telegram nằm trong **đường dẫn** (`api.telegram.org/bot123:ABC…/sendMessage`) — 02a §2.
_TELEGRAM_BOT_PATH = re.compile(r"/bot\d+:[A-Za-z0-9_-]+")
# Token link chia sẻ nằm trong **khóa đối tượng** `share/{token_urlsafe(32)}/…` (NFR-42) — lỗi kho lưu /
# botocore có thể in khóa đối tượng (T-228, DEC-781).
_SHARE_TOKEN_PATH = re.compile(r"share/[A-Za-z0-9_-]{20,}")
# Thân JSON lỗi nhà cung cấp (Zalo OA làm mới token, TikTok token/get) có thể bị ghi kèm: che giá trị.
_JSON_SECRET = re.compile(
    r"(\"(?:access_token|refresh_token|app_secret|secret_key|client_secret)\"\s*:\s*)\"[^\"]*\"",
    re.IGNORECASE,
)
# Giá trị secret đang dùng (khóa sao lưu, bot token, khóa S3…) — đăng ký lúc khởi động (`register_secrets`),
# che ở mọi chuỗi log dù nằm dưới tên trường nào (NFR-41: khóa sao lưu không có trong log — DEC-781).
_SECRET_VALUES: list[str] = []
SECRET_MASK = "[secret đã che]"  # noqa: S105 — nhãn thay thế, không phải secret
# Logger stdlib in nguyên URL gọi ra / vào: che query, httpx / httpcore chỉ ghi từ WARNING.
NOISY_HTTP_LOGGERS = ("httpx", "httpcore")
REDACTED_LOGGERS = ("uvicorn", "uvicorn.access", "uvicorn.error", "httpx", "httpcore", "celery")


def register_secrets(values: "list[str] | tuple[str, ...]") -> None:
    """Thêm giá trị secret cần che (≥ 8 ký tự; dài trước để che trọn khóa chứa khóa khác)."""
    for value in values:
        v = value.strip()
        if len(v) >= 8 and v not in _SECRET_VALUES:
            _SECRET_VALUES.append(v)
    _SECRET_VALUES.sort(key=len, reverse=True)


def redact_query(text: str) -> str:
    """`/ws/station?token=abc&x=1` → `/ws/station?[token đã che]&x=1`; bot token Telegram trong đường dẫn,
    token `share/…`, `"access_token": "…"` trong JSON, mọi giá trị secret đã đăng ký.

    Bỏ cả dấu `=` để log không còn mẫu `token=` / `sig=` (kiểm vận hành: `grep -E 'token=|sig='` phải rỗng).
    """
    for secret in _SECRET_VALUES:
        if secret in text:
            text = text.replace(secret, SECRET_MASK)
    text = _QUERY_SECRET.sub(r"\1[\2 đã che]", text)
    text = _TELEGRAM_BOT_PATH.sub("/bot[token đã che]", text)
    text = _SHARE_TOKEN_PATH.sub("share/[token đã che]", text)
    return _JSON_SECRET.sub(r'\1"[đã che]"', text)


def _clean_arg(value: object) -> object:
    if value is None or isinstance(value, (int, float)):  # giữ kiểu cho %d / %f
        return value
    text = str(value)  # str, httpx.URL…
    cleaned = redact_query(text)
    return cleaned if cleaned != text else value


class RedactQueryFilter(logging.Filter):
    """Che bí mật trong query của bản ghi stdlib (uvicorn access log, httpx…) — đi vòng bộ che structlog.

    Che từng tham số (không gộp thành chuỗi): `AccessFormatter` của uvicorn cần nguyên tuple `args`.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_query(record.msg)
        if isinstance(record.args, tuple):
            record.args = tuple(_clean_arg(a) for a in record.args)
        elif isinstance(record.args, dict):
            record.args = {k: _clean_arg(v) for k, v in record.args.items()}
        return True


_FILTER = RedactQueryFilter()


def install_stdlib_redaction(*extra: logging.Logger) -> None:
    """Gắn bộ che vào logger stdlib hay in URL + mọi handler của root; httpx / httpcore lên WARNING.

    Gọi ở api (`configure_logging`) và worker / beat Celery (`after_setup_logger`, `after_setup_task_logger`).
    """
    for name in NOISY_HTTP_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    loggers = [logging.getLogger(n) for n in REDACTED_LOGGERS] + [logging.getLogger(), *extra]
    for logger in loggers:
        if _FILTER not in logger.filters:
            logger.addFilter(_FILTER)
        for handler in logger.handlers:
            if _FILTER not in handler.filters:
                handler.addFilter(_FILTER)


def redact(_: Any, __: str, event: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Che mật khẩu / token / user:pass@ trong URL trước khi ghi log (review M1 #16)."""
    for key, value in event.items():
        if _SENSITIVE_KEYS.search(key):
            event[key] = "***"
        elif isinstance(value, str):
            if "@" in value:
                value = _URL_CREDENTIALS.sub(r"\1***:***@", value)
            event[key] = redact_query(value)
    return event


def configure_logging(
    level: str = "INFO", json: bool = True, secrets: "list[str] | tuple[str, ...]" = ()
) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    install_stdlib_redaction()
    configure_structlog(level, json, secrets)


def configure_structlog(
    level: str = "INFO", json: bool = True, secrets: "list[str] | tuple[str, ...]" = ()
) -> None:
    """Bộ xử lý structlog có `redact` — gọi ở api / vision (`configure_logging`) **và** worker / beat Celery
    (`after_setup_logger`): trước T-228 worker dùng cấu hình mặc định của structlog, không che gì (DEC-781).

    JSON: `format_exc_info` chạy **trước** `redact` — traceback (`log.exception`) cũng được che (production
    luôn JSON; dev console giữ traceback đẹp của `ConsoleRenderer`)."""
    register_secrets(secrets)
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )
    exc: list[structlog.typing.Processor] = [structlog.processors.format_exc_info] if json else []
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            *exc,
            redact,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )
