"""Log JSON có ngữ cảnh (02a §10)."""

import logging
import re
import sys
from collections.abc import MutableMapping
from typing import Any

import structlog

_SENSITIVE_KEYS = re.compile(r"pass(word)?|secret|token|authorization|cookie|key", re.IGNORECASE)
_URL_CREDENTIALS = re.compile(r"(\w+://)[^/@\s:]+:[^/@\s]+@")


def redact(_: Any, __: str, event: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Che mật khẩu / token / user:pass@ trong URL trước khi ghi log (review M1 #16)."""
    for key, value in event.items():
        if _SENSITIVE_KEYS.search(key):
            event[key] = "***"
        elif isinstance(value, str) and "@" in value:
            event[key] = _URL_CREDENTIALS.sub(r"\1***:***@", value)
    return event


def configure_logging(level: str = "INFO", json: bool = True) -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            redact,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        cache_logger_on_first_use=True,
    )
