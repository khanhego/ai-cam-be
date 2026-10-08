"""Ngân sách thời gian của một lượt job sàn (02a §2 `budget.py`, §7 — chuyển từ `shopee/client.py`).

`time_budget(seconds)` đặt hạn chót (monotonic) cho coroutine hiện tại; client HTTP (Shopee, TikTok) đọc
`deadline()` để không chờ `Retry-After` / giãn cách vượt thời gian còn lại; job đọc `expired()` giữa các đơn
để dừng sớm (cursor không tiến — lượt sau làm lại). G3 F-14: tránh Celery `soft_time_limit` giết task giữa
chừng.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_deadline: ContextVar[float | None] = ContextVar("platform_deadline", default=None)


@contextmanager
def time_budget(seconds: float) -> Iterator[None]:
    """Đặt hạn chót cho mọi lời gọi sàn trong khối (lồng nhau → lấy hạn sớm hơn)."""
    end = time.monotonic() + seconds
    outer = _deadline.get()
    token = _deadline.set(end if outer is None else min(outer, end))
    try:
        yield
    finally:
        _deadline.reset(token)


def deadline() -> float | None:
    """Hạn chót monotonic hiện tại; None = không giới hạn (API, test)."""
    return _deadline.get()


def remaining() -> float | None:
    """Số giây còn lại (≥ 0); None = không giới hạn."""
    end = _deadline.get()
    return None if end is None else max(0.0, end - time.monotonic())


def expired() -> bool:
    end = _deadline.get()
    return end is not None and time.monotonic() >= end
