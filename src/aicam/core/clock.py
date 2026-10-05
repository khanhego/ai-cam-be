"""Nguồn giờ duy nhất của hệ thống (architecture §15). Giả lập được trong test (BR-16, retention)."""

from datetime import UTC, datetime, timedelta

_offset = timedelta(0)
_frozen: datetime | None = None


def now() -> datetime:
    """Giờ hiện tại, luôn có múi giờ UTC."""
    if _frozen is not None:
        return _frozen
    return datetime.now(UTC) + _offset


def freeze(at: datetime) -> None:
    """Chỉ dùng trong test: cố định giờ."""
    global _frozen
    if at.tzinfo is None:
        raise ValueError("at phải có tzinfo")
    _frozen = at.astimezone(UTC)


def advance(delta: timedelta) -> None:
    """Chỉ dùng trong test: tua giờ (cộng dồn)."""
    global _frozen, _offset
    if _frozen is not None:
        _frozen += delta
    else:
        _offset += delta


def reset() -> None:
    global _frozen, _offset
    _frozen = None
    _offset = timedelta(0)


def iso_z(at: datetime) -> str:
    """ISO-8601 UTC hậu tố `Z` (02 §6 "Thời gian") — cùng dạng Pydantic xuất cho response API.

    Dùng cho mốc giờ tự ghép vào JSON ngoài schema Pydantic: WS `at`, `alert.data`, `error.details`,
    jsonb `last_error`.
    """
    if at.tzinfo is None:
        raise ValueError("at phải có tzinfo")
    return at.astimezone(UTC).isoformat().replace("+00:00", "Z")
