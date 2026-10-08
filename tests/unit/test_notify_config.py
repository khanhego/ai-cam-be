"""T-226: che bot token trong log, validator `NOTIFY_TRANSPORT`, giờ yên lặng (BR-36 (4))."""

from datetime import UTC, datetime, time

import pytest

from aicam.core.logging import redact, redact_query
from aicam.core.settings import Settings
from aicam.modules.notify.service import in_quiet, quiet_end_after
from aicam.modules.settings.models import Setting

TZ = "Asia/Ho_Chi_Minh"


def test_redacts_telegram_bot_token_in_path() -> None:
    url = "https://api.telegram.org/bot123456:AAH-xyz_Tok3n/sendMessage"
    assert redact_query(url) == "https://api.telegram.org/bot[token đã che]/sendMessage"
    event = redact(None, "", {"event": "x", "url": url})
    assert "AAH-xyz_Tok3n" not in str(event)


def test_production_rejects_mock_transport() -> None:
    prod = {
        "app_env": "production",
        "jwt_secret": "x" * 40,
        "media_signing_key": "y" * 40,
        "fernet_key": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
        "platform_adapter": "shopee",
    }
    with pytest.raises(ValueError, match="NOTIFY_TRANSPORT=mock"):
        Settings(**prod, notify_transport="mock")  # type: ignore[arg-type]
    assert Settings(**prod).notify_transport == "real"  # type: ignore[arg-type]


def _cfg(start: time = time(22), end: time = time(7), enabled: bool = True) -> Setting:
    return Setting(id=1, quiet_hours_enabled=enabled, quiet_start=start, quiet_end=end)


@pytest.mark.parametrize(
    ("utc_hour", "minute", "expected"),
    [(15, 0, True), (14, 59, False), (23, 59, True), (0, 0, False), (23, 0, True), (16, 10, True)],
)
def test_in_quiet_overnight(utc_hour: int, minute: int, expected: bool) -> None:
    # 15:00 UTC = 22:00 VN; 00:00 UTC = 07:00 VN (hết giờ yên lặng); 16:10 UTC = 23:10 VN.
    at = datetime(2026, 10, 7, utc_hour, minute, tzinfo=UTC)
    assert in_quiet(_cfg(), at, TZ) is expected


def test_in_quiet_daytime_range_and_disabled() -> None:
    at = datetime(2026, 10, 7, 5, 0, tzinfo=UTC)  # 12:00 VN
    assert in_quiet(_cfg(time(11), time(13)), at, TZ)
    assert not in_quiet(_cfg(enabled=False), datetime(2026, 10, 7, 16, 0, tzinfo=UTC), TZ)


def test_quiet_end_after() -> None:
    # 23:10 VN 07/10 → 07:00 VN 08/10 = 00:00 UTC 08/10; 03:00 VN → 07:00 VN cùng ngày.
    assert quiet_end_after(_cfg(), datetime(2026, 10, 7, 16, 10, tzinfo=UTC), TZ) == datetime(
        2026, 10, 8, 0, 0, tzinfo=UTC
    )
    assert quiet_end_after(_cfg(), datetime(2026, 10, 7, 20, 0, tzinfo=UTC), TZ) == datetime(
        2026, 10, 8, 0, 0, tzinfo=UTC
    )
