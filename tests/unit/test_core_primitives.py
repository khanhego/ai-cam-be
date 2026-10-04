from datetime import UTC, datetime, timedelta

import pytest

from aicam.core import clock
from aicam.core.ids import uuid7
from aicam.core.settings import Settings


def test_uuid7_has_version_and_variant() -> None:
    value = uuid7()

    assert value.version == 7
    assert value.variant == "specified in RFC 4122"


def test_uuid7_sorts_by_creation_time() -> None:
    first = uuid7()
    clock_ms_later = [uuid7() for _ in range(5)]

    assert all(first.int >> 80 <= later.int >> 80 for later in clock_ms_later)


def test_clock_freeze_and_advance() -> None:
    at = datetime(2026, 10, 4, 8, 0, tzinfo=UTC)

    clock.freeze(at)
    clock.advance(timedelta(minutes=30))

    assert clock.now() == at + timedelta(minutes=30)


def test_clock_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="tzinfo"):
        clock.freeze(datetime(2026, 10, 4, 8, 0))


def test_clock_now_is_utc_aware() -> None:
    assert clock.now().tzinfo is UTC


def test_production_rejects_dev_secrets() -> None:
    with pytest.raises(ValueError, match="jwt_secret"):
        Settings(app_env="production")


def test_production_accepts_real_secrets() -> None:
    settings = Settings(
        app_env="production",
        jwt_secret="a" * 40,
        media_signing_key="b" * 40,
        fernet_key="c2VjcmV0LWZlcm5ldC1rZXktZm9yLXRlc3QtMzJiISE=",
    )

    assert settings.is_production
    assert not settings.fake_clock_allowed
