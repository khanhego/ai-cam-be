from datetime import UTC, datetime, timedelta

import pytest

from aicam.core import clock
from aicam.core.ids import uuid7
from aicam.core.settings import Settings


def test_uuid7_has_version_and_variant() -> None:
    value = uuid7()

    assert value.version == 7
    assert value.variant == "specified in RFC 4122"


def test_uuid7_is_strictly_monotonic_within_same_millisecond() -> None:
    ids = [uuid7() for _ in range(5000)]

    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)
    assert all(i.version == 7 for i in ids)


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
        platform_adapter="shopee",
    )

    assert settings.is_production
    assert not settings.fake_clock_allowed


def test_production_rejects_mock_platform_adapter() -> None:
    """G3-N13: adapter mock ở production trả đơn giả cho mọi mã quét."""
    with pytest.raises(ValueError, match="PLATFORM_ADAPTER=mock"):
        Settings(
            app_env="production",
            jwt_secret="a" * 40,
            media_signing_key="b" * 40,
            fernet_key="c2VjcmV0LWZlcm5ldC1rZXktZm9yLXRlc3QtMzJiISE=",
            platform_adapter="mock",
        )
    assert Settings(app_env="dev", platform_adapter="mock").platform_adapter == "mock"


def test_log_redacts_secrets_and_url_credentials() -> None:
    """Review M1 #16."""
    from aicam.core.logging import redact

    event = redact(
        None,
        "info",
        {
            "event": "probe rtsp://admin:pw123@10.0.0.5/stream",
            "password": "pw123",
            "access_token": "eyJ...",
            "source": "rtsp://u:p@cam/1",
            "code": "SPXTST0000001",
        },
    )

    assert event == {
        "event": "probe rtsp://***:***@10.0.0.5/stream",
        "password": "***",
        "access_token": "***",
        "source": "rtsp://***:***@cam/1",
        "code": "SPXTST0000001",
    }


def test_staging_requires_real_secrets() -> None:
    """Review M1 #17."""
    import pytest
    from pydantic import ValidationError

    from aicam.core.settings import Settings

    with pytest.raises(ValidationError, match="staging cần đặt secret thật"):
        Settings(app_env="staging")


@pytest.mark.parametrize(
    ("code", "expected"), [("40P01", True), ("55P03", True), ("23505", False), (None, False)]
)
def test_is_lock_conflict_by_sqlstate(code: str | None, expected: bool) -> None:
    """DEC-162 (G3-V1): chỉ deadlock / hết lock_timeout mới thành 409 IMPORT_CONFLICT; lỗi DB khác vẫn 500."""
    from sqlalchemy.exc import DBAPIError

    from aicam.core.db import is_lock_conflict

    class PgError(Exception):
        sqlstate = code

    assert is_lock_conflict(DBAPIError("SELECT 1", {}, PgError("x"))) is expected
