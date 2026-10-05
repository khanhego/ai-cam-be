"""WS-01 / WS-02 (02 §6): khung `{"type", "data", "at"}`, `at` ISO-8601 UTC `Z` (RB-11)."""

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from aicam.core import clock
from aicam.realtime import publish


def test_ws_message_envelope_uses_utc_z() -> None:
    clock.freeze(datetime(2026, 10, 4, 7, 27, 5, tzinfo=UTC))

    msg = json.loads(publish._message("report.updated", {"date": "2026-10-04"}))

    assert msg == {"type": "report.updated", "data": {"date": "2026-10-04"}, "at": "2026-10-04T07:27:05Z"}


def test_iso_z_converts_to_utc() -> None:
    vn = timezone(timedelta(hours=7))

    assert clock.iso_z(datetime(2026, 10, 4, 14, 27, 5, 120000, tzinfo=vn)) == "2026-10-04T07:27:05.120000Z"
    with pytest.raises(ValueError, match="tzinfo"):
        clock.iso_z(datetime(2026, 10, 4))
