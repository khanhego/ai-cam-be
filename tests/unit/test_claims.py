"""`claims.transition` mọi cặp (02a §5 "Chuyển trạng thái hồ sơ khiếu nại", §11), cờ hạn (FR-08.04)."""

from datetime import UTC, datetime, timedelta
from itertools import product

import pytest

from aicam.modules.claims.models import CLAIM_STATUSES, Claim
from aicam.modules.claims.service import allowed_transitions
from aicam.modules.claims.views import due_flags

ALLOWED = {
    ("NEW", "SUBMITTED"),
    ("NEW", "CLOSED"),
    ("SUBMITTED", "WAITING"),
    ("SUBMITTED", "WON"),
    ("SUBMITTED", "LOST"),
    ("SUBMITTED", "CLOSED"),
    ("WAITING", "WON"),
    ("WAITING", "LOST"),
    ("WAITING", "CLOSED"),
    ("WON", "CLOSED"),
    ("LOST", "CLOSED"),
}


@pytest.mark.parametrize(("src", "dst"), list(product(CLAIM_STATUSES, CLAIM_STATUSES)))
def test_transition_table(src: str, dst: str) -> None:
    assert (dst in allowed_transitions(src)) == ((src, dst) in ALLOWED)


NOW = datetime(2026, 10, 6, 3, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("status", "delta", "expected"),
    [
        ("NEW", timedelta(hours=47), (True, False)),
        ("WAITING", timedelta(hours=48), (True, False)),
        ("SUBMITTED", timedelta(hours=49), (False, False)),
        ("NEW", timedelta(hours=-1), (False, True)),
        ("WON", timedelta(hours=-1), (False, False)),
        ("CLOSED", timedelta(hours=1), (False, False)),
    ],
)
def test_due_flags(status: str, delta: timedelta, expected: tuple[bool, bool]) -> None:
    claim = Claim(status=status, deadline_at=NOW + delta)
    assert due_flags(claim, NOW, 48) == expected
