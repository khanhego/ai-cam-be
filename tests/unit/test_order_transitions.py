"""Mọi cặp chuyển trạng thái kho hợp lệ / không hợp lệ (02a §5, 01 §7 v0.3)."""

import itertools
from types import SimpleNamespace

import pytest

from aicam.modules.orders.models import WAREHOUSE_STATUSES
from aicam.modules.orders.service import ALLOWED_TRANSITIONS, InvalidTransition, transition

VALID = sorted(ALLOWED_TRANSITIONS)
INVALID = [p for p in itertools.permutations(WAREHOUSE_STATUSES, 2) if p not in ALLOWED_TRANSITIONS]


class _Session:
    def __init__(self) -> None:
        self.added: list[object] = []

    def add(self, obj: object) -> None:
        self.added.append(obj)


@pytest.mark.parametrize(("frm", "to"), VALID)
async def test_valid_transition_writes_history(frm: str, to: str) -> None:
    package = SimpleNamespace(id="p", warehouse_status=frm)
    session = _Session()

    changed = await transition(session, package, to, source="WAREHOUSE")  # type: ignore[arg-type]

    assert changed is True
    assert package.warehouse_status == to
    assert len(session.added) == 1
    history = session.added[0]
    assert (history.from_status, history.to_status) == (frm, to)  # type: ignore[attr-defined]


@pytest.mark.parametrize(("frm", "to"), INVALID)
async def test_invalid_transition_raises(frm: str, to: str) -> None:
    package = SimpleNamespace(id="p", warehouse_status=frm)

    with pytest.raises(InvalidTransition):
        await transition(_Session(), package, to, source="WAREHOUSE")  # type: ignore[arg-type]

    assert package.warehouse_status == frm


async def test_same_status_is_noop() -> None:
    session = _Session()
    package = SimpleNamespace(id="p", warehouse_status="PACKED")

    assert await transition(session, package, "PACKED", source="WAREHOUSE") is False  # type: ignore[arg-type]
    assert session.added == []


def test_handed_over_cannot_be_repacked() -> None:
    """AC-21: chuyển cấm HANDED_OVER → PACKING."""
    assert ("HANDED_OVER", "PACKING") not in ALLOWED_TRANSITIONS


# ---- Phase 2 (T-102; 02 §5.3 v0.4, 01 §7.1) — TC-ST.01, TC-ST.02, TC-ST.03

from aicam.modules.orders.service import MANUAL_TRANSITIONS  # noqa: E402

PHASE2_REQUIRED = [
    ("HANDED_OVER", "RETURN_EXPECTED"),
    ("DELIVERED", "RETURN_EXPECTED"),
    ("NEW", "RETURN_EXPECTED"),
    ("PACKED", "HANDED_OVER"),  # bước 1 của PACKED → HANDED_OVER → RETURN_EXPECTED (DEC-259)
    ("RETURN_EXPECTED", "DELIVERED"),
    ("RETURN_EXPECTED", "HANDED_OVER"),
    ("RETURN_EXPECTED", "RETURN_MISSING"),
    ("RETURN_MISSING", "HANDED_OVER"),
    *[
        (s, "RETURN_INSPECTING")
        for s in ("RETURN_EXPECTED", "RETURN_MISSING", "HANDED_OVER", "DELIVERED", "NEW")
    ],
    ("RETURN_INSPECTING", "RETURN_RECEIVED_OK"),
    ("RETURN_INSPECTING", "RETURN_RECEIVED_ISSUE"),
    *[(s, "RETURN_RECEIVED_OK") for s in ("RETURN_EXPECTED", "RETURN_MISSING", "HANDED_OVER", "DELIVERED")],
    *[
        ("RETURN_INSPECTING", s)
        for s in ("RETURN_EXPECTED", "RETURN_MISSING", "HANDED_OVER", "DELIVERED", "NEW")
    ],
    ("RETURN_RECEIVED_OK", "RETURN_RECEIVED_ISSUE"),
    ("RETURN_RECEIVED_ISSUE", "RETURN_RECEIVED_OK"),
    ("PACKING", "CANCELLED_AFTER_PACK"),
]
PHASE2_FORBIDDEN = [
    ("RETURN_RECEIVED_OK", "RETURN_EXPECTED"),
    ("RETURN_INSPECTING", "PACKED"),
    ("CANCELLED", "RETURN_INSPECTING"),
    ("NEW", "RETURN_RECEIVED_OK"),  # không có phiên hoàn thì không "đã nhận"
    ("RETURN_RECEIVED_ISSUE", "DELIVERED"),
]


@pytest.mark.parametrize(("frm", "to"), PHASE2_REQUIRED)
def test_phase2_transition_allowed(frm: str, to: str) -> None:
    assert (frm, to) in ALLOWED_TRANSITIONS


@pytest.mark.parametrize(("frm", "to"), PHASE2_FORBIDDEN)
def test_phase2_transition_forbidden(frm: str, to: str) -> None:
    assert (frm, to) not in ALLOWED_TRANSITIONS


def test_manual_transitions_match_srs() -> None:
    """01 §7.1 "Điều chỉnh tay": đúng 8 cặp, không có đích RETURN_RECEIVED_*, đều nằm trong ALLOWED."""
    pairs = {(f, t) for f, targets in MANUAL_TRANSITIONS.items() for t in targets}
    assert pairs == {
        ("NEW", "HANDED_OVER"),
        ("PACKED", "HANDED_OVER"),
        ("CANCELLED_AFTER_PACK", "HANDED_OVER"),
        ("HANDED_OVER", "DELIVERED"),
        ("RETURN_MISSING", "RETURN_EXPECTED"),
        ("RETURN_EXPECTED", "DELIVERED"),
        ("RETURN_MISSING", "DELIVERED"),
    }
    assert pairs <= ALLOWED_TRANSITIONS


async def test_transition_sets_status_changed_at() -> None:
    from datetime import UTC, datetime

    from aicam.core import clock

    clock.freeze(datetime(2026, 10, 6, 3, 0, tzinfo=UTC))
    package = SimpleNamespace(id="p", warehouse_status="HANDED_OVER", status_changed_at=None)
    await transition(_Session(), package, "RETURN_EXPECTED", source="PLATFORM")  # type: ignore[arg-type]

    assert package.status_changed_at == datetime(2026, 10, 6, 3, 0, tzinfo=UTC)
