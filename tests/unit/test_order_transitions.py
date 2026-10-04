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
