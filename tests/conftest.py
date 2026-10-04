from collections.abc import Iterator

import pytest

from aicam.core import clock


@pytest.fixture(autouse=True)
def _reset_clock() -> Iterator[None]:
    clock.reset()
    yield
    clock.reset()
