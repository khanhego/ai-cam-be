from collections.abc import Iterator
from typing import Any

import pytest

from aicam.core import clock
from aicam.modules.media import jobs


@pytest.fixture(autouse=True)
def _reset_clock() -> Iterator[None]:
    clock.reset()
    yield
    clock.reset()


@pytest.fixture(autouse=True)
def sent_jobs() -> Iterator[list[tuple[str, list[Any], str, float]]]:
    """Job Celery được đẩy trong test (không gửi lên broker thật của stack dev)."""
    sent: list[tuple[str, list[Any], str, float]] = []
    jobs.set_sender(lambda task, args, queue, countdown: sent.append((task, args, queue, countdown)))
    yield sent
    jobs.set_sender(None)
