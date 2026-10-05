"""Task Celery J-01: chờ settle không tiêu lượt thử (G3)."""

import uuid
from typing import Any

import pytest

from aicam.modules.media.service import BuildResult
from aicam.workers import tasks


@pytest.mark.parametrize("retries", [0, 3])
def test_settle_wait_reschedules_without_consuming_retry(
    monkeypatch: pytest.MonkeyPatch, retries: int
) -> None:
    sent: list[dict[str, Any]] = []
    task = tasks.build_session_clips
    monkeypatch.setattr(tasks, "_run", lambda _job: BuildResult(retry_in=4.0, waiting=True))
    monkeypatch.setattr(task, "apply_async", lambda **kw: sent.append(kw))
    task.push_request(retries=retries)
    try:
        out = task.run(str(uuid.uuid4()))
    finally:
        task.pop_request()

    assert out["waiting_s"] == 4.0
    assert len(sent) == 1
    assert (sent[0]["countdown"], sent[0]["retries"]) == (4.0, retries)


def test_missing_video_still_consumes_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    task = tasks.build_session_clips
    monkeypatch.setattr(tasks, "_run", lambda _job: BuildResult(retry_in=10.0))

    class _Retried(Exception):
        pass

    def _retry(**kw: Any) -> Exception:
        return _Retried(kw)

    monkeypatch.setattr(task, "retry", _retry)
    task.push_request(retries=1)
    try:
        with pytest.raises(_Retried):
            task.run(str(uuid.uuid4()))
    finally:
        task.pop_request()
