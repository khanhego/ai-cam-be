"""Task Celery J-01: chờ settle không tiêu lượt thử (G3); lịch beat J-13 (TC-N2.07)."""

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


def test_tc_n2_07_j13_sync_returns_beat_within_15_minutes() -> None:
    """TC-N2.07, NFR-35: lịch beat J-13 (`platforms.sync_returns`) ≤ 900 giây → yêu cầu trả mới trên sàn thành
    kiện `RETURN_EXPECTED` trong ≤ 15 phút; task đã đăng ký, chạy ở queue `sync`, giới hạn thời gian < chu kỳ
    (lượt trước xong trước khi lượt sau tới)."""
    from fnmatch import fnmatch

    from aicam.workers.celery_app import app

    entries = [e for e in app.conf.beat_schedule.values() if e["task"] == "platforms.sync_returns"]
    assert len(entries) == 1
    schedule = entries[0]["schedule"]
    assert isinstance(schedule, float)
    assert 0 < schedule <= 900.0
    task = app.tasks["platforms.sync_returns"]
    assert task.name == tasks.sync_returns.name
    assert task.time_limit is not None
    assert task.time_limit < schedule
    queues = [r["queue"] for pattern, r in app.conf.task_routes.items() if fnmatch(task.name, pattern)]
    assert queues == ["sync"]
