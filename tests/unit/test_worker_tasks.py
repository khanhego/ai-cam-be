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
    queues = {r["queue"] for pattern, r in app.conf.task_routes.items() if fnmatch(task.name, pattern)}
    assert queues == {"sync"}  # tên chính xác + mẫu `platforms.*` cùng trỏ queue `sync` (T-276)


def test_j20_backup_db_schedule_route_and_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """02a §7 J-20: 01 / 07 / 13 / 19 giờ VN (= 18, 0, 6, 12 UTC), queue `backup`; lượt FAILED → thử lại 10
    phút
    (tối đa 2), lượt bỏ qua do trạng thái không thử lại."""
    from aicam.workers.celery_app import app

    (entry,) = [e for e in app.conf.beat_schedule.values() if e["task"] == "backup.run_db"]
    assert entry["schedule"].hour == {0, 6, 12, 18}
    assert entry["schedule"].minute == {0}
    assert app.conf.task_routes["backup.*"] == {"queue": "backup"}

    task = tasks.backup_run_db
    calls: list[dict[str, Any]] = []

    class _Retried(Exception):
        pass

    def _retry(**kw: Any) -> Exception:
        calls.append(kw)
        return _Retried()

    monkeypatch.setattr(task, "retry", _retry)
    monkeypatch.setattr(tasks, "_run", lambda _job: {"status": "FAILED", "error": "CLOUD_UNREACHABLE"})
    task.push_request(retries=0)
    try:
        with pytest.raises(_Retried):
            task.run()
    finally:
        task.pop_request()
    assert calls[0]["countdown"] == 600
    monkeypatch.setattr(tasks, "_run", lambda _job: {"status": "FAILED", "state": "DISABLED"})
    task.push_request(retries=0)
    try:
        assert task.run()["state"] == "DISABLED"
    finally:
        task.pop_request()


def test_j21_j22_j23_schedule() -> None:
    from aicam.workers.celery_app import app

    by_task = {e["task"]: e["schedule"] for e in app.conf.beat_schedule.values()}
    assert by_task["backup.enqueue_evidence"] == 600.0
    assert by_task["backup.upload_evidence"] == 300.0  # + ngân sách 240 giây → bằng chứng lên ≤ 1 giờ
    assert by_task["backup.prune"].hour == {20}  # 03:00 VN, sau J-02 (19 UTC)
    for name in ("backup.enqueue_evidence", "backup.upload_evidence", "backup.prune"):
        assert name in app.tasks


def test_j22_beat_message_expires_g3_bk8() -> None:
    """G3-BK-8: tin lịch J-22 hết hạn sau 300 giây (worker-backup -c 1 bận lượt dài → không dồn hàng chờ)."""
    from aicam.workers.celery_app import app

    (entry,) = [e for e in app.conf.beat_schedule.values() if e["task"] == "backup.upload_evidence"]
    assert entry["options"]["expires"] == 300
