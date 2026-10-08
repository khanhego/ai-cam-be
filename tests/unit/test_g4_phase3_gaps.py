"""G4 item 03 — test unit bổ sung cho case `04-test-cases.md` chưa có bằng chứng (chỉ thêm test).

- TC-ST3.03: `backup.state` ưu tiên NOT_CONFIGURED > RESTORE_PENDING > KEY_UNCONFIRMED > KEY_CHANGED >
  DISABLED > ON.
- TC-05.67 / NFR-38 (phần lịch): J-04 `platforms.sync_orders` chạy mỗi ≤ 300 giây ở queue `sync_fast`.
"""

import base64
from fnmatch import fnmatch
from types import SimpleNamespace
from typing import Any

import pytest

from aicam.core.settings import Settings
from aicam.modules.backup import service as backup
from aicam.modules.cloud import crypto

K1 = base64.b64encode(b"1" * 32).decode()
K2 = base64.b64encode(b"2" * 32).decode()
S3 = {
    "s3_endpoint": "http://localhost:59000",
    "s3_access_key_id": "ak",
    "s3_secret_access_key": "sk",
    "s3_bucket": "aicam-test-backup",
    "s3_share_bucket": "aicam-test-share",
}
FP1 = crypto.fingerprint(crypto.parse_key(K1))
FP2 = crypto.fingerprint(crypto.parse_key(K2))


def _cfg(*, pending: bool = False, confirmed: str | None = None, enabled: bool = False) -> Any:
    return SimpleNamespace(
        backup_restore_pending=pending, backup_confirmed_fingerprint=confirmed, backup_enabled=enabled
    )


@pytest.mark.parametrize(
    ("s3", "key", "cfg", "expected"),
    [
        # thiếu S3_* + RESTORE_PENDING → NOT_CONFIGURED thắng
        (False, K1, _cfg(pending=True, confirmed=FP1, enabled=True), "NOT_CONFIGURED"),
        # thiếu khóa sao lưu (đủ S3) → NOT_CONFIGURED
        (True, "", _cfg(confirmed=FP1, enabled=True), "NOT_CONFIGURED"),
        # RESTORE_PENDING + chưa xác nhận khóa → RESTORE_PENDING
        (True, K1, _cfg(pending=True, confirmed=None, enabled=False), "RESTORE_PENDING"),
        # RESTORE_PENDING + khóa đổi + đang bật → RESTORE_PENDING
        (True, K2, _cfg(pending=True, confirmed=FP1, enabled=True), "RESTORE_PENDING"),
        # chưa xác nhận + đang bật → KEY_UNCONFIRMED
        (True, K1, _cfg(confirmed=None, enabled=True), "KEY_UNCONFIRMED"),
        # khóa đổi + tắt → KEY_CHANGED (trước DISABLED)
        (True, K2, _cfg(confirmed=FP1, enabled=False), "KEY_CHANGED"),
        # khóa khớp + tắt → DISABLED
        (True, K1, _cfg(confirmed=FP1, enabled=False), "DISABLED"),
        # đủ hết → ON
        (True, K1, _cfg(confirmed=FP1, enabled=True), "ON"),
        (True, K2, _cfg(confirmed=FP2, enabled=True), "ON"),
    ],
)
def test_tc_st3_03_backup_state_priority(s3: bool, key: str, cfg: Any, expected: str) -> None:
    """TC-ST3.03 (02 §5.2, API-180): tổ hợp trạng thái → đúng thứ tự ưu tiên; chỉ `ON` là trạng thái chạy
    J-20..J-23 (các job kiểm `state == ON` — `test_backup_jobs` / `test_backup_db_job`)."""
    settings = Settings(app_env="test", backup_encryption_key=key, **(S3 if s3 else {}))  # type: ignore[arg-type]
    assert backup.state(cfg, settings) == expected


def test_tc_05_67_j04_beat_within_5_minutes() -> None:
    """TC-05.67 / NFR-38 (phần lịch): beat J-04 `platforms.sync_orders` ≤ 300 giây → đơn mới của mọi shop
    (cả TikTok — fan-out một task / shop, `test_fanout_isolation`) vào hệ thống trong ≤ 5 phút; task chạy ở
    queue `sync_fast`, giới hạn thời gian < chu kỳ (lượt trước xong trước lượt sau)."""
    from aicam.workers import tasks  # noqa: F401 — đăng ký task
    from aicam.workers.celery_app import app

    entries = [e for e in app.conf.beat_schedule.values() if e["task"] == "platforms.sync_orders"]
    assert len(entries) == 1
    schedule = entries[0]["schedule"]
    assert isinstance(schedule, float)
    assert 0 < schedule <= 300.0
    for name in ("platforms.sync_orders", "platforms.sync_shop_orders"):
        task = app.tasks[name]
        assert task.time_limit is not None
        assert task.time_limit < schedule
        queues = {r["queue"] for pattern, r in app.conf.task_routes.items() if fnmatch(name, pattern)}
        assert "sync_fast" in queues
