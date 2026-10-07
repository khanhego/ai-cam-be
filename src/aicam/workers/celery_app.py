"""Celery app + lịch beat (02a §7). Queue: default, video, export, sync_fast, sync, backup, notify (DEC-46,
DEC-503, DEC-504 — T-276)."""

import logging
from typing import Any

from celery import Celery
from celery.schedules import crontab
from celery.signals import after_setup_logger, after_setup_task_logger, beat_init, worker_init

from aicam.core import schema_guard
from aicam.core.logging import install_stdlib_redaction
from aicam.core.settings import get_settings

settings = get_settings()

app = Celery("aicam", broker=settings.redis_url, backend=None, include=["aicam.workers.tasks"])
app.conf.update(
    # Mọi queue (gồm `sync*` — 02a §7): task dài không giữ task khác trong bộ đệm; worker chết → task về hàng.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_queue="default",
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "media.build_session_clips": {"queue": "video"},
        "media.render_export": {"queue": "export"},
        "media.capture_pack_snapshot": {"queue": "video"},  # J-17
        "claims.build_evidence_pack": {"queue": "export"},  # J-16 — cùng worker encode J-03
        # Sàn (02a §7, DEC-503): J-04 / J-05 / J-12 ngắn → `sync_fast` (worker-sync -c 3); J-06 / J-13 dài
        # (300 giây / shop) → `sync` (worker-sync-long -c 2) — NFR-39.
        "platforms.sync_orders": {"queue": "sync_fast"},
        "platforms.sync_shop_orders": {"queue": "sync_fast"},
        "platforms.verify_unverified": {"queue": "sync_fast"},
        "platforms.refresh_tokens": {"queue": "sync_fast"},
        "platforms.sync_shipping_status": {"queue": "sync"},
        "platforms.sync_shop_shipping": {"queue": "sync"},
        "platforms.sync_returns": {"queue": "sync"},
        "platforms.sync_shop_returns": {"queue": "sync"},
        "platforms.*": {"queue": "sync"},  # task sàn khác (nếu thêm) — không lẫn vào cắt clip
        "notify.*": {"queue": "notify"},  # J-26..J-28 (worker-notify -c 1 — DEC-504)
        "backup.*": {"queue": "backup"},  # J-20..J-23 (worker-backup -c 1 — 02a §7, DEC-434)
        "shares.build": {"queue": "export"},  # J-24 — cùng worker encode J-03 / J-16 (02a §7)
        "shares.cleanup": {"queue": "default"},  # J-25
    },
    beat_schedule={
        "j07-session-timeouts": {"task": "sessions.check_timeouts", "schedule": 30.0},
        "j09-check-clock-drift": {"task": "stations.check_clock_drift", "schedule": 600.0},
        "j10-index-segments": {"task": "media.index_segments", "schedule": 60.0},
        "j11-housekeeping": {"task": "maintenance.housekeeping", "schedule": 300.0},
        # Shopee (02a §7, ADR-007 polling). Không làm gì khi SHOPEE_ENABLED=false.
        "j04-sync-orders": {"task": "platforms.sync_orders", "schedule": 300.0},
        "j05-verify-unverified": {"task": "platforms.verify_unverified", "schedule": 600.0},
        "j06-sync-shipping-status": {"task": "platforms.sync_shipping_status", "schedule": 900.0},
        "j13-sync-returns": {"task": "platforms.sync_returns", "schedule": 900.0},  # NFR-35 ≤ 15 phút
        "j12-refresh-tokens": {"task": "platforms.refresh_tokens", "schedule": 1800.0},
        "j14-recon-rules": {"task": "reconciliation.run_rules", "schedule": 1800.0},
        "j15-claim-deadlines": {"task": "claims.check_deadlines", "schedule": 3600.0},
        # 02:00 giờ VN (UTC+7, không đổi giờ mùa hè) = 19:00 UTC.
        "j02-enforce-retention": {"task": "media.enforce_retention", "schedule": crontab(hour=19, minute=0)},
        # Sao lưu cloud (02a §7): J-20 01, 07, 13, 19 giờ VN = 18, 0, 6, 12 UTC (RPO DB ≤ 6 giờ — NFR-40).
        "j20-backup-db": {"task": "backup.run_db", "schedule": crontab(hour="0,6,12,18", minute=0)},
        "j21-backup-enqueue": {"task": "backup.enqueue_evidence", "schedule": 600.0},
        "j22-backup-upload": {"task": "backup.upload_evidence", "schedule": 300.0},  # RPO bằng chứng ≤ 1 giờ
        # 03:00 giờ VN = 20:00 UTC — sau J-02 (02:00) để xóa bản cloud ≤ 24 giờ sau retention (FR-02.14).
        "j23-backup-prune": {"task": "backup.prune", "schedule": crontab(hour=20, minute=0)},
        # Link chia sẻ: hết hạn (≤ 1 giờ sau hạn — BR-34), thu hồi khi kho mất mạng thử lại mỗi phút (EX-S7).
        "j25-share-cleanup": {"task": "shares.cleanup", "schedule": 60.0},
        # Thông báo (02a §7, NFR-43: 30 giây + gom 2 phút + 15 giây ≈ ≤ 2,9 phút).
        "j26-notify-scan": {"task": "notify.scan", "schedule": 30.0},
        "j27-notify-dispatch": {"task": "notify.dispatch", "schedule": 15.0},
        # 18:00 giờ VN = 11:00 UTC (FR-06.11).
        "j28-notify-daily-summary": {"task": "notify.daily_summary", "schedule": crontab(hour=11, minute=0)},
    },
)


def _redact_logs(logger: logging.Logger | None = None, **_: Any) -> None:
    """Worker / beat: httpx / httpcore lên WARNING + che query (token / sign Shopee) — G3-N1."""
    install_stdlib_redaction(*([logger] if logger is not None else []))


after_setup_logger.connect(_redact_logs, weak=False)
after_setup_task_logger.connect(_redact_logs, weak=False)


def _check_schema(sender: Any = None, **_: Any) -> None:
    """G3 M-F1 (DEC-336): worker / beat thoát khi schema DB lệch head của image (staging / production)."""
    component = "beat" if sender is not None and type(sender).__name__ == "Service" else "worker"
    schema_guard.enforce_blocking(get_settings(), component)


worker_init.connect(_check_schema, weak=False)
beat_init.connect(_check_schema, weak=False)
