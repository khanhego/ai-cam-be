"""Celery app + lịch beat (02a §7). Queue: default, video, export, sync (DEC-46)."""

from celery import Celery
from celery.schedules import crontab

from aicam.core.settings import get_settings

settings = get_settings()

app = Celery("aicam", broker=settings.redis_url, backend=None, include=["aicam.workers.tasks"])
app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_default_queue="default",
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "media.build_session_clips": {"queue": "video"},
        "media.render_export": {"queue": "export"},
    },
    beat_schedule={
        "j07-session-timeouts": {"task": "sessions.check_timeouts", "schedule": 30.0},
        "j09-check-clock-drift": {"task": "stations.check_clock_drift", "schedule": 600.0},
        "j10-index-segments": {"task": "media.index_segments", "schedule": 60.0},
        "j11-housekeeping": {"task": "maintenance.housekeeping", "schedule": 300.0},
        # 02:00 giờ VN (UTC+7, không đổi giờ mùa hè) = 19:00 UTC.
        "j02-enforce-retention": {"task": "media.enforce_retention", "schedule": crontab(hour=19, minute=0)},
    },
)
