"""Worker Celery: `worker` (-Q default,video), `worker-sync` (-Q sync), `worker-export` (-Q export)."""

from aicam.workers.celery_app import app

__all__ = ["app"]
