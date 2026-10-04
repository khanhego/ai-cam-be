"""Tiến trình `worker`: `celery -A aicam.entrypoints.worker worker -Q default,video,export,sync`."""

from aicam.workers.celery_app import app

__all__ = ["app"]
