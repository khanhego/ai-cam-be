"""Tiến trình `beat`: `celery -A aicam.entrypoints.beat beat`."""

from aicam.workers.celery_app import app

__all__ = ["app"]
