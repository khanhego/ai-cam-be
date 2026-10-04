"""Tiến trình `api`: `uvicorn aicam.entrypoints.api:app`."""

from aicam.main import create_app

app = create_app()
