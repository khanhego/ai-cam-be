from fastapi.testclient import TestClient

from aicam import __version__
from aicam.core.settings import Settings
from aicam.main import create_app


def test_healthz_returns_ok() -> None:
    client = TestClient(create_app(Settings(log_json=False)))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}


def test_openapi_is_versioned() -> None:
    client = TestClient(create_app(Settings(log_json=False)))

    response = client.get("/api/v1/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "Hệ thống X API"
