from fastapi.testclient import TestClient

from aicam import __version__
from aicam.core.settings import Settings
from aicam.main import create_app


def test_healthz_returns_ok() -> None:
    client = TestClient(create_app(Settings(log_json=False)))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "version": __version__}
    assert __version__ == "0.3.0"  # Phase 3 (item 03) — DEC-1003


def test_version_matches_pyproject() -> None:
    import tomllib
    from pathlib import Path

    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))
    assert pyproject["project"]["version"] == __version__


def test_openapi_is_versioned() -> None:
    client = TestClient(create_app(Settings(log_json=False)))

    response = client.get("/api/v1/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "Hệ thống X API"
