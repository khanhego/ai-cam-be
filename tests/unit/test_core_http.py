"""Format lỗi chung (02 §6) và phân quyền theo role."""

import uuid
from typing import Annotated

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field

from aicam.core.deps import Principal, require_roles
from aicam.core.errors import AppError, install_error_handlers
from aicam.core.security import encode_access_token
from aicam.core.settings import Settings, get_settings

SETTINGS = Settings(log_json=False)


class Body(BaseModel):
    name: str = Field(min_length=1, max_length=40)


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    install_error_handlers(app)
    app.dependency_overrides[get_settings] = lambda: SETTINGS

    @app.get("/app-error")
    async def app_error() -> None:
        raise AppError("NAME_TAKEN", "Tên station đã tồn tại.", 409, {"field": "name"})

    @app.post("/validate")
    async def validate(body: Body) -> Body:
        return body

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("bất ngờ")

    @app.get("/admin-only")
    async def admin_only(
        p: Annotated[Principal, Depends(require_roles("ADMIN", "SUPERVISOR"))],
    ) -> dict[str, str]:
        return {"role": p.role}

    return TestClient(app, raise_server_exceptions=False)


def _token(role: str) -> str:
    token, _ = encode_access_token(SETTINGS.jwt_secret, uuid.uuid4(), role, None, minutes=15)
    return token


def test_app_error_uses_common_format(client: TestClient) -> None:
    response = client.get("/app-error")

    assert response.status_code == 409
    assert response.json() == {
        "error": {"code": "NAME_TAKEN", "message": "Tên station đã tồn tại.", "details": {"field": "name"}}
    }


def test_validation_error_lists_fields(client: TestClient) -> None:
    response = client.post("/validate", json={"name": ""})

    assert response.status_code == 422
    body = response.json()["error"]
    assert body["code"] == "VALIDATION_ERROR"
    assert "name" in body["details"]["fields"]


def test_unknown_route_is_not_found(client: TestClient) -> None:
    response = client.get("/khong-co")

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


def test_unhandled_error_is_internal_without_leaking(client: TestClient) -> None:
    response = client.get("/boom")

    assert response.status_code == 500
    assert response.json()["error"] == {
        "code": "INTERNAL",
        "message": "Có lỗi hệ thống. Thử lại sau ít phút.",
        "details": {},
    }


def test_missing_token_is_unauthenticated(client: TestClient) -> None:
    response = client.get("/admin-only")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.parametrize(
    ("role", "status"), [("ADMIN", 200), ("SUPERVISOR", 200), ("CSKH", 403), ("STATION", 403)]
)
def test_require_roles(client: TestClient, role: str, status: int) -> None:
    response = client.get("/admin-only", headers={"Authorization": f"Bearer {_token(role)}"})

    assert response.status_code == status
    if status == 403:
        assert response.json()["error"]["code"] == "FORBIDDEN"


def test_require_roles_rejects_unknown_role() -> None:
    with pytest.raises(ValueError, match="Role không tồn tại"):
        require_roles("PACKER")
