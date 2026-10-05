"""G3-N2: chặn body quá lớn trước khi Starlette spool multipart ra đĩa."""

from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI, UploadFile
from httpx import ASGITransport, AsyncClient

from aicam.core.errors import install_error_handlers
from aicam.core.limits import DEFAULT_MAX_BYTES, IMPORT_MAX_BYTES, BodySizeLimitMiddleware

reached: list[str] = []


def _app() -> FastAPI:
    app = FastAPI()
    install_error_handlers(app)
    app.add_middleware(BodySizeLimitMiddleware)

    @app.post("/api/v1/imports")
    async def upload(file: UploadFile) -> dict[str, int]:
        reached.append("imports")
        return {"size": len(await file.read())}

    @app.post("/api/v1/other")
    async def other(body: dict[str, str]) -> dict[str, int]:
        reached.append("other")
        return {"n": len(body)}

    return app


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    reached.clear()
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as c:
        yield c


async def test_declared_too_large_rejected_before_reading(client: AsyncClient) -> None:
    res = await client.post(
        "/api/v1/imports", content=b"x" * 10, headers={"content-length": str(IMPORT_MAX_BYTES + 1)}
    )
    assert res.status_code == 413
    assert res.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"
    assert reached == []


async def test_upload_within_limit_passes(client: AsyncClient) -> None:
    res = await client.post("/api/v1/imports", files={"file": ("a.csv", b"x" * 1024, "text/csv")})
    assert (res.status_code, res.json()) == (200, {"size": 1024})


async def test_streamed_body_over_limit_without_length(client: AsyncClient) -> None:
    """Client không khai Content-Length (chunked) mà gửi quá giới hạn → 413 (đếm byte thật khi đọc)."""

    async def chunks() -> AsyncIterator[bytes]:
        yield (
            b'--zz\r\nContent-Disposition: form-data; name="file"; filename="a.csv"\r\n'
            b"Content-Type: text/csv\r\n\r\n"
        )
        for _ in range(8):
            yield b"y" * (1024 * 1024)
        yield b"\r\n--zz--\r\n"

    res = await client.post(
        "/api/v1/imports", content=chunks(), headers={"content-type": "multipart/form-data; boundary=zz"}
    )
    assert res.status_code == 413
    assert reached == []


async def test_other_api_limit_is_1mb(client: AsyncClient) -> None:
    big = {"k": "z" * (DEFAULT_MAX_BYTES + 10)}
    res = await client.post("/api/v1/other", json=big)
    assert res.status_code == 413
    assert (await client.post("/api/v1/other", json={"k": "v"})).status_code == 200
