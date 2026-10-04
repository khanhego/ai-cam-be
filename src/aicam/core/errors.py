"""Lỗi ứng dụng và format lỗi chung (02 §6 Quy ước chung)."""

from typing import Any

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

log = structlog.get_logger()


class AppError(Exception):
    """Lỗi nghiệp vụ / kỹ thuật có mã ổn định để FE xử lý theo `code`."""

    def __init__(
        self,
        code: str,
        message: str,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        self.headers = headers


def error_body(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details or {}}}


_HTTP_CODES = {
    401: ("UNAUTHENTICATED", "Phiên đăng nhập đã hết hạn. Đăng nhập lại."),
    403: ("FORBIDDEN", "Tài khoản không có quyền thực hiện thao tác này."),
    404: ("NOT_FOUND", "Không tìm thấy dữ liệu."),
    405: ("METHOD_NOT_ALLOWED", "Thao tác không được hỗ trợ."),
    429: ("RATE_LIMITED", "Thao tác quá nhanh, thử lại sau."),
}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            error_body(exc.code, exc.message, exc.details),
            status_code=exc.status_code,
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        fields: dict[str, str] = {}
        for err in exc.errors():
            loc = [str(p) for p in err.get("loc", ()) if p not in ("body", "query", "path")]
            fields[".".join(loc) or "_"] = str(err.get("msg", "Không hợp lệ"))
        return JSONResponse(
            error_body("VALIDATION_ERROR", "Dữ liệu không hợp lệ.", {"fields": fields}),
            status_code=422,
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code, message = _HTTP_CODES.get(exc.status_code, ("HTTP_ERROR", str(exc.detail)))
        return JSONResponse(error_body(code, message), status_code=exc.status_code, headers=exc.headers)

    @app.exception_handler(Exception)
    async def _unhandled(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled_error", error=str(exc))
        return JSONResponse(
            error_body("INTERNAL", "Có lỗi hệ thống. Thử lại sau ít phút."),
            status_code=500,
        )
