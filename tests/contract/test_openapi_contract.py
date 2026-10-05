"""Contract test (02a §11, NFR/02 §6): `/openapi.json` sinh từ FastAPI phải phủ hợp đồng 02 §6.

Không cần DB: dựng app, đọc schema OpenAPI, so với bảng `spec.CONTRACT`.
Snapshot `openapi.json` ở gốc repo cho FE sinh client — lệch → chạy `uv run python scripts/export_openapi.py`.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from aicam.main import create_app

from .spec import CONTRACT, Api

ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT = ROOT / "openapi.json"
PREFIX = "/api/v1"


@pytest.fixture(scope="module")
def openapi() -> dict[str, Any]:
    return create_app().openapi()


def _resolve(spec: dict[str, Any], schema: dict[str, Any]) -> list[dict[str, Any]]:
    """Trả mọi nhánh object của một schema (bỏ `null`, mở `$ref`, `anyOf` / `oneOf` / `allOf`)."""
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        return _resolve(spec, spec["components"]["schemas"][name])
    branches: list[dict[str, Any]] = []
    for key in ("anyOf", "oneOf", "allOf"):
        for sub in schema.get(key, []):
            branches.extend(_resolve(spec, sub))
    if branches:
        return branches
    if schema.get("type") == "null":
        return []
    return [schema]


def _lookup(spec: dict[str, Any], schema: dict[str, Any], path: str) -> list[dict[str, Any]]:
    """Đi theo đường dẫn chấm (`a.b[].c`); trả các schema lá tìm được (rỗng = thiếu trường)."""
    current = _resolve(spec, schema)
    for part in path.split("."):
        is_array = part.endswith("[]")
        name = part.removesuffix("[]")
        nxt: list[dict[str, Any]] = []
        for node in current:
            prop = node.get("properties", {}).get(name)
            if prop is None:
                continue
            for resolved in _resolve(spec, prop):
                if is_array:
                    if resolved.get("type") == "array":
                        nxt.extend(_resolve(spec, resolved.get("items", {})))
                else:
                    nxt.append(resolved)
        current = nxt
    return current


def _response_schema(spec: dict[str, Any], api: Api) -> dict[str, Any]:
    op = spec["paths"][PREFIX + api.path][api.method.lower()]
    content = op["responses"][str(api.status)].get("content", {})
    return content.get("application/json", {}).get("schema", {})


def _enum_values(leaves: list[dict[str, Any]]) -> set[str]:
    values: set[str] = set()
    for leaf in leaves:
        values.update(leaf.get("enum", []))
        if "const" in leaf:
            values.add(leaf["const"])
    return values


@pytest.mark.parametrize("api", CONTRACT, ids=lambda a: a.id)
def test_path_method_and_status(openapi: dict[str, Any], api: Api) -> None:
    ops = openapi["paths"].get(PREFIX + api.path)
    assert ops is not None, f"{api.id}: thiếu path {api.path}"
    assert api.method.lower() in ops, f"{api.id}: thiếu method {api.method}"
    assert str(api.status) in ops[api.method.lower()]["responses"], f"{api.id}: thiếu response {api.status}"


@pytest.mark.parametrize("api", [a for a in CONTRACT if a.fields], ids=lambda a: a.id)
def test_response_fields(openapi: dict[str, Any], api: Api) -> None:
    schema = _response_schema(openapi, api)
    missing = [f for f in api.fields if not _lookup(openapi, schema, f)]
    assert not missing, f"{api.id}: response thiếu trường {missing}"


@pytest.mark.parametrize("api", [a for a in CONTRACT if a.enums], ids=lambda a: a.id)
def test_response_enums(openapi: dict[str, Any], api: Api) -> None:
    schema = _response_schema(openapi, api)
    for path, expected in api.enums.items():
        actual = _enum_values(_lookup(openapi, schema, path))
        assert actual, f"{api.id}: {path} không khai báo enum"
        assert expected <= actual, f"{api.id}: {path} thiếu giá trị {sorted(expected - actual)}"


@pytest.mark.parametrize("api", [a for a in CONTRACT if a.request_fields], ids=lambda a: a.id)
def test_request_fields(openapi: dict[str, Any], api: Api) -> None:
    op = openapi["paths"][PREFIX + api.path][api.method.lower()]
    schema = op["requestBody"]["content"]["application/json"]["schema"]
    missing = [f for f in api.request_fields if not _lookup(openapi, schema, f)]
    assert not missing, f"{api.id}: request thiếu trường {missing}"


def test_no_undocumented_api(openapi: dict[str, Any]) -> None:
    """Mọi route `/api/v1` phải có API-xx trong 02 §6 (thêm API mới → architect cập nhật 02 trước)."""
    known = {(a.method.lower(), PREFIX + a.path) for a in CONTRACT}
    extra = [
        f"{method.upper()} {path}"
        for path, ops in openapi["paths"].items()
        if path.startswith(PREFIX)
        for method in ops
        if (method, path) not in known
    ]
    assert not extra, f"API không có trong 02 §6: {extra}"


def test_datetime_fields_are_date_time(openapi: dict[str, Any]) -> None:
    """Trường `*_at` là chuỗi `date-time` (02 §6: ISO-8601 UTC có `Z`; định dạng `Z` kiểm ở test runtime)."""
    wrong: list[str] = []
    for name, schema in openapi["components"]["schemas"].items():
        for prop, sub in schema.get("properties", {}).items():
            if not prop.endswith("_at") and prop not in ("at", "retention_until", "until"):
                continue
            formats = {b.get("format") for b in _resolve(openapi, sub)}
            if formats - {"date-time"}:
                wrong.append(f"{name}.{prop}: {formats}")
    assert not wrong, wrong


def test_openapi_snapshot_up_to_date(openapi: dict[str, Any]) -> None:
    generated = json.dumps(openapi, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    assert SNAPSHOT.exists(), "Thiếu openapi.json — chạy `uv run python scripts/export_openapi.py`"
    assert SNAPSHOT.read_text(encoding="utf-8") == generated, (
        "openapi.json lệch code — chạy `uv run python scripts/export_openapi.py` rồi commit"
    )
