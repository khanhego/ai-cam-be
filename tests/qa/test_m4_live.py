"""QA phạm vi M4 (nguồn đơn) trên stack dev thật: nhập CSV qua HTTP thật; Shopee bằng adapter mock.

Chạy: `QA_BASE_URL=http://localhost:8180 uv run pytest tests/qa/test_m4_live.py -v`
Bắt đầu bằng `scripts/qa-reset.sh` (fixture module). Không chạy song song với E2E FE (dùng chung DB).
Phần Shopee **chưa test với Shopee thật — thiếu tài khoản partner (T-3)**.
Mỗi test ghi mã TC trong docstring.
"""

import hashlib
import os
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

BASE = os.environ.get("QA_BASE_URL")
pytestmark = [
    pytest.mark.qa,
    pytest.mark.skipif(not BASE, reason="đặt QA_BASE_URL để chạy QA trên stack thật"),
]

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "csv"
PASSWORD = "matkhau123"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker/compose.dev.yml")]


def _psql(sql: str) -> str:
    out = subprocess.run(  # noqa: S603
        [*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "aicam", "-d", "aicam", "-tA", "-c", sql],
        capture_output=True,
        text=True,
        check=False,
    )
    return (out.stdout + out.stderr).strip()


@pytest.fixture(scope="module", autouse=True)
def _reset() -> None:
    subprocess.run([str(ROOT / "scripts/qa-reset.sh")], check=True, capture_output=True)  # noqa: S603


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{BASE}/api/v1", timeout=60) as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, str]:
    out = {}
    for role, username in [("ADMIN", "tst_admin"), ("SUPERVISOR", "tst_sup"), ("CSKH", "tst_cskh")]:
        res = client.post(
            "/auth/login", json={"username": username, "password": PASSWORD, "client": "DASHBOARD"}
        )
        assert res.status_code == 200, res.text
        out[role] = res.json()["access_token"]
    return out


def _h(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _upload(client: httpx.Client, token: str, name: str) -> httpx.Response:
    content = (FIXTURES / name).read_bytes()
    return client.post("/imports", headers=_h(token), files={"file": (name, content, "text/csv")})


# ---------------------------------------------------------------- CSV (T-17)


def test_csv_ok_500(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-05.11 (phần API) + TC-05.18: 500 đơn ≤ 30 giây; API-30 thấy nguồn CSV; file gốc trùng SHA-256."""
    started = time.monotonic()
    res = _upload(client, tokens["SUPERVISOR"], "ok_500.csv")
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["counts"] == {"new": 500, "updated": 0, "skipped": 0, "error": 0}
    assert len(body["sample"]) == 20
    res = client.post(f"/imports/{body['id']}/commit", headers=_h(tokens["SUPERVISOR"]))
    elapsed = time.monotonic() - started
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "COMMITTED"
    assert elapsed <= 30, elapsed
    print(f"\nTC-05.11 tải + nhập 500 đơn: {elapsed:.2f} giây")

    found = client.get("/packages", headers=_h(tokens["CSKH"]), params={"q": "SPXCSV0000250"}).json()
    assert found["total"] == 1
    assert found["items"][0]["source"] == "CSV"

    hist = client.get("/imports", headers=_h(tokens["SUPERVISOR"])).json()
    assert hist["items"][0]["id"] == body["id"]
    assert hist["items"][0]["created_by"]["display_name"]

    res = client.get(f"/imports/{body['id']}/file", headers=_h(tokens["SUPERVISOR"]))
    assert res.status_code == 200
    expected = hashlib.sha256((FIXTURES / "ok_500.csv").read_bytes()).hexdigest()
    assert hashlib.sha256(res.content).hexdigest() == expected
    # File gốc lưu trong volume imports, đường dẫn tương đối (DEC-105).
    rel = _psql(f"SELECT file_path FROM csv_import WHERE id = '{body['id']}'")  # noqa: S608 — id là UUID
    assert not rel.startswith("/"), rel


def test_csv_one_error(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-05.12: dòng 12 bỏ trống mã vận đơn → không nhập dòng nào, commit 409 IMPORT_HAS_ERRORS."""
    res = _upload(client, tokens["SUPERVISOR"], "one_error.csv")
    body = res.json()
    assert body["errors"] == [{"row": 12, "column": "tracking_number", "message": "Bỏ trống"}]
    res = client.post(f"/imports/{body['id']}/commit", headers=_h(tokens["SUPERVISOR"]))
    assert (res.status_code, res.json()["error"]["code"]) == (409, "IMPORT_HAS_ERRORS")
    assert _psql("SELECT count(*) FROM \"order\" WHERE platform_order_sn LIKE '2410CSE%'") == "0"


def test_csv_missing_column(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-05.13: thiếu cột → 422 FILE_INVALID, `details.missing_columns`."""
    res = _upload(client, tokens["SUPERVISOR"], "missing_column.csv")
    assert res.status_code == 422
    assert res.json()["error"]["details"]["missing_columns"] == ["tracking_number"]


def test_csv_too_big(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-05.14: file 5,1 MB → 422 FILE_INVALID."""
    content = b"platform_order_sn,tracking_number,product_name,quantity\n" + b"x" * int(5.1 * 1024 * 1024)
    res = client.post(
        "/imports", headers=_h(tokens["SUPERVISOR"]), files={"file": ("big.csv", content, "text/csv")}
    )
    assert (res.status_code, res.json()["error"]["code"]) == (422, "FILE_INVALID")


def test_csv_overlap_api(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-05.15 / BR-17: 5 đơn đã có từ Shopee (seed mock) → bỏ qua; giữ nguồn API, sản phẩm không đổi."""
    body = _upload(client, tokens["SUPERVISOR"], "overlap_api.csv").json()
    assert body["counts"]["skipped"] == 5
    res = client.post(f"/imports/{body['id']}/commit", headers=_h(tokens["SUPERVISOR"]))
    assert res.json()["counts"]["skipped"] == 5
    assert _psql("SELECT DISTINCT source FROM \"order\" WHERE platform_order_sn LIKE '2410TST0000_'") == "API"
    assert _psql("SELECT count(*) FROM order_item WHERE quantity = 9") == "0"


def test_csv_permissions_and_template(client: httpx.Client, tokens: dict[str, str]) -> None:
    """Ma trận quyền 01 §5.1: CSKH không nhập được; API-53 trả file mẫu."""
    assert _upload(client, tokens["CSKH"], "overlap_api.csv").status_code == 403
    res = client.get("/imports/template", headers=_h(tokens["ADMIN"]))
    assert res.status_code == 200
    assert res.content.decode("utf-8-sig").startswith("platform_order_sn,tracking_number")


# ---------------------------------------------------------------- Shopee (T-16, adapter mock)


def _shopee_enabled(client: httpx.Client, tokens: dict[str, str]) -> httpx.Response:
    return client.post("/shops/shopee/auth-url", headers=_h(tokens["ADMIN"]))


def test_shopee_connect_or_not_configured(client: httpx.Client, tokens: dict[str, str]) -> None:
    """TC-05.03 khi `SHOPEE_ENABLED=false` (mặc định stack dev); khi bật + adapter mock: TC-05.01 phần API
    (auth-url → callback → shop CONNECTED). Shopee thật: chưa test — thiếu tài khoản partner (T-3)."""
    assert client.get("/shops", headers=_h(tokens["SUPERVISOR"])).status_code == 403
    res = _shopee_enabled(client, tokens)
    if res.status_code == 503:
        assert res.json()["error"]["code"] == "PLATFORM_NOT_CONFIGURED"
        assert client.get("/shops", headers=_h(tokens["ADMIN"])).json() == {"items": []}
        pytest.skip(
            "SHOPEE_ENABLED=false: đã kiểm TC-05.03; chạy lại với SHOPEE_ENABLED=true cho luồng kết nối"
        )
    assert res.status_code == 200, res.text
    url = httpx.URL(res.json()["url"])
    redirect = client.get(f"{BASE}{url.raw_path.decode()}", follow_redirects=False)
    assert redirect.status_code == 302
    assert redirect.headers["location"] == "/admin/settings/shopee?result=connected"
    items = client.get("/shops", headers=_h(tokens["ADMIN"])).json()["items"]
    assert [(s["auth_status"], s["name"]) for s in items] == [("CONNECTED", "TST Shop (mock)")]
    assert _psql("SELECT count(*) FROM audit_log WHERE action = 'SHOP_CONNECT'") == "1"
    # Token lưu mã hóa (Fernet), không có chữ rõ.
    assert _psql("SELECT position('mock-access' in convert_from(access_token_enc, 'UTF8')) FROM shop") == "0"
