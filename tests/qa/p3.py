"""Tiện ích chung QA live Phase 3 (T-229, `tests/qa/test_m12..m18_live.py`) — HTTP thật + `docker compose`.

Stack theo `tests/qa/stack.py` (mặc định stack dev; stack QA riêng: `. docker/qa.env`). Mỗi module tự
`qa-reset.sh` (migrate lại + `seed-demo` Phase 3: 4 shop mock, J-04 + J-13 thật qua adapter mock).
"""

import contextlib
import json
import os
import subprocess
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

from tests.qa import stack

BASE = os.environ.get("QA_BASE_URL")
PASSWORD = "matkhau123"
COMPOSE = stack.COMPOSE
ROOT = stack.ROOT
# MinIO nhìn từ máy chạy test (W1 / video link chia sẻ = S3_PUBLIC_ENDPOINT).
MINIO_URL = os.environ.get("AICAM_MINIO_URL", "http://localhost:59000")
TEMP_API_PORT = int(os.environ.get("QA_TEMP_API_PORT", "8282" if stack.PROJECT != "aicam-dev" else "8183"))

pytestmark = [
    pytest.mark.qa,
    pytest.mark.skipif(not BASE, reason="đặt QA_BASE_URL để chạy QA trên stack thật"),
]


def run(args: list[str], *, check: bool = False, timeout: float = 600) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, capture_output=True, text=True, check=check, timeout=timeout)  # noqa: S603


def compose(*args: str, check: bool = False, timeout: float = 600) -> subprocess.CompletedProcess[str]:
    return run([*COMPOSE, *args], check=check, timeout=timeout)


def psql(sql: str) -> str:
    out = compose("exec", "-T", "postgres", "psql", "-U", "aicam", "-d", "aicam", "-tA", "-c", sql)
    return (out.stdout + out.stderr).strip()


def redis(*args: str) -> str:
    return compose("exec", "-T", "redis", "redis-cli", *args).stdout.strip()


def job(expr: str, service: str = "api") -> str:
    """Chạy một biểu thức Python (vd. task Celery gọi đồng bộ) trong container — trả dòng in cuối."""
    code = f"from aicam.workers import tasks; print({expr})"
    out = compose("exec", "-T", service, "python", "-c", code)
    assert out.returncode == 0, out.stderr[-3000:]
    return out.stdout.strip().splitlines()[-1] if out.stdout.strip() else ""


def send_task(name: str, *args: Any) -> None:
    """Đẩy task lên broker thật (định tuyến theo `task_routes`) — worker của queue đó chạy."""
    code = f"from aicam.workers.celery_app import app; app.send_task({name!r}, args={json.dumps(list(args))})"
    out = compose("exec", "-T", "api", "python", "-c", code)
    assert out.returncode == 0, out.stderr[-3000:]


def logs(service: str, since: str = "10m") -> str:
    out = compose("logs", "--no-color", "--since", since, service)
    return out.stdout + out.stderr


def reset(*args: str) -> None:
    out = run([str(ROOT / "scripts/qa-reset.sh"), *args])
    assert out.returncode == 0, out.stdout[-2000:] + out.stderr[-2000:]


def login(client: httpx.Client, username: str, kind: str = "DASHBOARD") -> dict[str, str]:
    res = client.post("/auth/login", json={"username": username, "password": PASSWORD, "client": kind})
    assert res.status_code == 200, res.text
    return {"Authorization": f"Bearer {res.json()['access_token']}"}


@contextlib.contextmanager
def api_client(base: str | None = None) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=f"{base or BASE}/api/v1", timeout=60) as c:
        yield c


def tokens_for(client: httpx.Client) -> dict[str, dict[str, str]]:
    return {
        "ADMIN": login(client, "tst_admin"),
        "SUPERVISOR": login(client, "tst_sup"),
        "CSKH": login(client, "tst_cskh"),
        "STATION": login(client, "tst_station01", "STATION"),
    }


def scan(client: httpx.Client, headers: dict[str, str], code: str) -> dict[str, Any]:
    res = client.post(
        "/station/scan", headers=headers, json={"code": code, "client_scan_id": str(uuid.uuid4())}
    )
    assert res.status_code == 200, res.text
    return res.json()  # type: ignore[no-any-return]


def package_by_code(client: httpx.Client, headers: dict[str, str], code: str) -> dict[str, Any]:
    items = client.get("/packages", params={"q": code}, headers=headers).json()["items"]
    found = [i for i in items if i["tracking_number"] == code]
    assert found, (code, items)
    return found[0]  # type: ignore[no-any-return]


def wait_for[T](fn: Callable[[], T | None], timeout: float, interval: float = 2.0, what: str = "") -> T:
    deadline = time.monotonic() + timeout
    last: T | None = None
    while time.monotonic() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    pytest.fail(f"quá {timeout:.0f} giây chờ {what}: {last!r}")


def shops(client: httpx.Client, admin: dict[str, str]) -> dict[str, dict[str, Any]]:
    """Tên shop → shop (API-70)."""
    return {s["name"]: s for s in client.get("/shops", headers=admin).json()["items"]}


def pack_with_clips(
    client: httpx.Client, tokens: dict[str, dict[str, str]], code: str, hold_s: float = 8
) -> dict[str, Any]:
    """Station 01 đóng gói `code` (quét mở → giữ `hold_s` giây → quét đóng) rồi chờ J-01 cắt clip READY.
    Trả chi tiết kiện API-31."""
    st = tokens["STATION"]
    opened = scan(client, st, code)
    assert opened["outcome"] == "SESSION_OPENED", opened
    time.sleep(hold_s)
    closed = scan(client, st, code)
    assert closed["outcome"] in {"SESSION_CLOSED", "SESSION_COMPLETED", "CLOSED"} or (
        closed["state"]["session"] is None
    ), closed
    package_id = package_by_code(client, tokens["ADMIN"], code)["id"]

    def ready() -> dict[str, Any] | None:
        detail: dict[str, Any] = client.get(f"/packages/{package_id}", headers=tokens["ADMIN"]).json()
        sessions = [s for s in detail["sessions"] if s["type"] == "PACK"]
        clips = [c for s in sessions for c in s.get("clips", [])]
        if clips and all(c["status"] == "READY" for c in clips):
            return detail
        return None

    return wait_for(ready, 150, 3, f"clip READY của {code}")


@contextlib.contextmanager
def temp_api(env: dict[str, str]) -> Iterator[httpx.Client]:
    """Một container API tạm (cùng image / DB / Redis của stack) ở cổng riêng với biến môi trường khác — kiểm
    "chưa cấu hình" mà không đổi container `api` dùng chung (cùng cách session-refresh-g4)."""
    name = f"{stack.PROJECT}-qa-tmp-api"
    run(["docker", "rm", "-f", name])
    env_args = [a for k, v in env.items() for a in ("-e", f"{k}={v}")]
    out = compose(
        "run", "-d", "--rm", "--no-deps", "--name", name, "-p", f"{TEMP_API_PORT}:8000", *env_args, "api"
    )
    assert out.returncode == 0, out.stderr[-2000:]
    base = f"http://localhost:{TEMP_API_PORT}"
    try:
        deadline = time.monotonic() + 90
        while True:
            with contextlib.suppress(httpx.HTTPError):
                if httpx.get(f"{base}/healthz", timeout=3).status_code == 200:
                    break
            if time.monotonic() > deadline:
                pytest.fail(f"API tạm không lên: {logs(name)[-1500:]}")
            time.sleep(1)
        with api_client(base) as c:
            yield c
    finally:
        run(["docker", "rm", "-f", name])


def mc(*args: str) -> subprocess.CompletedProcess[str]:
    """`mc` với tài khoản root MinIO của stack (container minio-init tạm)."""
    script = 'mc alias set local "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc "$@"'
    return compose(
        "run", "--rm", "--no-deps", "-T", "--entrypoint", "bash", "minio-init", "-c", script, "mc", *args
    )


def audit_rows(client: httpx.Client, admin: dict[str, str], action: str) -> list[dict[str, Any]]:
    res = client.get("/audit-logs", params={"action": action, "page_size": 100}, headers=admin)
    assert res.status_code == 200, res.text
    return res.json()["items"]  # type: ignore[no-any-return]


def err(res: httpx.Response) -> tuple[int, str]:
    body = res.json()
    return res.status_code, body.get("error", {}).get("code", "")
