"""Test tải (02a §11 "Hiệu năng"): NFR-01 (API-11 quét ≤ 1 giây p95) và NFR-05 (2 station, 120 lần quét / giờ
/ station + 1 CSKH tra cứu; thiết kế mở rộng 4 station).

Chạy trên stack dev (adapter sàn `mock`) — **không chạy trên production** (tạo tài khoản, station,
kiện LOAD*):

    LOAD_PROFILE=nfr05 LOAD_STATIONS=2 uvx --from locust locust -f tests/load/locustfile.py \
        --host http://localhost:8180 --headless -u 3 -r 3 -t 10m --csv /tmp/load-nfr05

  (`-u` = `LOAD_STATIONS` + 1.) Kết quả đo: 02a mục "Test tải (T-19)".

Biến môi trường:
- `LOAD_PROFILE`: `nfr05` (mặc định — nhịp thật: mỗi station mở + đóng 1 kiện / 60 giây = 120 lần quét / giờ)
  hoặc `stress` (quét liên tục: đóng gói 1–2 giây rồi quét đóng).
- `LOAD_STATIONS`: số station giả (mặc định 2; thử 4). Số user locust = `LOAD_STATIONS` + 1 (CSKH).
- `LOAD_ADMIN_USER` / `LOAD_ADMIN_PASSWORD`: Admin dùng để tạo tài khoản + station LOAD (mặc định seed-demo).
- `LOAD_CSKH_USER` / `LOAD_CSKH_PASSWORD`: tài khoản tra cứu (mặc định `tst_cskh` của seed-demo).

Station LOAD không có camera → `tray.match = UNAVAILABLE` (không chặn phiên, cờ `CAM2_UNVERIFIED`); mã vận đơn
ngẫu nhiên không có trên sàn mock → phiên "chưa xác minh" (BR-04) — đường đi API-11 đầy đủ (khóa station,
tra sàn, ghi phiên, publish WS). Dọn sau test: `scripts/qa-reset.sh` (stack dev).
"""

import os
import random
import string
import uuid
from typing import Any

import gevent
import requests
from locust import HttpUser, between, constant_pacing, events, task
from locust.env import Environment

PROFILE = os.environ.get("LOAD_PROFILE", "nfr05")
STATIONS = int(os.environ.get("LOAD_STATIONS", "2"))
ADMIN = (os.environ.get("LOAD_ADMIN_USER", "tst_admin"), os.environ.get("LOAD_ADMIN_PASSWORD", "matkhau123"))
CSKH = (os.environ.get("LOAD_CSKH_USER", "tst_cskh"), os.environ.get("LOAD_CSKH_PASSWORD", "matkhau123"))
STATION_PASSWORD = "load-test-123"  # tài khoản giả trên stack dev
API = "/api/v1"

_free_stations: list[int] = []
_recent_codes: list[str] = []


def _code() -> str:
    return "LOAD" + "".join(random.choices(string.ascii_uppercase + string.digits, k=10))  # noqa: S311


@events.test_start.add_listener
def _setup(environment: Environment, **_: Any) -> None:
    """Tạo (idempotent) tài khoản STATION + station LOAD 01..N bằng Admin."""
    base = (environment.host or "http://localhost:8180") + API
    c = requests.Session()
    res = c.post(
        f"{base}/auth/login", json={"username": ADMIN[0], "password": ADMIN[1], "client": "DASHBOARD"}
    )
    res.raise_for_status()
    c.headers["Authorization"] = f"Bearer {res.json()['access_token']}"
    users = c.get(f"{base}/users", params={"role": "STATION", "page_size": 100}).json()["items"]
    by_name = {u["username"]: u for u in users}
    stations = {s["name"]: s for s in c.get(f"{base}/stations").json()["items"]}
    for n in range(1, STATIONS + 1):
        username, name = f"load_station{n:02d}", f"LOAD Station {n:02d}"
        user = by_name.get(username)
        if user is None:
            body = {
                "username": username,
                "display_name": name,
                "role": "STATION",
                "password": STATION_PASSWORD,
            }
            res = c.post(f"{base}/users", json=body)
            res.raise_for_status()
            user = res.json()
        if name not in stations:
            c.post(f"{base}/stations", json={"name": name, "account_user_id": user["id"]}).raise_for_status()
        elif not stations[name]["is_active"]:
            c.patch(f"{base}/stations/{stations[name]['id']}", json={"is_active": True}).raise_for_status()
    _free_stations[:] = list(range(1, STATIONS + 1))


class Station(HttpUser):
    """Một station: quét mở → đóng gói → quét đóng (API-11), thỉnh thoảng tải lại trạng thái (API-10)."""

    fixed_count = STATIONS
    wait_time = constant_pacing(60) if PROFILE == "nfr05" else between(0.2, 0.5)

    def on_start(self) -> None:
        self.n = _free_stations.pop(0) if _free_stations else 1
        self._login()
        state = self.client.get(f"{API}/station/state", headers=self.h, name="API-10 state").json()
        if state.get("session"):  # phiên dở từ lần chạy trước: hủy để bắt đầu sạch
            self.client.post(
                f"{API}/station/sessions/{state['session']['id']}/cancel",
                headers=self.h,
                json={"reason": "WRONG_SCAN", "note": None},
                name="API-12 cancel",
            )

    def _login(self) -> None:
        res = self.client.post(
            f"{API}/auth/login",
            json={"username": f"load_station{self.n:02d}", "password": STATION_PASSWORD, "client": "STATION"},
            name="API-01 login",
        )
        self.h = {"Authorization": f"Bearer {res.json()['access_token']}"}

    def _scan(self, code: str, name: str, expected: str) -> None:
        with self.client.post(
            f"{API}/station/scan",
            headers=self.h,
            json={"code": code, "client_scan_id": str(uuid.uuid4())},
            name=name,
            catch_response=True,
        ) as res:
            if res.status_code == 401:
                res.success()
                self._login()
                return
            outcome = res.json().get("outcome") if res.status_code == 200 else None
            if outcome != expected:
                res.failure(f"outcome {outcome} (HTTP {res.status_code}), cần {expected}")

    @task
    def pack_one(self) -> None:
        code = _code()
        self._scan(code, "API-11 scan open", "SESSION_OPENED")
        # Nhịp NFR-05: 120 lần quét / giờ / station = 1 kiện / 60 giây (constant_pacing), đóng gói ~20 giây.
        gevent.sleep(20 if PROFILE == "nfr05" else random.uniform(1, 2))  # noqa: S311
        self._scan(code, "API-11 scan close", "SESSION_COMPLETED")
        _recent_codes.append(code)
        del _recent_codes[:-200]
        if random.random() < 0.2:  # noqa: S311
            self.client.get(f"{API}/station/state", headers=self.h, name="API-10 state")


class Cskh(HttpUser):
    """1 CSKH: tra cứu mã vận đơn (API-30), mở chi tiết (API-31), xem tổng quan (API-32)."""

    fixed_count = 1
    wait_time = between(5, 10)

    def on_start(self) -> None:
        res = self.client.post(
            f"{API}/auth/login",
            json={"username": CSKH[0], "password": CSKH[1], "client": "DASHBOARD"},
            name="API-01 login",
        )
        self.h = {"Authorization": f"Bearer {res.json()['access_token']}"}

    @task(3)
    def search(self) -> None:
        q = random.choice(_recent_codes) if _recent_codes else "SPXTST0000010"  # noqa: S311
        res = self.client.get(f"{API}/packages", headers=self.h, params={"q": q}, name="API-30 search")
        if res.status_code == 200 and res.json()["items"]:
            pid = res.json()["items"][0]["id"]
            self.client.get(f"{API}/packages/{pid}", headers=self.h, name="API-31 detail")

    @task(1)
    def dashboard(self) -> None:
        self.client.get(f"{API}/reports/daily", headers=self.h, name="API-32 daily")
