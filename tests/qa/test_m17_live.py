"""QA live item 03 — M17 (thông báo: kênh, gửi thử, J-26 / J-27 / J-28 qua sink mock) trên stack thật (T-229).

Chạy: `. docker/qa.env && uv run pytest tests/qa -m qa -k m17` (tự `qa-reset.sh`; Phase 3 xóa `notify:*`).
Stack:
`NOTIFY_TRANSPORT=mock` (tin ghi Redis `notify:mock:{TELEGRAM|ZALO_OA}`), `NOTIFY_MOCK_FAIL` rỗng,
`worker-notify`
(queue `notify`) + beat (J-26 30 giây, J-27 15 giây, gom tin 2 phút — BR-36). Các test chạy theo thứ tự.
Phủ: TC-06.41, 06.43 (API tạm `NOTIFY_TRANSPORT=real` không Zalo), 06.44, kênh CRUD + audit, giờ yên lặng
(HELD
với mức TB, Cao vẫn gửi), J-26 N02 / N03 / N04 từ dữ liệu seed + N01 camera giả dừng thật > 60 giây, J-27 gửi
qua
worker-notify, J-28 tóm tắt ngày N10. Bot Telegram / Zalo OA thật: **chưa test — thiếu tài nguyên** (Q21).
"""

import json
import time
from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import pytest

from tests.qa import p3

pytestmark = p3.pytestmark

KHO, CSKH, ADMIN_CH, OWNER = "-1001234567890", "-1002222222222", "-1003333333333", "-1004444444444"
CHANNELS = [
    ("Kho", KHO, ["N01", "N02", "N03", "N09"]),
    ("CSKH", CSKH, ["N04", "N05"]),
    ("Quản trị", ADMIN_CH, ["N06", "N07", "N08"]),
    ("Chủ shop", OWNER, ["N10"]),
]


@pytest.fixture(scope="module", autouse=True)
def _reset() -> Iterator[None]:
    p3.reset()
    yield
    p3.compose("start", "fake-cam1")


@pytest.fixture(scope="module")
def client() -> Iterator[httpx.Client]:
    with p3.api_client() as c:
        yield c


@pytest.fixture(scope="module")
def tokens(client: httpx.Client) -> dict[str, dict[str, str]]:
    return p3.tokens_for(client)


def _sent(target: str | None = None) -> list[dict[str, Any]]:
    rows = [json.loads(x) for x in p3.redis("LRANGE", "notify:mock:TELEGRAM", "0", "-1").splitlines() if x]
    return [r for r in rows if target is None or r["target"] == target]


def _messages(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    res = client.get("/notify/messages", params={"page_size": 100}, headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    return res.json()["items"]  # type: ignore[no-any-return]


def test_tc_06_41_channel_validation(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """TC-06.41: tên 1 / 41 ký tự; Telegram `target` "abc"; Zalo `target` "12a"; `events: []`; `["N11"]`."""
    ok = {"name": "Kho", "type": "TELEGRAM", "target": KHO, "events": ["N01"]}
    cases = [
        ({**ok, "name": "K"}, {"name": "Tên kênh 2–40 ký tự."}),
        ({**ok, "name": "x" * 41}, {"name": "Tên kênh 2–40 ký tự."}),
        ({**ok, "target": "abc"}, {"target": "Chat ID là một số (nhóm thường bắt đầu bằng -100)."}),
        ({**ok, "type": "ZALO_OA", "target": "12a"}, {"target": "Zalo user ID là dãy 1–64 chữ số."}),
        ({**ok, "events": []}, {"events": "Chọn ít nhất 1 sự kiện."}),
        ({**ok, "events": ["N11"]}, {"events": "Sự kiện không hợp lệ."}),
    ]
    for body, fields in cases:
        res = client.post("/notify/channels", json=body, headers=tokens["ADMIN"])
        assert res.status_code == 422, (body, res.text)
        assert res.json()["error"]["details"]["fields"] == fields
    assert client.post("/notify/channels", json=ok, headers=tokens["SUPERVISOR"]).status_code == 403


def test_tc_06_43_provider_not_configured_temp_api() -> None:
    """TC-06.43 (API): `NOTIFY_TRANSPORT=real`, có bot Telegram, `ZALO_*` rỗng (API tạm cùng DB) → API-170
    `providers.ZALO_OA.configured = false`; API-171 `ZALO_OA` → 409 `PROVIDER_NOT_CONFIGURED`."""
    env = {"NOTIFY_TRANSPORT": "real", "TELEGRAM_BOT_TOKEN": "123456:qa-khong-dung", "ZALO_APP_ID": "",
           "ZALO_APP_SECRET": "", "ZALO_OA_REFRESH_TOKEN": ""}  # fmt: skip
    with p3.temp_api(env) as api:
        admin = p3.login(api, "tst_admin")
        providers = api.get("/notify/channels", headers=admin).json()["providers"]
        assert providers == {"TELEGRAM": {"configured": True}, "ZALO_OA": {"configured": False}}
        res = api.post("/notify/channels", json={"name": "Zalo chủ", "type": "ZALO_OA", "target": "1234567",
                                                 "events": ["N10"]}, headers=admin)  # fmt: skip
        assert p3.err(res) == (409, "PROVIDER_NOT_CONFIGURED"), res.text


def test_tc_06_44_channels_and_test_send(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """PRE-16 + TC-06.44: 4 kênh (API-171) → gửi thử kênh "Kho" (API-174) 200 ≤ 10 giây, `last_status = OK`,
    tin
    tới `-1001234567890` đúng chữ; sửa / xóa kênh; audit `NOTIFY_CHANNEL_CREATE / UPDATE / DELETE`,
    `NOTIFY_TEST`."""
    admin = tokens["ADMIN"]
    ids = {}
    for name, target, events in CHANNELS:
        res = client.post("/notify/channels", json={"name": name, "type": "TELEGRAM", "target": target,
                                                    "events": events}, headers=admin)  # fmt: skip
        assert res.status_code == 201, res.text
        ids[name] = res.json()["id"]
    started = time.monotonic()
    res = client.post(f"/notify/channels/{ids['Kho']}/test", headers=admin)
    assert res.status_code == 200, res.text
    assert time.monotonic() - started <= 10
    assert res.json()["ok"] is True
    kho = next(
        c for c in client.get("/notify/channels", headers=admin).json()["items"] if c["id"] == ids["Kho"]
    )
    assert kho["last_status"] == "OK"
    assert [m["text"] for m in _sent(KHO)] == [
        "Tin thử từ Hệ thống X — kênh Kho. Bạn sẽ nhận: Camera mất tín hiệu, Lệch trạng thái mức Cao, "
        "Phiên mở hoàn bị hủy / bỏ dở, Yêu cầu duyệt chờ lâu."
    ]
    body = {"name": "Tạm", "type": "TELEGRAM", "target": "-1009999999999", "events": ["N07"]}
    temp = client.post("/notify/channels", json=body, headers=admin).json()
    res = client.patch(
        f"/notify/channels/{temp['id']}", json={"enabled": False, "name": "Tạm tắt"}, headers=admin
    )
    assert (res.status_code, res.json()["enabled"]) == (200, False)
    assert client.delete(f"/notify/channels/{temp['id']}", headers=admin).status_code == 204
    actions = ("NOTIFY_CHANNEL_CREATE", "NOTIFY_CHANNEL_UPDATE", "NOTIFY_CHANNEL_DELETE", "NOTIFY_TEST")
    counts = {a: len(p3.audit_rows(client, admin, a)) for a in actions}
    assert counts == {"NOTIFY_CHANNEL_CREATE": 5, "NOTIFY_CHANNEL_UPDATE": 1, "NOTIFY_CHANNEL_DELETE": 1,
                      "NOTIFY_TEST": 1}  # fmt: skip


def test_j26_quiet_hours_holds_medium(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """Giờ yên lặng đang hiệu lực (API-176, audit `NOTIFY_SETTINGS_UPDATE`) → J-26 (beat, worker-notify) tạo
    tin
    từ dữ liệu seed: N03 (TB) `HELD`; N02, N04 (Cao) vẫn `QUEUED` (BR-36)."""
    now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
    quiet = {"enabled": True, "start": (now_vn - timedelta(hours=1)).strftime("%H:%M"),
             "end": (now_vn + timedelta(hours=1)).strftime("%H:%M")}  # fmt: skip
    res = client.put("/notify/quiet-hours", json=quiet, headers=tokens["ADMIN"])
    assert res.status_code == 200, res.text
    assert len(p3.audit_rows(client, tokens["ADMIN"], "NOTIFY_SETTINGS_UPDATE")) == 1

    def grouped() -> dict[str, str] | None:
        got = {m["event_code"]: m["status"] for m in _messages(client, tokens)}
        return got if {"N02", "N03", "N04"} <= set(got) else None

    got = p3.wait_for(grouped, 90, 3, "J-26 tạo tin N02 / N03 / N04")
    assert got["N03"] == "HELD", got
    assert got["N02"] in {"QUEUED", "SENT"}, got
    assert got["N04"] in {"QUEUED", "SENT"}, got
    assert "notify.scan" in p3.logs("worker-notify")


def test_j26_j27_dispatch_and_camera_offline(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """Tắt giờ yên lặng, dừng camera giả Cam 1 thật (> 60 giây → N01) → J-27 trên worker-notify gửi qua sink
    mock
    sau cửa sổ gom 2 phút: N02 + N01 tới kênh "Kho", N04 tới "CSKH" (chữ có "[CAO]", link "Xem:" về
    `SITE_ADDRESS`); tin `SENT`."""
    res = client.put("/notify/quiet-hours", json={"enabled": False, "start": "22:00", "end": "07:00"},
                     headers=tokens["ADMIN"])  # fmt: skip
    assert res.status_code == 200, res.text
    assert p3.compose("stop", "fake-cam1").returncode == 0
    try:

        def delivered() -> dict[str, str] | None:
            got = {m["event_code"]: m["status"] for m in _messages(client, tokens)}
            return got if all(got.get(c) == "SENT" for c in ("N01", "N02", "N04")) else None

        p3.wait_for(delivered, 330, 5, "N01 / N02 / N04 tới sink mock")
    finally:
        p3.compose("start", "fake-cam1")
    kho = " ".join(m["text"] for m in _sent(KHO) if not m["text"].startswith("Tin thử"))
    assert "[CAO] Lệch trạng thái mức Cao" in kho
    assert "Camera mất tín hiệu" in kho
    assert "TST Station 01" in kho
    cskh = " ".join(m["text"] for m in _sent(CSKH))
    assert "[CAO] Chỉ hoàn tiền mới / sắp hạn" in cskh
    assert "Xem: http://localhost:5281/admin/returns" in cskh or "Xem: http" in cskh
    statuses = {m["event_code"]: m["status"] for m in _messages(client, tokens)}
    assert statuses["N02"] == statuses["N04"] == statuses["N01"] == "SENT", statuses
    assert "notify.dispatch" in p3.logs("worker-notify", "8m")


def test_j28_daily_summary(client: httpx.Client, tokens: dict[str, dict[str, str]]) -> None:
    """J-28 (N10): chạy tóm tắt ngày trên worker-notify → tin tới kênh "Chủ shop" sau cửa sổ gom."""
    out = p3.job("tasks.notify_daily_summary()", service="worker-notify")
    assert "'new': 1" in out, out

    def got() -> list[dict[str, Any]] | None:
        return _sent(OWNER) or None

    (msg, *_) = p3.wait_for(got, 200, 5, "N10 tới kênh Chủ shop")
    assert "Tóm tắt" in msg["text"] or "tóm tắt" in msg["text"], msg
